package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"taskt114-jobsched/internal/clock"
	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
	"taskt114-jobsched/internal/worker"
)

func newServer(t *testing.T) (*store.Store, *worker.Pool, http.Handler) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	p := worker.New(s, clock.New())
	p.RegisterHandler("noop", func(ctx context.Context, j *model.Job) (string, error) { return "", nil })
	p.RegisterHandler("echo", func(ctx context.Context, j *model.Job) (string, error) { return j.Args, nil })
	h := New(s, p, Config{}).Handler()
	return s, p, h
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = bytes.NewBufferString(body)
	}
	req := httptest.NewRequest(method, path, r)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateAndGetJob(t *testing.T) {
	_, p, h := newServer(t)
	rec := do(t, h, "POST", "/jobs", `{"id":"a1","queue":"q","type":"echo","args":{"x":1},"max_attempts":3}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create code %d body %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h, "GET", "/jobs/a1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get code %d", rec.Code)
	}
	// flush and confirm success
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	rec = do(t, h, "GET", "/jobs/a1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get after flush code %d", rec.Code)
	}
	var j map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if j["state"] != "succeeded" {
		t.Fatalf("expected succeeded, got %v", j["state"])
	}
}

func TestBatchAndList(t *testing.T) {
	_, _, h := newServer(t)
	rec := do(t, h, "POST", "/jobs/batch", `{"jobs":[{"id":"b1","queue":"q","type":"noop"},{"id":"b2","queue":"q","type":"noop"}]}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("batch code %d body %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h, "GET", "/jobs", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list code %d", rec.Code)
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Count < 2 {
		t.Fatalf("expected at least 2 jobs, got %d", out.Count)
	}
}

// TestBatchRollbackOnDuplicate verifies that when a batch contains a job whose
// id collides with an existing job, the whole batch is rejected and the jobs
// written earlier in the same batch do not linger as a half batch.
func TestBatchRollbackOnDuplicate(t *testing.T) {
	_, _, h := newServer(t)
	// Seed a job whose id will collide with the second job in the batch.
	rec := do(t, h, "POST", "/jobs", `{"id":"dup","queue":"q","type":"noop"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("seed code %d body %s", rec.Code, rec.Body.String())
	}
	// First job is fresh (ok1), second collides with the seeded "dup"; the
	// batch must fail and neither ok1 nor dup's original record should be
	// affected — specifically ok1 must not have leaked through.
	rec = do(t, h, "POST", "/jobs/batch", `{"jobs":[{"id":"ok1","queue":"q","type":"noop"},{"id":"dup","queue":"q","type":"noop"}]}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on duplicate-in-batch, got %d body %s", rec.Code, rec.Body.String())
	}
	for _, id := range []string{"ok1"} {
		rec := do(t, h, "GET", "/jobs/"+id, "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected %s to be absent after rolled-back batch, got %d body %s", id, rec.Code, rec.Body.String())
		}
	}
	// The pre-existing dup record should still be a single job (not duplicated).
	rec = do(t, h, "GET", "/jobs", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list code %d", rec.Code)
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 {
		t.Fatalf("expected exactly 1 job (the seeded dup) after rolled-back batch, got %d", out.Count)
	}
}

// TestBatchValidationAbortsBeforeWrite verifies that an invalid job anywhere in
// the batch rejects the entire request before any job is persisted.
func TestBatchValidationAbortsBeforeWrite(t *testing.T) {
	_, _, h := newServer(t)
	// Second job is missing a type, so it fails validation; the valid first
	// job must not be persisted.
	rec := do(t, h, "POST", "/jobs/batch", `{"jobs":[{"id":"v1","queue":"q","type":"noop"},{"id":"v2","queue":"q"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid job in batch, got %d body %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h, "GET", "/jobs/v1", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected v1 absent after validation-aborted batch, got %d", rec.Code)
	}
}


func TestHealthAndStats(t *testing.T) {
	_, _, h := newServer(t)
	rec := do(t, h, "GET", "/health", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("health code %d", rec.Code)
	}
	rec = do(t, h, "GET", "/stats", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("stats code %d", rec.Code)
	}
}

func TestFlushEndpoint(t *testing.T) {
	_, _, h := newServer(t)
	rec := do(t, h, "POST", "/flush", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("flush code %d body %s", rec.Code, rec.Body.String())
	}
}
