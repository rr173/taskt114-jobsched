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

// TestDeleteRunningJobReturns409 verifies that a management delete issued while
// a worker is executing the job is refused with 409, so the worker's later
// completion writes never land on a deleted row.
func TestDeleteRunningJobReturns409(t *testing.T) {
	s, p, h := newServer(t)

	started := make(chan struct{})
	release := make(chan struct{})
	p.RegisterHandler("slow-gated", func(ctx context.Context, j *model.Job) (string, error) {
		close(started)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "done", nil
		}
	})

	rec := do(t, h, "POST", "/jobs", `{"id":"race","queue":"q","type":"slow-gated","max_attempts":3}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create code %d body %s", rec.Code, rec.Body.String())
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	<-started // job is now running in the handler

	rec = do(t, h, "DELETE", "/jobs/race", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 deleting running job, got %d body %s", rec.Code, rec.Body.String())
	}

	// Let the worker finish; the job must still exist and have a complete record.
	close(release)
	p.Wait()

	rec = do(t, h, "GET", "/jobs/race", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected job to survive refused delete, got %d", rec.Code)
	}
	var j map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	if j["state"] != "succeeded" {
		t.Fatalf("expected succeeded after finish, got %v", j["state"])
	}

	// Now that the job is terminal, delete must succeed.
	rec = do(t, h, "DELETE", "/jobs/race", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected delete to succeed after finish, got %d body %s", rec.Code, rec.Body.String())
	}
	if _, err := s.GetJob("race"); err == nil {
		t.Fatal("expected job to be gone after delete")
	}
}
