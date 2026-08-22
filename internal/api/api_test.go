package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

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

// TestSucceededJobNotRescheduled guards the regression where an ordinary update
// could roll a finished job back to pending, re-entering the scheduler. A
// succeeded job must stay succeeded even after update-like flows, and must not
// be picked up by a subsequent flush.
func TestSucceededJobNotRescheduled(t *testing.T) {
	s, p, h := newServer(t)
	ctx := context.Background()

	// Create and run a job to completion.
	rec := do(t, h, "POST", "/jobs", `{"id":"s1","queue":"q","type":"noop","max_attempts":3}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create code %d body %s", rec.Code, rec.Body.String())
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	p.Wait()

	j, err := s.GetJob("s1")
	if err != nil {
		t.Fatal(err)
	}
	if j.State != model.StateSucceeded {
		t.Fatalf("expected succeeded, got %s", j.State)
	}

	// Simulate an ordinary update attempting to revive the finished job back to
	// pending through the store API (the path that previously caused the bug).
	j.State = model.StatePending
	j.RunAt = time.Now()
	if err := s.UpdateJob(j); !errors.Is(err, store.ErrTerminalJob) {
		t.Fatalf("expected ErrTerminalJob reviving succeeded job, got %v", err)
	}

	// The finished job must not be re-dispatched by a subsequent flush.
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	got, _ := s.GetJob("s1")
	if got.State != model.StateSucceeded {
		t.Fatalf("succeeded job was rescheduled, state is now %s", got.State)
	}
}

// TestRetryEndpointDead verifies the retry endpoint revives a dead job through
// the explicit lifecycle path, while a succeeded job is refused.
func TestRetryEndpoint(t *testing.T) {
	s, p, h := newServer(t)
	ctx := context.Background()

	// A "fail" job exhausts retries and becomes a dead letter.
	rec := do(t, h, "POST", "/jobs", `{"id":"d1","queue":"q","type":"fail","max_attempts":1}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create code %d body %s", rec.Code, rec.Body.String())
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	dead, _ := s.GetJob("d1")
	if dead.State != model.StateDead {
		t.Fatalf("expected dead, got %s", dead.State)
	}

	// Retry must revive the dead job to pending.
	rec = do(t, h, "POST", "/jobs/d1/retry", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("retry code %d body %s", rec.Code, rec.Body.String())
	}
	got, _ := s.GetJob("d1")
	if got.State != model.StatePending {
		t.Fatalf("expected pending after retry, got %s", got.State)
	}

	// A succeeded job must be refused.
	rec = do(t, h, "POST", "/jobs", `{"id":"ok1","queue":"q","type":"noop","max_attempts":1}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create ok1 code %d", rec.Code)
	}
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	rec = do(t, h, "POST", "/jobs/ok1/retry", "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected conflict retrying succeeded job, got %d body %s", rec.Code, rec.Body.String())
	}
	ok, _ := s.GetJob("ok1")
	if ok.State != model.StateSucceeded {
		t.Fatalf("succeeded job was revived to %s", ok.State)
	}
}
