package api

import (
	"net/http"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug10_RunningJobsCannotBeDeleted(t *testing.T) {
	s, _, h := newServer(t)
	apiJob := &model.Job{ID: "running-api", Queue: "q", Type: "noop", Args: "{}", State: model.StateRunning, RunAt: time.Now(), MaxAttempts: 1}
	if err := s.CreateJob(apiJob); err != nil {
		t.Fatalf("create api job: %v", err)
	}
	rec := do(t, h, "DELETE", "/jobs/"+apiJob.ID, "")
	if rec.Code != http.StatusConflict {
		t.Fatalf("delete running job status %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if _, err := s.GetJob(apiJob.ID); err != nil {
		t.Fatalf("API delete removed running job: %v", err)
	}

	directJob := &model.Job{ID: "running-direct", Queue: "q", Type: "noop", Args: "{}", State: model.StateRunning, RunAt: time.Now(), MaxAttempts: 1}
	if err := s.CreateJob(directJob); err != nil {
		t.Fatalf("create direct job: %v", err)
	}
	if err := s.DeleteJob(directJob.ID); err == nil {
		t.Fatal("direct delete unexpectedly removed a running job")
	}
	if _, err := s.GetJob(directJob.ID); err != nil {
		t.Fatalf("direct delete removed running job: %v", err)
	}
}
