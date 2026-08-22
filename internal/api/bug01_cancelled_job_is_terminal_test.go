package api

import (
	"net/http"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug01_CancelledRunningJobCannotBeOverwrittenByCompletion(t *testing.T) {
	s, _, h := newServer(t)
	job := &model.Job{
		ID:          "cancel-race",
		Queue:       "q",
		Type:        "noop",
		State:       model.StateRunning,
		RunAt:       time.Now(),
		MaxAttempts: 3,
	}
	if err := s.CreateJob(job); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, h, "POST", "/jobs/cancel-race/cancel", ""); rec.Code != http.StatusOK {
		t.Fatalf("cancel code %d: %s", rec.Code, rec.Body.String())
	}
	if err := s.Succeed("cancel-race", "late completion"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob("cancel-race")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != model.StateCancelled {
		t.Fatalf("late completion overwrote cancellation: got %s", got.State)
	}
}
