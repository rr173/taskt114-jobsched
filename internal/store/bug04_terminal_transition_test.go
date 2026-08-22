package store

import (
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug04_TerminalJobCannotBeReopenedThroughUpdate(t *testing.T) {
	s := openTest(t)
	j := newJob("terminal-transition", "q", "noop", time.Now())
	if err := s.CreateJob(j); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.Succeed(j.ID, "done"); err != nil {
		t.Fatalf("succeed: %v", err)
	}
	completed, err := s.GetJob(j.ID)
	if err != nil {
		t.Fatalf("get completed: %v", err)
	}
	completed.State = model.StatePending
	completed.RunAt = time.Now()
	if err := s.UpdateJob(completed); err == nil {
		t.Fatal("terminal job update unexpectedly reopened the job")
	}
	got, err := s.GetJob(j.ID)
	if err != nil {
		t.Fatalf("get after rejected update: %v", err)
	}
	if got.State != model.StateSucceeded {
		t.Fatalf("terminal state changed to %q", got.State)
	}
}
