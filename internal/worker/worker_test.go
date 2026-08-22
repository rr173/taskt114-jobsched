package worker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"taskt114-jobsched/internal/clock"
	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
)

func newPool(t *testing.T) (*store.Store, *Pool) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "w.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	p := New(s, clock.New())
	p.RegisterHandler("noop", func(ctx context.Context, j *model.Job) (string, error) {
		return "", nil
	})
	p.RegisterHandler("echo", func(ctx context.Context, j *model.Job) (string, error) {
		return j.Args, nil
	})
	return s, p
}

func TestFlushSucceeds(t *testing.T) {
	s, p := newPool(t)
	now := time.Now()
	for i := 0; i < 4; i++ {
		j := &model.Job{
			ID:          "j" + string(rune('0'+i)),
			Queue:       "q",
			Type:        "echo",
			Args:        `{"i":` + string(rune('0'+i)) + `}`,
			State:       model.StatePending,
			RunAt:       now,
			MaxAttempts: 3,
		}
		if err := s.CreateJob(j); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	p.Wait()
	for i := 0; i < 4; i++ {
		j, err := s.GetJob("j" + string(rune('0'+i)))
		if err != nil {
			t.Fatal(err)
		}
		if j.State != model.StateSucceeded {
			t.Fatalf("job %s expected succeeded, got %s", j.ID, j.State)
		}
		if j.Result != `{"i":`+string(rune('0'+i))+`}` {
			t.Fatalf("job %s wrong result: %q", j.ID, j.Result)
		}
	}
}

func TestFlushSkipsFuture(t *testing.T) {
	s, p := newPool(t)
	now := time.Now()
	due := &model.Job{ID: "due", Queue: "q", Type: "noop", State: model.StatePending, RunAt: now, MaxAttempts: 1}
	future := &model.Job{ID: "future", Queue: "q", Type: "noop", State: model.StateScheduled, RunAt: now.Add(time.Hour), MaxAttempts: 1}
	if err := s.CreateJob(due); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateJob(future); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Wait()
	d, _ := s.GetJob("due")
	if d.State != model.StateSucceeded {
		t.Fatalf("due job expected succeeded, got %s", d.State)
	}
	f, _ := s.GetJob("future")
	if f.State != model.StateScheduled {
		t.Fatalf("future job expected scheduled, got %s", f.State)
	}
}

// TestDeleteRunningJobBlockedWhileWorkerExecutes reproduces the lifecycle gap:
// a management delete issued while a worker is mid-execution must be refused so
// the worker's later attempt/terminal writes never land on a missing row.
func TestDeleteRunningJobBlockedWhileWorkerExecutes(t *testing.T) {
	s, p := newPool(t)

	// Gate the handler so the job is claimed (state=running) and stays in the
	// handler until the test releases it.
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

	j := &model.Job{
		ID:          "race",
		Queue:       "q",
		Type:        "slow-gated",
		State:       model.StatePending,
		RunAt:       time.Now(),
		MaxAttempts: 3,
	}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}

	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	<-started // ensure the handler is executing and the job is running

	// A management delete while the job is running must be refused.
	err := s.DeleteJob("race")
	if !errors.Is(err, store.ErrJobRunning) {
		t.Fatalf("expected ErrJobRunning while worker executing, got %v", err)
	}
	// The job row must still exist and remain running.
	got, err := s.GetJob("race")
	if err != nil {
		t.Fatalf("running job vanished after refused delete: %v", err)
	}
	if got.State != model.StateRunning {
		t.Fatalf("expected running, got %s", got.State)
	}

	// Let the worker finish and wait for it to record the outcome.
	close(release)
	p.Wait()

	got, err = s.GetJob("race")
	if err != nil {
		t.Fatalf("job vanished after worker finished: %v", err)
	}
	if got.State != model.StateSucceeded {
		t.Fatalf("expected succeeded after worker finish, got %s", got.State)
	}
	// Exactly one attempt must be recorded for the in-flight execution.
	attempts, err := s.ListAttempts("race")
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}

	// Now that the job is terminal, the delete must succeed and clean up its
	// attempt history, leaving a consistent (empty) record.
	if err := s.DeleteJob("race"); err != nil {
		t.Fatalf("delete after finish: %v", err)
	}
	if _, err := s.GetJob("race"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if left, _ := s.ListAttempts("race"); len(left) != 0 {
		t.Fatalf("expected no orphaned attempts, got %d", len(left))
	}
}
