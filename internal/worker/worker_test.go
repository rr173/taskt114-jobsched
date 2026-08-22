package worker

import (
	"context"
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

// TestLateSuccessDoesNotOverrideCancel reproduces the reported bug end-to-end.
// A cancellable handler completes successfully just after the job is cancelled
// by an operator. The late success must not flip the cancelled job back to
// succeeded.
func TestLateSuccessDoesNotOverrideCancel(t *testing.T) {
	s, p := newPool(t)
	// Handler blocks until released, then returns success regardless of ctx.
	release := make(chan struct{})
	fired := make(chan struct{})
	p.RegisterHandler("block-then-succeed", func(ctx context.Context, j *model.Job) (string, error) {
		close(fired)
		<-release
		return "done", nil
	})
	j := &model.Job{ID: "c1", Queue: "q", Type: "block-then-succeed", State: model.StatePending, RunAt: time.Now(), MaxAttempts: 3}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-fired // handler is running

	// Operator cancels the running job. This must become the terminal state.
	p.Cancel("c1")
	if err := s.Cancel("c1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got, _ := s.GetJob("c1"); got.State != model.StateCancelled {
		t.Fatalf("expected cancelled, got %s", got.State)
	}

	// The handler now completes successfully — a late success arriving after
	// cancellation. It must be a no-op.
	close(release)
	p.Wait()

	got, err := s.GetJob("c1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != model.StateCancelled {
		t.Fatalf("late success overrode cancellation: state=%s result=%q", got.State, got.Result)
	}
	if got.Result != "" {
		t.Fatalf("late success wrote result despite cancellation: %q", got.Result)
	}
}
