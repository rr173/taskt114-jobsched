package worker

import (
	"context"
	"fmt"
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

// TestFlushReleaseOnCancel reproduces the bug where a job that was claimed
// (moved to "running" in the DB) but never dispatched — because the dispatch
// context was cancelled while the pool waited for a free worker slot — was
// stranded in the running state forever. After the fix the claimed job must
// return to a processable state (pending) so a later flush can run it.
func TestFlushReleaseOnCancel(t *testing.T) {
	s, p := newPool(t)

	// A blocker handler holds its worker slot until released, so we can
	// saturate the pool and force the next claim to block on the semaphore.
	releaseBlockers := make(chan struct{})
	started := make(chan struct{}, 16)
	p.RegisterHandler("blocker", func(ctx context.Context, j *model.Job) (string, error) {
		started <- struct{}{}
		<-releaseBlockers
		return "", nil
	})

	// Submit 16 blocker jobs to fill every worker slot.
	now := time.Now()
	for i := 0; i < 16; i++ {
		j := &model.Job{
			ID:          fmt.Sprintf("block-%d", i),
			Queue:       "q",
			Type:        "blocker",
			State:       model.StatePending,
			RunAt:       now,
			MaxAttempts: 1,
		}
		if err := s.CreateJob(j); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("flush blockers: %v", err)
	}
	// Wait until every slot is occupied.
	for i := 0; i < 16; i++ {
		<-started
	}

	// The pool is now saturated. A further due job will be claimed (moved to
	// running in the DB) but block on the semaphore; cancelling the dispatch
	// context mid-wait must release the claim rather than stranding the job.
	extra := &model.Job{ID: "extra", Queue: "q", Type: "noop", State: model.StatePending, RunAt: now, MaxAttempts: 1}
	if err := s.CreateJob(extra); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Give Flush time to claim "extra" and start waiting on p.sem, then
		// cancel so the select hits the ctx.Done() branch.
		for {
			j, err := s.GetJob("extra")
			if err == nil && j.State == model.StateRunning {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		cancel()
	}()
	_ = p.Flush(ctx) // returns ctx.Err() once cancelled

	j, err := s.GetJob("extra")
	if err != nil {
		t.Fatalf("get extra: %v", err)
	}
	if j.State != model.StatePending {
		t.Fatalf("expected extra released back to pending, got %s", j.State)
	}

	// The stranded job is now processable: release the blockers, then flush
	// again and confirm "extra" actually runs to completion.
	close(releaseBlockers)
	p.Wait()
	if err := p.Flush(context.Background()); err != nil {
		t.Fatalf("reflush: %v", err)
	}
	p.Wait()
	j, err = s.GetJob("extra")
	if err != nil {
		t.Fatalf("get extra after reflush: %v", err)
	}
	if j.State != model.StateSucceeded {
		t.Fatalf("expected extra to succeed after reflush, got %s", j.State)
	}
}
