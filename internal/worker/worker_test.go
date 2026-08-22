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

// TestScheduleWithBadArgsDoesNotSpawnJobs covers the non-HTTP ingestion
// path: a recurring schedule whose args are unparseable JSON must not
// enqueue jobs a worker could only fail on. fireDueSchedules should skip
// the firing entirely.
func TestScheduleWithBadArgsDoesNotSpawnJobs(t *testing.T) {
	s, p := newPool(t)
	// Persist a schedule with broken args directly, bypassing the HTTP layer
	// (which would otherwise validate). CreateSchedule does not validate, so
	// this simulates a non-HTTP / pre-existing corrupted schedule.
	sc := &model.Schedule{
		ID:          "bad-sched",
		Queue:       "q",
		Type:        "noop",
		Args:        "{not json",
		Interval:    time.Second,
		Enabled:     true,
		MaxAttempts: 3,
	}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	p.fireDueSchedules(context.Background())

	jobs, err := s.ListJobs(store.ListFilter{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no jobs spawned from schedule with bad args, got %d: %+v", len(jobs), jobs)
	}
}
