package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newJob(id, queue, typ string, runAt time.Time) *model.Job {
	return &model.Job{
		ID:          id,
		Queue:       queue,
		Type:        typ,
		Args:        "{}",
		State:       model.StatePending,
		RunAt:       runAt,
		MaxAttempts: 3,
		Priority:    0,
	}
}

func TestCreateAndGet(t *testing.T) {
	s := openTest(t)
	j := newJob("j1", "q1", "noop", time.Now())
	if err := s.CreateJob(j); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.GetJob("j1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Queue != "q1" || got.Type != "noop" {
		t.Fatalf("unexpected job: %+v", got)
	}
}

func TestScanDue(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	due := newJob("due", "q", "noop", now.Add(-time.Minute))
	future := newJob("future", "q", "noop", now.Add(time.Hour))
	if err := s.CreateJob(due); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateJob(future); err != nil {
		t.Fatal(err)
	}
	got, err := s.ScanDue(10, now)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 1 || got[0].ID != "due" {
		t.Fatalf("expected only due job, got %v", got)
	}
}

func TestSucceed(t *testing.T) {
	s := openTest(t)
	j := newJob("j1", "q", "noop", time.Now())
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := s.Succeed("j1", "ok"); err != nil {
		t.Fatalf("succeed: %v", err)
	}
	got, _ := s.GetJob("j1")
	if got.State != model.StateSucceeded {
		t.Fatalf("expected succeeded, got %s", got.State)
	}
}

func TestRecordAttempts(t *testing.T) {
	s := openTest(t)
	j := newJob("j1", "q", "noop", time.Now())
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	_ = s.RecordAttempt(model.Attempt{JobID: "j1", Index: 0, StartedAt: time.Now(), EndedAt: time.Now()})
	attempts, err := s.ListAttempts("j1")
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(attempts))
	}
}

func TestRequeueDead(t *testing.T) {
	s := openTest(t)
	j := newJob("j1", "q", "noop", time.Now())
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	// Mark the job dead directly (no failure path involved) then requeue it.
	j.State = model.StateDead
	if err := s.UpdateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := s.RequeueDead("j1"); err != nil {
		t.Fatalf("requeue dead: %v", err)
	}
	got, _ := s.GetJob("j1")
	if got.State != model.StatePending {
		t.Fatalf("requeue should move to pending, got %s", got.State)
	}
}

// TestUpdateJobGuardsTerminal ensures an ordinary UpdateJob cannot roll a
// finished job back into the pending pool, which would re-enter scheduling.
func TestUpdateJobGuardsTerminal(t *testing.T) {
	for _, terminal := range []model.State{model.StateSucceeded, model.StateDead, model.StateCancelled} {
		t.Run(string(terminal), func(t *testing.T) {
			s := openTest(t)
			j := newJob("j1", "q", "noop", time.Now())
			// Seed the job already in the terminal state by transitioning from
			// pending (an active state), which UpdateJob permits.
			if err := s.CreateJob(j); err != nil {
				t.Fatal(err)
			}
			j.State = terminal
			if err := s.UpdateJob(j); err != nil {
				t.Fatalf("seed terminal state: %v", err)
			}

			// Now attempt an ordinary update that would revive it to pending.
			revive := *j
			revive.State = model.StatePending
			revive.RunAt = time.Now()
			err := s.UpdateJob(&revive)
			if !errors.Is(err, ErrTerminalJob) {
				t.Fatalf("expected ErrTerminalJob, got %v", err)
			}

			got, _ := s.GetJob("j1")
			if got.State != terminal {
				t.Fatalf("terminal state %s was rolled back to %s", terminal, got.State)
			}
		})
	}
}

// TestUpdateJobAllowsActiveTransitions confirms ordinary updates still work for
// jobs in active states (pending/scheduled/running), including moving into a
// terminal state such as cancelled.
func TestUpdateJobAllowsActiveTransitions(t *testing.T) {
	s := openTest(t)
	j := newJob("j1", "q", "noop", time.Now())
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	j.State = model.StateCancelled
	if err := s.UpdateJob(j); err != nil {
		t.Fatalf("update active->cancelled: %v", err)
	}
	got, _ := s.GetJob("j1")
	if got.State != model.StateCancelled {
		t.Fatalf("expected cancelled, got %s", got.State)
	}
}

// TestRetryRevivesDead verifies the explicit Retry lifecycle path can move a
// dead job back to pending (the legitimate revival route), while refusing
// succeeded/cancelled/running jobs.
func TestRetryRevivesDead(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	j := newJob("j1", "q", "noop", now)
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	j.State = model.StateDead
	if err := s.UpdateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := s.Retry("j1", now); err != nil {
		t.Fatalf("retry dead: %v", err)
	}
	got, _ := s.GetJob("j1")
	if got.State != model.StatePending {
		t.Fatalf("expected pending after retry, got %s", got.State)
	}
	if got.LastError != "" {
		t.Fatalf("retry should clear last error, got %q", got.LastError)
	}

	// Retry must refuse jobs that are not retryable.
	for _, st := range []model.State{model.StateSucceeded, model.StateCancelled} {
		t.Run(string(st), func(t *testing.T) {
			s := openTest(t)
			j := newJob("j2", "q", "noop", now)
			if err := s.CreateJob(j); err != nil {
				t.Fatal(err)
			}
			j.State = st
			if err := s.UpdateJob(j); err != nil {
				t.Fatal(err)
			}
			if err := s.Retry("j2", now); !errors.Is(err, ErrNotRetryable) {
				t.Fatalf("expected ErrNotRetryable for %s, got %v", st, err)
			}
			got, _ := s.GetJob("j2")
			if got.State != st {
				t.Fatalf("%s job was revived to %s", st, got.State)
			}
		})
	}
}

func TestQueuePause(t *testing.T) {
	s := openTest(t)
	if err := s.SetPaused("q1", true); err != nil {
		t.Fatalf("set paused: %v", err)
	}
	paused, err := s.IsPaused("q1")
	if err != nil {
		t.Fatal(err)
	}
	if !paused {
		t.Fatal("expected q1 paused")
	}
}

func TestListJobsLimit(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	for i := 0; i < 5; i++ {
		j := newJob(string(rune('a'+i)), "q", "noop", now)
		j.ID = "id" + string(rune('0'+i))
		if err := s.CreateJob(j); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListJobs(ListFilter{Limit: 3})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(got))
	}
}

func TestStats(t *testing.T) {
	s := openTest(t)
	for i := 0; i < 3; i++ {
		j := newJob("s"+string(rune('0'+i)), "q", "noop", time.Now())
		if err := s.CreateJob(j); err != nil {
			t.Fatal(err)
		}
	}
	st, err := s.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Total != 3 {
		t.Fatalf("expected total 3, got %d", st.Total)
	}
}
