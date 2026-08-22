package store

import (
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

// TestOldestRunnableCoversScheduled ensures the maintenance "oldest" view does
// not ignore scheduled jobs in favour of a later pending job. The scheduler
// claims both pending and scheduled jobs, so the earliest executable time must
// span both states and be ordered purely by run_at.
func TestOldestRunnableCoversScheduled(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	// A pending job due in the near future — the bug would surface this one.
	laterPending := newJob("pending-later", "q", "noop", now.Add(time.Minute))
	laterPending.Priority = 9
	if err := s.CreateJob(laterPending); err != nil {
		t.Fatal(err)
	}
	// An already-due scheduled job with lower priority and an earlier run_at.
	earlierScheduled := newJob("scheduled-earlier", "q", "noop", now.Add(-time.Hour))
	earlierScheduled.State = model.StateScheduled
	earlierScheduled.Priority = 0
	if err := s.CreateJob(earlierScheduled); err != nil {
		t.Fatal(err)
	}
	got, err := s.OldestRunnable("")
	if err != nil {
		t.Fatalf("oldest runnable: %v", err)
	}
	if got.ID != "scheduled-earlier" {
		t.Fatalf("expected earliest scheduled job, got %s (run_at=%v)", got.ID, got.RunAt)
	}
}

func TestOldestRunnableEmpty(t *testing.T) {
	s := openTest(t)
	if _, err := s.OldestRunnable(""); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for empty store, got %v", err)
	}
}

func TestOldestRunnableByQueue(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	other := newJob("a", "other", "noop", now.Add(-2*time.Hour))
	other.State = model.StateScheduled
	if err := s.CreateJob(other); err != nil {
		t.Fatal(err)
	}
	matched := newJob("b", "q", "noop", now.Add(-time.Hour))
	if err := s.CreateJob(matched); err != nil {
		t.Fatal(err)
	}
	got, err := s.OldestRunnable("q")
	if err != nil {
		t.Fatalf("oldest runnable q: %v", err)
	}
	if got.ID != "b" {
		t.Fatalf("expected queue-scoped job b, got %s", got.ID)
	}
}

// TestMaintenanceOldestSpansStates exercises the maintenance endpoint path: the
// Oldest field in the report must reflect the earliest executable job across
// pending and scheduled states, ordered by run_at — not just pending jobs.
func TestMaintenanceOldestSpansStates(t *testing.T) {
	s := openTest(t)
	now := time.Now()
	// Pending job, due, but with a later run_at than the scheduled one.
	pending := newJob("p", "q", "noop", now.Add(time.Minute))
	if err := s.CreateJob(pending); err != nil {
		t.Fatal(err)
	}
	// Scheduled job whose run_at is earlier — the one the report must surface.
	scheduled := newJob("s", "q", "noop", now.Add(-2*time.Minute))
	scheduled.State = model.StateScheduled
	if err := s.CreateJob(scheduled); err != nil {
		t.Fatal(err)
	}
	rep, err := s.Maintenance(now)
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if rep.Oldest == nil || rep.Oldest.ID != "s" {
		t.Fatalf("expected scheduled job s as oldest, got %+v", rep.Oldest)
	}
}
