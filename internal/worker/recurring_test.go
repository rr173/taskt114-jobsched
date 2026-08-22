package worker

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
)

// stepClock is a manually-advanced clock so schedule logic can be tested
// deterministically without sleeping.
type stepClock struct{ now time.Time }

func (s *stepClock) Now() time.Time                          { return s.now }
func (s *stepClock) After(d time.Duration) <-chan time.Time { return make(chan time.Time) }

func newSchedulePool(t *testing.T, clk *stepClock) (*store.Store, *Pool) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	p := New(s, clk)
	p.RegisterHandler("noop", func(ctx context.Context, j *model.Job) (string, error) {
		return "", nil
	})
	return s, p
}

// jobIDFor mirrors the deterministic id worker.fireDueSchedules assigns to a
// scheduled fire, so the test can pre-seed exactly the row the scheduler would
// create.
func jobIDFor(sc model.Schedule, runAt time.Time) string {
	return fmt.Sprintf("sched-%s-%d", sc.ID, runAt.UnixNano())
}

// seedSchedule stores a schedule and pins its last_run to lastRun via
// TouchSchedule, so the in-memory and persisted cursor agree on the starting
// point.
func seedSchedule(t *testing.T, s *store.Store, id, queue, typ string, interval time.Duration, lastRun time.Time) model.Schedule {
	t.Helper()
	sc := &model.Schedule{
		ID:       id,
		Queue:    queue,
		Type:     typ,
		Args:     "{}",
		Interval: interval,
		Enabled:  true,
	}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule %s: %v", id, err)
	}
	if !lastRun.IsZero() {
		if err := s.TouchSchedule(id, lastRun); err != nil {
			t.Fatalf("touch schedule %s: %v", id, err)
		}
	}
	got, err := s.GetSchedule(id)
	if err != nil {
		t.Fatalf("get schedule %s: %v", id, err)
	}
	return *got
}

// TestFireDueSchedulesAdvancesPastExistingRun is the regression test for the
// bug where a recurring schedule stalled whenever the job for its next fire
// time already existed. The schedule must treat that as already fired and
// advance last_run, so subsequent polls produce the *next* run instead of
// re-deriving the same one forever.
func TestFireDueSchedulesAdvancesPastExistingRun(t *testing.T) {
	start := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	clk := &stepClock{now: start.Add(time.Hour)} // 10:00
	s, p := newSchedulePool(t, clk)

	// The schedule last fired at 09:00, so its next run is 10:00 — exactly now.
	sc := seedSchedule(t, s, "daily", "q", "noop", time.Hour, start)

	nextRunAt := sc.NextRun(clk.Now()) // 10:00
	// Seed exactly the job the scheduler is about to create, mimicking a fire
	// already recorded by a prior tick (e.g. the process was restarted between
	// CreateJob and TouchSchedule).
	seed := &model.Job{
		ID:          jobIDFor(sc, nextRunAt),
		Queue:       sc.Queue,
		Type:        sc.Type,
		Args:        sc.Args,
		State:       model.StatePending,
		RunAt:       nextRunAt,
		MaxAttempts: 3,
	}
	if err := s.CreateJob(seed); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	// Before the fix this hit the UNIQUE constraint, broke out of the loop, and
	// never touched the schedule: last_run stayed at 09:00 and every later poll
	// re-derived nextRunAt (10:00), stalling the cursor permanently.
	p.fireDueSchedules(context.Background())

	got, err := s.GetSchedule("daily")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if !got.LastRun.Equal(nextRunAt) {
		t.Fatalf("last_run = %v, want %v (should advance to the already-recorded slot)",
			got.LastRun, nextRunAt)
	}

	// Advance the clock one more interval and fire again. The schedule must now
	// create the *next* run (11:00), not re-process 10:00.
	clk.now = nextRunAt.Add(time.Hour) // 11:00
	p.fireDueSchedules(context.Background())

	followUpRunAt := nextRunAt.Add(sc.Interval) // 11:00
	if _, err := s.GetJob(jobIDFor(sc, followUpRunAt)); err != nil {
		t.Fatalf("expected follow-up job %s to be created, got %v",
			jobIDFor(sc, followUpRunAt), err)
	}
	got, err = s.GetSchedule("daily")
	if err != nil {
		t.Fatalf("get schedule after follow-up fire: %v", err)
	}
	if !got.LastRun.Equal(followUpRunAt) {
		t.Fatalf("last_run = %v, want %v after follow-up fire", got.LastRun, followUpRunAt)
	}
}

// TestFireDueSchedulesSkipsAllExistingRuns ensures that when several
// consecutive fire slots are already recorded (a catch-up burst where each
// slot's job exists), the scheduler advances through all of them rather than
// stopping at the first duplicate.
func TestFireDueSchedulesSkipsAllExistingRuns(t *testing.T) {
	start := time.Date(2026, 8, 22, 9, 0, 0, 0, time.UTC)
	clk := &stepClock{now: start.Add(3 * time.Hour)} // 12:00
	s, p := newSchedulePool(t, clk)

	// Last fired 09:00; next runs at 10:00 and 11:00 are already seeded, so the
	// scheduler should skip both and create the 12:00 run.
	sc := seedSchedule(t, s, "hourly", "q", "noop", time.Hour, start)

	for i := 0; i < 2; i++ {
		at := sc.NextRun(start).Add(time.Duration(i) * sc.Interval) // 10:00, 11:00
		seed := &model.Job{
			ID:          jobIDFor(sc, at),
			Queue:       sc.Queue,
			Type:        sc.Type,
			Args:        sc.Args,
			State:       model.StatePending,
			RunAt:       at,
			MaxAttempts: 3,
		}
		if err := s.CreateJob(seed); err != nil {
			t.Fatalf("seed job %d (%s): %v", i, at, err)
		}
	}

	// MissedRuns(12:00) reports 3 due slots (10:00, 11:00, 12:00). The first
	// two already exist; the scheduler must advance past both and create 12:00.
	p.fireDueSchedules(context.Background())

	thirdRunAt := sc.NextRun(start).Add(2 * sc.Interval) // 12:00
	if _, err := s.GetJob(jobIDFor(sc, thirdRunAt)); err != nil {
		t.Fatalf("expected run %s to be created after skipping duplicates: %v",
			jobIDFor(sc, thirdRunAt), err)
	}
	got, err := s.GetSchedule("hourly")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if !got.LastRun.Equal(thirdRunAt) {
		t.Fatalf("last_run = %v, want %v (should advance past all existing runs)",
			got.LastRun, thirdRunAt)
	}
}
