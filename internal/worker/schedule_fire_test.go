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

func newSchedulePool(t *testing.T) (*store.Store, *Pool, *clock.FixedClock) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "sched.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	clk := clock.NewFixed(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	p := New(s, clk)
	p.RegisterHandler("noop", func(ctx context.Context, j *model.Job) (string, error) {
		return "", nil
	})
	return s, p, clk
}

// countScheduleJobs reports how many jobs the given schedule has produced.
func countScheduleJobs(t *testing.T, s *store.Store, schedID string) int {
	t.Helper()
	jobs, err := s.ListJobs(store.ListFilter{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	n := 0
	for _, j := range jobs {
		if len(j.ID) > len("sched-") && j.ID[:len("sched-")] == "sched-" {
			// job ids look like "sched-<schedID>-<unixnano>"; match the prefix.
			if len(j.ID) >= len("sched-")+len(schedID)+1 {
				if j.ID[:len("sched-")+len(schedID)] == "sched-"+schedID {
					n++
				}
			}
		}
	}
	return n
}

// TestFireDueSchedulesFirstTriggerSingleJob is the regression test for the
// backfill bug: a schedule firing for the first time must enqueue exactly one
// job, not a batch of "missed" historical runs.
func TestFireDueSchedulesFirstTriggerSingleJob(t *testing.T) {
	s, p, clk := newSchedulePool(t)
	sc := &model.Schedule{
		ID:          "hourly",
		Queue:       "q",
		Type:        "noop",
		Args:        "{}",
		Interval:    time.Hour,
		Enabled:     true,
		MaxAttempts: 3,
	}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	p.fireDueSchedules(t.Context())

	if got := countScheduleJobs(t, s, "hourly"); got != 1 {
		t.Fatalf("first trigger expected 1 job, got %d (backfill bug regressed)", got)
	}
	// LastRun must advance past zero so the next fire is real backlog, not another
	// "first trigger".
	got, err := s.GetSchedule("hourly")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if got.LastRun.IsZero() {
		t.Fatal("TouchSchedule should have set LastRun after first fire")
	}
	if !got.LastRun.Equal(clk.Now()) {
		t.Fatalf("LastRun want %v, got %v", clk.Now(), got.LastRun)
	}
}

// TestFireDueSchedulesGenuineBacklogCatchesUp confirms the real backlog path
// still catches up missed intervals (capped at 10) once a schedule has fired at
// least once.
func TestFireDueSchedulesGenuineBacklogCatchesUp(t *testing.T) {
	s, p, clk := newSchedulePool(t)
	lastRun := clk.Now().Add(-3 * time.Hour) // 3 missed hourly intervals
	sc := &model.Schedule{
		ID:          "hourly",
		Queue:       "q",
		Type:        "noop",
		Args:        "{}",
		Interval:    time.Hour,
		Enabled:     true,
		MaxAttempts: 3,
		LastRun:     lastRun,
	}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	p.fireDueSchedules(t.Context())

	if got := countScheduleJobs(t, s, "hourly"); got != 3 {
		t.Fatalf("backlog expected 3 caught-up jobs, got %d", got)
	}
}

// TestFireDueSchedulesBacklogCappedAtTen verifies the catch-up cap is retained
// for extreme backlog so a long-down schedule does not flood the queue.
func TestFireDueSchedulesBacklogCappedAtTen(t *testing.T) {
	s, p, _ := newSchedulePool(t)
	// 50 missed intervals — must be capped to 10.
	sc := &model.Schedule{
		ID:          "hourly",
		Queue:       "q",
		Type:        "noop",
		Args:        "{}",
		Interval:    time.Hour,
		Enabled:     true,
		MaxAttempts: 3,
		LastRun:     time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC).Add(-50 * time.Hour),
	}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}

	p.fireDueSchedules(t.Context())

	if got := countScheduleJobs(t, s, "hourly"); got != 10 {
		t.Fatalf("extreme backlog expected 10 (cap), got %d", got)
	}
}

// TestFireDueSchedulesFirstTriggerAfterStoreRoundTrip guards the data-layer
// fix: a schedule persisted with a zero LastRun and read back must still be
// treated as a first trigger (one job), proving the write/read asymmetry no
// longer turns "never run" into "ancient last run".
func TestFireDueSchedulesFirstTriggerAfterStoreRoundTrip(t *testing.T) {
	s, p, _ := newSchedulePool(t)
	sc := &model.Schedule{
		ID:          "daily",
		Queue:       "q",
		Type:        "noop",
		Args:        "{}",
		Interval:    24 * time.Hour,
		Enabled:     true,
		MaxAttempts: 3,
	}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	// Round-trip: read it back through the store before firing, exactly as the
	// real worker does via DueSchedules.
	due, err := s.DueSchedules(p.clk.Now())
	if err != nil {
		t.Fatalf("due schedules: %v", err)
	}
	if len(due) != 1 || due[0].ID != "daily" {
		t.Fatalf("expected one due schedule 'daily', got %v", due)
	}
	if !due[0].LastRun.IsZero() {
		t.Fatalf("persisted never-run schedule should read back as zero LastRun, got %v", due[0].LastRun)
	}

	p.fireDueSchedules(t.Context())

	if got := countScheduleJobs(t, s, "daily"); got != 1 {
		t.Fatalf("round-tripped first trigger expected 1 job, got %d", got)
	}
}
