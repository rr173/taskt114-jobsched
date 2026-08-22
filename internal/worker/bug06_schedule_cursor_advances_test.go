package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug06_ExistingScheduleRunStillAdvancesCursor(t *testing.T) {
	s, p := newPool(t)
	now := time.Now().Truncate(time.Millisecond)
	sc := &model.Schedule{ID: "cursor-schedule", Queue: "q", Type: "noop", Args: "{}", Interval: time.Second, Enabled: true, LastRun: now.Add(-time.Second), MaxAttempts: 1}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	loaded, err := s.GetSchedule(sc.ID)
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	runAt := loaded.NextRun(time.Now())
	duplicate := &model.Job{ID: fmt.Sprintf("sched-%s-%d", loaded.ID, runAt.UnixNano()), Queue: loaded.Queue, Type: loaded.Type, Args: loaded.Args, State: model.StatePending, RunAt: runAt, MaxAttempts: loaded.MaxAttempts}
	if err := s.CreateJob(duplicate); err != nil {
		t.Fatalf("precreate schedule run: %v", err)
	}
	p.fireDueSchedules(context.Background())
	updated, err := s.GetSchedule(sc.ID)
	if err != nil {
		t.Fatalf("get updated schedule: %v", err)
	}
	if updated.LastRun.Before(runAt) {
		t.Fatalf("schedule cursor stayed at %s after an already-enqueued run %s", updated.LastRun, runAt)
	}
}
