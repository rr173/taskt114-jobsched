package worker

import (
	"context"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
)

func TestBug05_NewScheduleCreatesExactlyOneCurrentRun(t *testing.T) {
	s, p := newPool(t)
	before := time.Now()
	sc := &model.Schedule{ID: "first-schedule", Queue: "q", Type: "noop", Args: "{}", Interval: time.Minute, Enabled: true, MaxAttempts: 1}
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	p.fireDueSchedules(context.Background())
	jobs, err := s.ListJobs(store.ListFilter{})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("first schedule fire created %d jobs, want exactly one", len(jobs))
	}
	if jobs[0].RunAt.Before(before) {
		t.Fatalf("first schedule run was backdated to %s", jobs[0].RunAt)
	}
}
