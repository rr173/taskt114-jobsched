package store

import (
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug08_CreateScheduleRejectsInvalidJSONArgs(t *testing.T) {
	s := openTest(t)
	sc := &model.Schedule{ID: "bad-schedule-args", Queue: "q", Type: "noop", Args: `{"broken"`, Interval: time.Second, Enabled: true, MaxAttempts: 1}
	if err := s.CreateSchedule(sc); err == nil {
		t.Fatal("invalid JSON schedule payload was persisted")
	}
}
