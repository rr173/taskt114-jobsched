package store

import (
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug09_MaintenanceReportsOldestReadyScheduledJob(t *testing.T) {
	s := openTest(t)
	now := time.Now().Truncate(time.Millisecond)
	oldScheduled := newJob("old-scheduled", "q", "noop", now.Add(-time.Minute))
	oldScheduled.State = model.StateScheduled
	oldScheduled.CreatedAt = now.Add(-time.Hour)
	newPending := newJob("new-pending", "q", "noop", now)
	newPending.CreatedAt = now.Add(-time.Minute)
	if err := s.CreateJob(oldScheduled); err != nil {
		t.Fatalf("create scheduled: %v", err)
	}
	if err := s.CreateJob(newPending); err != nil {
		t.Fatalf("create pending: %v", err)
	}
	report, err := s.Maintenance(now)
	if err != nil {
		t.Fatalf("maintenance: %v", err)
	}
	if report.Oldest == nil || report.Oldest.ID != oldScheduled.ID {
		t.Fatalf("maintenance oldest = %#v, want due scheduled job %q", report.Oldest, oldScheduled.ID)
	}
}
