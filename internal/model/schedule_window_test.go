package model

import (
	"testing"
	"time"
)

func TestNextRunFirstTrigger(t *testing.T) {
	// A never-run schedule fires immediately on its first trigger.
	sc := Schedule{Enabled: true, Interval: time.Minute}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if got := sc.NextRun(now); !got.Equal(now) {
		t.Fatalf("first trigger NextRun want %v, got %v", now, got)
	}
}

func TestNextRunAfterLastRun(t *testing.T) {
	// After firing once, the next run is one interval past the last run.
	last := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	sc := Schedule{Enabled: true, Interval: time.Minute, LastRun: last}
	now := last.Add(90 * time.Second)
	want := last.Add(time.Minute)
	if got := sc.NextRun(now); !got.Equal(want) {
		t.Fatalf("NextRun want %v, got %v", want, got)
	}
}

func TestMissedRunsFirstTriggerIsZero(t *testing.T) {
	// The very first trigger must NOT be counted as backlog, regardless of how
	// much wall time has passed since the zero time. This is the core invariant
	// that prevents backfilling many historical runs on first fire.
	sc := Schedule{Enabled: true, Interval: time.Minute}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if got := sc.MissedRuns(now); got != 0 {
		t.Fatalf("first trigger MissedRuns want 0, got %d", got)
	}
}

func TestMissedRunsGenuineBacklog(t *testing.T) {
	// A schedule that last ran a while ago genuinely missed intervals and the
	// caller should catch up.
	last := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	sc := Schedule{Enabled: true, Interval: time.Minute, LastRun: last}
	now := last.Add(5*time.Minute + 30*time.Second)
	if got := sc.MissedRuns(now); got != 5 {
		t.Fatalf("backlog MissedRuns want 5, got %d", got)
	}
}

func TestMissedRunsDisabledSchedule(t *testing.T) {
	last := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	sc := Schedule{Enabled: false, Interval: time.Minute, LastRun: last}
	now := last.Add(5 * time.Minute)
	if got := sc.MissedRuns(now); got != 0 {
		t.Fatalf("disabled schedule MissedRuns want 0, got %d", got)
	}
}

func TestMissedRunsBeforeLastRun(t *testing.T) {
	// Clock skew: now earlier than last run must not produce negative backlog.
	last := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	sc := Schedule{Enabled: true, Interval: time.Minute, LastRun: last}
	now := last.Add(-30 * time.Second)
	if got := sc.MissedRuns(now); got != 0 {
		t.Fatalf("pre-last-run MissedRuns want 0, got %d", got)
	}
}
