package model

import "time"

func (s Schedule) NextRun(now time.Time) time.Time {
	if s.LastRun.IsZero() {
		// First trigger: fire immediately at now rather than scheduling one
		// interval in the past, which would otherwise be picked up as backlog.
		return now
	}
	return s.LastRun.Add(s.Interval)
}

// MissedRuns reports how many scheduled intervals elapsed since LastRun without
// firing. A schedule that has never run (zero LastRun) reports 0: the very
// first trigger is the current round, not accumulated backlog, so the caller
// should enqueue exactly one job for it.
func (s Schedule) MissedRuns(now time.Time) int {
	if !s.Enabled || s.LastRun.IsZero() || now.Before(s.LastRun) {
		return 0
	}
	return int(now.Sub(s.LastRun) / s.Interval)
}
