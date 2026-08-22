package model

import "time"

func (s Schedule) NextRun(now time.Time) time.Time {
	if s.LastRun.IsZero() {
		return now
	}
	return s.LastRun.Add(s.Interval)
}
func (s Schedule) MissedRuns(now time.Time) int {
	if !s.Enabled || s.LastRun.IsZero() || now.Before(s.LastRun) {
		return 0
	}
	return int(now.Sub(s.LastRun) / s.Interval)
}
