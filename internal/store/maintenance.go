package store

import (
	"taskt114-jobsched/internal/model"
	"time"
)

type MaintenanceReport struct {
	Total    int        `json:"total"`
	Due      int        `json:"due"`
	Running  int        `json:"running"`
	Dead     int        `json:"dead"`
	Oldest   *model.Job `json:"oldest"`
	Capacity Capacity   `json:"capacity"`
}

func (s *Store) Maintenance(now time.Time) (MaintenanceReport, error) {
	stats, err := s.Stats()
	if err != nil {
		return MaintenanceReport{}, err
	}
	due, err := s.DueCount("", now)
	if err != nil {
		return MaintenanceReport{}, err
	}
	oldest, err := s.OldestRunnable("")
	if err != nil && err != ErrNotFound {
		return MaintenanceReport{}, err
	}
	capacity, err := s.Capacity()
	if err != nil {
		return MaintenanceReport{}, err
	}
	return MaintenanceReport{Total: stats.Total, Due: due, Running: stats.ByState[model.StateRunning], Dead: stats.DeadLetters, Oldest: oldest, Capacity: capacity}, nil
}
