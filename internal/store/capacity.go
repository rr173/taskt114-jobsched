package store

import "taskt114-jobsched/internal/model"

type Capacity struct {
	Pending  int `json:"pending"`
	Running  int `json:"running"`
	Terminal int `json:"terminal"`
}

func (s *Store) Capacity() (Capacity, error) {
	stats, err := s.Stats()
	if err != nil {
		return Capacity{}, err
	}
	return Capacity{Pending: stats.ByState[model.StatePending] + stats.ByState[model.StateScheduled], Running: stats.ByState[model.StateRunning], Terminal: stats.ByState[model.StateSucceeded] + stats.ByState[model.StateDead] + stats.ByState[model.StateCancelled]}, nil
}
