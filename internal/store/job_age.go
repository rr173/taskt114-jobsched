package store

import (
	"taskt114-jobsched/internal/model"
	"time"
)

func (s *Store) OldestPending(queue string) (*model.Job, error) {
	rows, err := s.ListJobs(ListFilter{Queue: queue, State: model.StatePending, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return &rows[0], nil
}
func (s *Store) DueCount(queue string, now time.Time) (int, error) {
	rows, err := s.ScanDue(10000, now)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, j := range rows {
		if queue == "" || j.Queue == queue {
			n++
		}
	}
	return n, nil
}
