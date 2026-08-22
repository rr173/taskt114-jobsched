package store

import "taskt114-jobsched/internal/model"

type QueueReport struct {
	Queue  string              `json:"queue"`
	Paused bool                `json:"paused"`
	Stats  map[model.State]int `json:"stats"`
}

func (s *Store) QueueReport(queue string) (QueueReport, error) {
	paused, err := s.IsPaused(queue)
	if err != nil {
		return QueueReport{}, err
	}
	stats, err := s.QueueStats(queue)
	if err != nil {
		return QueueReport{}, err
	}
	return QueueReport{Queue: queue, Paused: paused, Stats: stats}, nil
}
