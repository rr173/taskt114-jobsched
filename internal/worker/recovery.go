package worker

import (
	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
	"time"
)

type RecoveryResult struct {
	Scanned  int `json:"scanned"`
	Requeued int `json:"requeued"`
	Failed   int `json:"failed"`
}

// RecoverInterrupted converts jobs left in running state by a process crash
// back into pending work. It is run before the scheduler starts polling so a
// restart cannot strand a durable job indefinitely.
func RecoverInterrupted(s *store.Store, now time.Time) (RecoveryResult, error) {
	rows, err := s.ListJobs(store.ListFilter{State: model.StateRunning, Limit: 10000})
	if err != nil {
		return RecoveryResult{}, err
	}
	out := RecoveryResult{Scanned: len(rows)}
	for i := range rows {
		job := rows[i]
		job.State = model.StatePending
		job.RunAt = now
		job.UpdatedAt = now
		job.LastError = "requeued after scheduler restart"
		if err := s.UpdateJob(&job); err != nil {
			out.Failed++
			return out, err
		}
		out.Requeued++
	}
	return out, nil
}
