package store

import (
	"database/sql"
	"errors"
	"fmt"

	"taskt114-jobsched/internal/model"
	"time"
)

// OldestRunnable returns the job with the earliest run_at among all jobs that
// the scheduler can still claim, i.e. those in the pending or scheduled
// state. It orders purely by run_at so the maintenance view reports the
// genuinely earliest executable time regardless of state or priority; a
// later high-priority pending job must not mask an already-due scheduled job.
func (s *Store) OldestRunnable(queue string) (*model.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs WHERE state IN ('pending','scheduled')`
	args := []interface{}{}
	if queue != "" {
		q += ` AND queue=?`
		args = append(args, queue)
	}
	q += ` ORDER BY run_at ASC, id ASC LIMIT 1`
	row := s.db.QueryRow(q, args...)
	j, err := s.scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("oldest runnable: %w", err)
	}
	return j, nil
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
