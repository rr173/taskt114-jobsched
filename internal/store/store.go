// Package store persists jobs, attempts and queue state in SQLite.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"taskt114-jobsched/internal/model"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned when a requested job does not exist.
var ErrNotFound = errors.New("store: job not found")

// Store is a SQLite-backed persistence layer for the scheduler.
type Store struct {
	db *sql.DB
}

// ListFilter narrows a ListJobs query.
type ListFilter struct {
	State  model.State
	Queue  string
	Limit  int
	Offset int
}

var schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id TEXT PRIMARY KEY,
	queue TEXT NOT NULL,
	type TEXT NOT NULL,
	args TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL,
	run_at INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	max_attempts INTEGER NOT NULL DEFAULT 1,
	last_error TEXT NOT NULL DEFAULT '',
	result TEXT NOT NULL DEFAULT '',
	priority INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_jobs_state_run ON jobs(state, run_at);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue);

CREATE TABLE IF NOT EXISTS attempts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id TEXT NOT NULL,
	idx INTEGER NOT NULL,
	started_at INTEGER NOT NULL,
	ended_at INTEGER NOT NULL,
	error TEXT NOT NULL DEFAULT '',
	duration_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_attempts_job ON attempts(job_id, idx);

CREATE TABLE IF NOT EXISTS queues (
	name TEXT PRIMARY KEY,
	paused INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS schedules (
	id TEXT PRIMARY KEY,
	queue TEXT NOT NULL,
	type TEXT NOT NULL,
	args TEXT NOT NULL DEFAULT '',
	interval_ms INTEGER NOT NULL,
	enabled INTEGER NOT NULL DEFAULT 1,
	last_run INTEGER NOT NULL DEFAULT 0,
	max_attempts INTEGER NOT NULL DEFAULT 3,
	priority INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, last_run);
`

// Open connects to the SQLite database at path, applies the schema and
// recovers any jobs interrupted mid-flight by a previous process.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // single writer avoids SQLITE_BUSY for the training workload
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set journal mode: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	s := &Store{db: db}
	// Restart recovery: any job left "running" was interrupted; return it to
	// the pending pool so it can be retried after a short grace period.
	if _, err := db.Exec(
		`UPDATE jobs SET state='pending', run_at=strftime('%s','now')*1000000000 WHERE state='running'`,
	); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("recover running jobs: %w", err)
	}
	return s, nil
}

// Close releases the underlying database handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) scanJob(row interface {
	Scan(...interface{}) error
}) (*model.Job, error) {
	j := &model.Job{}
	var (
		runAt, createdAt, updatedAt  int64
		args, state, lastErr, result string
	)
	if err := row.Scan(
		&j.ID, &j.Queue, &j.Type, &args, &state,
		&runAt, &createdAt, &updatedAt,
		&j.Attempts, &j.MaxAttempts, &lastErr, &result, &j.Priority,
	); err != nil {
		return nil, err
	}
	j.Args = args
	j.State = model.State(state)
	j.LastError = lastErr
	j.Result = result
	j.RunAt = time.Unix(0, runAt)
	j.CreatedAt = time.Unix(0, createdAt)
	j.UpdatedAt = time.Unix(0, updatedAt)
	return j, nil
}

const jobColumns = `id,queue,type,args,state,run_at,created_at,updated_at,attempts,max_attempts,last_error,result,priority`

// CreateJob inserts a new job. It validates first so unparseable args
// (and other malformed fields) are rejected before they reach the database
// and only surface later when a worker executes the job. This guards the
// non-HTTP ingestion paths (direct callers, schedule firing) that do not go
// through the HTTP request decoder.
func (s *Store) CreateJob(j *model.Job) error {
	if err := j.Validate(); err != nil {
		return fmt.Errorf("create job: %w", err)
	}
	now := time.Now()
	if j.CreatedAt.IsZero() {
		j.CreatedAt = now
	}
	j.UpdatedAt = now
	if j.MaxAttempts < 1 {
		j.MaxAttempts = 1
	}
	if j.State == "" {
		j.State = model.StatePending
	}
	if j.RunAt.IsZero() {
		j.RunAt = now
	}
	_, err := s.db.Exec(
		`INSERT INTO jobs(id,queue,type,args,state,run_at,created_at,updated_at,attempts,max_attempts,last_error,result,priority)
		 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		j.ID, j.Queue, j.Type, j.Args, string(j.State),
		j.RunAt.UnixNano(), j.CreatedAt.UnixNano(), j.UpdatedAt.UnixNano(),
		j.Attempts, j.MaxAttempts, j.LastError, j.Result, j.Priority,
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

// GetJob returns the job with the given id, or ErrNotFound.
func (s *Store) GetJob(id string) (*model.Job, error) {
	row := s.db.QueryRow(`SELECT `+jobColumns+` FROM jobs WHERE id=?`, id)
	j, err := s.scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// UpdateJob persists mutable fields of an existing job.
func (s *Store) UpdateJob(j *model.Job) error {
	j.UpdatedAt = time.Now()
	_, err := s.db.Exec(
		`UPDATE jobs SET queue=?, type=?, args=?, state=?, run_at=?, updated_at=?, attempts=?, max_attempts=?, last_error=?, result=?, priority=? WHERE id=?`,
		j.Queue, j.Type, j.Args, string(j.State),
		j.RunAt.UnixNano(), j.UpdatedAt.UnixNano(),
		j.Attempts, j.MaxAttempts, j.LastError, j.Result, j.Priority, j.ID,
	)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	return nil
}

// ScanDue returns up to limit jobs that are pending/scheduled and due now,
// ordered by priority then earliest run time.
func (s *Store) ScanDue(limit int, now time.Time) ([]*model.Job, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT `+jobColumns+` FROM jobs WHERE state IN ('pending','scheduled') AND run_at<=? ORDER BY priority DESC, run_at ASC, id ASC LIMIT ?`,
		now.UnixNano(), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("scan due: %w", err)
	}
	defer rows.Close()
	var out []*model.Job
	for rows.Next() {
		j, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan due row: %w", err)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// Claim atomically transitions a due job into running state. It returns false
// if the job is no longer claimable (already running/finished).
func (s *Store) Claim(id string) (bool, error) {
	res, err := s.db.Exec(
		`UPDATE jobs SET state='running', updated_at=? WHERE id=? AND state IN ('pending','scheduled')`,
		time.Now().UnixNano(), id,
	)
	if err != nil {
		return false, fmt.Errorf("claim job: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("claim rows: %w", err)
	}
	return n > 0, nil
}

// Succeed marks a job completed with its result payload.
func (s *Store) Succeed(id, result string) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET state='succeeded', result=?, updated_at=? WHERE id=?`,
		result, time.Now().UnixNano(), id,
	)
	if err != nil {
		return fmt.Errorf("succeed job: %w", err)
	}
	return nil
}

// Fail records a failed attempt. When willRetry is true the job returns to the
// pending pool with run_at set to nextRunAt; otherwise it becomes a dead
// letter. Either way the attempt counter is incremented.
func (s *Store) Fail(id, errMsg string, willRetry bool, nextRunAt time.Time) error {
	now := time.Now().UnixNano()
	if willRetry {
		_, err := s.db.Exec(
			`UPDATE jobs SET state='pending', attempts=attempts+1, last_error=?, run_at=?, updated_at=? WHERE id=?`,
			errMsg, nextRunAt.UnixNano(), now, id,
		)
		if err != nil {
			return fmt.Errorf("fail retry: %w", err)
		}
		return nil
	}
	_, err := s.db.Exec(
		`UPDATE jobs SET state='dead', attempts=attempts+1, last_error=?, updated_at=? WHERE id=?`,
		errMsg, now, id,
	)
	if err != nil {
		return fmt.Errorf("fail dead: %w", err)
	}
	return nil
}

// RecordAttempt persists a single execution attempt.
func (s *Store) RecordAttempt(a model.Attempt) error {
	_, err := s.db.Exec(
		`INSERT INTO attempts(job_id, idx, started_at, ended_at, error, duration_ms) VALUES(?,?,?,?,?,?)`,
		a.JobID, a.Index, a.StartedAt.UnixNano(), a.EndedAt.UnixNano(), a.Error, a.Duration.Milliseconds(),
	)
	if err != nil {
		return fmt.Errorf("record attempt: %w", err)
	}
	return nil
}

// ListAttempts returns the attempts recorded for a job in order.
func (s *Store) ListAttempts(jobID string) ([]model.Attempt, error) {
	rows, err := s.db.Query(
		`SELECT id, job_id, idx, started_at, ended_at, error, duration_ms FROM attempts WHERE job_id=? ORDER BY idx ASC`,
		jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()
	var out []model.Attempt
	for rows.Next() {
		var (
			id, idx, started, ended, dur int64
			jobID, errMsg                string
		)
		if err := rows.Scan(&id, &jobID, &idx, &started, &ended, &errMsg, &dur); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		out = append(out, model.Attempt{
			ID:        id,
			JobID:     jobID,
			Index:     int(idx),
			StartedAt: time.Unix(0, started),
			EndedAt:   time.Unix(0, ended),
			Error:     errMsg,
			Duration:  time.Duration(dur) * time.Millisecond,
		})
	}
	return out, rows.Err()
}

// ListJobs returns jobs matching the filter. When Limit>0 the result is
// paginated in memory (the dataset is small for the training workload).
func (s *Store) ListJobs(f ListFilter) ([]model.Job, error) {
	q := `SELECT ` + jobColumns + ` FROM jobs WHERE 1=1`
	args := []interface{}{}
	if f.State != "" {
		q += ` AND state=?`
		args = append(args, string(f.State))
	}
	if f.Queue != "" {
		q += ` AND queue=?`
		args = append(args, f.Queue)
	}
	q += ` ORDER BY priority DESC, run_at ASC, id ASC`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var all []model.Job
	for rows.Next() {
		j, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan list row: %w", err)
		}
		all = append(all, *j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if f.Limit <= 0 {
		return all, nil
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > len(all) {
		return []model.Job{}, nil
	}
	end := offset + f.Limit
	if end > len(all) {
		end = len(all)
	}
	return all[offset:end], nil
}

// DeleteJob removes a job and its attempts.
func (s *Store) DeleteJob(id string) error {
	if _, err := s.db.Exec(`DELETE FROM attempts WHERE job_id=?`, id); err != nil {
		return fmt.Errorf("delete attempts: %w", err)
	}
	res, err := s.db.Exec(`DELETE FROM jobs WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// Stats aggregates counts by state.
func (s *Store) Stats() (model.Stats, error) {
	st := model.Stats{ByState: map[model.State]int{}}
	row := s.db.QueryRow(`SELECT COUNT(*), COUNT(DISTINCT queue) FROM jobs`)
	if err := row.Scan(&st.Total, &st.Queues); err != nil {
		return st, fmt.Errorf("stats total: %w", err)
	}
	rows, err := s.db.Query(`SELECT state, COUNT(*) FROM jobs GROUP BY state`)
	if err != nil {
		return st, fmt.Errorf("stats by state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return st, err
		}
		st.ByState[model.State(state)] = n
	}
	row = s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE state='dead'`)
	if err := row.Scan(&st.DeadLetters); err != nil {
		return st, fmt.Errorf("stats dead: %w", err)
	}
	return st, nil
}

// QueueStats returns per-queue counts for a single queue.
func (s *Store) QueueStats(name string) (map[model.State]int, error) {
	out := map[model.State]int{}
	rows, err := s.db.Query(`SELECT state, COUNT(*) FROM jobs WHERE queue=? GROUP BY state`, name)
	if err != nil {
		return out, fmt.Errorf("queue stats: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return out, err
		}
		out[model.State(state)] = n
	}
	return out, nil
}

// SetPaused flips the paused flag of a queue.
func (s *Store) SetPaused(name string, paused bool) error {
	v := 0
	if paused {
		v = 1
	}
	if _, err := s.db.Exec(
		`INSERT INTO queues(name, paused) VALUES(?, ?) ON CONFLICT(name) DO UPDATE SET paused=excluded.paused`,
		name, v,
	); err != nil {
		return fmt.Errorf("set paused: %w", err)
	}
	return nil
}

// IsPaused reports the paused flag of a queue (false when unknown).
func (s *Store) IsPaused(name string) (bool, error) {
	row := s.db.QueryRow(`SELECT paused FROM queues WHERE name=?`, name)
	var v int
	if err := row.Scan(&v); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("is paused: %w", err)
	}
	return v == 1, nil
}

// DeadLetters returns jobs in the dead state.
func (s *Store) DeadLetters() ([]model.Job, error) {
	rows, err := s.db.Query(`SELECT ` + jobColumns + ` FROM jobs WHERE state='dead' ORDER BY updated_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("dead letters: %w", err)
	}
	defer rows.Close()
	var out []model.Job
	for rows.Next() {
		j, err := s.scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan dead: %w", err)
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// RequeueDead resets a dead job back to pending so it can be retried.
func (s *Store) RequeueDead(id string) error {
	res, err := s.db.Exec(
		`UPDATE jobs SET state='pending', run_at=strftime('%s','now')*1000000000, updated_at=? WHERE id=? AND state='dead'`,
		time.Now().UnixNano(), id,
	)
	if err != nil {
		return fmt.Errorf("requeue dead: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// CreateSchedule persists a recurring schedule.
func (s *Store) CreateSchedule(sc *model.Schedule) error {
	now := time.Now()
	if sc.CreatedAt.IsZero() {
		sc.CreatedAt = now
	}
	_, err := s.db.Exec(
		`INSERT INTO schedules(id, queue, type, args, interval_ms, enabled, last_run, max_attempts, priority, created_at)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		sc.ID, sc.Queue, sc.Type, sc.Args,
		sc.Interval.Milliseconds(), boolToInt(sc.Enabled), sc.LastRun.UnixNano(),
		sc.MaxAttempts, sc.Priority, sc.CreatedAt.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("insert schedule: %w", err)
	}
	return nil
}

// GetSchedule returns a schedule by id, or ErrNotFound.
func (s *Store) GetSchedule(id string) (*model.Schedule, error) {
	row := s.db.QueryRow(
		`SELECT id, queue, type, args, interval_ms, enabled, last_run, max_attempts, priority, created_at FROM schedules WHERE id=?`,
		id,
	)
	sc, err := s.scanSchedule(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get schedule: %w", err)
	}
	return sc, nil
}

func (s *Store) scanSchedule(row interface {
	Scan(...interface{}) error
}) (*model.Schedule, error) {
	sc := &model.Schedule{}
	var (
		intervalMs, lastRun, createdAt int64
		enabled                        int
		args                           string
	)
	if err := row.Scan(
		&sc.ID, &sc.Queue, &sc.Type, &args, &intervalMs, &enabled,
		&lastRun, &sc.MaxAttempts, &sc.Priority, &createdAt,
	); err != nil {
		return nil, err
	}
	sc.Args = args
	sc.Interval = time.Duration(intervalMs) * time.Millisecond
	sc.Enabled = enabled != 0
	sc.LastRun = time.Unix(0, lastRun)
	sc.CreatedAt = time.Unix(0, createdAt)
	return sc, nil
}

// ListSchedules returns all schedules ordered by id.
func (s *Store) ListSchedules() ([]model.Schedule, error) {
	rows, err := s.db.Query(
		`SELECT id, queue, type, args, interval_ms, enabled, last_run, max_attempts, priority, created_at FROM schedules ORDER BY id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()
	var out []model.Schedule
	for rows.Next() {
		sc, err := s.scanSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		out = append(out, *sc)
	}
	return out, rows.Err()
}

// DeleteSchedule removes a schedule.
func (s *Store) DeleteSchedule(id string) error {
	res, err := s.db.Exec(`DELETE FROM schedules WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// SetScheduleEnabled flips the enabled flag.
func (s *Store) SetScheduleEnabled(id string, enabled bool) error {
	res, err := s.db.Exec(`UPDATE schedules SET enabled=? WHERE id=?`, boolToInt(enabled), id)
	if err != nil {
		return fmt.Errorf("set schedule enabled: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// TouchSchedule records that a schedule fired at now.
func (s *Store) TouchSchedule(id string, now time.Time) error {
	_, err := s.db.Exec(`UPDATE schedules SET last_run=? WHERE id=?`, now.UnixNano(), id)
	if err != nil {
		return fmt.Errorf("touch schedule: %w", err)
	}
	return nil
}

// DueSchedules returns schedules whose next fire time is at or before now.
func (s *Store) DueSchedules(now time.Time) ([]model.Schedule, error) {
	rows, err := s.db.Query(
		`SELECT id, queue, type, args, interval_ms, enabled, last_run, max_attempts, priority, created_at
		 FROM schedules WHERE enabled=1 AND (last_run=0 OR last_run + interval_ms*1000000 <= ?) ORDER BY id ASC`,
		now.UnixNano(),
	)
	if err != nil {
		return nil, fmt.Errorf("due schedules: %w", err)
	}
	defer rows.Close()
	var out []model.Schedule
	for rows.Next() {
		sc, err := s.scanSchedule(rows)
		if err != nil {
			return nil, fmt.Errorf("scan due schedule: %w", err)
		}
		out = append(out, *sc)
	}
	return out, rows.Err()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
