// Package model defines the durable job scheduler domain types.
package model

import (
	"fmt"
	"time"
)

// State enumerates the lifecycle of a job.
type State string

const (
	// StatePending means the job is ready to run as soon as a worker claims it.
	StatePending State = "pending"
	// StateScheduled means the job is waiting until its RunAt time.
	StateScheduled State = "scheduled"
	// StateRunning means a worker is currently executing the job.
	StateRunning State = "running"
	// StateSucceeded means the handler returned without error.
	StateSucceeded State = "succeeded"
	// StateDead means attempts were exhausted and the job is a dead letter.
	StateDead State = "dead"
	// StateCancelled means an operator cancelled the job before completion.
	StateCancelled State = "cancelled"
)

// ValidStates is the full set of states a job may occupy.
var ValidStates = map[State]bool{
	StatePending:   true,
	StateScheduled: true,
	StateRunning:   true,
	StateSucceeded: true,
	StateDead:      true,
	StateCancelled: true,
}

// Job is a single unit of work persisted in the store.
type Job struct {
	ID          string    `json:"id"`
	Queue       string    `json:"queue"`
	Type        string    `json:"type"`
	Args        string    `json:"args"` // JSON payload handed to the handler
	State       State     `json:"state"`
	RunAt       time.Time `json:"run_at"` // earliest time the job may execute
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	LastError   string    `json:"last_error"`
	Result      string    `json:"result"`
	Priority    int       `json:"priority"` // higher runs first among due jobs
}

// Attempt records a single handler execution.
type Attempt struct {
	ID        int64         `json:"id"`
	JobID     string        `json:"job_id"`
	Index     int           `json:"index"`
	StartedAt time.Time     `json:"started_at"`
	EndedAt   time.Time     `json:"ended_at"`
	Error     string        `json:"error"`
	Duration  time.Duration `json:"duration_ms"`
}

// Validate reports whether the job carries the minimum information required to
// be enqueued.
func (j *Job) Validate() error {
	if j.ID == "" {
		return fmt.Errorf("job id must not be empty")
	}
	if j.Queue == "" {
		return fmt.Errorf("job queue must not be empty")
	}
	if j.Type == "" {
		return fmt.Errorf("job type must not be empty")
	}
	if !ValidStates[j.State] {
		return fmt.Errorf("invalid job state %q", j.State)
	}
	if j.MaxAttempts < 1 {
		return fmt.Errorf("max attempts must be >= 1")
	}
	return nil
}

// IsDue reports whether the job may be claimed at the given time.
func (j *Job) IsDue(now time.Time) bool {
	return !j.RunAt.After(now)
}

// IsTerminal reports whether the job has reached a stable state.
func (j *Job) IsTerminal() bool {
	switch j.State {
	case StateSucceeded, StateDead, StateCancelled:
		return true
	default:
		return false
	}
}

// DeadLetter is a dead job surfaced for operator inspection.
type DeadLetter struct {
	Job      Job
	FailedAt time.Time
}

// Stats summarises queue activity.
type Stats struct {
	Total       int
	ByState     map[State]int
	Queues      int
	DeadLetters int
}
