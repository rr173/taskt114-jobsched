// Package worker runs due jobs through registered handlers with bounded
// concurrency and durable retry/dead-letter handling.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"taskt114-jobsched/internal/backoff"
	"taskt114-jobsched/internal/clock"
	"taskt114-jobsched/internal/metrics"
	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
)

// Handler executes a single job and returns its result payload.
type Handler func(ctx context.Context, j *model.Job) (string, error)

// Pool dispatches due jobs to handlers.
type Pool struct {
	store    *store.Store
	clk      clock.Clock
	mu       sync.Mutex
	handlers map[string]Handler
	active   map[string]bool
	cancels  map[string]context.CancelFunc
	sem      chan struct{}
	backoff  time.Duration
	strategy backoff.Strategy
	metrics  *metrics.Counters
	scanSize int
	wg       sync.WaitGroup
}

// New builds a pool backed by the given store and clock.
func New(s *store.Store, clk clock.Clock) *Pool {
	return &Pool{
		store:    s,
		clk:      clk,
		handlers: map[string]Handler{},
		active:   map[string]bool{},
		cancels:  map[string]context.CancelFunc{},
		sem:      make(chan struct{}, 16),
		backoff:  time.Second,
		strategy: backoff.New("exponential", time.Second),
		metrics:  metrics.New(),
		scanSize: 200,
	}
}

// RegisterHandler binds a job type to an execution function.
func (p *Pool) RegisterHandler(typ string, h Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[typ] = h
}

func (p *Pool) handlerFor(typ string) (Handler, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.handlers[typ]
	return h, ok
}

// Cancel requests cancellation of a running job's handler context.
func (p *Pool) Cancel(id string) {
	p.mu.Lock()
	c, ok := p.cancels[id]
	p.mu.Unlock()
	if ok {
		c()
	}
}

// Flush claims and dispatches every job that is due right now. It is safe to
// call concurrently; each job is executed at most once per successful claim.
func (p *Pool) Flush(ctx context.Context) error {
	due, err := p.store.ScanDue(p.scanSize, p.clk.Now())
	if err != nil {
		return err
	}
	for _, j := range due {
		if paused, perr := p.store.IsPaused(j.Queue); perr == nil && paused {
			continue
		}
		claimed, cerr := p.store.Claim(j.ID)
		if cerr != nil {
			return cerr
		}
		if !claimed {
			continue
		}
		p.mu.Lock()
		if p.active[j.ID] {
			p.mu.Unlock()
			continue
		}
		p.active[j.ID] = true
		p.mu.Unlock()

		select {
		case p.sem <- struct{}{}:
		case <-ctx.Done():
			p.mu.Lock()
			delete(p.active, j.ID)
			p.mu.Unlock()
			return ctx.Err()
		}
		p.wg.Add(1)
		go func(j *model.Job) {
			defer p.wg.Done()
			defer func() { <-p.sem }()
			defer func() {
				p.mu.Lock()
				delete(p.active, j.ID)
				delete(p.cancels, j.ID)
				p.mu.Unlock()
			}()
			p.executeJob(ctx, j)
		}(j)
	}
	return nil
}

func (p *Pool) executeJob(parent context.Context, j *model.Job) {
	jobCtx, cancel := context.WithCancel(parent)
	p.mu.Lock()
	p.cancels[j.ID] = cancel
	p.mu.Unlock()

	started := p.clk.Now()
	h, ok := p.handlerFor(j.Type)
	if !ok {
		p.finish(j, "", fmt.Errorf("no handler registered for type %q", j.Type), started)
		return
	}
	result, herr := h(jobCtx, j)
	p.finish(j, result, herr, started)
}

func (p *Pool) finish(j *model.Job, result string, herr error, started time.Time) {
	ended := p.clk.Now()
	attempt := model.Attempt{
		JobID:     j.ID,
		Index:     j.Attempts,
		StartedAt: started,
		EndedAt:   ended,
		Duration:  ended.Sub(started),
	}
	if herr != nil {
		attempt.Error = herr.Error()
	}
	_ = p.store.RecordAttempt(attempt)

	if herr == nil {
		p.metrics.IncSucceeded()
		_ = p.store.Succeed(j.ID, result)
		return
	}
	p.metrics.IncFailed()
	next, willRetry := p.nextRun(j)
	if willRetry {
		p.metrics.IncRetried()
	} else {
		p.metrics.IncDead()
	}
	_ = p.store.Fail(j.ID, herr.Error(), willRetry, next)
}

func (p *Pool) nextRun(j *model.Job) (time.Time, bool) {
	if !j.Retryable() {
		return time.Time{}, false
	}
	if j.Attempts+1 >= j.MaxAttempts {
		return time.Time{}, false
	}
	delay := backoff.Window(p.strategy, j.Attempts, p.backoff, p.backoff*16)
	return p.clk.Now().Add(delay), true
}

// SetBackoffStrategy overrides the retry backoff computation.
func (p *Pool) SetBackoffStrategy(s backoff.Strategy) {
	p.mu.Lock()
	p.strategy = s
	p.mu.Unlock()
}

// Metrics returns the live activity counters.
func (p *Pool) Metrics() *metrics.Counters { return p.metrics }

// RunRecurring fires due recurring schedules on a ticker until ctx is done.
func (p *Pool) RunRecurring(ctx context.Context, interval time.Duration) {
	go func() {
		t := p.clk.After(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t:
				p.fireDueSchedules(ctx)
				t = p.clk.After(interval)
			}
		}
	}()
}

func (p *Pool) fireDueSchedules(ctx context.Context) {
	due, err := p.store.DueSchedules(p.clk.Now())
	if err != nil {
		return
	}
	for _, sc := range due {
		select {
		case <-ctx.Done():
			return
		default:
		}
		runs := sc.MissedRuns(p.clk.Now())
		if runs < 1 {
			runs = 1
		}
		if runs > 10 {
			runs = 10
		}
		runAt := sc.NextRun(p.clk.Now())
		for i := 0; i < runs; i++ {
			j := &model.Job{
				ID:          fmt.Sprintf("sched-%s-%d", sc.ID, runAt.UnixNano()),
				Queue:       sc.Queue,
				Type:        sc.Type,
				Args:        sc.Args,
				State:       model.StatePending,
				RunAt:       runAt,
				MaxAttempts: sc.MaxAttempts,
				Priority:    sc.Priority,
			}
			if err := j.Validate(); err != nil {
				// A schedule with unparseable args must not spawn jobs that a
				// worker can only fail later; skip this firing and touch the
				// schedule so it does not busy-loop.
				_ = p.store.TouchSchedule(sc.ID, runAt)
				break
			}
			if err := p.store.CreateJob(j); err != nil {
				break
			}
			p.metrics.IncEnqueued()
			_ = p.store.TouchSchedule(sc.ID, runAt)
			runAt = runAt.Add(sc.Interval)
		}
	}
}

// Wait blocks until all in-flight executions finish.
func (p *Pool) Wait() { p.wg.Wait() }

// Active returns the number of jobs currently being executed.
func (p *Pool) Active() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.active)
}

// Start runs Flush on a ticker until ctx is cancelled.
func (p *Pool) Start(ctx context.Context, interval time.Duration) {
	go func() {
		t := p.clk.After(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t:
				_ = p.Flush(ctx)
				t = p.clk.After(interval)
			}
		}
	}()
}
