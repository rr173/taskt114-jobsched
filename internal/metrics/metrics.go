// Package metrics tracks scheduler activity counters in memory.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Counters records aggregate scheduler activity.
type Counters struct {
	mu        sync.Mutex
	enqueued  int64
	succeeded int64
	failed    int64
	retried   int64
	dead      int64
	byState   map[string]int64
}

// New returns an empty Counters.
func New() *Counters {
	return &Counters{byState: map[string]int64{}}
}

// IncEnqueued records a newly created job.
func (c *Counters) IncEnqueued() { atomic.AddInt64(&c.enqueued, 1) }

// IncSucceeded records a completed job.
func (c *Counters) IncSucceeded() { atomic.AddInt64(&c.succeeded, 1) }

// IncFailed records a failed attempt.
func (c *Counters) IncFailed() { atomic.AddInt64(&c.failed, 1) }

// IncRetried records a retry that returned the job to the pending pool.
func (c *Counters) IncRetried() { atomic.AddInt64(&c.retried, 1) }

// IncDead records a job that exhausted its attempts.
func (c *Counters) IncDead() { atomic.AddInt64(&c.dead, 1) }

// ObserveState records the latest known distribution of job states.
func (c *Counters) ObserveState(dist map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byState = make(map[string]int64, len(dist))
	for k, v := range dist {
		c.byState[k] = int64(v)
	}
}

// Snapshot is an immutable view of the counters.
type Snapshot struct {
	Enqueued  int64            `json:"enqueued"`
	Succeeded int64            `json:"succeeded"`
	Failed    int64            `json:"failed"`
	Retried   int64            `json:"retried"`
	Dead      int64            `json:"dead"`
	ByState   map[string]int64 `json:"by_state"`
	Captured  time.Time        `json:"captured_at"`
}

// Read returns a consistent snapshot.
func (c *Counters) Read() Snapshot {
	c.mu.Lock()
	byState := make(map[string]int64, len(c.byState))
	for k, v := range c.byState {
		byState[k] = v
	}
	c.mu.Unlock()
	return Snapshot{
		Enqueued:  atomic.LoadInt64(&c.enqueued),
		Succeeded: atomic.LoadInt64(&c.succeeded),
		Failed:    atomic.LoadInt64(&c.failed),
		Retried:   atomic.LoadInt64(&c.retried),
		Dead:      atomic.LoadInt64(&c.dead),
		ByState:   byState,
		Captured:  time.Now(),
	}
}
