// Package clock provides a swappable time source so tests can be
// deterministic.
package clock

import "time"

// Clock abstracts time operations used by the scheduler.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// SystemClock uses the real wall clock.
type SystemClock struct{}

// Now returns the current local time.
func (SystemClock) Now() time.Time { return time.Now() }

// After behaves like time.After.
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// New returns the default system clock.
func New() Clock { return SystemClock{} }

// FixedClock returns a clock pinned to a constant time, useful for tests.
type FixedClock struct {
	now time.Time
}

// NewFixed returns a fixed clock at t.
func NewFixed(t time.Time) *FixedClock { return &FixedClock{now: t} }

// Now returns the pinned time.
func (f *FixedClock) Now() time.Time { return f.now }

// After returns a channel that never fires; fixed clocks do not advance.
func (f *FixedClock) After(d time.Duration) <-chan time.Time {
	return make(chan time.Time)
}
