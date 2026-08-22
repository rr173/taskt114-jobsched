// Package backoff computes the delay before a retried job may run again.
package backoff

import (
	"math"
	"time"
)

// Strategy decides how long to wait before the n-th retry (0-indexed attempt).
type Strategy interface {
	// Next returns the backoff duration for the given attempt count.
	Next(attempt int, base time.Duration) time.Duration
}

// Constant waits a fixed interval regardless of attempt.
type Constant struct {
	Interval time.Duration
}

// Next implements Strategy.
func (c Constant) Next(attempt int, base time.Duration) time.Duration {
	if c.Interval > 0 {
		return c.Interval
	}
	return base
}

// Linear increases the delay by a constant step per attempt.
type Linear struct {
	Step time.Duration
}

// Next implements Strategy.
func (l Linear) Next(attempt int, base time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	return base + time.Duration(attempt)*l.Step
}

// Exponential grows the delay by factor each attempt, capped at Max.
type Exponential struct {
	Factor float64
	Max    time.Duration
}

// Next implements Strategy.
func (e Exponential) Next(attempt int, base time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if e.Factor <= 1 {
		return base
	}
	d := time.Duration(float64(base) * math.Pow(e.Factor, float64(attempt)))
	if e.Max > 0 && d > e.Max {
		return e.Max
	}
	return d
}

// New returns a strategy by name. Unknown kinds fall back to exponential.
func New(kind string, base time.Duration) Strategy {
	switch kind {
	case "constant":
		return Constant{Interval: base}
	case "linear":
		return Linear{Step: base / 2}
	case "exponential", "":
		return Exponential{Factor: 2, Max: base * 16}
	default:
		return Exponential{Factor: 2, Max: base * 16}
	}
}
