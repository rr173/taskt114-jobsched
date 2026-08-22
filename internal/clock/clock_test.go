package clock

import (
	"testing"
	"time"
)

func TestSystemClockNow(t *testing.T) {
	c := New()
	if d := time.Since(c.Now()); d < 0 || d > time.Second {
		t.Fatalf("system clock now out of range: %v", d)
	}
}

func TestFixedClock(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := NewFixed(base)
	if !c.Now().Equal(base) {
		t.Fatalf("fixed clock now = %v, want %v", c.Now(), base)
	}
	select {
	case <-c.After(time.Hour):
		t.Fatal("fixed clock After should not fire")
	default:
	}
}
