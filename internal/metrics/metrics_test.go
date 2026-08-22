package metrics

import (
	"testing"
)

func TestCounters(t *testing.T) {
	c := New()
	c.IncEnqueued()
	c.IncEnqueued()
	c.IncSucceeded()
	c.IncFailed()
	c.IncRetried()
	c.IncDead()

	snap := c.Read()
	if snap.Enqueued != 2 {
		t.Fatalf("enqueued: got %d want 2", snap.Enqueued)
	}
	if snap.Succeeded != 1 || snap.Failed != 1 || snap.Retried != 1 || snap.Dead != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestObserveState(t *testing.T) {
	c := New()
	c.ObserveState(map[string]int{"pending": 3, "succeeded": 7})
	snap := c.Read()
	if snap.ByState["pending"] != 3 || snap.ByState["succeeded"] != 7 {
		t.Fatalf("unexpected by_state: %+v", snap.ByState)
	}
}
