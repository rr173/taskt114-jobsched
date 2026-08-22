package model

import (
	"testing"
	"time"
)

func TestJobValidate(t *testing.T) {
	valid := Job{ID: "a", Queue: "q", Type: "t", State: StatePending, MaxAttempts: 3}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid job, got %v", err)
	}
	missingID := Job{Queue: "q", Type: "t", State: StatePending}
	if err := missingID.Validate(); err == nil {
		t.Fatal("expected error for missing id")
	}
	zeroAttempts := Job{ID: "a", Queue: "q", Type: "t", State: StatePending, MaxAttempts: 0}
	if err := zeroAttempts.Validate(); err == nil {
		t.Fatal("expected error for zero max attempts")
	}
}

func TestJobIsDue(t *testing.T) {
	now := time.Now()
	past := Job{RunAt: now.Add(-time.Hour)}
	future := Job{RunAt: now.Add(time.Hour)}
	if !past.IsDue(now) {
		t.Fatal("past job should be due")
	}
	if future.IsDue(now) {
		t.Fatal("future job should not be due")
	}
}

func TestJobIsTerminal(t *testing.T) {
	for _, s := range []State{StateSucceeded, StateDead, StateCancelled} {
		j := Job{State: s}
		if !j.IsTerminal() {
			t.Fatalf("state %s should be terminal", s)
		}
	}
	running := Job{State: StateRunning}
	if running.IsTerminal() {
		t.Fatal("running should not be terminal")
	}
}
