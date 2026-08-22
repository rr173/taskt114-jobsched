package model

import (
	"testing"
	"time"
)

func TestScheduleValidateRejectsMalformedArgs(t *testing.T) {
	sc := Schedule{ID: "s", Queue: "q", Type: "t", Interval: time.Minute, Args: `{"broken`}
	if err := sc.Validate(); err == nil {
		t.Fatal("expected error for malformed schedule args")
	}
}

func TestScheduleValidateAcceptsValidArgs(t *testing.T) {
	sc := Schedule{ID: "s", Queue: "q", Type: "t", Interval: time.Minute, Args: `{"k":"v"}`}
	if err := sc.Validate(); err != nil {
		t.Fatalf("expected valid schedule args to pass, got %v", err)
	}
}

func TestScheduleValidateAcceptsEmptyArgs(t *testing.T) {
	// Empty args are tolerated by the validator; callers default to "{}".
	sc := Schedule{ID: "s", Queue: "q", Type: "t", Interval: time.Minute, Args: ""}
	if err := sc.Validate(); err != nil {
		t.Fatalf("expected empty schedule args to pass, got %v", err)
	}
}
