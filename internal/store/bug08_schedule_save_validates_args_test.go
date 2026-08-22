package store

import (
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

// newSchedule builds a minimal but otherwise-valid schedule so each test only
// varies the field under inspection.
func newSchedule(id string) *model.Schedule {
	return &model.Schedule{
		ID:          id,
		Queue:       "q",
		Type:        "noop",
		Args:        "{}",
		Interval:    time.Minute,
		Enabled:     true,
		MaxAttempts: 3,
	}
}

// TestBug08_CreateScheduleRejectsInvalidJSONArgs is the regression test for the
// bug where a schedule written directly to the store layer bypassed argument
// validation: a malformed Args payload was persisted and only surfaced when the
// schedule later fired and handed the broken payload to a worker. CreateSchedule
// must now run the same validation as the HTTP path so the save stage rejects
// it up front.
func TestBug08_CreateScheduleRejectsInvalidJSONArgs(t *testing.T) {
	s := openTest(t)
	sc := newSchedule("bad-sched-args")
	sc.Args = `{"broken"`
	if err := s.CreateSchedule(sc); err == nil {
		t.Fatal("invalid JSON schedule args were persisted")
	}
}

// TestBug08_CreateSchedulePersistsValidArgs confirms a well-formed payload still
// round-trips through the store, guarding against an over-eager validator that
// would reject legitimate JSON.
func TestBug08_CreateSchedulePersistsValidArgs(t *testing.T) {
	s := openTest(t)
	sc := newSchedule("good-sched-args")
	sc.Args = `{"k":"v"}`
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("valid schedule args were rejected: %v", err)
	}
	got, err := s.GetSchedule("good-sched-args")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if got.Args != `{"k":"v"}` {
		t.Fatalf("expected args to round-trip, got %q", got.Args)
	}
}

// TestBug08_CreateScheduleRejectsEmptyRequiredFields ensures the store-layer
// validation also catches other malformed schedule fields, not just args, so
// direct ingestion cannot land an incomplete schedule that the scheduler would
// choke on later.
func TestBug08_CreateScheduleRejectsEmptyRequiredFields(t *testing.T) {
	s := openTest(t)
	sc := newSchedule("no-type")
	sc.Type = ""
	if err := s.CreateSchedule(sc); err == nil {
		t.Fatal("schedule with empty type was persisted")
	}
}

// TestBug08_CreateScheduleDefaultsMaxAttempts verifies the validator still
// applies the documented default for max attempts when none is supplied, so the
// persistence layer keeps the convenience behaviour the HTTP path relies on.
func TestBug08_CreateScheduleDefaultsMaxAttempts(t *testing.T) {
	s := openTest(t)
	sc := newSchedule("default-attempts")
	sc.MaxAttempts = 0
	if err := s.CreateSchedule(sc); err != nil {
		t.Fatalf("create schedule: %v", err)
	}
	got, err := s.GetSchedule("default-attempts")
	if err != nil {
		t.Fatalf("get schedule: %v", err)
	}
	if got.MaxAttempts != 3 {
		t.Fatalf("expected default max attempts 3, got %d", got.MaxAttempts)
	}
}
