package store

import (
	"testing"
	"time"
)

func TestBug07_CreateJobRejectsInvalidJSONArgs(t *testing.T) {
	s := openTest(t)
	j := newJob("bad-job-args", "q", "noop", time.Now())
	j.Args = `{"broken"`
	if err := s.CreateJob(j); err == nil {
		t.Fatal("invalid JSON job payload was persisted")
	}
}
