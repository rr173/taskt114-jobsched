package main

import "testing"

// TestRunSmokeTest exercises the self-contained smoke path used by --smoke-test.
func TestRunSmokeTest(t *testing.T) {
	if err := runSmokeTest(); err != nil {
		t.Fatalf("smoke test failed: %v", err)
	}
}
