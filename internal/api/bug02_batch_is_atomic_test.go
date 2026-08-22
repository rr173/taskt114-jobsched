package api

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestBug02_BatchCreateDoesNotLeavePartialJobs(t *testing.T) {
	_, _, h := newServer(t)
	rec := do(t, h, "POST", "/jobs/batch", `{"jobs":[{"id":"batch-duplicate","queue":"q","type":"noop"},{"id":"batch-duplicate","queue":"q","type":"noop"}]}`)
	if rec.Code == http.StatusCreated {
		t.Fatalf("duplicate batch unexpectedly succeeded: %s", rec.Body.String())
	}

	rec = do(t, h, "GET", "/jobs", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list jobs status %d: %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode jobs: %v", err)
	}
	if out.Count != 0 {
		t.Fatalf("failed batch left %d persisted jobs", out.Count)
	}
}
