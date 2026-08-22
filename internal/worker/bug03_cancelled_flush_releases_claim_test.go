package worker

import (
	"context"
	"testing"
	"time"

	"taskt114-jobsched/internal/model"
)

func TestBug03_CancelledFlushReleasesClaim(t *testing.T) {
	s, p := newPool(t)
	j := &model.Job{ID: "cancelled-flush", Queue: "q", Type: "noop", Args: "{}", State: model.StatePending, RunAt: time.Now(), MaxAttempts: 1}
	if err := s.CreateJob(j); err != nil {
		t.Fatalf("create job: %v", err)
	}
	for i := 0; i < cap(p.sem); i++ {
		p.sem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(p.sem); i++ {
			<-p.sem
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Flush(ctx); err == nil {
		t.Fatal("expected cancelled flush to return an error")
	}
	got, err := s.GetJob(j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if got.State != model.StatePending {
		t.Fatalf("cancelled flush stranded job in %q", got.State)
	}
}
