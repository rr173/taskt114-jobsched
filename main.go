// Command taskt114-jobsched starts a durable delayed job scheduler service, or
// runs a self-contained smoke test when invoked with --smoke-test.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"taskt114-jobsched/internal/api"
	"taskt114-jobsched/internal/clock"
	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
	"taskt114-jobsched/internal/worker"
)

func main() {
	var (
		addr       string
		dbPath     string
		adminToken string
		smokeTest  bool
	)
	flag.StringVar(&addr, "addr", ":8080", "HTTP 监听地址")
	flag.StringVar(&dbPath, "db", "jobsched.db", "SQLite 数据库文件路径")
	flag.StringVar(&adminToken, "admin-token", "admin-secret", "管理接口所需的 X-Admin-Token")
	flag.BoolVar(&smokeTest, "smoke-test", false, "执行内置自检后退出（不启动 HTTP 服务）")
	flag.Parse()

	if smokeTest {
		if err := runSmokeTest(); err != nil {
			fmt.Fprintln(os.Stderr, "SMOKE TEST FAILED:", err)
			os.Exit(1)
		}
		fmt.Println("SMOKE TEST PASSED")
		os.Exit(0)
	}

	absDB, err := filepath.Abs(dbPath)
	if err != nil {
		log.Fatalf("resolve db path: %v", err)
	}
	st, err := store.Open(absDB)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	pool := worker.New(st, clock.New())
	if _, err := worker.RecoverInterrupted(st, time.Now()); err != nil {
		log.Fatalf("recover interrupted jobs: %v", err)
	}
	registerBuiltinHandlers(pool)

	srv := &http.Server{
		Addr:    addr,
		Handler: api.New(st, pool, api.Config{AdminToken: adminToken}).Handler(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx, 500*time.Millisecond)
	pool.RunRecurring(ctx, 1*time.Second)

	log.Printf("taskt114-jobsched listening on %s (db=%s)", addr, absDB)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}

func registerBuiltinHandlers(pool *worker.Pool) {
	pool.RegisterHandler("noop", func(ctx context.Context, j *model.Job) (string, error) {
		return "", nil
	})
	pool.RegisterHandler("echo", func(ctx context.Context, j *model.Job) (string, error) {
		return j.Args, nil
	})
	pool.RegisterHandler("fail", func(ctx context.Context, j *model.Job) (string, error) {
		return "", fmt.Errorf("intentional failure for job %s", j.ID)
	})
	pool.RegisterHandler("slow", func(ctx context.Context, j *model.Job) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
			return "done", nil
		}
	})
}

func runSmokeTest() error {
	dir, err := os.MkdirTemp("", "jobsched-smoke-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "smoke.db")

	st, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer st.Close()

	pool := worker.New(st, clock.New())
	registerBuiltinHandlers(pool)

	specs := []struct {
		typ   string
		queue string
		args  string
	}{
		{"echo", "q1", `{"hello":"world"}`},
		{"noop", "q1", `{}`},
		{"echo", "q2", `{"n":2}`},
		{"noop", "q2", `{}`},
		{"echo", "q3", `{"x":1}`},
	}
	for i, sp := range specs {
		j := &model.Job{
			ID:          fmt.Sprintf("smoke-%d", i),
			Queue:       sp.queue,
			Type:        sp.typ,
			Args:        sp.args,
			State:       model.StatePending,
			RunAt:       time.Now(),
			MaxAttempts: 3,
		}
		if err := st.CreateJob(j); err != nil {
			return fmt.Errorf("create job: %w", err)
		}
	}

	ctx := context.Background()
	if err := pool.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	pool.Wait()

	for i := range specs {
		j, err := st.GetJob(fmt.Sprintf("smoke-%d", i))
		if err != nil {
			return fmt.Errorf("get smoke-%d: %w", i, err)
		}
		if j.State != model.StateSucceeded {
			return fmt.Errorf("job smoke-%d expected succeeded, got %s", i, j.State)
		}
	}
	stats, err := st.Stats()
	if err != nil {
		return fmt.Errorf("stats: %w", err)
	}
	if stats.Total != len(specs) {
		return fmt.Errorf("expected %d jobs, got %d", len(specs), stats.Total)
	}
	return nil
}
