// Package api exposes the job scheduler over HTTP.
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"taskt114-jobsched/internal/metrics"
	"taskt114-jobsched/internal/model"
	"taskt114-jobsched/internal/store"
	"taskt114-jobsched/internal/worker"
)

// Config configures the HTTP API.
type Config struct {
	// AdminToken, when non-empty, is required on management endpoints via the
	// X-Admin-Token header.
	AdminToken string
}

// Server bundles dependencies behind the HTTP handler.
type Server struct {
	store   *store.Store
	pool    *worker.Pool
	config  Config
	metrics *metrics.Counters
}

// New builds the API server.
func New(s *store.Store, p *worker.Pool, cfg Config) *Server {
	return &Server{store: s, pool: p, config: cfg, metrics: p.Metrics()}
}

// Handler returns the configured http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /jobs", s.createJob)
	mux.HandleFunc("GET /jobs", s.listJobs)
	mux.HandleFunc("POST /jobs/batch", s.batchJobs)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("DELETE /jobs/{id}", s.deleteJob)
	mux.HandleFunc("POST /jobs/{id}/retry", s.retryJob)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancelJob)
	mux.HandleFunc("POST /jobs/{id}/requeue", s.requeueJob)
	mux.HandleFunc("GET /jobs/{id}/attempts", s.jobAttempts)

	mux.HandleFunc("GET /queues", s.listQueues)
	mux.HandleFunc("GET /queues/{name}", s.queueDetail)
	mux.HandleFunc("GET /queues/{name}/report", s.queueReport)
	mux.HandleFunc("POST /queues/{name}/pause", s.pauseQueue)
	mux.HandleFunc("POST /queues/{name}/resume", s.resumeQueue)

	mux.HandleFunc("GET /stats", s.stats)
	mux.HandleFunc("GET /maintenance", s.maintenance)
	mux.HandleFunc("GET /stats/queue/{name}", s.queueStats)

	mux.HandleFunc("GET /deadletters", s.deadLetters)
	mux.HandleFunc("GET /deadletters/{id}", s.getDead)
	mux.HandleFunc("POST /deadletters/{id}/requeue", s.requeueDead)
	mux.HandleFunc("DELETE /deadletters/{id}", s.deleteDead)

	mux.HandleFunc("GET /workers", s.workers)
	mux.HandleFunc("POST /flush", s.flush)
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /metrics", s.metricsView)

	mux.HandleFunc("POST /schedules", s.createSchedule)
	mux.HandleFunc("GET /schedules", s.listSchedules)
	mux.HandleFunc("GET /schedules/{id}", s.getSchedule)
	mux.HandleFunc("DELETE /schedules/{id}", s.deleteSchedule)
	mux.HandleFunc("POST /schedules/{id}/enable", s.enableSchedule)
	mux.HandleFunc("POST /schedules/{id}/disable", s.disableSchedule)

	return mux
}

func (s *Server) requireAdmin(r *http.Request) bool {
	if s.config.AdminToken == "" {
		return true
	}
	return r.Header.Get("X-Admin-Token") == s.config.AdminToken
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

type jobRequest struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	Type        string          `json:"type"`
	Args        json.RawMessage `json:"args"`
	RunAt       string          `json:"run_at"`
	MaxAttempts int             `json:"max_attempts"`
	Priority    int             `json:"priority"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req jobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Queue == "" {
		req.Queue = "default"
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}
	runAt := time.Now()
	if req.RunAt != "" {
		parsed, err := time.Parse(time.RFC3339, req.RunAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid run_at: "+err.Error())
			return
		}
		runAt = parsed
	}
	args := "{}"
	if len(req.Args) > 0 {
		args = string(req.Args)
	}
	id := req.ID
	if id == "" {
		id = newID()
	}
	j := &model.Job{
		ID:          id,
		Queue:       req.Queue,
		Type:        req.Type,
		Args:        args,
		State:       model.StatePending,
		RunAt:       runAt,
		Attempts:    0,
		MaxAttempts: req.MaxAttempts,
		Priority:    req.Priority,
	}
	if err := j.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.CreateJob(j); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, j)
}

type batchRequest struct {
	Jobs []jobRequest `json:"jobs"`
}

func (s *Server) batchJobs(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	ids := make([]string, 0, len(req.Jobs))
	for _, jr := range req.Jobs {
		if jr.Queue == "" {
			jr.Queue = "default"
		}
		if jr.MaxAttempts <= 0 {
			jr.MaxAttempts = 3
		}
		args := "{}"
		if len(jr.Args) > 0 {
			args = string(jr.Args)
		}
		runAt := time.Now()
		if jr.RunAt != "" {
			if parsed, err := time.Parse(time.RFC3339, jr.RunAt); err == nil {
				runAt = parsed
			}
		}
		id := jr.ID
		if id == "" {
			id = newID()
		}
		j := &model.Job{
			ID:          id,
			Queue:       jr.Queue,
			Type:        jr.Type,
			Args:        args,
			State:       model.StatePending,
			RunAt:       runAt,
			MaxAttempts: jr.MaxAttempts,
			Priority:    jr.Priority,
		}
		if err := j.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, "job "+id+": "+err.Error())
			return
		}
		if err := s.store.CreateJob(j); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		ids = append(ids, id)
	}
	writeJSON(w, http.StatusCreated, map[string]interface{}{"ids": ids, "count": len(ids)})
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListFilter{}
	if v := q.Get("state"); v != "" {
		f.State = model.State(v)
	}
	if v := q.Get("queue"); v != "" {
		f.Queue = v
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Offset = n
		}
	}
	jobs, err := s.store.ListJobs(f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"jobs": jobs, "count": len(jobs)})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.GetJob(id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(r) {
		writeError(w, http.StatusForbidden, "admin token required")
		return
	}
	id := r.PathValue("id")
	if err := s.store.DeleteJob(id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.GetJob(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if j.State == model.StateRunning {
		writeError(w, http.StatusConflict, "job is running")
		return
	}
	if !model.CanRetry(j.State) {
		writeError(w, http.StatusConflict, "job cannot be retried from state "+string(j.State))
		return
	}
	j.State = model.StatePending
	j.RunAt = time.Now()
	j.LastError = ""
	if err := s.store.UpdateJob(j); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.GetJob(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if !model.CanCancel(j.State) {
		writeError(w, http.StatusConflict, "job cannot be cancelled from state "+string(j.State))
		return
	}
	if j.State == model.StateRunning {
		s.pool.Cancel(id)
	}
	j.State = model.StateCancelled
	j.UpdatedAt = time.Now()
	if err := s.store.UpdateJob(j); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) requeueJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.RequeueDead(id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "dead job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	j, _ := s.store.GetJob(id)
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) jobAttempts(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	attempts, err := s.store.ListAttempts(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"attempts": attempts, "count": len(attempts)})
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"queues": stats.Queues})
}

func (s *Server) queueDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	paused, err := s.store.IsPaused(name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	qs, err := s.store.QueueStats(name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"name": name, "paused": paused, "by_state": qs})
}

func (s *Server) pauseQueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.SetPaused(name, true); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"name": name, "paused": "true"})
}

func (s *Server) resumeQueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.SetPaused(name, false); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"name": name, "paused": "false"})
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.store.Stats()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) queueStats(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	qs, err := s.store.QueueStats(name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"name": name, "by_state": qs})
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request) {
	dead, err := s.store.DeadLetters()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"dead_letters": dead, "count": len(dead)})
}

func (s *Server) getDead(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := s.store.GetJob(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if j.State != model.StateDead {
		writeError(w, http.StatusBadRequest, "job is not a dead letter")
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) requeueDead(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.RequeueDead(id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "dead job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	j, _ := s.store.GetJob(id)
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) deleteDead(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(r) {
		writeError(w, http.StatusForbidden, "admin token required")
		return
	}
	id := r.PathValue("id")
	if err := s.store.DeleteJob(id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

func (s *Server) workers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"active": s.pool.Active(),
	})
}

func (s *Server) flush(w http.ResponseWriter, r *http.Request) {
	if err := s.pool.Flush(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "flushed"})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) metricsView(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics.Read())
}

type scheduleRequest struct {
	ID           string `json:"id"`
	Queue        string `json:"queue"`
	Type         string `json:"type"`
	Args         string `json:"args"`
	IntervalSecs int    `json:"interval_seconds"`
	MaxAttempts  int    `json:"max_attempts"`
	Priority     int    `json:"priority"`
}

func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	var req scheduleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if req.Queue == "" {
		req.Queue = "default"
	}
	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}
	if req.IntervalSecs <= 0 {
		writeError(w, http.StatusBadRequest, "interval_seconds must be positive")
		return
	}
	args := req.Args
	if args == "" {
		args = "{}"
	}
	id := req.ID
	if id == "" {
		id = "sched-" + newID()
	}
	sc := &model.Schedule{
		ID:          id,
		Queue:       req.Queue,
		Type:        req.Type,
		Args:        args,
		Interval:    time.Duration(req.IntervalSecs) * time.Second,
		Enabled:     true,
		MaxAttempts: req.MaxAttempts,
		Priority:    req.Priority,
	}
	if err := sc.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.store.CreateSchedule(sc); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, sc)
}

func (s *Server) listSchedules(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListSchedules()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"schedules": list, "count": len(list)})
}

func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sc, err := s.store.GetSchedule(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, sc)
}

func (s *Server) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(r) {
		writeError(w, http.StatusForbidden, "admin token required")
		return
	}
	id := r.PathValue("id")
	if err := s.store.DeleteSchedule(id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "id": id})
}

func (s *Server) enableSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.SetScheduleEnabled(id, true); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "enabled": "true"})
}

func (s *Server) disableSchedule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.SetScheduleEnabled(id, false); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "schedule not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": id, "enabled": "false"})
}

// newID returns a short random identifier.
func newID() string {
	const hex = "0123456789abcdef"
	b := make([]byte, 16)
	for i := range b {
		b[i] = hex[(int64(i)*2654435761+int64(time.Now().Nanosecond()))%16]
	}
	return "job-" + string(b)
}
