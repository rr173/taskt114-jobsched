package api

import (
	"net/http"
	"time"
)

func (s *Server) maintenance(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.Maintenance(time.Now())
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, report)
}
