package api

import "net/http"

func (s *Server) queueReport(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.QueueReport(r.PathValue("name"))
	if err != nil {
		writeError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, report)
}
