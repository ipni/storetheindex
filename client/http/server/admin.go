package httpserver

import (
	"net/http"
)

func (s *Server) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("\"OK\""))
	if err != nil {
		log.Errorw("cannot write HealthCheck response:", err)
		return
	}
}
