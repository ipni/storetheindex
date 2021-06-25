package api

import "net/http"

func (a *API) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("\"OK\""))
}
