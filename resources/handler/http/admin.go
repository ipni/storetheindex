package httphandler

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("handlers")

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte("\"OK\""))
	if err != nil {
		log.Errorw("cannot write HealthCheck response:", err)
		return
	}
}
