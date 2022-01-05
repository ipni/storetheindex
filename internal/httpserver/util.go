// Package httpserver provides functionality common to all storetheindex HTTP
// servers
package httpserver

import (
	"errors"
	"net/http"

	"github.com/filecoin-project/storetheindex/api/v0"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer/http")

func WriteJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if _, err := w.Write(body); err != nil {
		log.Errorw("cannot write response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func HandleError(w http.ResponseWriter, err error, reqType string) {
	status := http.StatusBadRequest
	var apierr *v0.Error
	if errors.As(err, &apierr) {
		if apierr.Status() >= 500 {
			log.Errorw("Cannot handle request", "regType", reqType, "err", apierr.Error(), "status", apierr.Status())
			http.Error(w, "", apierr.Status())
			return
		}
		status = apierr.Status()
	}
	log.Infow("Bad request", "reqType", reqType, "err", err, "status", status)
	http.Error(w, err.Error(), status)
}
