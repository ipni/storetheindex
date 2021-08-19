package handler

import (
	"encoding/json"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/internal/providers"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("ingestion_handler")

type Handler struct {
	indexer  indexer.Interface
	registry *providers.Registry
}

func New(indexer indexer.Interface, registry *providers.Registry) *Handler {
	return &Handler{
		indexer:  indexer,
		registry: registry,
	}
}

func writeResponse(w http.ResponseWriter, body []byte) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if _, err := w.Write(body); err != nil {
		return err
	}

	return nil
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(http.StatusBadRequest)
	if err == nil {
		return
	}

	e := v0.Error{
		Message: err.Error(),
	}
	rb, err := json.Marshal(&e)
	if err != nil {
		log.Errorw("failed to marshal error response", "err", err)
		return
	}

	err = writeResponse(w, rb)
	if err != nil {
		log.Errorw("failed writing error response", "err", err)
	}
}
