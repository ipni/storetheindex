package handler

import (
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
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
