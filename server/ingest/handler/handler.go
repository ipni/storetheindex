package handler

import (
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/providers"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("ingestion_handler")

type Handler struct {
	engine   *indexer.Engine
	registry *providers.Registry
}

func New(engine *indexer.Engine, registry *providers.Registry) *Handler {
	return &Handler{
		engine:   engine,
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
