package httpingestserver

import (
	"io"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/httpserver"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/server/ingest/handler"
)

type httpHandler struct {
	ingestHandler *handler.IngestHandler
}

func newHandler(indexer indexer.Interface, registry *registry.Registry) *httpHandler {
	return &httpHandler{
		ingestHandler: handler.NewIngestHandler(indexer, registry),
	}
}

// POST /discover
func (h *httpHandler) discoverProvider(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = h.ingestHandler.DiscoverProvider(body)
	if err != nil {
		httpserver.HandleError(w, err, "discover")
		return
	}

	// Retrun accepted (202) response
	w.WriteHeader(http.StatusAccepted)
}

// POST /register
func (h *httpHandler) registerProvider(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = h.ingestHandler.RegisterProvider(r.Context(), body)
	if err != nil {
		httpserver.HandleError(w, err, "register")
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DELETE /register/{providerid}
func (h *httpHandler) removeProvider(w http.ResponseWriter, r *http.Request) {
	/*
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Errorw("failed reading body", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
		unregReq := new(model.UnregisterRequest)
		err = unregReq.UnmarshalJSON(body)
		if err != nil {
			log.Errorw("error unmarshalling unregistration request", "err", err)
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		err = h.registry.RemoveProvider(providerID)
		if err != nil {
			log.Errorw("cannot process unregistration request", "err", err)
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		go func() {
			err := h.engine.RemoveProvider(providerID)
			if err != nil {
				log.Errorw("cannot remove content for provider", "err", err)
			}
		}()

		// Retrun accepted (202) response
		w.WriteHeader(http.StatusAccepted)
	*/

	w.WriteHeader(http.StatusNotImplemented)
}

// ----- ingest handlers -----
// PUT /ingest/advertisement
func (h *httpHandler) advertise(w http.ResponseWriter, r *http.Request) {
	/*
		w.Header().Set("Content-Type", "application/json")
		adBuild := ingestion.Type.Advertisement.NewBuilder()
		err := dagjson.Decoder(ptp, r.Body)
		if err != nil {
			log.Errorw("Advertise request json decode", "err", err.Error())
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		_, err := h.registry.Advertise(r.Context(), adBuild.Build().(ingestion.Advertisement))
		if err != nil {
			log.Errorw("Advertise failed:", "err", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
	*/
	w.WriteHeader(http.StatusNoContent)
}

// POST /ingest/content
func (h *httpHandler) indexContent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	err = h.ingestHandler.IndexContent(r.Context(), body)
	if err != nil {
		httpserver.HandleError(w, err, "ingest")
		return
	}

	w.WriteHeader(http.StatusOK)
}
