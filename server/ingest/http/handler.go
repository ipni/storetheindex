package httpingestserver

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/handler"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/gorilla/mux"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type httpHandler struct {
	ingestHandler *handler.IngestHandler
}

func newHandler(indexer indexer.Interface, registry *providers.Registry) *httpHandler {
	return &httpHandler{
		ingestHandler: handler.NewIngestHandler(indexer, registry),
	}
}

// ----- provider handlers -----

// GET /providers",
func (h *httpHandler) ListProviders(w http.ResponseWriter, r *http.Request) {
	data, err := h.ingestHandler.ListProviders()
	if err != nil {
		log.Errorw("cannot list providers", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	writeJsonResponse(w, http.StatusOK, data)
}

// GET /providers/{providerid}
func (h *httpHandler) GetProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	data, err := h.ingestHandler.GetProvider(providerID)
	if err != nil {
		log.Error("cannot get provider", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	if len(data) == 0 {
		http.Error(w, "provider not found", http.StatusNotFound)
		return
	}

	writeJsonResponse(w, http.StatusOK, data)
}

// POST /discover
func (h *httpHandler) DiscoverProvider(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = h.ingestHandler.DiscoverProvider(body)
	if err != nil {
		handleError(w, err, "discover")
		return
	}

	// Retrun accepted (202) response
	w.WriteHeader(http.StatusAccepted)
}

// POST /providers
func (h *httpHandler) RegisterProvider(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = h.ingestHandler.RegisterProvider(body)
	if err != nil {
		handleError(w, err, "register")
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DELETE /providers/{providerid}
func (h *httpHandler) RemoveProvider(w http.ResponseWriter, r *http.Request) {
	/*
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Errorw("failed reading body", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		unregReq := new(models.UnregisterRequest)
		err = unregReq.UnmarshalJSON(body)
		if err != nil {
			log.Errorw("error unmarshalling unregistration request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = h.registry.RemoveProvider(providerID)
		if err != nil {
			log.Errorw("cannot process unregistration request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
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
// PUT /ingestion/advertisement
func (h *httpHandler) Advertise(w http.ResponseWriter, r *http.Request) {
	/*
		w.Header().Set("Content-Type", "application/json")
		adBuild := ingestion.Type.Advertisement.NewBuilder()
		err := dagjson.Decoder(ptp, r.Body)
		if err != nil {
			log.Errorw("Advertise request json decode", "err", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		_, err := h.registry.Advertise(r.Context(), adBuild.Build().(ingestion.Advertisement))
		if err != nil {
			log.Errorw("Advertise failed:", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	*/
	w.WriteHeader(http.StatusNoContent)
}

// POST /ingestion
func (h *httpHandler) IndexContent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	ok, err := h.ingestHandler.IndexContent(body)
	if err != nil {
		handleError(w, err, "ingest")
		return
	}

	if ok {
		log.Info("indexed new content")
	}

	w.WriteHeader(http.StatusNoContent)
}

func getProviderID(r *http.Request) (peer.ID, error) {
	vars := mux.Vars(r)
	pid := vars["providerid"]
	providerID, err := peer.Decode(pid)
	if err != nil {
		return providerID, fmt.Errorf("cannot decode provider id: %s", err)
	}
	return providerID, nil
}

func handleError(w http.ResponseWriter, err error, reqType string) {
	status := http.StatusBadRequest
	var se *syserr.SysError
	if errors.As(err, &se) {
		if se.Status() >= 500 {
			log.Errorw(fmt.Sprint("cannot handle", reqType, "request"), "err", se)
			http.Error(w, "", se.Status())
			return
		}
		status = se.Status()
	}
	log.Infow(fmt.Sprint("bad", reqType, "request"), "err", err.Error(), "status", status)
	http.Error(w, err.Error(), status)
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if _, err := w.Write(body); err != nil {
		log.Errorw("cannot write response", "err", err)
		http.Error(w, "", http.StatusInternalServerError)
	}
}
