package httpfinderserver

import (
	"io"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/internal/handler"
	"github.com/filecoin-project/storetheindex/internal/httpserver"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/gorilla/mux"
	"github.com/multiformats/go-multihash"
)

// handler handles requests for the finder resource
type httpHandler struct {
	finderHandler *handler.FinderHandler
}

func newHandler(indexer indexer.Interface, registry *providers.Registry) *httpHandler {
	return &httpHandler{
		finderHandler: handler.NewFinderHandler(indexer, registry),
	}
}

func (h *httpHandler) find(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	mhVar := vars["multihash"]
	m, err := multihash.FromB58String(mhVar)
	if err != nil {
		log.Errorw("error decoding multihash", "multihash", mhVar, "err", err)
		httpserver.HandleError(w, err, "find")
		return
	}
	h.getIndexes(w, []multihash.Multihash{m})
}

func (h *httpHandler) findBatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading get batch request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req, err := models.UnmarshalFindRequest(body)
	if err != nil {
		log.Errorw("error unmarshalling get batch request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.getIndexes(w, req.Multihashes)
}

func (h *httpHandler) getIndexes(w http.ResponseWriter, mhs []multihash.Multihash) {
	response, err := h.finderHandler.MakeFindResponse(mhs)
	if err != nil {
		httpserver.HandleError(w, err, "get")
		return
	}

	// If no info for any multihashes, then 404
	if len(response.MultihashResults) == 0 {
		http.Error(w, "no results for query", http.StatusNotFound)
		return
	}

	rb, err := models.MarshalFindResponse(response)
	if err != nil {
		log.Errorw("failed marshalling query response", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, rb)
}
