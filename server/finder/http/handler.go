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
	"github.com/ipfs/go-cid"
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

func (h *httpHandler) GetSingleCid(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	mhCid := vars["cid"]
	c, err := cid.Decode(mhCid)
	if err != nil {
		log.Errorw("error decoding cid", "cid", mhCid, "err", err)
		httpserver.HandleError(w, err, "get")
		return
	}
	h.getCids(w, []cid.Cid{c})
}

func (h *httpHandler) GetBatchCid(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading get batch request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req, err := models.UnmarshalReq(body)
	if err != nil {
		log.Errorw("error unmarshalling get batch request", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.getCids(w, req.Cids)
}

func (h *httpHandler) getCids(w http.ResponseWriter, cids []cid.Cid) {
	response, err := h.finderHandler.PopulateResponse(cids)
	if err != nil {
		httpserver.HandleError(w, err, "get")
		return
	}

	// If no info for any Cids, then 404
	if len(response.CidResults) == 0 {
		http.Error(w, "no results for query", http.StatusNotFound)
		return
	}

	rb, err := models.MarshalResp(response)
	if err != nil {
		log.Errorw("failed marshalling query response", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	httpserver.WriteJsonResponse(w, http.StatusOK, rb)
}
