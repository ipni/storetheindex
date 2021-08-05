package handler

import (
	"io/ioutil"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("find_handler")

type FindHandler struct {
	engine   *indexer.Engine
	registry *providers.Registry
}

func New(engine *indexer.Engine, registry *providers.Registry) *FindHandler {
	return &FindHandler{
		engine:   engine,
		registry: registry,
	}
}

func (h *FindHandler) GetSingleCid(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	mhCid := vars["cid"]
	c, err := cid.Decode(mhCid)
	if err != nil {
		log.Errorw("error decoding cid", "cid", mhCid, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	h.getCids(w, []cid.Cid{c})
}

func (h *FindHandler) GetBatchCid(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req, err := models.UnmarshalReq(body)
	if err != nil {
		log.Errorw("error unmarshalling body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	h.getCids(w, req.Cids)
}

func writeResponse(w http.ResponseWriter, body []byte) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	if _, err := w.Write(body); err != nil {
		return err
	}

	return nil
}

func (h *FindHandler) getCids(w http.ResponseWriter, cids []cid.Cid) {
	response, err := models.PopulateResponse(h.engine, h.registry, cids)
	if err != nil {
		log.Errorw("query failed", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	rb, err := models.MarshalResp(response)
	if err != nil {
		log.Errorw("failed marshalling response", "cid", cids, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = writeResponse(w, rb)
	if err != nil {
		log.Errorw("failed writing response", "cid", cids, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
