package handler

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/filecoin-project/go-indexer-core/entry"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
)

// PUT /ingestion/advertisement
func (h *Handler) Advertise(w http.ResponseWriter, r *http.Request) {
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
func (h *Handler) IndexContent(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	req := new(models.IngestRequest)
	err = json.Unmarshal(body, &req)
	if err != nil {
		log.Errorw("error unmarshaling body", "err", err)
		w.WriteHeader(http.StatusBadRequest)
	}

	// Check that the provider has been discovered and validated
	if !h.registry.IsRegistered(req.Provider) {
		log.Errorw("cannot accept ingest request from unknown provider", "provider", req.Provider)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// TODO: validate CID and Metadata

	value := entry.MakeValue(req.Provider, uint64(req.Protocol), req.Metadata)
	ok, err := h.engine.Put(req.Cid, value)
	if err != nil {
		log.Errorw("cannot store content", "cid", req.Cid, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	if ok {
		log.Infow("stored new content", "cid", req.Cid)
	}

	w.WriteHeader(http.StatusNoContent)
}
