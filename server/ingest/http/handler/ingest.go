package handler

import (
	"encoding/json"
	"io"
	"net/http"

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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ireq := new(models.IngestRequest)
	err = json.Unmarshal(body, &ireq)
	if err != nil {
		log.Errorw("error unmarshaling body", "err", err)
		w.WriteHeader(http.StatusBadRequest)
	}

	// Check that the provider has been discovered and validated
	if !h.registry.IsRegistered(ireq.Value.ProviderID) {
		log.Infow("cannot accept ingest request from unknown provider", "provider", ireq.Value.ProviderID)
		w.WriteHeader(http.StatusForbidden)
		return
	}

	err = ireq.VerifySignature()
	if err != nil {
		log.Infow("signature not verified", "err", err)
		w.WriteHeader(http.StatusBadRequest)
	}

	ok, err := h.indexer.Put(ireq.Cid, ireq.Value)
	if err != nil {
		log.Errorw("cannot store content", "cid", ireq.Cid, "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	if ok {
		log.Infow("stored new content", "cid", ireq.Cid)
	}

	w.WriteHeader(http.StatusNoContent)
}
