package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/gorilla/mux"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

func (h *AdminHandler) Subscribe(w http.ResponseWriter, r *http.Request) {
	if ret := h.checkIngester(w, r); ret {
		return
	}
	vars := mux.Vars(r)
	m := vars["provider"]
	miner, err := peer.Decode(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID", "miner", m, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Subscribing to provider", "provider", m)
	err = h.ingester.Subscribe(r.Context(), miner)
	if err != nil {
		log.Errorw("error subscribing to provider", "err", err)
		writeError(w, http.StatusInternalServerError, err)
	}

	// Return OK
	w.WriteHeader(http.StatusOK)

}

func (h *AdminHandler) Unsubscribe(w http.ResponseWriter, r *http.Request) {
	if ret := h.checkIngester(w, r); ret {
		return
	}
	vars := mux.Vars(r)
	m := vars["provider"]
	miner, err := peer.Decode(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID", "miner", m, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Unsubscribing to provider", "provider", m)
	err = h.ingester.Unsubscribe(r.Context(), miner)
	if err != nil {
		log.Errorw("error unssubscribing to provider", "err", err)
		writeError(w, http.StatusInternalServerError, err)
	}

	// Return OK
	w.WriteHeader(http.StatusOK)

}

func (h *AdminHandler) Sync(w http.ResponseWriter, r *http.Request) {
	if ret := h.checkIngester(w, r); ret {
		return
	}
	vars := mux.Vars(r)
	m := vars["provider"]
	miner, err := peer.Decode(m)
	if err != nil {
		log.Errorw("error decoding miner id into peerID", "miner", m, "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Infow("Syncing with provider", "provider", m)

	// We accept the request but do nothing with the channel.
	// We can include an ingestion API to check the latest sync for
	// a provider in the indexer. This would show if the indexer
	// has finally synced or not.
	_, err = h.ingester.Sync(r.Context(), miner)
	if err != nil {
		log.Errorw("error syncing with provider", "err", err)
		writeError(w, http.StatusInternalServerError, err)
	}

	// Return (202) Accepted
	w.WriteHeader(http.StatusAccepted)

}

func (h *AdminHandler) checkIngester(w http.ResponseWriter, r *http.Request) bool {
	if h.ingester == nil {
		log.Errorw("no ingester set in indexer")
		writeError(w, http.StatusInternalServerError, errors.New("no ingester set in indexer"))
		return true
	}
	return false
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
