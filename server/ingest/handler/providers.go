package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/gorilla/mux"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// GET /providers",
func (h *Handler) ListProviders(w http.ResponseWriter, r *http.Request) {
}

// GET /providers/{providerid}
func (h *Handler) GetProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debugw("GetProvider", "provider", providerID)

	provInfo, found := h.registry.ProviderInfo(providerID)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	rsp := models.ProviderData{
		Provider:  providerID,
		Addrs:     provInfo.Addresses,
		LastIndex: provInfo.LastIndex,
	}

	rb, err := json.Marshal(&rsp)
	if err != nil {
		log.Errorw("failed marshalling response", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = writeResponse(w, rb)
	if err != nil {
		log.Errorw("failed writing response", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// POST /providers/{providerid}
func (h *Handler) DiscoverProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debugw("DiscoverProvider", "provider", providerID)
}

// PUT /providers/{providerid}
func (h *Handler) UpdateProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debugw("UpdateProvider", "provider", providerID)
}

// DELETE /providers/{providerid}
func (h *Handler) RemoveProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debugw("RemoveProvider", "provider", providerID)
}

// GET /providers/{providerid}/lastindex
func (h *Handler) LastIndex(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debugw("LastIndex", "provider", providerID)

	provInfo, found := h.registry.ProviderInfo(providerID)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	jsonString, err := json.Marshal(
		struct {
			lastIndex cid.Cid
		}{
			provInfo.LastIndex,
		},
	)
	if err != nil {
		fmt.Println("error:", err)
	}

	w.WriteHeader(http.StatusOK)

	_, err = w.Write(jsonString)
	if err != nil {
		log.Errorw("cannot write lastindex response:", err)
		return
	}
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
