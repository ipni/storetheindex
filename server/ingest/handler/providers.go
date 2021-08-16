package handler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/gorilla/mux"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// GET /providers",
func (h *Handler) ListProviders(w http.ResponseWriter, r *http.Request) {
	infos := h.registry.AllProviderInfo()

	responses := make([]models.ProviderInfo, len(infos))
	for i := range infos {
		provInfoToApi(infos[i], &responses[i])
	}

	b, err := json.Marshal(responses)
	if err != nil {
		log.Errorw("cannot marshal response", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	err = writeResponse(w, b)
	if err != nil {
		log.Errorw("cannot write response", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// GET /providers/{providerid}
func (h *Handler) GetProvider(w http.ResponseWriter, r *http.Request) {
	providerID, err := getProviderID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Debugw("GetProvider", "provider", providerID)

	info := h.registry.ProviderInfo(providerID)
	if info == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	var rsp models.ProviderInfo
	provInfoToApi(info, &rsp)

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
		return
	}
}

// POST /discover
func (h *Handler) DiscoverProvider(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var discoReq models.DiscoverRequest
	if err = json.Unmarshal(body, &discoReq); err != nil {
		log.Errorw("error unmarshalling discovery request", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = h.registry.Discover(discoReq.DiscoveryAddr, discoReq.Nonce, discoReq.Signature, false)
	if err != nil {
		log.Errorw("cannot process discovery request", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Retrun accepted (202) response
	w.WriteHeader(http.StatusAccepted)
}

// POST /providers
func (h *Handler) RegisterProvider(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorw("failed reading body", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var regReq models.RegisterRequest
	if err = json.Unmarshal(body, &regReq); err != nil {
		log.Errorw("error unmarshalling registration request", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = regReq.VerifySignature()
	if err != nil {
		log.Errorw("signature not verified", "err", err, "provider", regReq.AddrInfo.ID)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	info := &providers.ProviderInfo{
		AddrInfo: regReq.AddrInfo,
	}
	err = h.registry.Register(info)
	if err != nil {
		log.Errorw("cannot process registration request", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Debugw("registered provider", "provider", regReq.AddrInfo.ID)
	w.WriteHeader(http.StatusOK)
}

// DELETE /providers/{providerid}
func (h *Handler) UnregisterProvider(w http.ResponseWriter, r *http.Request) {
	/*
		providerID, err := getProviderID(r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		unregReq := new(models.UnregisterRequest)
		err = unregReq.UnmarshalJSON(body)
		if err != nil {
			log.Errorw("error unmarshalling unregistration request", "err", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = h.registry.RemoveProvider(providerID, unregReq.Nonce, unregReq.Signature)
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
	*/

	// Retrun accepted (202) response
	w.WriteHeader(http.StatusAccepted)
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

// iso8601 returns the given time as an ISO8601 formatted string.
func iso8601(t time.Time) string {
	tstr := t.Format("2006-01-02T15:04:05")
	_, zoneOffset := t.Zone()
	if zoneOffset == 0 {
		return fmt.Sprintf("%sZ", tstr)
	}
	if zoneOffset < 0 {
		return fmt.Sprintf("%s-%02d%02d", tstr, -zoneOffset/3600,
			(-zoneOffset%3600)/60)
	}
	return fmt.Sprintf("%s+%02d%02d", tstr, zoneOffset/3600,
		(zoneOffset%3600)/60)
}

func provInfoToApi(pinfo *providers.ProviderInfo, apiModel *models.ProviderInfo) {
	apiModel.AddrInfo = pinfo.AddrInfo
	apiModel.LastIndex = pinfo.LastIndex

	if pinfo.LastIndex.Defined() {
		apiModel.LastIndexTime = iso8601(pinfo.LastIndexTime)
	}
}
