package ingestserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/gorilla/mux"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

type handler struct {
	indexer  indexer.Interface
	registry *providers.Registry
}

// ----- provider handlers -----

// GET /providers",
func (h *handler) ListProviders(w http.ResponseWriter, r *http.Request) {
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
func (h *handler) GetProvider(w http.ResponseWriter, r *http.Request) {
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
func (h *handler) DiscoverProvider(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
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

	err = discoReq.VerifySignature()
	if err != nil {
		log.Errorw("signature not verified", "err", err, "provider", discoReq.ProviderID, "discover_addr", discoReq.DiscoveryAddr)
		writeError(w, http.StatusBadRequest, err)
		return
	}

	err = h.registry.Discover(discoReq.ProviderID, discoReq.DiscoveryAddr, false)
	if err != nil {
		log.Errorw("cannot process discovery request", "err", err)
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// Retrun accepted (202) response
	w.WriteHeader(http.StatusAccepted)
}

// POST /providers
func (h *handler) RegisterProvider(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
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
		writeError(w, http.StatusBadRequest, err)
		return
	}

	info := &providers.ProviderInfo{
		AddrInfo: regReq.AddrInfo,
	}
	err = h.registry.Register(info)
	if err != nil {
		log.Errorw("cannot process registration request", "err", err)
		writeError(w, http.StatusBadRequest, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// DELETE /providers/{providerid}
func (h *handler) RemoveProvider(w http.ResponseWriter, r *http.Request) {
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
func (h *handler) Advertise(w http.ResponseWriter, r *http.Request) {
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
func (h *handler) IndexContent(w http.ResponseWriter, r *http.Request) {
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

func getProviderID(r *http.Request) (peer.ID, error) {
	vars := mux.Vars(r)
	pid := vars["providerid"]
	providerID, err := peer.Decode(pid)
	if err != nil {
		return providerID, fmt.Errorf("cannot decode provider id: %s", err)
	}
	return providerID, nil
}

func provInfoToApi(pinfo *providers.ProviderInfo, apiModel *models.ProviderInfo) {
	*apiModel = models.MakeProviderInfo(pinfo.AddrInfo, pinfo.LastIndex, pinfo.LastIndexTime)
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
