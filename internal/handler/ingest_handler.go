package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/libp2p/go-libp2p-core/peer"
)

// IngestHandler provides request handling functionality for the ingest server
// that is common to all protocols
type IngestHandler struct {
	indexer  indexer.Interface
	registry *providers.Registry
}

func NewIngestHandler(indexer indexer.Interface, registry *providers.Registry) *IngestHandler {
	return &IngestHandler{
		indexer:  indexer,
		registry: registry,
	}
}

func (h *IngestHandler) DiscoverProvider(data []byte) error {
	discoReq, err := models.ReadDiscoverRequest(data)
	if err != nil {
		return fmt.Errorf("connot read discover request: %s", err)
	}

	if err = h.registry.CheckSequence(discoReq.ProviderID, discoReq.Seq); err != nil {
		return err
	}

	return h.registry.Discover(discoReq.ProviderID, discoReq.DiscoveryAddr, false)
}

func (h *IngestHandler) RegisterProvider(data []byte) error {
	peerRec, err := models.ReadRegisterRequest(data)
	if err != nil {
		return fmt.Errorf("cannot read register request: %s", err)
	}

	if len(peerRec.PeerID) == 0 {
		return errors.New("missing peer id")
	}

	if err = h.registry.CheckSequence(peerRec.PeerID, peerRec.Seq); err != nil {
		return err
	}

	info := &providers.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerRec.PeerID,
			Addrs: peerRec.Addrs,
		},
	}
	return h.registry.Register(info)
}

func (h *IngestHandler) ListProviders() ([]byte, error) {
	infos := h.registry.AllProviderInfo()

	responses := make([]models.ProviderInfo, len(infos))
	for i := range infos {
		responses[i] = models.MakeProviderInfo(infos[i].AddrInfo, infos[i].LastIndex, infos[i].LastIndexTime)
	}

	return json.Marshal(responses)
}

func (h *IngestHandler) GetProvider(providerID peer.ID) ([]byte, error) {
	info := h.registry.ProviderInfo(providerID)
	if info == nil {
		return nil, nil
	}

	rsp := models.MakeProviderInfo(info.AddrInfo, info.LastIndex, info.LastIndexTime)

	return json.Marshal(&rsp)
}

func (h *IngestHandler) IndexContent(data []byte) (bool, error) {
	ingReq, err := models.ReadIngestRequest(data)
	if err != nil {
		return false, fmt.Errorf("cannot read ingest request: %s", err)
	}

	providerID := ingReq.Value.ProviderID
	if err = h.registry.CheckSequence(providerID, ingReq.Seq); err != nil {
		return false, err
	}

	// Check that the provider has been discovered and validated
	if !h.registry.IsRegistered(providerID) {
		err = errors.New("cannot accept ingest request from unverified provider")
		return false, syserr.New(err, http.StatusForbidden)
	}

	ok, err := h.indexer.Put(ingReq.Cid, ingReq.Value)
	if err != nil {
		err = fmt.Errorf("cannot index content: %s", err)
		return false, syserr.New(err, http.StatusInternalServerError)
	}

	// TODO: update last update time for provider

	return ok, nil
}
