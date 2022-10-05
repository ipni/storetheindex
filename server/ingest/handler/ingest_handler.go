package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-legs/dtsync"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/model"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// IngestHandler provides request handling functionality for the ingest server
// that is common to all protocols
type IngestHandler struct {
	indexer  indexer.Interface
	ingester *ingest.Ingester
	registry *registry.Registry
}

func NewIngestHandler(indexer indexer.Interface, ingester *ingest.Ingester, registry *registry.Registry) *IngestHandler {
	return &IngestHandler{
		indexer:  indexer,
		ingester: ingester,
		registry: registry,
	}
}

func (h *IngestHandler) DiscoverProvider(data []byte) error {
	discoReq, err := model.ReadDiscoverRequest(data)
	if err != nil {
		return fmt.Errorf("connot read discover request: %s", err)
	}

	if err = h.registry.CheckSequence(discoReq.ProviderID, discoReq.Seq); err != nil {
		return err
	}

	return h.registry.Discover(discoReq.ProviderID, discoReq.DiscoveryAddr, false)
}

func (h *IngestHandler) RegisterProvider(ctx context.Context, data []byte) error {
	peerRec, err := model.ReadRegisterRequest(data)
	if err != nil {
		return fmt.Errorf("cannot read register request: %s", err)
	}

	if len(peerRec.PeerID) == 0 {
		return errors.New("missing peer id")
	}

	if err = h.registry.CheckSequence(peerRec.PeerID, peerRec.Seq); err != nil {
		return err
	}

	info := &registry.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerRec.PeerID,
			Addrs: peerRec.Addrs,
		},
	}
	return h.registry.Register(ctx, info)
}

// IndexContent handles an IngestRequest
//
// Returning error is the same as return v0.NewError(err, http.StatusBadRequest)
func (h *IngestHandler) IndexContent(ctx context.Context, data []byte) error {
	ingReq, err := model.ReadIngestRequest(data)
	if err != nil {
		return fmt.Errorf("cannot read ingest request: %s", err)
	}

	if len(ingReq.ContextID) > schema.MaxContextIDLen {
		return errors.New("context id too long")
	}

	if len(ingReq.Metadata) > schema.MaxMetadataLen {
		return errors.New("metadata too long")
	}

	if err = h.registry.CheckSequence(ingReq.ProviderID, ingReq.Seq); err != nil {
		return err
	}

	maddrs, err := stringsToMultiaddrs(ingReq.Addrs)
	if err != nil {
		return err
	}

	provider := peer.AddrInfo{
		ID:    ingReq.ProviderID,
		Addrs: maddrs,
	}

	// Register provider if not registered, or update addreses if already registered
	err = h.registry.RegisterOrUpdate(ctx, provider, cid.Undef, peer.AddrInfo{})
	if err != nil {
		return err
	}

	value := indexer.Value{
		ProviderID:    ingReq.ProviderID,
		ContextID:     ingReq.ContextID,
		MetadataBytes: ingReq.Metadata,
	}
	err = h.indexer.Put(value, ingReq.Multihash)
	if err != nil {
		err = fmt.Errorf("cannot index content: %s", err)
		return v0.NewError(err, http.StatusInternalServerError)
	}

	// TODO: update last update time for provider

	return nil
}

func (h *IngestHandler) Announce(r io.Reader) error {
	// Decode CID and originator addresses from message.
	an := dtsync.Message{}
	if err := an.UnmarshalCBOR(r); err != nil {
		return err
	}

	if len(an.Addrs) == 0 {
		return fmt.Errorf("must specify location to fetch on direct announcments")
	}

	// todo: require auth?

	addrs, err := an.GetAddrs()
	if err != nil {
		return fmt.Errorf("could not decode addrs from announce message: %w", err)
	}

	ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
	if err != nil {
		return err
	}
	if len(ais) > 1 {
		return errors.New("peer id must be the same for all addresses")
	}
	addrInfo := ais[0]

	if !h.registry.Allowed(addrInfo.ID) {
		err = fmt.Errorf("announce requests not allowed from peer %s", addrInfo.ID)
		return v0.NewError(err, http.StatusForbidden)
	}
	cur, err := h.ingester.GetLatestSync(addrInfo.ID)
	if err == nil {
		if cur.Equals(an.Cid) {
			return nil
		}
	}

	// Use background context because this will be an async process. We don't
	// want to attach the context to the request context that started this.
	return h.ingester.Announce(context.Background(), an.Cid, addrInfo)
}

func stringsToMultiaddrs(addrs []string) ([]multiaddr.Multiaddr, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("bad address: %s", err)
		}
	}
	return maddrs, nil
}
