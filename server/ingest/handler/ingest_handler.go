package handler

import (
	"context"
	"encoding/json"
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
	"github.com/libp2p/go-libp2p-core/peer"
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

	if err = h.registry.CheckSequence(ingReq.ProviderID, ingReq.Seq); err != nil {
		return err
	}

	// Register provider if not registered, or update addreses if already registered
	err = h.registry.RegisterOrUpdate(ctx, ingReq.ProviderID, ingReq.Addrs, cid.Undef)
	if err != nil {
		return err
	}
	encMetadata, err := ingReq.Metadata.MarshalBinary()
	if err != nil {
		return err
	}

	value := indexer.Value{
		ProviderID:    ingReq.ProviderID,
		ContextID:     ingReq.ContextID,
		MetadataBytes: encMetadata,
	}
	err = h.indexer.Put(value, ingReq.Multihash)
	if err != nil {
		err = fmt.Errorf("cannot index content: %s", err)
		return v0.NewError(err, http.StatusInternalServerError)
	}

	// TODO: update last update time for provider

	return nil
}

const maxAnnounceSize = 512

type announceMessage dtsync.Message

// custom unmarshal because multiaddr.Multiaddr doesn't natively support json.Unmarshal
func (a *announceMessage) UnmarshalJSON(data []byte) error {
	top := map[string]*json.RawMessage{}
	if err := json.Unmarshal(data, &top); err != nil {
		return err
	}
	fmt.Printf("top: %+v\n", top)
	ci, ok := top["Cid"]
	if !ok || ci == nil {
		return fmt.Errorf("missing cid")
	}
	c := cid.Cid{}
	if err := json.Unmarshal(*ci, &c); err != nil {
		return err
	}
	a.Cid = c

	addrs, ok := top["Addrs"]
	if !ok {
		return fmt.Errorf("missing addrs")
	}
	addrList := make([]*json.RawMessage, 0)
	if err := json.Unmarshal(*addrs, &addrList); err != nil {
		return err
	}
	for _, addr := range addrList {
		addrStr := ""
		if err := json.Unmarshal(*addr, &addrStr); err != nil {
			return err
		}
		ma, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return err
		}
		a.Addrs = append(a.Addrs, ma)
	}
	return nil
}

func (h *IngestHandler) Announce(ctx context.Context, data io.Reader) error {
	bytes, err := io.ReadAll(io.LimitReader(data, maxAnnounceSize))
	if err != nil {
		return err
	}
	an := announceMessage{}
	if err := json.Unmarshal(bytes, &an); err != nil {
		return err
	}
	// todo: support mulitple multiaddrs?
	if len(an.Addrs) > 1 {
		return fmt.Errorf("must specify 1 location to fetch on direct announcments")
	}
	// todo: require auth?

	pid, err := peer.AddrInfoFromP2pAddr(an.Addrs[0])
	if err != nil {
		return err
	}
	allow, err := h.registry.Authorized(pid.ID)
	if err != nil {
		err = fmt.Errorf("error checking if peer allowed: %w", err)
		return v0.NewError(err, http.StatusInternalServerError)
	}
	if !allow {
		return v0.NewError(errors.New("not authorized to announce"), http.StatusForbidden)
	}
	cur, err := h.ingester.GetLatestSync(pid.ID)
	if err == nil {
		if cur.Equals(an.Cid) {
			return nil
		}
	}
	h.ingester.Sync(ctx, pid.ID, pid.Addrs[0])

	return nil
}
