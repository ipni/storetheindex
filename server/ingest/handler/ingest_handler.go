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
	"github.com/libp2p/go-libp2p-core/peer"
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

// readBeforErrReader implements io.Reader but guarantees the if bytes read is > 0 err is nil.
type readBeforErrReader struct {
	lastErr error
	r       io.Reader
}

func (r *readBeforErrReader) Read(p []byte) (n int, err error) {
	if r.lastErr != nil {
		return n, r.lastErr
	}

	n, r.lastErr = r.r.Read(p)
	if n != 0 {
		return n, nil
	}

	return 0, r.lastErr
}

func (h *IngestHandler) Announce(r io.Reader) error {
	// Decode CID and originator addresses from message.
	an := dtsync.Message{}
	// TODO remove readBeforErrReader when https://github.com/whyrusleeping/cbor-gen/pull/44 is merged
	if err := an.UnmarshalCBOR(&readBeforErrReader{r: r}); err != nil {
		return err
	}

	// todo: support mulitple multiaddrs?
	if len(an.Addrs) > 1 {
		return fmt.Errorf("must specify 1 location to fetch on direct announcments")
	}

	// todo: require auth?

	addrs, err := an.GetAddrs()
	if err != nil {
		return fmt.Errorf("could not decode addrs from announce message: %w", err)
	}

	pid, err := peer.AddrInfoFromP2pAddr(addrs[0])
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

	// We set context background because this will be an async process. We don't
	// want to attach the context to the request context that started this.
	h.ingester.Sync(context.Background(), pid.ID, pid.Addrs[0], 0)

	return nil
}
