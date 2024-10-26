package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/ingest/model"
	"github.com/ipni/storetheindex/ingest"
	"github.com/ipni/storetheindex/registry"
	"github.com/libp2p/go-libp2p/core/peer"
)

// handler provides request handling functionality for the ingest server.
type handler struct {
	indexer  indexer.Interface
	ingester *ingest.Ingester
	registry *registry.Registry
}

func (h handler) registerProvider(ctx context.Context, data []byte) error {
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

	provider := peer.AddrInfo{
		ID:    peerRec.PeerID,
		Addrs: peerRec.Addrs,
	}
	publisher := peer.AddrInfo{}

	return h.registry.Update(ctx, provider, publisher, cid.Undef, nil, 0)
}

func (h handler) announce(an message.Message) error {
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
		return apierror.New(err, http.StatusForbidden)
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

// TODO: Uncomment when supporting puts directly to indexer.
/*
// indexContent handles an IngestRequest
//
// Returning error is the same as return apierror.New(err, http.StatusBadRequest)
func (h handler) indexContent(ctx context.Context, data []byte) error {
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
	err = h.registry.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
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
		return apierror.New(err, http.StatusInternalServerError)
	}

	// TODO: update last update time for provider

	return nil
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
*/
