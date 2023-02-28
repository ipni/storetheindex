package reframe

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/server"
	"github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/storetheindex/internal/metrics"
	stimetrics "github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/server/finder/handler"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"go.opentelemetry.io/otel/attribute"
)

func NewReframeHTTPHandler(indexer indexer.Interface, registry *registry.Registry, sm *stimetrics.StiMetrics) http.HandlerFunc {
	return server.DelegatedRoutingAsyncHandler(NewReframeService(handler.NewFinderHandler(indexer, registry, nil), sm))
}

func NewReframeService(fh *handler.FinderHandler, sm *stimetrics.StiMetrics) *ReframeService {
	return &ReframeService{finderHandler: fh, sm: sm}
}

type ReframeService struct {
	finderHandler *handler.FinderHandler
	sm            *stimetrics.StiMetrics
}

func (x *ReframeService) FindProviders(ctx context.Context, key cid.Cid) (<-chan client.FindProvidersAsyncResult, error) {
	startTime := time.Now()
	var found bool
	defer func() {
		msecPerMh := coremetrics.MsecSince(startTime)
		x.sm.FindLatency.Record(context.Background(), msecPerMh, attribute.String(metrics.Method, "reframe"), attribute.Bool(metrics.Found, found))
	}()

	mh := key.Hash()
	fr, err := x.finderHandler.Find([]multihash.Multihash{mh})
	if err != nil {
		return nil, err
	}
	ch := make(chan client.FindProvidersAsyncResult, 1)
	var peerAddrs []peer.AddrInfo
	for _, mhr := range fr.MultihashResults {
		if !bytes.Equal(mhr.Multihash, mh) {
			continue
		}
		for _, pr := range mhr.ProviderResults {
			if !containsTransportBitswap(pr.Metadata) {
				continue
			}
			peerAddrs = append(peerAddrs, *pr.Provider)
		}
	}
	go func() {
		defer close(ch)
		ch <- client.FindProvidersAsyncResult{AddrInfo: peerAddrs}
	}()
	return ch, nil
}

func (x *ReframeService) GetIPNS(context.Context, []byte) (<-chan client.GetIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) PutIPNS(context.Context, []byte, []byte) (<-chan client.PutIPNSAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

func (x *ReframeService) Provide(context.Context, *client.ProvideRequest) (<-chan client.ProvideAsyncResult, error) {
	return nil, routing.ErrNotSupported
}

var BitswapMetadataBytes = varint.ToUvarint(uint64(multicodec.TransportBitswap))

func containsTransportBitswap(meta []byte) bool {
	// Metadata must be sorted according to the specification; see:
	// - https://github.com/ipni/index-provider/blob/main/metadata/metadata.go#L143
	// This implies that if it includes Bitswap, its codec must appear at the beginning
	// of the metadata value. Hence, bytes.HasPrefix.
	return bytes.HasPrefix(meta, BitswapMetadataBytes)
}
