package p2pingestserver

import (
	"context"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p-core/host"
)

// New creates a new libp2p server
func New(ctx context.Context, h host.Host, indexer indexer.Interface, ingester *ingest.Ingester, registry *registry.Registry) *libp2pserver.Server {
	log.Infow("ingest libp2p server listening", "addrs", h.Addrs())
	return libp2pserver.New(ctx, h, newHandler(indexer, ingester, registry))
}
