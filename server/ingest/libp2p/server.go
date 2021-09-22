package p2pingestserver

import (
	"context"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/libp2p/go-libp2p-core/host"
)

// New creates a new libp2p server
func New(ctx context.Context, h host.Host, indexer indexer.Interface, registry *providers.Registry) *libp2pserver.Server {
	log.Infow("ingest libp2p server listening", "addrs", h.Addrs())
	return libp2pserver.New(ctx, h, newHandler(indexer, registry))
}
