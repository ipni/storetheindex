package p2pfinderserver

import (
	"context"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/counter"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/host"
)

// New creates a new libp2p server
func New(ctx context.Context, h host.Host, indexer indexer.Interface, registry *registry.Registry, indexCounts *counter.IndexCounts) *libp2pserver.Server {
	return libp2pserver.New(ctx, h, newHandler(indexer, registry, indexCounts))
}
