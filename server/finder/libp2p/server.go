package p2pfinderserver

import (
	"context"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/internal/counter"
	"github.com/filecoin-project/storetheindex/internal/libp2pserver"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/host"
)

type FinderServer struct {
	libp2pserver.Server
	p2pHandler *libp2pHandler
}

// New creates a new libp2p server
func New(ctx context.Context, h host.Host, indexer indexer.Interface, registry *registry.Registry, indexCounts *counter.IndexCounts) *FinderServer {
	p2ph := newHandler(indexer, registry, indexCounts)
	s := &FinderServer{
		p2pHandler: p2ph,
	}
	s.Server = *libp2pserver.New(ctx, h, p2ph)
	return s
}

func (s *FinderServer) RefreshStats() {
	s.p2pHandler.finderHandler.RefreshStats()
}
