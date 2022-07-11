package client

import (
	"context"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// Finder is the interface implemented by all finder client protocols.
type Finder interface {
	// Find queries for provider content records for a single multihash.
	Find(context.Context, multihash.Multihash) (*model.FindResponse, error)
	// FindBatch queries for provider content records for a batch of multihashes.
	FindBatch(context.Context, []multihash.Multihash) (*model.FindResponse, error)

	// GetProvider gets information about the provider identified by peer.ID.
	GetProvider(context.Context, peer.ID) (*model.ProviderInfo, error)
	// ListPrividers gets information about all providers known to the indexer.
	ListProviders(context.Context) ([]*model.ProviderInfo, error)

	// GetStats get statistics for indexer.
	GetStats(context.Context) (*model.Stats, error)
}
