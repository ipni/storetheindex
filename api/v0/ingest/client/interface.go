package client

import (
	"context"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// Ingest is the interface implemented by all ingest client protocols
type Ingest interface {
	GetProvider(ctx context.Context, providerID peer.ID) (*models.ProviderInfo, error)
	ListProviders(ctx context.Context) ([]*models.ProviderInfo, error)
	Register(ctx context.Context, providerID, privateKey string, addrs []string) error
	IndexContent(ctx context.Context, providerID, privateKey string, m multihash.Multihash, protocol uint64, metadata []byte) error
}
