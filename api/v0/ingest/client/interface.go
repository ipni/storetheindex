package client

import (
	"context"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Ingest is the interface implemented by all ingest client protocols
type Ingest interface {
	GetProvider(ctx context.Context, providerID peer.ID) (*models.ProviderInfo, error)
	ListProviders(ctx context.Context) ([]*models.ProviderInfo, error)
	Register(ctx context.Context, providerIdent config.Identity, addrs []string) error

	// Sync with a data provider up to latest ID
	Sync(ctx context.Context, p peer.ID, cid cid.Cid) error
	// Subscribe to advertisements of a specific provider in the pubsub channel
	Subscribe(ctx context.Context, p peer.ID) error
}
