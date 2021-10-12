package client

import (
	"context"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/model"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// Ingest is the interface implemented by all ingest client protocols
type Ingest interface {
	GetProvider(ctx context.Context, providerID peer.ID) (*model.ProviderInfo, error)
	ListProviders(ctx context.Context) ([]*model.ProviderInfo, error)
	Register(ctx context.Context, providerID peer.ID, privateKey crypto.PrivKey, addrs []string) error
	IndexContent(ctx context.Context, providerID peer.ID, privateKey crypto.PrivKey, m multihash.Multihash, contextID []byte, metadata indexer.Metadata, addrs []string) error
}
