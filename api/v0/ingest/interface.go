package ingestion

import (
	"context"

	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Ingester is the interface implemented to subscribe to
// advertisements from a provider
type Ingester interface {
	// Sync with a data provider up to latest ID
	Sync(ctx context.Context, p peer.ID, cid cid.Cid) error

	// Subscribe to advertisements of a specific provider in the pubsub channel
	Subscribe(ctx context.Context, p peer.ID) error
}
