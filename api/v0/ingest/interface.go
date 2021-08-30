package ingestion

import (
	"context"

	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Ingester is the interface implemented to subscribe to
// advertisements from a provider
type Ingester interface {
	// Sync with a data provider up to latest ID
	Sync(ctx context.Context, p peer.ID) error

	// Subscribe to advertisements of a specific provider in the pubsub channel
	Subscribe(ctx context.Context, p peer.ID) error

	// Unsubscribe to stop listening to advertisement from a specific provider.
	Unsubscribe(ctx context.Context, p peer.ID) error
}
