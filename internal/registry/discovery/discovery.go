package discovery

import (
	"context"

	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	// Provider types
	OtherType = iota
	MinerType
)

// Discoverer is the interface that supplies functionality to discover providers
type Discoverer interface {
	Discover(ctx context.Context, peerID peer.ID, discoveryAddr string) (*Discovered, error)
}

// Discovered holds information about a provider that is discovered
type Discovered struct {
	AddrInfo peer.AddrInfo
	Type     int
}
