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

// Discovery is the interface that supplies functionality to discover providers
type Discovery interface {
	Discover(ctx context.Context, filecoinAddr string, signature, signed []byte) (*Discovered, error)
}

// Discovered holds information about a provider that is discovered
type Discovered struct {
	AddrInfo peer.AddrInfo
	Type     int
}
