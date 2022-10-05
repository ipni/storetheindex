package config

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

// Peering configures the peering service. Peering is similar to bootstrapping,
// but peering maintains connection with all peers configured in the peering
// service.
type Peering struct {
	// Peers lists the nodes to attempt to stay connected with.
	Peers []string
}

// NewPeering returns Peering with values set to their defaults.
func NewPeering() Peering {
	return Peering{}
}

// PeerAddrs returns the peering peers as a list of AddrInfo.
func (p Peering) PeerAddrs() ([]peer.AddrInfo, error) {
	return parsePeers(p.Peers)
}
