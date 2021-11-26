package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
	defaultMinimumPeers     = 1
	defaultBootstrapPeriod  = 30 * time.Second
	defaulConnectionTimeout = defaultBootstrapPeriod / 3
)

// defaultBootstrapAddresses are the hardcoded bootstrap addresses.
var defaultBootstrapAddresses = []string{}

// Bootstrap configures other nodes to connect to for the purpose of exchanging
// gossip pubsub.  The nodes listed here must be running pubsub and must also
// be subscribed to the indexer/ingest topic.  The peers can be other indexers,
// or IPFS nodes with pubsub enabled and subscribed to the topic.
type Bootstrap struct {
	// Peers is the local node's bootstrap peer addresses
	Peers []string
	// MinimumPeers governs whether to bootstrap more connections. If the node
	// has less open connections than this number, it will open connections to
	// the bootstrap nodes.  Set to 0 to disable bootstrapping.
	MinimumPeers int
}

// ErrInvalidPeerAddr signals an address is not a valid peer address.
var ErrInvalidPeerAddr = errors.New("invalid peer address")

// PeerAddrs returns the bootstrap peers as a list of AddrInfo.
func (b Bootstrap) PeerAddrs() ([]peer.AddrInfo, error) {
	return parsePeers(b.Peers)
}

// SetPeers sets the bootstrap peers from a list of AddrInfo.
func (b *Bootstrap) SetPeers(addrs []peer.AddrInfo) {
	b.Peers = addrsToPeers(addrs)
}

// defaultBootstrapPeers returns the (parsed) set of default bootstrap peers.
// Panics on failure as that is a problem with the hardcoded addresses.
func defaultBootstrapPeers() []string {
	addrs, err := parsePeers(defaultBootstrapAddresses)
	if err != nil {
		panic(fmt.Sprintf("failed to parse hardcoded bootstrap peers: %s", err))
	}
	return addrsToPeers(addrs)
}

// parsePeers parses a peer list into a list of AddrInfo.
func parsePeers(addrs []string) ([]peer.AddrInfo, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, err
		}
	}
	return peer.AddrInfosFromP2pAddrs(maddrs...)
}

// addrsToPeers formats a list of AddrInfos as a peer list suitable for
// serialization.
func addrsToPeers(addrs []peer.AddrInfo) []string {
	peers := make([]string, 0, len(addrs))
	for _, pi := range addrs {
		addrs, err := peer.AddrInfoToP2pAddrs(&pi)
		if err != nil {
			// programmer error.
			panic(err)
		}
		for _, addr := range addrs {
			peers = append(peers, addr.String())
		}
	}
	return peers
}
