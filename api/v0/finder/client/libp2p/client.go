// Deprecated: The same functionality is provided by package
// github.com/ipni/go-libipni/find/client/p2p.
package finderp2pclient

import (
	"github.com/ipni/go-libipni/find/client/p2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Deprecated: Use github.com/ipni/go-libipni/find/client/p2p.Client instead.
type Client = p2pclient.Client

// Deprecated: Use github.com/ipni/go-libipni/find/client/p2p.New instead.
func New(p2pHost host.Host, peerID peer.ID) (*Client, error) {
	return p2pclient.New(p2pHost, peerID)
}
