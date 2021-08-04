package config

import "github.com/libp2p/go-libp2p-core/peer"

const (
	defaultTopic = "ContentIndex"
)

// Discovery holds addresses of peers to connect to for subscribing to a pubsub
// channel to receive index advertisements
type Discovery struct {
	// Bootstrap is a Set of nodes to try to connect to at startup
	Bootstrap []string
	// Peers lists nodes to attempt to stay connected with
	Peers []peer.AddrInfo
	// Topic is the pubsub topic to subscribe to
	Topic string
}
