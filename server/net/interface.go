package net

import "github.com/libp2p/go-libp2p-core/peer"

var _ Endpoint = HTTPEndpoint("")
var _ Endpoint = P2PEndpoint("")

// Endpoint of an indexer network server.
// According to the protocol implementation, endpoints may
// differ. We currently support two main protocols http and libp2p
type Endpoint interface {
	Addr() interface{}
}

// P2PEndpoint wraps a peer.ID to use in client interface
type P2PEndpoint peer.ID

//NewP2PEndpoint creates a new HTTPEndpoint
func NewP2PEndpoint(s string) (P2PEndpoint, error) {
	p, err := peer.Decode(s)
	if err != nil {
		return P2PEndpoint(peer.ID("")), err
	}
	return P2PEndpoint(p), nil
}

// Addr for p2pEndpoint
func (end P2PEndpoint) Addr() interface{} {
	return peer.ID(end)
}

// HTTPEndpoint wraps a url to use in client interface
type HTTPEndpoint string

//NewHTTPEndpoint creates a new HTTPEndpoint
func NewHTTPEndpoint(s string) HTTPEndpoint {
	return HTTPEndpoint(s)
}

// Addr for p2pEndpoint
func (end HTTPEndpoint) Addr() interface{} {
	return string(end)
}
