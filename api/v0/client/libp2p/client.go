package p2pclient

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("p2pclient")

// p2pclient is responsible for sending
// requests to other peers.
type p2pclient struct {
	ctx       context.Context
	host      host.Host
	self      peer.ID
	peerID    peer.ID
	protocols []protocol.ID

	senderManager *messageSender
}

// newClient creates a new p2pclient
func newClient(ctx context.Context, h host.Host, peerID peer.ID, options ...ClientOption) (*p2pclient, error) {
	var cfg clientConfig
	if err := cfg.apply(options...); err != nil {
		return nil, err
	}
	protocols := []protocol.ID{pid}

	// Start a client
	return &p2pclient{
		ctx:       ctx,
		host:      h,
		self:      h.ID(),
		peerID:    peerID,
		protocols: protocols,

		senderManager: &messageSender{
			host:      h,
			strmap:    make(map[peer.ID]*peerMessageSender),
			protocols: protocols,
		},
	}, nil
}
