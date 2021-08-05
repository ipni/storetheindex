package p2pclient

import (
	"context"
	"errors"

	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/finder"
	"github.com/filecoin-project/storetheindex/server/net"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("p2pclient")

var _ finder.Interface = &p2pclient{}

// p2pclient is responsible for sending
// requests to other peers.
type p2pclient struct {
	ctx       context.Context
	host      host.Host
	self      peer.ID
	protocols []protocol.ID

	senderManager *messageSender
}

// New creates a new p2pclient
func New(ctx context.Context, h host.Host, options ...ClientOption) (*p2pclient, error) {
	var cfg clientConfig
	if err := cfg.apply(append([]ClientOption{clientDefaults}, options...)...); err != nil {
		return nil, err
	}
	protocols := []protocol.ID{pid}

	// Start a client
	e := &p2pclient{
		ctx:       ctx,
		host:      h,
		self:      h.ID(),
		protocols: protocols,

		senderManager: &messageSender{
			host:      h,
			strmap:    make(map[peer.ID]*peerMessageSender),
			protocols: protocols,
		},
	}

	return e, nil
}

func (cl *p2pclient) Get(ctx context.Context, c cid.Cid, e net.Endpoint) (*models.Response, error) {
	return cl.get(ctx, []cid.Cid{c}, e)
}

func (cl *p2pclient) GetBatch(ctx context.Context, cs []cid.Cid, e net.Endpoint) (*models.Response, error) {
	return cl.get(ctx, cs, e)
}

func (cl *p2pclient) get(ctx context.Context, cs []cid.Cid, e net.Endpoint) (*models.Response, error) {
	data, err := models.MarshalReq(&models.Request{Cids: cs})
	if err != nil {
		return nil, err
	}
	req := &pb.Message{
		Type: pb.Message_GET,
		Data: data,
	}
	end, ok := e.Addr().(peer.ID)
	if !ok {
		return nil, errors.New("endpoint is of wrong type for libp2p client. Use peer.ID")
	}

	resp, err := cl.senderManager.SendRequest(ctx, end, req)
	if err != nil {
		return nil, err
	}
	if resp.GetType() != pb.Message_RESPONSE {
		return nil, errors.New("message type received not a Response")
	}

	return models.UnmarshalResp(resp.GetData())

}
