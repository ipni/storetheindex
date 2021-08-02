package p2pclient

import (
	"context"
	"errors"

	"github.com/filecoin-project/storetheindex/client"
	pb "github.com/filecoin-project/storetheindex/client/libp2p/pb"
	"github.com/filecoin-project/storetheindex/client/models"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("p2pclient")

var _ client.Interface = &p2pclient{}
var _ client.Endpoint = P2pEndpoint("")

// P2pEndpoint wraps a peer.ID to use in client interface
type P2pEndpoint peer.ID

//NewEndpoint creates a new HTTPEndpoint
func NewEndpoint(s string) (P2pEndpoint, error) {
	p, err := peer.Decode(s)
	if err != nil {
		return P2pEndpoint(peer.ID("")), err
	}
	return P2pEndpoint(p), nil
}

// Addr for p2pEndpoint
func (end P2pEndpoint) Addr() interface{} {
	return peer.ID(end)
}

// p2pclient is responsible for sending
// requests to other peers.
type p2pclient struct {
	ctx       context.Context
	host      host.Host
	self      peer.ID
	protocols []protocol.ID

	senderManager *messageSenderImpl
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

		senderManager: &messageSenderImpl{
			host:      h,
			strmap:    make(map[peer.ID]*peerMessageSender),
			protocols: protocols,
		},
	}

	return e, nil
}

func (cl *p2pclient) Get(ctx context.Context, c cid.Cid, e client.Endpoint) (*models.Response, error) {
	return cl.get(ctx, []cid.Cid{c}, e)
}

func (cl *p2pclient) GetBatch(ctx context.Context, cs []cid.Cid, e client.Endpoint) (*models.Response, error) {
	return cl.get(ctx, cs, e)
}

func (cl *p2pclient) get(ctx context.Context, cs []cid.Cid, e client.Endpoint) (*models.Response, error) {
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
