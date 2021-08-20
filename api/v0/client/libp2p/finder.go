package p2pclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Finder p2pclient

func NewFinder(ctx context.Context, h host.Host, peerID peer.ID, options ...ClientOption) (*Finder, error) {
	client, err := newClient(ctx, h, peerID, options...)
	if err != nil {
		return nil, err
	}
	return (*Finder)(client), nil
}

func (cl *Finder) Get(ctx context.Context, c cid.Cid) (*models.Response, error) {
	return cl.GetBatch(ctx, []cid.Cid{c})
}

func (cl *Finder) GetBatch(ctx context.Context, cs []cid.Cid) (*models.Response, error) {
	data, err := models.MarshalReq(&models.Request{Cids: cs})
	if err != nil {
		return nil, err
	}
	req := &pb.Message{
		Type: pb.Message_GET,
		Data: data,
	}

	resp, err := cl.senderManager.SendRequest(ctx, cl.peerID, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request to indexer: %s", err)
	}
	if resp.GetType() != pb.Message_RESPONSE {
		return nil, errors.New("message type received is not a Response")
	}

	return models.UnmarshalResp(resp.GetData())

}
