package finderp2pclient

import (
	"context"
	"fmt"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/libp2pclient"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Finder struct {
	p2pc *libp2pclient.Client
}

func NewFinder(ctx context.Context, h host.Host, peerID peer.ID, options ...libp2pclient.ClientOption) (*Finder, error) {
	client, err := libp2pclient.NewClient(ctx, h, peerID, v0.FinderProtocolID, options...)
	if err != nil {
		return nil, err
	}
	return &Finder{
		p2pc: client,
	}, nil
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

	data, err = cl.sendRecv(ctx, req, pb.Message_GET_RESPONSE)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalResp(data)
}

func (cl *Finder) sendRecv(ctx context.Context, req *pb.Message, expectRspType pb.Message_MessageType) ([]byte, error) {
	resp := new(pb.Message)
	err := cl.p2pc.SendRequest(ctx, req, func(data []byte) error {
		return resp.Unmarshal(data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send request to indexer: %s", err)
	}
	if resp.GetType() != expectRspType {
		if resp.GetType() == pb.Message_ERROR_RESPONSE {
			return nil, v0.DecodeError(resp.GetData())
		}
		return nil, fmt.Errorf("response type is not %s", expectRspType.String())
	}
	return resp.GetData(), nil
}
