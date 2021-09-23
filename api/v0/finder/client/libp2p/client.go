package finderp2pclient

import (
	"context"
	"fmt"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/internal/libp2pclient"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type Finder struct {
	p2pc *libp2pclient.Client
}

func NewFinder(ctx context.Context, peerID peer.ID, options ...libp2pclient.Option) (*Finder, error) {
	client, err := libp2pclient.NewClient(ctx, peerID, v0.FinderProtocolID, options...)
	if err != nil {
		return nil, err
	}
	return &Finder{
		p2pc: client,
	}, nil
}

func (cl *Finder) Find(ctx context.Context, m multihash.Multihash) (*models.FindResponse, error) {
	return cl.FindBatch(ctx, []multihash.Multihash{m})
}

func (cl *Finder) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*models.FindResponse, error) {
	if len(mhs) == 0 {
		return &models.FindResponse{}, nil
	}

	data, err := models.MarshalFindRequest(&models.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req := &pb.FinderMessage{
		Type: pb.FinderMessage_GET,
		Data: data,
	}

	data, err = cl.sendRecv(ctx, req, pb.FinderMessage_GET_RESPONSE)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalFindResponse(data)
}

func (cl *Finder) sendRecv(ctx context.Context, req *pb.FinderMessage, expectRspType pb.FinderMessage_MessageType) ([]byte, error) {
	resp := new(pb.FinderMessage)
	err := cl.p2pc.SendRequest(ctx, req, func(data []byte) error {
		return resp.Unmarshal(data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send request to indexer: %s", err)
	}
	if resp.GetType() != expectRspType {
		if resp.GetType() == pb.FinderMessage_ERROR_RESPONSE {
			return nil, v0.DecodeError(resp.GetData())
		}
		return nil, fmt.Errorf("response type is not %s", expectRspType.String())
	}
	return resp.GetData(), nil
}
