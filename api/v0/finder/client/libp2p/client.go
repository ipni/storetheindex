package finderp2pclient

import (
	"context"
	"fmt"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	pb "github.com/filecoin-project/storetheindex/api/v0/finder/pb"
	"github.com/filecoin-project/storetheindex/api/v0/libp2pclient"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type Client struct {
	p2pc *libp2pclient.Client
}

func New(p2pHost host.Host, peerID peer.ID) (*Client, error) {
	client, err := libp2pclient.New(p2pHost, peerID, v0.FinderProtocolID)
	if err != nil {
		return nil, err
	}
	return &Client{
		p2pc: client,
	}, nil
}

// Connect connects the client to the host at the location specified by
// hostname.  The value of hostname is a host or host:port, where the host is a
// hostname or IP address.
func (c *Client) Connect(ctx context.Context, hostname string) error {
	return c.p2pc.Connect(ctx, hostname)
}

func (c *Client) ConnectAddrs(ctx context.Context, maddrs ...multiaddr.Multiaddr) error {
	return c.p2pc.ConnectAddrs(ctx, maddrs...)
}

func (c *Client) Find(ctx context.Context, m multihash.Multihash) (*model.FindResponse, error) {
	return c.FindBatch(ctx, []multihash.Multihash{m})
}

func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}

	data, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req := &pb.FinderMessage{
		Type: pb.FinderMessage_GET,
		Data: data,
	}

	data, err = c.sendRecv(ctx, req, pb.FinderMessage_GET_RESPONSE)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalFindResponse(data)
}

func (c *Client) sendRecv(ctx context.Context, req *pb.FinderMessage, expectRspType pb.FinderMessage_MessageType) ([]byte, error) {
	resp := new(pb.FinderMessage)
	err := c.p2pc.SendRequest(ctx, req, func(data []byte) error {
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
