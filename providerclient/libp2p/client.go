package p2pclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/storetheindex/internal/libp2pclient"
	"github.com/filecoin-project/storetheindex/providerclient"
	pb "github.com/filecoin-project/storetheindex/providerclient/libp2p/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Client struct {
	p2pc *libp2pclient.Client
}

func New(p2pHost host.Host, peerID peer.ID) (*Client, error) {
	client, err := libp2pclient.New(p2pHost, peerID, client.ProviderProtocolID)
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

func (c *Client) Close() error {
	return c.p2pc.Close()
}

func (c *Client) GetLatestAdv(ctx context.Context) (*client.AdResponse, error) {
	req := &pb.ProviderMessage{
		Type: pb.ProviderMessage_GET_LATEST,
	}

	data, err := c.sendRecv(ctx, req, pb.ProviderMessage_AD_RESPONSE)
	if err != nil {
		return nil, err
	}

	return client.UnmarshalAdResponse(data)
}

func (c *Client) GetAdv(ctx context.Context, id cid.Cid) (*client.AdResponse, error) {
	data, err := client.MarshalAdRequest(&client.AdRequest{ID: id})
	if err != nil {
		return nil, err
	}
	req := &pb.ProviderMessage{
		Type: pb.ProviderMessage_GET_AD,
		Data: data,
	}

	data, err = c.sendRecv(ctx, req, pb.ProviderMessage_AD_RESPONSE)
	if err != nil {
		return nil, err
	}

	return client.UnmarshalAdResponse(data)
}

func (c *Client) sendRecv(ctx context.Context, req *pb.ProviderMessage, expectRspType pb.ProviderMessage_MessageType) ([]byte, error) {
	resp := new(pb.ProviderMessage)
	err := c.p2pc.SendRequest(ctx, req, func(data []byte) error {
		return resp.Unmarshal(data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send request to provider: %s", err)
	}
	if resp.GetType() != expectRspType {
		if resp.GetType() == pb.ProviderMessage_ERROR_RESPONSE {
			return nil, errors.New(string(resp.GetData()))
		}
		return nil, fmt.Errorf("response type is not %s", expectRspType.String())
	}
	return resp.GetData(), nil
}
