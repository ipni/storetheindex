package p2pclient

import (
	"context"
	"fmt"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/model"
	pb "github.com/filecoin-project/storetheindex/api/v0/ingest/pb"
	"github.com/filecoin-project/storetheindex/api/v0/libp2pclient"
	"github.com/ipfs/go-cid"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

type Client struct {
	p2pc *libp2pclient.Client
}

func New(p2pHost host.Host, peerID peer.ID) (*Client, error) {
	client, err := libp2pclient.New(p2pHost, peerID, v0.IngestProtocolID)
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

func (c *Client) Register(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, addrs []string) error {
	data, err := model.MakeRegisterRequest(providerID, privateKey, addrs)
	if err != nil {
		return err
	}

	req := &pb.IngestMessage{
		Type: pb.IngestMessage_REGISTER_PROVIDER,
		Data: data,
	}

	_, err = c.sendRecv(ctx, req, pb.IngestMessage_REGISTER_PROVIDER_RESPONSE)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) IndexContent(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, m multihash.Multihash, contextID []byte, metadata []byte, addrs []string) error {
	data, err := model.MakeIngestRequest(providerID, privateKey, m, contextID, metadata, addrs)
	if err != nil {
		return err
	}

	req := &pb.IngestMessage{
		Type: pb.IngestMessage_INDEX_CONTENT,
		Data: data,
	}

	_, err = c.sendRecv(ctx, req, pb.IngestMessage_INDEX_CONTENT_RESPONSE)
	if err != nil {
		return err
	}

	return nil
}

// Deprecated: Use gossip sub instead for sending announce message,
func (c *Client) Announce(ctx context.Context, provider *peer.AddrInfo, root cid.Cid) error {
	return fmt.Errorf("note implemented")
}

func (c *Client) sendRecv(ctx context.Context, req *pb.IngestMessage, expectRspType pb.IngestMessage_MessageType) ([]byte, error) {
	resp := new(pb.IngestMessage)
	err := c.p2pc.SendRequest(ctx, req, func(data []byte) error {
		return resp.Unmarshal(data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send request to indexer: %s", err)
	}
	if resp.GetType() != expectRspType {
		if resp.GetType() == pb.IngestMessage_ERROR_RESPONSE {
			return nil, v0.DecodeError(resp.GetData())
		}
		return nil, fmt.Errorf("response type is not %s", expectRspType.String())
	}
	return resp.GetData(), nil
}
