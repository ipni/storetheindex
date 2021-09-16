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
)

type Provider struct {
	p2pc *libp2pclient.Client
}

func NewProvider(ctx context.Context, h host.Host, p peer.ID, options ...libp2pclient.ClientOption) (*Provider, error) {
	client, err := libp2pclient.NewClient(h, p, client.ProviderProtocolID, options...)
	if err != nil {
		return nil, err
	}
	return &Provider{
		p2pc: client,
	}, nil
}

func (cl *Provider) Close() error {
	return cl.p2pc.Close()
}

func (cl *Provider) GetLatestAdv(ctx context.Context) (*client.AdResponse, error) {
	req := &pb.ProviderMessage{
		Type: pb.ProviderMessage_GET_LATEST,
	}

	data, err := cl.sendRecv(ctx, req, pb.ProviderMessage_AD_RESPONSE)
	if err != nil {
		return nil, err
	}

	return client.UnmarshalAdResponse(data)
}

func (cl *Provider) GetAdv(ctx context.Context, id cid.Cid) (*client.AdResponse, error) {
	data, err := client.MarshalAdRequest(&client.AdRequest{ID: id})
	if err != nil {
		return nil, err
	}
	req := &pb.ProviderMessage{
		Type: pb.ProviderMessage_GET_AD,
		Data: data,
	}

	data, err = cl.sendRecv(ctx, req, pb.ProviderMessage_AD_RESPONSE)
	if err != nil {
		return nil, err
	}

	return client.UnmarshalAdResponse(data)
}

func (cl *Provider) sendRecv(ctx context.Context, req *pb.ProviderMessage, expectRspType pb.ProviderMessage_MessageType) ([]byte, error) {
	resp := new(pb.ProviderMessage)
	err := cl.p2pc.SendRequest(ctx, req, func(data []byte) error {
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
