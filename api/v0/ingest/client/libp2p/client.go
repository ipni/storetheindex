package p2pclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	pb "github.com/filecoin-project/storetheindex/api/v0/ingest/pb"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/libp2pclient"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Ingest struct {
	p2pc *libp2pclient.Client
}

func NewIngest(ctx context.Context, h host.Host, peerID peer.ID, options ...libp2pclient.ClientOption) (*Ingest, error) {
	client, err := libp2pclient.NewClient(ctx, h, peerID, v0.IngestProtocolID, options...)
	if err != nil {
		return nil, err
	}
	return &Ingest{
		p2pc: client,
	}, nil
}

func (cl *Ingest) ListProviders(ctx context.Context) ([]*models.ProviderInfo, error) {
	req := &pb.IngestMessage{
		Type: pb.IngestMessage_LIST_PROVIDERS,
	}

	data, err := cl.sendRecv(ctx, req, pb.IngestMessage_LIST_PROVIDERS_RESPONSE)
	if err != nil {
		return nil, err
	}

	var providers []*models.ProviderInfo
	err = json.Unmarshal(data, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}

func (cl *Ingest) GetProvider(ctx context.Context, providerID peer.ID) (*models.ProviderInfo, error) {
	data, err := json.Marshal(providerID)
	if err != nil {
		return nil, err
	}

	req := &pb.IngestMessage{
		Type: pb.IngestMessage_GET_PROVIDER,
		Data: data,
	}

	data, err = cl.sendRecv(ctx, req, pb.IngestMessage_GET_PROVIDER_RESPONSE)
	if err != nil {
		return nil, err
	}

	var providerInfo models.ProviderInfo
	err = json.Unmarshal(data, &providerInfo)
	if err != nil {
		return nil, err
	}
	return &providerInfo, nil
}

func (cl *Ingest) Register(ctx context.Context, providerIdent config.Identity, addrs []string) error {
	regReq, err := models.MakeRegisterRequest(providerIdent, addrs)
	if err != nil {
		return err
	}
	data, err := json.Marshal(regReq)
	if err != nil {
		return err
	}

	req := &pb.IngestMessage{
		Type: pb.IngestMessage_REGISTER_PROVIDER,
		Data: data,
	}

	_, err = cl.sendRecv(ctx, req, pb.IngestMessage_REGISTER_PROVIDER_RESPONSE)
	if err != nil {
		return err
	}

	return nil
}

func (cl *Ingest) sendRecv(ctx context.Context, req *pb.IngestMessage, expectRspType pb.IngestMessage_MessageType) ([]byte, error) {
	resp := new(pb.IngestMessage)
	err := cl.p2pc.SendRequest(ctx, req, func(data []byte) error {
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

// Sync with a data provider up to latest ID
func (cl *Ingest) Sync(ctx context.Context, p peer.ID, cid cid.Cid) error {
	return errors.New("not implemented")
}

// Subscribe to advertisements of a specific provider in the pubsub channel
func (cl *Ingest) Subscribe(ctx context.Context, p peer.ID) error {
	return errors.New("not implemented")
}
