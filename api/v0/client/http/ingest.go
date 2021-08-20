package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
	ingestPort        = 3001
	providersResource = "providers"
)

// IndestClient is an http client for the indexer ingest API
type IngestClient struct {
	c       *http.Client
	baseURL string
}

// NewIngest creates a new IngestClient
func NewIngest(baseURL string, options ...ClientOption) (*IngestClient, error) {
	u, c, err := newClient(baseURL, providersResource, ingestPort, options...)
	if err != nil {
		return nil, err
	}
	return &IngestClient{
		c:       c,
		baseURL: u.String(),
	}, nil
}

func (cl *IngestClient) Register(ctx context.Context, peerID string, privateKey ic.PrivKey, addrs []string) error {
	providerID, err := peer.Decode(peerID)
	if err != nil {
		return fmt.Errorf("could not decode peer id: %s", err)
	}

	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, m := range addrs {
		maddrs[i], err = multiaddr.NewMultiaddr(m)
		if err != nil {
			return fmt.Errorf("bad provider address: %s", err)
		}
	}

	regReq := &models.RegisterRequest{
		AddrInfo: peer.AddrInfo{
			ID:    providerID,
			Addrs: maddrs,
		},
	}

	if err = regReq.Sign(privateKey); err != nil {
		return fmt.Errorf("cannot sign request: %s", err)
	}

	data, err := json.Marshal(regReq)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cl.baseURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := cl.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return readError(resp.StatusCode, body)
	}
	return nil
}
