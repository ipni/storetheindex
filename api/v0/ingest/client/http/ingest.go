package ingesthttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/models"
	"github.com/filecoin-project/storetheindex/config"
	httpclient "github.com/filecoin-project/storetheindex/internal/httpclient"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

const (
	ingestPort       = 3001
	providersPath    = "/providers"
	indexContentPath = "/ingest/content"
)

// IndestClient is an http client for the indexer ingest API
type IngestClient struct {
	c               *http.Client
	indexContentURL string
	providersURL    string
}

// NewIngest creates a new IngestClient
func NewIngest(baseURL string, options ...httpclient.ClientOption) (*IngestClient, error) {
	u, c, err := httpclient.NewClient(baseURL, "", ingestPort, options...)
	if err != nil {
		return nil, err
	}
	baseURL = u.String()
	return &IngestClient{
		c:               c,
		indexContentURL: baseURL + indexContentPath,
		providersURL:    baseURL + providersPath,
	}, nil
}

func (cl *IngestClient) ListProviders(ctx context.Context) ([]*models.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", cl.providersURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := cl.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	var providers []*models.ProviderInfo
	err = json.Unmarshal(body, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}

func (cl *IngestClient) GetProvider(ctx context.Context, providerID peer.ID) (*models.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", cl.providersURL+"/"+providerID.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := cl.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	var providerInfo models.ProviderInfo
	err = json.Unmarshal(body, &providerInfo)
	if err != nil {
		return nil, err
	}
	return &providerInfo, nil
}

func (cl *IngestClient) IndexContent(ctx context.Context, providerIdent config.Identity, c cid.Cid, protocol uint64, metadata []byte) error {
	data, err := models.MakeIngestRequest(providerIdent, c, protocol, metadata)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cl.indexContentURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

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
		return httpclient.ReadError(resp.StatusCode, body)
	}
	return nil
}

func (cl *IngestClient) Register(ctx context.Context, providerIdent config.Identity, addrs []string) error {
	data, err := models.MakeRegisterRequest(providerIdent, addrs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cl.providersURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

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
		return httpclient.ReadError(resp.StatusCode, body)
	}
	return nil
}

// Sync with a data provider up to latest ID
func (cl *IngestClient) Sync(ctx context.Context, p peer.ID, cid cid.Cid) error {
	return errors.New("not implemented")
}

// Subscribe to advertisements of a specific provider in the pubsub channel
func (cl *IngestClient) Subscribe(ctx context.Context, p peer.ID) error {
	return errors.New("not implemented")
}
