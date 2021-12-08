package ingesthttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/model"
	httpclient "github.com/filecoin-project/storetheindex/internal/httpclient"
	p2pcrypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	ingestPort       = 3001
	providersPath    = "/providers"
	indexContentPath = "/ingest/content"
)

// Client is an http client for the indexer ingest API
type Client struct {
	c               *http.Client
	indexContentURL string
	providersURL    string
}

// New creates a new ingest http Client
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	u, c, err := httpclient.New(baseURL, "", ingestPort, options...)
	if err != nil {
		return nil, err
	}
	baseURL = u.String()
	return &Client{
		c:               c,
		indexContentURL: baseURL + indexContentPath,
		providersURL:    baseURL + providersPath,
	}, nil
}

func (c *Client) ListProviders(ctx context.Context) ([]*model.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.providersURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
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

	var providers []*model.ProviderInfo
	err = json.Unmarshal(body, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}

func (c *Client) GetProvider(ctx context.Context, providerID peer.ID) (*model.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.providersURL+"/"+providerID.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
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

	var providerInfo model.ProviderInfo
	err = json.Unmarshal(body, &providerInfo)
	if err != nil {
		return nil, err
	}
	return &providerInfo, nil
}

func (c *Client) IndexContent(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, m multihash.Multihash, contextID []byte, metadata v0.Metadata, addrs []string) error {
	data, err := model.MakeIngestRequest(providerID, privateKey, m, contextID, metadata, addrs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.indexContentURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.c.Do(req)
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

func (c *Client) Register(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, addrs []string) error {
	data, err := model.MakeRegisterRequest(providerID, privateKey, addrs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.providersURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.c.Do(req)
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
