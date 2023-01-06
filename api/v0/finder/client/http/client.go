package finderhttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/api/v0/httpclient"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	finderPath    = "/multihash"
	providersPath = "/providers"
	statsPath     = "/stats"
)

// Client is an http client for the indexer finder API
type Client struct {
	c            *http.Client
	finderURL    string
	providersURL string
	statsURL     string
}

// New creates a new finder HTTP client.
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	u, c, err := httpclient.New(baseURL, "", options...)
	if err != nil {
		return nil, err
	}
	baseURL = u.String()
	return &Client{
		c:            c,
		finderURL:    baseURL + finderPath,
		providersURL: baseURL + providersPath,
		statsURL:     baseURL + statsPath,
	}, nil
}

// Find queries indexer entries for a multihash
func (c *Client) Find(ctx context.Context, m multihash.Multihash) (*model.FindResponse, error) {
	u := fmt.Sprint(c.finderURL, "/", m.B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	return c.sendRequest(req)
}

// FindBatch queries indexer entries for a batch of multihashes
func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}
	data, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.finderURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return c.sendRequest(req)
}

func (c *Client) ListProviders(ctx context.Context, withExtMetadata bool) ([]*model.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.providersURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	if withExtMetadata {
		q := url.Values{}
		q.Add("metadata", "true")
		req.URL.RawQuery = q.Encode()
	}

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
	u := fmt.Sprint(c.providersURL, "/", providerID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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

func (c *Client) GetStats(ctx context.Context) (*model.Stats, error) {
	u := fmt.Sprint(c.statsURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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

	return model.UnmarshalStats(body)
}

func (c *Client) sendRequest(req *http.Request) (*model.FindResponse, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return &model.FindResponse{}, nil
		}
		return nil, fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalFindResponse(b)
}
