package finderhttpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/internal/httpclient"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("finderhttpclient")

const (
	finderResource = "multihash"
	finderPort     = 3000
)

// Client is an http client for the indexer finder API
type Client struct {
	c       *http.Client
	baseURL string
}

// New creates a new finder HTTP client.
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	u, c, err := httpclient.New(baseURL, finderResource, finderPort, options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		c:       c,
		baseURL: u.String(),
	}, nil
}

// Find queries indexer entries for a multihash
func (c *Client) Find(ctx context.Context, m multihash.Multihash) (*model.FindResponse, error) {
	u := c.baseURL + "/" + m.B58String()
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return c.sendRequest(req)
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
			log.Info("Entry not found in indexer")
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
