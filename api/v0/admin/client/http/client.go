package adminhttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"

	"github.com/filecoin-project/storetheindex/internal/httpclient"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("adminhttpclient")

const (
	adminPort = 3002

	importResource = "/import"
	ingestResource = "/ingest"
)

// Client is an http client for the indexer finder API,
type Client struct {
	c       *http.Client
	baseURL string
}

// New creates a new admin HTTP client.
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	u, c, err := httpclient.New(baseURL, "", adminPort, options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		c:       c,
		baseURL: u.String(),
	}, nil
}

// ImportFromManifest processes entries from manifest and imports them into the
// indexer.
func (c *Client) ImportFromManifest(ctx context.Context, fileName string, provID peer.ID, contextID, metadata []byte) error {
	u := c.baseURL + path.Join(importResource, "manifest", provID.String())
	req, err := c.newUploadRequest(ctx, u, fileName, contextID, metadata)
	if err != nil {
		return err
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("importing from manifest failed: %v", http.StatusText(resp.StatusCode))
	}
	log.Infow("Success")
	return nil
}

// ImportFromCidList process entries from a cidlist and imprts it into the
// indexer.
func (c *Client) ImportFromCidList(ctx context.Context, fileName string, provID peer.ID, contextID, metadata []byte) error {
	u := c.baseURL + path.Join(importResource, "cidlist", provID.String())
	req, err := c.newUploadRequest(ctx, u, fileName, contextID, metadata)
	if err != nil {
		return err
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("importing from cidlist failed: %v", http.StatusText(resp.StatusCode))
	}
	log.Infow("Success")
	return nil
}

// Sync with a data provider up to latest ID.
func (c *Client) Sync(ctx context.Context, provID peer.ID) error {
	return c.ingestRequest(ctx, provID, "sync")
}

// Subscribe to advertisements of a specific provider in the pubsub channel
func (c *Client) Subscribe(ctx context.Context, provID peer.ID) error {
	return c.ingestRequest(ctx, provID, "subscribe")
}

// Unsubscribe from advertisements of a specific provider in the pubsub channel
func (c *Client) Unsubscribe(ctx context.Context, provID peer.ID) error {
	return c.ingestRequest(ctx, provID, "unsubscribe")
}

func (c *Client) ingestRequest(ctx context.Context, provID peer.ID, action string) error {
	u := c.baseURL + path.Join(ingestResource, action, provID.String())
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return httpclient.ReadError(resp.StatusCode, body)
	}

	return nil
}

func (c *Client) newUploadRequest(ctx context.Context, uri, fileName string, contextID, metadata []byte) (*http.Request, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	params := map[string][]byte{
		"file":       []byte(fileName),
		"context_id": contextID,
		"metadata":   metadata,
	}

	bodyData, err := json.Marshal(&params)
	if err != nil {
		return nil, err
	}

	body := bytes.NewBuffer(bodyData)

	req, err := http.NewRequestWithContext(ctx, "POST", uri, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
