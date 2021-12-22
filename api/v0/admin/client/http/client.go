package adminhttpclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"

	"github.com/filecoin-project/storetheindex/internal/httpclient"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
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
		var errMsg string
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) != 0 {
			errMsg = ": " + string(body)
		}
		return fmt.Errorf("importing from manifest failed: %v%s", http.StatusText(resp.StatusCode), errMsg)
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
		var errMsg string
		body, err := io.ReadAll(resp.Body)
		if err == nil && len(body) != 0 {
			errMsg = ": " + string(body)
		}
		return fmt.Errorf("importing from cidlist failed: %v%s", http.StatusText(resp.StatusCode), errMsg)
	}
	log.Infow("Success")
	return nil
}

// Sync with a data provider up to latest ID.
func (c *Client) Sync(ctx context.Context, provID peer.ID, provAddr multiaddr.Multiaddr) error {
	var data []byte
	var err error
	if provAddr != nil {
		data, err = provAddr.MarshalJSON()
		if err != nil {
			return err
		}
	}

	return c.ingestRequest(ctx, provID, "sync", http.MethodPost, data)
}

// Allow advertisements from the specified provider from the pubsub channel.
func (c *Client) Allow(ctx context.Context, provID peer.ID) error {
	return c.ingestRequest(ctx, provID, "allow", http.MethodPut, nil)
}

// Block advertisements from the specified provider from the pubsub channel.
func (c *Client) Block(ctx context.Context, provID peer.ID) error {
	return c.ingestRequest(ctx, provID, "block", http.MethodPut, nil)
}

func (c *Client) ListLogSubSystems(ctx context.Context) ([]string, error) {
	u := c.baseURL + "/config/log/subsystems"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpclient.ReadErrorFrom(resp.StatusCode, resp.Body)
	}

	scanner := bufio.NewScanner(resp.Body)
	var subsystems []string
	for scanner.Scan() {
		subsystems = append(subsystems, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return subsystems, nil
}

func (c *Client) SetLogLevels(ctx context.Context, sysLvl map[string]string) error {
	u := c.baseURL + "/config/log/level"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}

	q := url.Values{}
	for ss, l := range sysLvl {
		q.Add(ss, l)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return httpclient.ReadErrorFrom(resp.StatusCode, resp.Body)
	}
	return nil
}

func (c *Client) ingestRequest(ctx context.Context, provID peer.ID, action, method string, data []byte) error {
	u := c.baseURL + path.Join(ingestResource, action, provID.String())

	var body io.Reader
	if data != nil {
		body = bytes.NewBuffer(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return httpclient.ReadErrorFrom(resp.StatusCode, resp.Body)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}
