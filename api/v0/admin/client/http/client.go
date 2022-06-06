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
	"strconv"

	"github.com/filecoin-project/storetheindex/api/v0/httpclient"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
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
	u, c, err := httpclient.New(baseURL, "", options...)
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
	return nil
}

// Sync with a data peer up to the latest ID.
func (c *Client) Sync(ctx context.Context, peerID peer.ID, peerAddr multiaddr.Multiaddr, depth int64, resync bool) error {
	var data []byte
	var err error
	if peerAddr != nil {
		data, err = peerAddr.MarshalJSON()
		if err != nil {
			return err
		}
	}

	var q []string
	// Only set the depth parameter if it is not zero, since zero
	// means "use the limit configured in config.Ingest".
	// Note that the value -1 means no-limit.
	if depth != 0 {
		q = append(q, "depth", strconv.FormatInt(depth, 10))
	}

	// Only set if true, since by default the latest sync is not ignored.
	if resync {
		q = append(q, "resync", strconv.FormatBool(resync))
	}

	return c.ingestRequest(ctx, peerID, "sync", http.MethodPost, data, q...)
}

// ReloadConfig reloads reloadable parts of the configuration file.
func (c *Client) ReloadConfig(ctx context.Context) error {
	u := c.baseURL + "/reloadconfig"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpclient.ReadErrorFrom(resp.StatusCode, resp.Body)
	}

	return nil
}

// Allow configures the indexer to allow the peer to publish messages and
// provide content.
func (c *Client) Allow(ctx context.Context, peerID peer.ID) error {
	return c.ingestRequest(ctx, peerID, "allow", http.MethodPut, nil)
}

// Block configures indexer to block the peer from publishing messages and
// providing content.
func (c *Client) Block(ctx context.Context, peerID peer.ID) error {
	return c.ingestRequest(ctx, peerID, "block", http.MethodPut, nil)
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return httpclient.ReadErrorFrom(resp.StatusCode, resp.Body)
	}
	return nil
}

func (c *Client) ingestRequest(ctx context.Context, peerID peer.ID, action, method string, data []byte, queryPairs ...string) error {
	u := c.baseURL + path.Join(ingestResource, action, peerID.String())

	var body io.Reader
	if data != nil {
		body = bytes.NewBuffer(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}

	qpLen := len(queryPairs)
	if qpLen%2 != 0 {
		return fmt.Errorf("number of query pairs must be even; got %d", qpLen)
	}

	values := req.URL.Query()
	for i := 0; i < qpLen; i += 2 {
		values.Add(queryPairs[i], queryPairs[i+1])
	}
	req.URL.RawQuery = values.Encode()

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
