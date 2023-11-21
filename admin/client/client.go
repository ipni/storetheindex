package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/storetheindex/admin/model"
	"github.com/ipni/storetheindex/rate"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
	assignedPath        = "assigned"
	freezePath          = "freeze"
	importPath          = "import"
	importProvidersPath = "importproviders"
	ingestPath          = "ingest"
	preferredPath       = "preferred"
	reloadConfigPath    = "reloadconfig"
	statusPath          = "status"
	telemetryPath       = "telemetry/providers"
)

// Client is an http client for the indexer finder API,
type Client struct {
	c       *http.Client
	baseURL *url.URL
}

// New creates a new admin HTTP client.
func New(baseURL string, options ...Option) (*Client, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = ""

	return &Client{
		c:       opts.httpClient,
		baseURL: u,
	}, nil
}

func (c *Client) Freeze(ctx context.Context) error {
	u := c.baseURL.JoinPath(freezePath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return apierror.FromResponse(resp.StatusCode, body)
	}

	return nil
}

func (c *Client) GetPendingSyncs(ctx context.Context) ([]string, error) {
	u := c.baseURL.JoinPath(ingestPath, "sync")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
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
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var peers []string
	err = json.Unmarshal(body, &peers)
	if err != nil {
		return nil, err
	}

	return peers, nil
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

// ImportProviders
func (c *Client) ImportProviders(ctx context.Context, fromURL *url.URL) error {
	if fromURL == nil || fromURL.String() == "" {
		return errors.New("missing indexer url")
	}

	binURL, err := fromURL.MarshalBinary()
	if err != nil {
		return err
	}
	params := map[string][]byte{
		"indexer": binURL,
	}
	bodyData, err := json.Marshal(&params)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(bodyData)

	u := c.baseURL.JoinPath(importProvidersPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return apierror.FromResponse(resp.StatusCode, body)
	}

	return nil
}

// ReloadConfig reloads reloadable parts of the configuration file.
func (c *Client) ReloadConfig(ctx context.Context) error {
	u := c.baseURL.JoinPath(reloadConfigPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return apierror.FromResponse(resp.StatusCode, body)
	}

	return nil
}

// ListAssignedPeers gets a list of explicitly allowed peers, if indexer is
// configured to work with an assigner service.
func (c *Client) ListAssignedPeers(ctx context.Context) (map[peer.ID]peer.ID, error) {
	u := c.baseURL.JoinPath(ingestPath, assignedPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var assigned []model.Assigned
	err = json.Unmarshal(body, &assigned)
	if err != nil {
		return nil, err
	}

	assignedMap := make(map[peer.ID]peer.ID, len(assigned))
	for i := range assigned {
		assignedMap[assigned[i].Publisher] = assigned[i].Continued
	}

	return assignedMap, nil
}

// ListPreferredPeers gets a list of unassigned peers that the indexer has
// previously retrieved advertisements from.
func (c *Client) ListPreferredPeers(ctx context.Context) ([]peer.ID, error) {
	u := c.baseURL.JoinPath(ingestPath, preferredPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var peers []peer.ID
	err = json.Unmarshal(body, &peers)
	if err != nil {
		return nil, err
	}

	return peers, nil
}

// Assign assigns a publish to an indexer, when the indexer is configured to
// work with an assigner service.
func (c *Client) Assign(ctx context.Context, peerID peer.ID) error {
	return c.ingestRequest(ctx, peerID, "assign", http.MethodPost, nil)
}

func (c *Client) Handoff(ctx context.Context, publisherID, frozenID peer.ID, frozenURL string) error {
	handoff := model.Handoff{
		FrozenID:  frozenID,
		FrozenURL: frozenURL,
	}

	data, err := json.Marshal(&handoff)
	if err != nil {
		return err
	}

	return c.ingestRequest(ctx, publisherID, "handoff", http.MethodPost, data)
}

// Unassign unassigns a publish from an indexer, when the indexer is configured
// to work with an assigner service.
func (c *Client) Unassign(ctx context.Context, peerID peer.ID) error {
	return c.ingestRequest(ctx, peerID, "unassign", http.MethodPut, nil)
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
	u := c.baseURL.JoinPath("config", "log", "subsystems")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, apierror.FromResponse(resp.StatusCode, body)
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
	u := c.baseURL.JoinPath("config", "log", "level")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), nil)
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
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return apierror.FromResponse(resp.StatusCode, body)
	}

	return nil
}

func (c *Client) Status(ctx context.Context) (*model.Status, error) {
	u := c.baseURL.JoinPath(statusPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
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
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var status model.Status
	err = json.Unmarshal(body, &status)
	if err != nil {
		return nil, err
	}

	return &status, nil
}

func (c *Client) GetAllTelemetry(ctx context.Context) (map[string]rate.Rate, error) {
	u := c.baseURL.JoinPath(telemetryPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
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

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, apierror.FromResponse(resp.StatusCode, body)
	}

	var ingestRates map[string]rate.Rate
	err = json.Unmarshal(body, &ingestRates)
	if err != nil {
		return nil, err
	}

	return ingestRates, nil
}

func (c *Client) GetTelemetry(ctx context.Context, providerID peer.ID) (rate.Rate, bool, error) {
	u := c.baseURL.JoinPath(telemetryPath, providerID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return rate.Rate{}, false, err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return rate.Rate{}, false, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return rate.Rate{}, false, err
	}

	if resp.StatusCode == http.StatusNoContent {
		return rate.Rate{}, false, nil
	}

	if resp.StatusCode != http.StatusOK {
		return rate.Rate{}, false, apierror.FromResponse(resp.StatusCode, body)
	}

	var ingestRate rate.Rate
	err = json.Unmarshal(body, &ingestRate)
	if err != nil {
		return rate.Rate{}, false, err
	}

	return ingestRate, true, nil
}

func (c *Client) ingestRequest(ctx context.Context, peerID peer.ID, action, method string, data []byte, queryPairs ...string) error {
	var body io.Reader
	if data != nil {
		body = bytes.NewBuffer(data)
	}

	u := c.baseURL.JoinPath(ingestPath, action, peerID.String())
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}

	if len(queryPairs) != 0 {
		qpLen := len(queryPairs)
		if qpLen%2 != 0 {
			return fmt.Errorf("number of query pairs must be even; got %d", qpLen)
		}

		values := req.URL.Query()
		for i := 0; i < qpLen; i += 2 {
			values.Add(queryPairs[i], queryPairs[i+1])
		}
		req.URL.RawQuery = values.Encode()
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return apierror.FromResponse(resp.StatusCode, body)
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
