package ingesthttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/go-legs/dtsync"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	httpclient "github.com/filecoin-project/storetheindex/api/v0/httpclient"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/model"
	"github.com/ipfs/go-cid"
	p2pcrypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

const (
	ingestPort       = 3001
	announcePath     = "/ingest/announce"
	registerPath     = "/register"
	indexContentPath = "/ingest/content"
)

// Client is an http client for the indexer ingest API
type Client struct {
	c               *http.Client
	indexContentURL string
	announceURL     string
	registerURL     string
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
		announceURL:     baseURL + announcePath,
		registerURL:     baseURL + registerPath,
	}, nil
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

// Announce a new root cid
func (c *Client) Announce(ctx context.Context, provider *peer.AddrInfo, root cid.Cid) error {
	p2paddrs, err := peer.AddrInfoToP2pAddrs(provider)
	if err != nil {
		return err
	}
	record := dtsync.Message{
		Cid:   root,
		Addrs: p2paddrs,
	}

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	fmt.Printf("req: %s\n", data)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.announceURL, bytes.NewBuffer(data))
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return httpclient.ReadError(resp.StatusCode, body)
	}
	return nil
}

func (c *Client) Register(ctx context.Context, providerID peer.ID, privateKey p2pcrypto.PrivKey, addrs []string) error {
	data, err := model.MakeRegisterRequest(providerID, privateKey, addrs)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.registerURL, bytes.NewBuffer(data))
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
