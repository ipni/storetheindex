package finderhttpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/internal/httpclient"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
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
func (c *Client) Find(ctx context.Context, m multihash.Multihash) (*models.FindResponse, error) {
	u := c.baseURL + "/" + m.B58String()
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	return c.sendRequest(req)
}

// FindBatch queries indexer entries for a batch of multihashes
func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*models.FindResponse, error) {
	if len(mhs) == 0 {
		return &models.FindResponse{}, nil
	}
	data, err := models.MarshalFindRequest(&models.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return c.sendRequest(req)
}

func (c *Client) sendRequest(req *http.Request) (*models.FindResponse, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			log.Info("Entry not found in indexer")
			return &models.FindResponse{}, nil
		}
		return nil, fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalFindResponse(b)
}

// ImportFromManifest processes entries from manifest and imports them into the
// indexer
func (c *Client) ImportFromManifest(ctx context.Context, dir string, provID peer.ID) error {
	u := c.baseURL + path.Join("/import", "manifest", provID.String())
	req, err := c.newUploadRequest(dir, u)
	if err != nil {
		return err
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("importing from manifest failed: %v", http.StatusText(resp.StatusCode))
	}
	log.Infow("Success")
	return nil
}

// ImportFromCidList process entries from a cidlist and imprts it into the
// indexer
func (c *Client) ImportFromCidList(ctx context.Context, dir string, provID peer.ID) error {
	u := c.baseURL + path.Join("/import", "cidlist", provID.String())
	req, err := c.newUploadRequest(dir, u)
	if err != nil {
		return err
	}
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("importing from cidlist failed: %v", http.StatusText(resp.StatusCode))
	}
	log.Infow("Success")
	return nil
}

func (c *Client) newUploadRequest(dir string, uri string) (*http.Request, error) {
	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(dir))
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(part, file)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", uri, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req, nil
}
