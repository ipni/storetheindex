package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("client")

type Client struct {
	client   *http.Client
	endpoint string
}

func New(c *cli.Context) *Client {
	endpoint := c.String("endpoint")
	return NewFromEndpoint(endpoint)
}

// NewFromEndpoint returns an API client at the given endpoint
func NewFromEndpoint(endpoint string) *Client {
	return &Client{
		client: &http.Client{
			// As a fallback, never take more than a minute.
			// Most client API calls should use a context.
			Timeout: time.Minute,
		},
		// TODO: Configure protocol handler dynamically
		endpoint: "http://" + endpoint,
	}
}

// TODO: Use a common function to create requests.
func (c *Client) Get(ctx context.Context, x cid.Cid) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.endpoint+"/cid/"+x.String(), nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	// TODO: This needs to change. Only for debugging and testing for now.
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	fmt.Println(string(b))
	return nil

}

func (c *Client) GetBatch(ctx context.Context, cids []cid.Cid) {
	data := []byte("Request here")
	_, _ = http.NewRequestWithContext(ctx, "POST", c.endpoint+"/cid", bytes.NewBuffer(data))
	log.Errorw("Batch get of cids not implemented")
}

func (c *Client) ImportFromManifest(ctx context.Context, dir string, provID peer.ID) error {
	req, err := c.newUploadRequest(dir, c.endpoint+"/import/manifest/"+provID.String())
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	// Handle failed requests
	// TODO: Send additional context for the error in body and handle it here.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("importing from manifest failed")
	}
	log.Infow("Success")
	return nil
}

func (c *Client) ImportFromCidList(ctx context.Context, dir string, provID peer.ID) error {
	req, err := c.newUploadRequest(dir, c.endpoint+"/import/cidlist/"+provID.String())
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("importing from cidlist failed")
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
