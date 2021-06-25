package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
)

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

func (c *Client) ImportFromManifest(ctx context.Context, dir string, provID peer.ID) error {
	req, err := c.newUploadRequest(dir, c.endpoint+"/import/manifest/"+provID.String())
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	fmt.Println("Response from request", resp)
	// TODO: Handle response and error
	return nil
}

func (c *Client) ImportFromCidList(ctx context.Context, dir string, provID peer.ID) error {
	req, err := c.newUploadRequest(dir, c.endpoint+"/import/cidlist/"+provID.String())
	fmt.Println("Endpoint", c.endpoint+"/import/cidlist/"+provID.String())

	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	fmt.Println("Response from request", resp)
	// TODO: Handle response and error
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
