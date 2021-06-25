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
)

type Client struct {
	client   *http.Client
	endpoint string
}

type Success struct {
}

// NewFromEndpoint returns an API client at the given endpoint
func NewFromEndpoint(endpoint string) *Client {
	return &Client{
		client: &http.Client{
			// As a fallback, never take more than a minute.
			// Most client API calls should use a context.
			Timeout: time.Minute,
		},
		endpoint: endpoint,
	}
}

func (c *Client) ImportFromManifest(ctx context.Context, dir string) error {
	req, err := c.newUploadRequest(dir, c.endpoint+"/import/manifest")
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

func (c *Client) ImportFromCidList(ctx context.Context, dir string) error {
	req, err := c.newUploadRequest(dir, c.endpoint+"/import/cidlist")
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

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", uri, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req, err
}
