package httpclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/filecoin-project/storetheindex/api/v1/finder/models"
	"github.com/filecoin-project/storetheindex/internal/finder"
	"github.com/filecoin-project/storetheindex/server/net"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("httpclient")

var _ finder.Interface = &Client{}

// Client is responsible for sending client
// requests to other peers using http client
type Client struct {
	c *http.Client
}

// New creates a new httpclient
func New(options ...ClientOption) (*Client, error) {
	var cfg clientConfig
	if err := cfg.apply(append([]ClientOption{clientDefaults}, options...)...); err != nil {
		return nil, err
	}

	// Start a client
	e := &Client{
		c: &http.Client{
			// As a fallback, never take more than a minute.
			// Most client API calls should use a context.
			Timeout: time.Minute,
		},
	}

	return e, nil
}

// Get indexeer entries for a CID
func (cl *Client) Get(ctx context.Context, c cid.Cid, e net.Endpoint) (*models.Response, error) {

	end, ok := e.Addr().(string)
	if !ok {
		return nil, errors.New("endpoint is of wrong type for http client. Use string")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", end+"/cid/"+c.String(), nil)
	if err != nil {
		return nil, err
	}

	return cl.sendRequest(req)
}

// GetBatch of indexeer entries for a CIDs
func (cl *Client) GetBatch(ctx context.Context, cs []cid.Cid, e net.Endpoint) (*models.Response, error) {
	end, ok := e.Addr().(string)
	if !ok {
		return nil, errors.New("endpoint is of wrong type for http client. Use string")
	}

	data, err := models.MarshalReq(&models.Request{Cids: cs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", end+"/cid", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return cl.sendRequest(req)
}

func (cl *Client) sendRequest(req *http.Request) (*models.Response, error) {
	resp, err := cl.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			log.Info("Cid not found in indexer")
			return &models.Response{}, nil
		}
		return nil, fmt.Errorf("getting batch cids failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalResp(b)
}

// ImportFromManifest process entries from manifest and imports it in the indexer at endpoint
func (cl *Client) ImportFromManifest(ctx context.Context, dir string, provID peer.ID, e net.Endpoint) error {
	end, ok := e.Addr().(string)
	if !ok {
		return errors.New("endpoint is of wrong type for http client. Use string")
	}
	req, err := cl.newUploadRequest(dir, end+"/import/manifest/"+provID.String())
	if err != nil {
		return err
	}
	resp, err := cl.c.Do(req)
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

// ImportFromCidList process entries from a cidlist and imprts it in the indexer at endpoint.
func (cl *Client) ImportFromCidList(ctx context.Context, dir string, provID peer.ID, e net.Endpoint) error {
	end, ok := e.Addr().(string)
	if !ok {
		return errors.New("endpoint is of wrong type for http client. Use string")
	}
	req, err := cl.newUploadRequest(dir, end+"/import/cidlist/"+provID.String())
	if err != nil {
		return err
	}
	resp, err := cl.c.Do(req)
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

func (cl *Client) newUploadRequest(dir string, uri string) (*http.Request, error) {
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
