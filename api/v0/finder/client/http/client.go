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
	httpclient "github.com/filecoin-project/storetheindex/internal/httpclient"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("finderhttpclient")

const (
	finderResource = "cid"
	finderPort     = 3000
)

// Finder is an http client for the indexer finder API
type Finder struct {
	c       *http.Client
	baseURL string
}

// NewFinder creates a new finder client
func NewFinder(baseURL string, options ...httpclient.ClientOption) (*Finder, error) {
	u, c, err := httpclient.NewClient(baseURL, finderResource, finderPort, options...)
	if err != nil {
		return nil, err
	}
	return &Finder{
		c:       c,
		baseURL: u.String(),
	}, nil
}

// Get indexeer entries for a CID
func (cl *Finder) Get(ctx context.Context, c cid.Cid) (*models.Response, error) {
	u := cl.baseURL + "/" + c.String()
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	return cl.sendRequest(req)
}

// GetBatch of indexeer entries for a CIDs
func (cl *Finder) GetBatch(ctx context.Context, cs []cid.Cid) (*models.Response, error) {
	data, err := models.MarshalReq(&models.Request{Cids: cs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", cl.baseURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return cl.sendRequest(req)
}

func (cl *Finder) sendRequest(req *http.Request) (*models.Response, error) {
	req.Header.Set("Content-Type", "application/json")
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
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalResp(b)
}

// ImportFromManifest process entries from manifest and imports it in the indexer
func (cl *Finder) ImportFromManifest(ctx context.Context, dir string, provID peer.ID) error {
	u := cl.baseURL + path.Join("/import", "manifest", provID.String())
	req, err := cl.newUploadRequest(dir, u)
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

// ImportFromCidList process entries from a cidlist and imprts it in the indexer
func (cl *Finder) ImportFromCidList(ctx context.Context, dir string, provID peer.ID) error {
	u := cl.baseURL + path.Join("/import", "cidlist", provID.String())
	req, err := cl.newUploadRequest(dir, u)
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

func (cl *Finder) newUploadRequest(dir string, uri string) (*http.Request, error) {
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
