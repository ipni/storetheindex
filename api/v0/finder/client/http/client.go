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
	finderResource = "index"
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

// Find queries indexer entries for a multihash
func (cl *Finder) Find(ctx context.Context, m multihash.Multihash) (*models.FindResponse, error) {
	u := cl.baseURL + "/" + m.B58String()
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	return cl.sendRequest(req)
}

// FindBatch queries indexer entries for a batch of multihashes
func (cl *Finder) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*models.FindResponse, error) {
	if len(mhs) == 0 {
		return &models.FindResponse{}, nil
	}
	data, err := models.MarshalFindRequest(&models.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", cl.baseURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return cl.sendRequest(req)
}

func (cl *Finder) sendRequest(req *http.Request) (*models.FindResponse, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := cl.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			log.Info("Index not found in indexer")
			return &models.FindResponse{}, nil
		}
		return nil, fmt.Errorf("getting batch indexes failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalFindResponse(b)
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
