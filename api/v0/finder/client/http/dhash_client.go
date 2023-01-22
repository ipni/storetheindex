package finderhttpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ipni/go-indexer-core/store/dhash"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/api/v0/httpclient"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
)

const (
	metadataPath = "metadata"
)

type DHashClient struct {
	Client

	metadataUrl string
	// defaultMetadata defines the metadata to use if one can't be found for some multihashes.
	// Metadata might be missing for IPFS multihashes as they assume bitswap protocol by default.
	// If defaultMetadata is not set - 404 errors to metadata lookups will result into error.
	defaultMetadata []byte
}

// NewDHashClient instantiates a new client that uses Reader Privacy API for querying data.
// It requires more roundtrips to fullfill one query however it also protects the user from a passive observer.
func NewDHashClient(baseURL string, options ...httpclient.Option) (*DHashClient, error) {
	c, err := New(baseURL, options...)
	if err != nil {
		return nil, err
	}

	return &DHashClient{
		Client:      *c,
		metadataUrl: baseURL + metadataPath,
	}, nil
}

func (c *DHashClient) Find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
	// query value keys from indexer
	smh, err := dhash.SecondMultihash(mh)
	if err != nil {
		return nil, err
	}
	u := fmt.Sprint(c.finderURL, "/", smh.B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	findResponse := &model.FindResponse{}
	err = json.Unmarshal(body, findResponse)
	if err != nil {
		return nil, err
	}

	err = c.decryptFindResponse(ctx, findResponse, map[string]multihash.Multihash{smh.B58String(): mh})
	if err != nil {
		return nil, err
	}

	return findResponse, nil
}

// decryptFindResponse decrypts EncMultihashResults and appends the decrypted values to
// MultihashResults. It also fetches provider info and metadata for each value key.
func (c *DHashClient) decryptFindResponse(ctx context.Context, resp *model.FindResponse, unhasher map[string]multihash.Multihash) error {
	pinfoCache := make(map[peer.ID]*model.ProviderInfo)

	// decrypt each value key using the original multihash
	// then for each decrypted value key fetch provider's addr info
	for _, encRes := range resp.EncryptedMultihashResults {
		mh, found := unhasher[encRes.Multihash.B58String()]
		if !found {
			continue
		}

		mhr := model.MultihashResult{
			Multihash: mh,
		}
		for _, evk := range encRes.EncryptedValueKeys {
			vk, err := dhash.DecryptValueKey(evk, mh)
			if err != nil {
				return err
			}

			pid, ctxId, err := dhash.SplitValueKey(vk)
			if err != nil {
				return err
			}

			// fetch provider info to populate peer.AddrInfo
			pinfo := pinfoCache[pid]
			if pinfo == nil {
				pinfo, err = c.GetProvider(ctx, pid)
				if err != nil {
					continue
				}
				pinfoCache[pid] = pinfo
			}

			// fetch metadata
			metadata, err := c.fetchMetadata(ctx, vk)
			if err != nil {
				continue
			}

			mhr.ProviderResults = append(mhr.ProviderResults, model.ProviderResult{
				ContextID: ctxId,
				Metadata:  metadata,
				Provider:  pinfo.AddrInfo,
			})
		}
		if len(mhr.ProviderResults) > 0 {
			resp.MultihashResults = append(resp.MultihashResults, mhr)
		}
	}
	return nil
}

func (c *DHashClient) fetchMetadata(ctx context.Context, vk []byte) ([]byte, error) {
	u := fmt.Sprint(c.metadataUrl, "/", b58.Encode(dhash.SHA256(vk, nil)))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()

	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		return dhash.DecryptMetadata(body, vk)
	}

	if resp.StatusCode == http.StatusNotFound && c.defaultMetadata != nil {
		return c.defaultMetadata, nil
	}

	return nil, httpclient.ReadError(resp.StatusCode, body)
}
