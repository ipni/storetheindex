package finderhttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/store/dhash"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/api/v0/httpclient"
	"github.com/libp2p/go-libp2p/core/peer"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/multiformats/go-multihash"
)

const (
	finderPath    = "/private/multihash"
	providersPath = "/providers"
	statsPath     = "/stats"
)

// Client is an http client for the indexer finder API
type Client struct {
	c            *http.Client
	keyer        *indexer.ValueKeyer
	finderURL    string
	providersURL string
	statsURL     string
}

// New creates a new finder HTTP client.
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	u, c, err := httpclient.New(baseURL, "", options...)
	if err != nil {
		return nil, err
	}
	baseURL = u.String()
	return &Client{
		c:            c,
		keyer:        indexer.NewKeyer(),
		finderURL:    baseURL + finderPath,
		providersURL: baseURL + providersPath,
		statsURL:     baseURL + statsPath,
	}, nil
}

// Find queries indexer entries for a multihash
func (c *Client) Find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {

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
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	privateFindResponse := &model.PrivateFindResponse{}
	err = json.Unmarshal(body, privateFindResponse)
	if err != nil {
		return nil, err
	}

	return c.buildFindResponse(ctx, []multihash.Multihash{mh}, privateFindResponse)
}

// FindBatch queries indexer entries for a batch of multihashes
func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}

	smhs := make([]multihash.Multihash, 0, len(mhs))
	for _, mh := range mhs {
		smh, err := dhash.SecondMultihash(mh)
		if err != nil {
			return nil, err
		}
		smhs = append(smhs, smh)
	}

	reqBody, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: smhs})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.finderURL, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}

	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httpclient.ReadError(resp.StatusCode, resBody)
	}

	privateFindResponse := &model.PrivateFindResponse{}
	err = json.Unmarshal(resBody, privateFindResponse)
	if err != nil {
		return nil, err
	}

	return c.buildFindResponse(ctx, mhs, privateFindResponse)
}

func (c *Client) buildFindResponse(ctx context.Context, mhs []multihash.Multihash, pfr *model.PrivateFindResponse) (*model.FindResponse, error) {
	// decrypt each value key using the original multihash
	// then for each decrypted value key fetch provider's addr info
	findResponse := &model.FindResponse{
		MultihashResults: make([]model.MultihashResult, 0, len(pfr.MultihashResults)),
	}
	mhi := 0
	for _, pmhr := range pfr.MultihashResults {
		var mh multihash.Multihash
		for {
			if mhi >= len(mhs) {
				break
			}
			mh = mhs[mhi]
			mhi++
			smh, err := dhash.SecondMultihash(mh)
			if err != nil {
				return nil, err
			}
			if bytes.Equal(smh, pmhr.Multihash) {
				break
			}
		}

		if mh == nil {
			break
		}

		mhr := model.MultihashResult{
			Multihash: mh,
		}
		for _, evk := range pmhr.ValueKeys {
			evkBytes, err := b58.Decode(evk)
			if err != nil {
				return nil, err
			}
			vk, err := dhash.DecryptValueKey(evkBytes, mh)
			if err != nil {
				return nil, err
			}

			pid, _ := c.keyer.SplitKey(vk)

			pinfo, err := c.GetProvider(ctx, pid)
			if err != nil {
				continue
			}

			mhr.ProviderResults = append(mhr.ProviderResults, model.ProviderResult{
				ContextID: nil,
				Metadata:  nil,
				Provider:  pinfo.AddrInfo,
			})
		}
		if len(mhr.ProviderResults) > 0 {
			findResponse.MultihashResults = append(findResponse.MultihashResults, mhr)
		}
	}
	return findResponse, nil
}

func (c *Client) ListProviders(ctx context.Context) ([]*model.ProviderInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.providersURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

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
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	var providers []*model.ProviderInfo
	err = json.Unmarshal(body, &providers)
	if err != nil {
		return nil, err
	}

	return providers, nil
}

func (c *Client) GetProvider(ctx context.Context, providerID peer.ID) (*model.ProviderInfo, error) {
	u := fmt.Sprint(c.providersURL, "/", providerID.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

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
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	var providerInfo model.ProviderInfo
	err = json.Unmarshal(body, &providerInfo)
	if err != nil {
		return nil, err
	}
	return &providerInfo, nil
}

func (c *Client) GetStats(ctx context.Context) (*model.Stats, error) {
	u := fmt.Sprint(c.statsURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")

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
		return nil, httpclient.ReadError(resp.StatusCode, body)
	}

	return model.UnmarshalStats(body)
}
