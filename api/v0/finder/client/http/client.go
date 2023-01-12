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
	"github.com/multiformats/go-multihash"
)

const (
	finderPath    = "/multihash"
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
	useEncApi    bool
}

// New creates a new finder HTTP client.
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	u, c, err := httpclient.New(baseURL, "", options...)
	if err != nil {
		return nil, err
	}
	var cfg httpclient.ClientConfig
	if err := cfg.Apply(options...); err != nil {
		return nil, err
	}

	baseURL = u.String()
	return &Client{
		c:            c,
		keyer:        indexer.NewKeyer(),
		finderURL:    baseURL + finderPath,
		providersURL: baseURL + providersPath,
		statsURL:     baseURL + statsPath,
		useEncApi:    cfg.UseEncApi,
	}, nil
}

// Find queries indexer entries for a multihash
func (c *Client) Find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
	if c.useEncApi {
		return c.encFind(ctx, mh)
	} else {
		return c.find(ctx, mh)
	}
}

func (c *Client) find(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
	u := fmt.Sprint(c.finderURL, "/", mh.B58String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	return c.sendRequest(req)
}

func (c *Client) encFind(ctx context.Context, mh multihash.Multihash) (*model.FindResponse, error) {
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

	findResponse := &model.FindResponse{}
	err = json.Unmarshal(body, findResponse)
	if err != nil {
		return nil, err
	}

	c.decryptFindResponse(ctx, findResponse, map[string]multihash.Multihash{smh.B58String(): mh})

	return findResponse, nil
}

// FindBatch queries indexer entries for a batch of multihashes
func (c *Client) FindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if c.useEncApi {
		return c.encFindBatch(ctx, mhs)
	} else {
		return c.findBatch(ctx, mhs)
	}
}

func (c *Client) findBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}
	data, err := model.MarshalFindRequest(&model.FindRequest{Multihashes: mhs})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.finderURL, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	return c.sendRequest(req)
}

func (c *Client) encFindBatch(ctx context.Context, mhs []multihash.Multihash) (*model.FindResponse, error) {
	if len(mhs) == 0 {
		return &model.FindResponse{}, nil
	}

	smhs := make([]multihash.Multihash, 0, len(mhs))
	dehasher := map[string]multihash.Multihash{}
	for _, mh := range mhs {
		smh, err := dhash.SecondMultihash(mh)
		if err != nil {
			return nil, err
		}
		smhs = append(smhs, smh)
		dehasher[smh.B58String()] = mh
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

	findResponse := &model.FindResponse{}
	err = json.Unmarshal(resBody, findResponse)
	if err != nil {
		return nil, err
	}

	err = c.decryptFindResponse(ctx, findResponse, dehasher)
	if err != nil {
		return nil, err
	}

	return findResponse, nil
}

func (c *Client) decryptFindResponse(ctx context.Context, resp *model.FindResponse, unhasher map[string]multihash.Multihash) error {
	// decrypt each value key using the original multihash
	// then for each decrypted value key fetch provider's addr info
	for _, encRes := range resp.EncMultihashResults {
		mh, found := unhasher[encRes.Multihash.B58String()]
		if !found {
			continue
		}

		mhr := model.MultihashResult{
			Multihash: mh,
		}
		for _, evk := range encRes.ValueKeys {
			vk, err := dhash.DecryptValueKey(evk, mh)
			if err != nil {
				return err
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
			resp.MultihashResults = append(resp.MultihashResults, mhr)
		}
	}
	return nil
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

func (c *Client) sendRequest(req *http.Request) (*model.FindResponse, error) {
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	// Handle failed requests
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return &model.FindResponse{}, nil
		}
		return nil, fmt.Errorf("batch find query failed: %v", http.StatusText(resp.StatusCode))
	}

	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return model.UnmarshalFindResponse(b)
}
