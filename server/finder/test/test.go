package test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	reframeclient "github.com/ipfs/go-delegated-routing/client"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/cache/radixcache"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/storetheindex/api/v0/finder/client"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/test/util"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const providerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"

var rng = rand.New(rand.NewSource(1413))

// InitIndex initialize a new indexer engine.
func InitIndex(t *testing.T, withCache bool) indexer.Interface {
	return engine.New(nil, memory.New())
}

// InitPebbleIndex initialize a new indexer engine using pebbel with cache.
func InitPebbleIndex(t *testing.T, withCache bool) indexer.Interface {
	valueStore, err := pebble.New(t.TempDir(), nil)
	require.NoError(t, err)
	if withCache {
		return engine.New(radixcache.New(1000), valueStore)
	}
	return engine.New(nil, valueStore)
}

func InitRegistry(t *testing.T) *registry.Registry {
	return InitRegistryWithRestrictivePolicy(t, true)
}

// InitRegistry initializes a new registry
func InitRegistryWithRestrictivePolicy(t *testing.T, restrictive bool) *registry.Registry {
	var discoveryCfg = config.Discovery{
		PollInterval: config.Duration(time.Minute),
	}
	if restrictive {
		discoveryCfg.Policy = config.Policy{
			Allow:   false,
			Except:  []string{providerID},
			Publish: false,
		}
	} else {
		discoveryCfg.Policy = config.Policy{
			Allow:   true,
			Publish: false,
		}
	}
	reg, err := registry.New(context.Background(), discoveryCfg, nil)
	require.NoError(t, err)
	return reg
}

// populateIndex with some multihashes
func populateIndex(ind indexer.Interface, mhs []multihash.Multihash, v indexer.Value, t *testing.T) {
	err := ind.Put(v, mhs...)
	require.NoError(t, err, "Error putting multihashes")
	vals, ok, err := ind.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, ok, "index not found")
	require.NotZero(t, len(vals), "no values returned")
	require.True(t, v.Equal(vals[0]), "stored and retrieved values are different")
}

func ReframeFindIndexTest(ctx context.Context, t *testing.T, c client.Finder, rc *reframeclient.Client, ind indexer.Interface, reg *registry.Registry) {
	// Generate some multihashes and populate indexer
	mhs := util.RandomMultihashes(15, rng)
	p, err := peer.Decode(providerID)
	require.NoError(t, err)
	ctxID := []byte("test-context-id")

	// Use a sample metadata with multiple protocols that includes BitSwap
	// among others to make a stronger test.
	metadata, err := base64.StdEncoding.DecodeString("gBKQEqNoUGllY2VDSUTYKlgoAAGB4gOSICAYVAKmPqL1mpkiiDhd9iBaXoU/3rXorXxzjiyESP4hB2xWZXJpZmllZERlYWz0bUZhc3RSZXRyaWV2YWz1")
	require.NoError(t, err)
	v := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs[:10], v, t)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	provider := peer.AddrInfo{
		ID:    p,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err, "could not register provider info")

	// Get single multihash
	peerAddrs, err := rc.FindProviders(ctx, cid.NewCidV1(cid.Raw, mhs[0]))
	require.NoError(t, err)

	require.Equal(t, 1, len(peerAddrs), "expecting one peer addr")
	require.Equal(t, p, peerAddrs[0].ID)
}

func FindIndexTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	// Generate some multihashes and populate indexer
	mhs := util.RandomMultihashes(15, rng)
	p, err := peer.Decode(providerID)
	require.NoError(t, err)
	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	v := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs[:10], v, t)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	provider := peer.AddrInfo{
		ID:    p,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err, "could not register provider info")

	// Get single multihash
	resp, err := c.Find(ctx, mhs[0])
	require.NoError(t, err)
	t.Log("index values in resp:", len(resp.MultihashResults))

	provResult := model.ProviderResult{
		ContextID: v.ContextID,
		Provider: &peer.AddrInfo{
			ID:    v.ProviderID,
			Addrs: provider.Addrs,
		},
		Metadata: v.MetadataBytes,
	}

	expectedResults := []model.ProviderResult{provResult}
	err = checkResponse(resp, mhs[:1], expectedResults)
	require.NoError(t, err)

	// Get a batch of multihashes
	resp, err = c.FindBatch(ctx, mhs[:10])
	require.NoError(t, err)
	err = checkResponse(resp, mhs[:10], expectedResults)
	require.NoError(t, err)

	// Get a batch of multihashes where only a subset is in the index
	resp, err = c.FindBatch(ctx, mhs)
	require.NoError(t, err)
	err = checkResponse(resp, mhs[:10], expectedResults)
	require.NoError(t, err)

	// Get empty batch
	_, err = c.FindBatch(ctx, []multihash.Multihash{})
	require.NoError(t, err)
	err = checkResponse(&model.FindResponse{}, []multihash.Multihash{}, nil)
	require.NoError(t, err)

	// Get batch with no multihashes in request
	_, err = c.FindBatch(ctx, mhs[10:])
	require.NoError(t, err)
	err = checkResponse(&model.FindResponse{}, []multihash.Multihash{}, nil)
	require.NoError(t, err)
}

func checkResponse(r *model.FindResponse, mhs []multihash.Multihash, expected []model.ProviderResult) error {
	// Check if everything was returned.
	if len(r.MultihashResults) != len(mhs) {
		return fmt.Errorf("number of values send in responses not correct, expected %d got %d", len(mhs), len(r.MultihashResults))
	}
	for i := range r.MultihashResults {
		// Check if multihash in list of multihashes
		if !hasMultihash(mhs, r.MultihashResults[i].Multihash) {
			return fmt.Errorf("multihash not found in response containing %d multihash", len(mhs))
		}

		// Check if same value
		for j, pr := range r.MultihashResults[i].ProviderResults {
			if !pr.Equal(expected[j]) {
				return fmt.Errorf("wrong ProviderResult included for a multihash: %s", expected[j])
			}
		}
	}
	return nil
}

func hasMultihash(mhs []multihash.Multihash, m multihash.Multihash) bool {
	for i := range mhs {
		if bytes.Equal([]byte(mhs[i]), []byte(m)) {
			return true
		}
	}
	return false
}

func GetProviderTest(t *testing.T, c client.Finder, providerID peer.ID) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provInfo, err := c.GetProvider(ctx, providerID)
	require.NoError(t, err)

	verifyProviderInfo(t, provInfo)
}

func ListProvidersTest(t *testing.T, c client.Finder, providerID peer.ID) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	providers, err := c.ListProviders(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(providers), "should have 1 provider")

	verifyProviderInfo(t, providers[0])
}

func verifyProviderInfo(t *testing.T, provInfo *model.ProviderInfo) {
	require.NotNil(t, provInfo, "nil provider info")
	require.Equal(t, providerID, provInfo.AddrInfo.ID.String(), "wrong peer id")
	require.Equal(t, uint64(939), provInfo.IndexCount, "expected IndexCount to be 939")
	require.NotNil(t, provInfo.ExtendedProviders, "expected to have extended providers")
	require.Equal(t, 1, len(provInfo.ExtendedProviders.Providers))
	require.Equal(t, 1, len(provInfo.ExtendedProviders.Contextual))
	require.Equal(t, 1, len(provInfo.ExtendedProviders.Contextual[0].Providers))
	require.Equal(t, *provInfo.ExtendedProviders, model.ExtendedProviders{
		Providers: []peer.AddrInfo{
			{
				ID:    provInfo.ExtendedProviders.Providers[0].ID,
				Addrs: provInfo.ExtendedProviders.Providers[0].Addrs,
			},
		},
		Metadatas: [][]byte{nil},
		Contextual: []model.ContextualExtendedProviders{
			{
				Override:  true,
				ContextID: "testContext",
				Providers: []peer.AddrInfo{
					{
						ID:    provInfo.ExtendedProviders.Contextual[0].Providers[0].ID,
						Addrs: provInfo.ExtendedProviders.Contextual[0].Providers[0].Addrs,
					},
				},
				Metadatas: [][]byte{nil},
			},
		},
	})
}

func RemoveProviderTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	// Generate some multihashes and populate indexer
	mhs := util.RandomMultihashes(15, rng)
	p, err := peer.Decode(providerID)
	require.NoError(t, err)
	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	require.NoError(t, err)
	v := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs[:10], v, t)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	provider := peer.AddrInfo{
		ID:    p,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err, "could not register provider info")

	// Get single multihash
	resp, err := c.Find(ctx, mhs[0])
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.MultihashResults), "expected 1 value in response")

	provResult := model.ProviderResult{
		ContextID: v.ContextID,
		Provider: &peer.AddrInfo{
			ID:    v.ProviderID,
			Addrs: provider.Addrs,
		},
		Metadata: v.MetadataBytes,
	}

	expectedResults := []model.ProviderResult{provResult}
	err = checkResponse(resp, mhs[:1], expectedResults)
	require.NoError(t, err)

	t.Log("removing provider from registry")
	err = reg.RemoveProvider(ctx, p)
	require.NoError(t, err)

	// Get single multihash
	resp, err = c.Find(ctx, mhs[0])
	require.NoError(t, err)
	t.Log("index values in resp:", len(resp.MultihashResults))
	require.Zero(t, len(resp.MultihashResults), "expected 0 multihashes in response")

	_, err = c.GetProvider(ctx, p)
	require.ErrorContains(t, err, "not found")
}

func GetStatsTest(ctx context.Context, t *testing.T, ind indexer.Interface, refreshStats func(), c client.Finder) {
	t.Parallel()
	mhs := util.RandomMultihashes(15, rng)
	p, err := peer.Decode(providerID)
	require.NoError(t, err)
	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	v := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs[:10], v, t)
	ind.Flush()
	// Tell stats to pick up new stats data from indexer.
	refreshStats()

	require.Eventually(t, func() bool {
		stats, err := c.GetStats(ctx)
		return err == nil && (stats.EntriesEstimate > 0 || stats.EntriesCount > 0)
	}, 5*time.Second, time.Second)
}

func Register(ctx context.Context, t *testing.T, reg *registry.Registry) peer.ID {
	peerID, err := peer.Decode(providerID)
	require.NoError(t, err)

	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	require.NoError(t, err)

	ep1, _, _ := util.RandomIdentity(t)
	ep2, _, _ := util.RandomIdentity(t)

	provider := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	extProviders := &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9998"}),
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{
			"testContext": {
				Override:  true,
				ContextID: []byte("testContext"),
				Providers: []registry.ExtendedProviderInfo{
					{
						PeerID: ep2,
						Addrs:  util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"}),
					},
				},
			},
		},
	}

	err = reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, extProviders, 0)
	require.NoError(t, err)

	return peerID
}
