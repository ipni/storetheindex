package find_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/cache/radixcache"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/test"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/registry"
	httpserver "github.com/ipni/storetheindex/server/find"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const providerID = "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA"

func setupServer(t *testing.T, ind indexer.Interface, reg *registry.Registry) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, reg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Close(), "error closing server")
	})
	return s
}

func setupClient(host string, t *testing.T) *client.Client {
	c, err := client.New(host)
	require.NoError(t, err)
	return c
}

func TestFindIndexData(t *testing.T) {
	// Initialize everything
	ind := initIndex(t, true)
	reg := initRegistry(t)
	s := setupServer(t, ind, reg)
	c := setupClient(s.URL(), t)

	// Start server
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	// Test must complete in 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	findIndexTest(ctx, t, c, ind, reg)

	err := s.Close()
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	require.NoError(t, err)
}

func TestFindIndexWithExtendedProviders(t *testing.T) {
	// Initialize everything
	ind := initIndex(t, true)
	// We don't want to have any restricitons around provider identities as they are generated in rkandom for extended providers
	reg := initRegistryWithRestrictivePolicy(t, false)
	s := setupServer(t, ind, reg)
	c := setupClient(s.URL(), t)

	// Start server
	go func() {
		err := s.Start()
		require.ErrorIs(t, err, http.ErrServerClosed)
	}()

	// Test must complete in 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	providersShouldBeUnaffectedByExtendedProvidersOfEachOtherTest(ctx, t, c, ind, reg)
	extendedProviderShouldHaveOwnMetadataTest(ctx, t, c, ind, reg)
	extendedProviderShouldInheritMetadataOfMainProviderTest(ctx, t, c, ind, reg)
	contextualExtendedProvidersShouldUnionUpWithChainLevelOnesTest(ctx, t, c, ind, reg)
	contextualExtendedProvidersShouldOverrideChainLevelOnesTest(ctx, t, c, ind, reg)
	mainProviderChainRecordIsIncludedIfItsMetadataIsDifferentTest(ctx, t, c, ind, reg)
	mainProviderContextRecordIsIncludedIfItsMetadataIsDifferentTest(ctx, t, c, ind, reg)
}

func TestProviderInfo(t *testing.T) {
	// Initialize everything
	ind := initIndex(t, true)
	reg := initRegistry(t)

	s := setupServer(t, ind, reg)
	findclient := setupClient(s.URL(), t)

	// Start server
	go func() {
		err := s.Start()
		require.ErrorIs(t, err, http.ErrServerClosed)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID := register(ctx, t, reg)

	getProviderTest(t, findclient, peerID)
	listProvidersTest(t, findclient, peerID)
}

func TestGetStats(t *testing.T) {
	ind := initPebbleIndex(t, false)
	reg := initRegistry(t)

	s := setupServer(t, ind, reg)
	findclient := setupClient(s.URL(), t)

	// Start server
	go func() {
		err := s.Start()
		require.ErrorIs(t, err, http.ErrServerClosed)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	getStatsTest(ctx, t, ind, s.RefreshStats, findclient)
}

func TestRemoveProvider(t *testing.T) {
	// Initialize everything
	ind := initIndex(t, true)
	reg := initRegistry(t)
	s := setupServer(t, ind, reg)
	c := setupClient(s.URL(), t)

	// Start server
	go func() {
		err := s.Start()
		require.ErrorIs(t, err, http.ErrServerClosed)
	}()

	// Test must complete in 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	removeProviderTest(ctx, t, c, ind, reg)
}

// InitIndex initialize a new indexer engine.
func initIndex(t *testing.T, withCache bool) indexer.Interface {
	ind := engine.New(memory.New())
	t.Cleanup(func() {
		require.NoError(t, ind.Close(), "Error closing indexer core")
	})
	return ind
}

// InitPebbleIndex initialize a new indexer engine using pebbel with cache.
func initPebbleIndex(t *testing.T, withCache bool) indexer.Interface {
	valueStore, err := pebble.New(t.TempDir(), nil)
	require.NoError(t, err)
	var ind indexer.Interface
	if withCache {
		ind = engine.New(valueStore, engine.WithCache(radixcache.New(1000)))
	} else {
		ind = engine.New(valueStore)
	}
	t.Cleanup(func() {
		require.NoError(t, ind.Close(), "Error closing indexer core")
	})
	return ind
}

func initRegistry(t *testing.T) *registry.Registry {
	return initRegistryWithRestrictivePolicy(t, true)
}

// InitRegistry initializes a new registry
func initRegistryWithRestrictivePolicy(t *testing.T, restrictive bool) *registry.Registry {
	var discoveryCfg = config.Discovery{}
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
	t.Cleanup(func() {
		reg.Close()
	})
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

func findIndexTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	// Generate some multihashes and populate indexer
	mhs := test.RandomMultihashes(2)
	p, err := peer.Decode(providerID)
	require.NoError(t, err)
	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	v := indexer.Value{
		ProviderID:    p,
		ContextID:     ctxID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs[:1], v, t)

	a, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	provider := peer.AddrInfo{
		ID:    p,
		Addrs: []multiaddr.Multiaddr{a},
	}
	err = reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err, "could not register provider info")

	// Get single multihash
	resp, err := f.Find(ctx, mhs[0])
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

	resp, err = f.Find(ctx, mhs[1])
	require.NoError(t, err)
	require.Zero(t, len(resp.MultihashResults))
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

func getProviderTest(t *testing.T, c *client.Client, providerID peer.ID) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provInfo, err := c.GetProvider(ctx, providerID)
	require.NoError(t, err)

	verifyProviderInfo(t, provInfo)
}

func listProvidersTest(t *testing.T, c *client.Client, providerID peer.ID) {
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

func removeProviderTest(ctx context.Context, t *testing.T, c *client.Client, ind indexer.Interface, reg *registry.Registry) {
	// Generate some multihashes and populate indexer
	mhs := test.RandomMultihashes(15)
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

func getStatsTest(ctx context.Context, t *testing.T, ind indexer.Interface, refreshStats func(), c *client.Client) {
	t.Parallel()
	mhs := test.RandomMultihashes(15)
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

func register(ctx context.Context, t *testing.T, reg *registry.Registry) peer.ID {
	peerID, err := peer.Decode(providerID)
	require.NoError(t, err)

	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	require.NoError(t, err)

	ep1, _, _ := test.RandomIdentity()
	ep2, _, _ := test.RandomIdentity()

	provider := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	maddrs := test.RandomMultiaddrs(2)
	provAddrs := maddrs[:1]
	ctxAddrs := maddrs[1:]

	extProviders := &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  provAddrs,
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{
			"testContext": {
				Override:  true,
				ContextID: []byte("testContext"),
				Providers: []registry.ExtendedProviderInfo{
					{
						PeerID: ep2,
						Addrs:  ctxAddrs,
					},
				},
			},
		},
	}

	err = reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, extProviders, 0)
	require.NoError(t, err)

	return peerID
}
