package registry

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/registry/discovery"
	"github.com/ipni/storetheindex/test/util"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

type mockDiscoverer struct {
	discoverRsp *discovery.Discovered
}

const (
	exceptID   = "12D3KooWK7CTS7cyWi51PeNE3cTjS2F2kDCZaQVU4A5xBmb9J1do"
	limitedID  = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
	limitedID2 = "12D3KooWKSNuuq77xqnpPLnU3fq1bTQW2TwSZL2Z4QTHEYpUVzfr"
	limitedID3 = "12D3KooWLjeDyvuv7rbfG2wWNvWn7ybmmU88PirmSckuqCgXBAph"

	minerDiscoAddr = "stitest999999"
	minerAddr      = "/ip4/127.0.0.1/tcp/9999"
	minerAddr2     = "/ip4/127.0.0.2/tcp/9999"

	publisherID   = "12D3KooWFNkdmdb38g4VVCGaJsKin4BzGpP4bedfxU76PF2AahoP"
	publisherAddr = "/ip4/127.0.0.3/tcp/1234"
)

var discoveryCfg = config.Discovery{
	Policy: config.Policy{
		Allow:         false,
		Except:        []string{exceptID, limitedID, limitedID2, publisherID},
		Publish:       false,
		PublishExcept: []string{publisherID},
	},
	PollInterval:   config.Duration(time.Minute),
	RediscoverWait: config.Duration(time.Minute),
}

func newMockDiscoverer(t *testing.T, providerID string) *mockDiscoverer {
	peerID, err := peer.Decode(providerID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}

	maddr, err := multiaddr.NewMultiaddr(minerAddr)
	if err != nil {
		t.Fatal("bad miner address:", err)
	}

	return &mockDiscoverer{
		discoverRsp: &discovery.Discovered{
			AddrInfo: peer.AddrInfo{
				ID:    peerID,
				Addrs: []multiaddr.Multiaddr{maddr},
			},
			Type: discovery.MinerType,
		},
	}
}

func (m *mockDiscoverer) Discover(ctx context.Context, peerID peer.ID, filecoinAddr string) (*discovery.Discovered, error) {
	if filecoinAddr == "bad1234" {
		return nil, errors.New("unknown miner")
	}

	return m.discoverRsp, nil
}

func TestNewRegistryDiscovery(t *testing.T) {
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := New(ctx, discoveryCfg, nil, WithDiscoverer(mockDiscoverer))
	require.NoError(t, err)
	t.Log("created new registry")

	peerID, err := peer.Decode(limitedID)
	require.NoError(t, err)
	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3002")
	require.NoError(t, err)
	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)
	pubAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	require.NoError(t, err)

	provider := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	publisher := peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{pubAddr},
	}

	err = r.Update(ctx, provider, publisher, cid.Undef, nil, 0)
	require.NoError(t, err)

	r.Close()

	// Check that 2nd call to Close is ok
	r.Close()
}

func TestDiscoveryAllowed(t *testing.T) {
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := New(ctx, discoveryCfg, nil, WithDiscoverer(mockDiscoverer))
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	t.Log("created new registry")

	peerID, err := peer.Decode(exceptID)
	require.NoError(t, err)

	err = r.Discover(peerID, minerDiscoAddr, true)
	require.NoError(t, err)
	t.Log("discovered mock miner", minerDiscoAddr)

	info, allowed := r.ProviderInfo(peerID)
	require.NotNil(t, info)
	require.True(t, allowed)
	t.Log("got provider info for miner")

	require.Equal(t, info.AddrInfo.ID, peerID, "did not get correct porvider id")

	peerID, err = peer.Decode(limitedID)
	require.NoError(t, err)
	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3002")
	require.NoError(t, err)
	provider := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}
	publisher := peer.AddrInfo{}

	err = r.Update(ctx, provider, publisher, cid.Undef, nil, 0)
	require.NoError(t, err)

	require.True(t, r.IsRegistered(peerID), "peer is not registered")

	infos := r.AllProviderInfo()
	require.Equal(t, 2, len(infos))

	r.cleanup()
	r.actions <- func() { r.rediscoverWait = 0 }
	require.NotZero(t, len(r.discoverTimes), "should not have cleaned up discovery times")

	r.cleanup()
	r.actions <- func() {}
	require.Zero(t, len(r.discoverTimes), "should have cleaned up discovery times")
}

func TestDiscoveryBlocked(t *testing.T) {
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID, err := peer.Decode(exceptID)
	require.NoError(t, err)

	r, err := New(ctx, discoveryCfg, nil, WithDiscoverer(mockDiscoverer))
	require.NoError(t, err)
	defer r.Close()
	t.Log("created new registry")

	r.BlockPeer(peerID)

	err = r.Discover(peerID, minerDiscoAddr, true)
	if !errors.Is(err, ErrNotAllowed) {
		t.Fatal("expected error:", ErrNotAllowed, "got:", err)
	}

	info, _ := r.ProviderInfo(peerID)
	require.Nil(t, info, "should not have found provider info for miner")
}

func TestDatastore(t *testing.T) {
	dataStorePath := t.TempDir()
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	provID1, err := peer.Decode(limitedID)
	require.NoError(t, err)
	maddr1, err := multiaddr.NewMultiaddr(minerAddr)
	require.NoError(t, err)
	provider1 := peer.AddrInfo{
		ID:    provID1,
		Addrs: []multiaddr.Multiaddr{maddr1},
	}

	provID2, err := peer.Decode(limitedID2)
	require.NoError(t, err)
	maddr2, err := multiaddr.NewMultiaddr(minerAddr2)
	require.NoError(t, err)
	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)
	pubAddr, err := multiaddr.NewMultiaddr(publisherAddr)
	require.NoError(t, err)

	epContextId := []byte("ep-context-id")
	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})
	ep1Metadata := []byte("ep1-metadata")
	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9998"})
	ep2Metadata := []byte("ep2-metadata")
	provider2 := peer.AddrInfo{
		ID:    provID2,
		Addrs: []multiaddr.Multiaddr{maddr2},
	}
	publisher := peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{pubAddr},
	}
	extProviders := &ExtendedProviders{
		Providers: []ExtendedProviderInfo{
			{
				PeerID:   ep1,
				Addrs:    ep1Addrs,
				Metadata: ep1Metadata,
			},
		},
		ContextualProviders: map[string]ContextualExtendedProviders{
			string(epContextId): {
				ContextID: epContextId,
				Override:  false,
				Providers: []ExtendedProviderInfo{
					{
						PeerID:   ep2,
						Addrs:    ep2Addrs,
						Metadata: ep2Metadata,
					},
				},
			},
		},
	}

	// Create datastore
	dstore, err := leveldb.NewDatastore(dataStorePath, nil)
	require.NoError(t, err)

	r, err := New(ctx, discoveryCfg, dstore, WithDiscoverer(mockDiscoverer))
	require.NoError(t, err)
	t.Log("created new registry with datastore")

	err = r.Update(ctx, provider1, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err)

	mh, err := multihash.Sum([]byte("somedata"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	adCid := cid.NewCidV1(cid.Raw, mh)
	err = r.Update(ctx, provider2, publisher, adCid, extProviders, 0)
	require.NoError(t, err)

	pinfo, allowed := r.ProviderInfo(provID1)
	require.NotNil(t, pinfo, "did not find registered provider")
	require.True(t, allowed)
	require.Nil(t, pinfo.ExtendedProviders)

	pinfo, allowed = r.ProviderInfo(provID2)
	require.NotNil(t, pinfo, "did not find registered provider")
	require.True(t, allowed)
	require.NotNil(t, pinfo.ExtendedProviders, "did not find registered extended provider")

	r.Close()
	require.NoError(t, dstore.Close())

	// Create datastore
	dstore, err = leveldb.NewDatastore(dataStorePath, nil)
	require.NoError(t, err)

	r, err = New(ctx, discoveryCfg, dstore, WithDiscoverer(mockDiscoverer))
	require.NoError(t, err)
	t.Log("re-created new registry with datastore")

	infos := r.AllProviderInfo()
	require.Equal(t, 2, len(infos))

	for _, provInfo := range infos {
		switch provInfo.AddrInfo.ID {
		case provider1.ID:
			if provInfo.Publisher.Validate() == nil {
				t.Fatal("provider1 should not have valid publisher")
			}
		case provider2.ID:
			require.Equal(t, provInfo.Publisher, publisher.ID, "info2 has wrong publisher ID")
			require.NotNil(t, provInfo.PublisherAddr, "info2 missing publisher address")
			require.True(t, provInfo.PublisherAddr.Equal(publisher.Addrs[0]), "provider2 has wrong publisher ID %q, expected %q", provInfo.PublisherAddr, publisher.Addrs[0])
			require.Equal(t, provInfo.ExtendedProviders, extProviders)
		default:
			t.Fatalf("loaded invalid provider ID: %q", provInfo.AddrInfo.ID)
		}
	}

	require.ErrorIs(t, r.AssignPeer(pubID), ErrNoAssigner)
	require.ErrorIs(t, r.UnassignPeer(pubID), ErrNoAssigner)

	// Check that extended provider missing address is caught.
	prevAddrs := extProviders.Providers[0].Addrs
	extProviders.Providers[0].Addrs = nil
	err = r.Update(ctx, provider2, publisher, adCid, extProviders, 0)
	require.ErrorContains(t, err, "missing address")
	extProviders.Providers[0].Addrs = prevAddrs

	// Check that contextual extended provider missing address is caught.
	prevAddrs = extProviders.ContextualProviders[string(epContextId)].Providers[0].Addrs
	extProviders.ContextualProviders[string(epContextId)].Providers[0].Addrs = nil
	err = r.Update(ctx, provider2, publisher, adCid, extProviders, 0)
	require.ErrorContains(t, err, "missing address")
	extProviders.ContextualProviders[string(epContextId)].Providers[0].Addrs = prevAddrs

	r.Close()
	require.NoError(t, dstore.Close())

	// Test that configuring existing registry to use assigner service finds
	// existing publishers.
	t.Log("Converted registry to work with assigner service")
	discoveryCfg.UseAssigner = true
	dstore, err = leveldb.NewDatastore(dataStorePath, nil)
	require.NoError(t, err)
	r, err = New(ctx, discoveryCfg, dstore, WithDiscoverer(mockDiscoverer))
	require.NoError(t, err)

	// There should not be any assigned yet.
	assigned, _, err := r.ListAssignedPeers()
	require.NoError(t, err)
	require.Zero(t, len(assigned))

	// There should be 1 preferred publisher.
	preferred, err := r.ListPreferredPeers()
	require.NoError(t, err)
	require.Equal(t, 1, len(preferred))

	// Assign peer and check that it is assigned.
	err = r.AssignPeer(preferred[0])
	require.NoError(t, err)
	assigned, _, err = r.ListAssignedPeers()
	require.NoError(t, err)
	require.Equal(t, 1, len(assigned))

	// Unassign peer and check that it is not assigned.
	err = r.UnassignPeer(preferred[0])
	require.NoError(t, err)
	assigned, _, err = r.ListAssignedPeers()
	require.NoError(t, err)
	require.Zero(t, len(assigned))

	// Should not be able to assign blocked peer.
	require.True(t, r.BlockPeer(preferred[0]))
	require.ErrorIs(t, r.AssignPeer(preferred[0]), ErrNotAllowed)

	r.Close()
	require.NoError(t, dstore.Close())
}

func TestAllowed(t *testing.T) {
	cfg := config.Discovery{
		Policy: config.Policy{
			Allow:   true,
			Publish: true,
		},
		RediscoverWait: config.Duration(time.Minute),
	}

	ctx := context.Background()

	r, err := New(ctx, cfg, nil)
	require.NoError(t, err)

	provID, err := peer.Decode(limitedID)
	require.NoError(t, err)
	maddr, err := multiaddr.NewMultiaddr(minerAddr)
	require.NoError(t, err)
	provider := peer.AddrInfo{
		ID:    provID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)
	pubAddr, err := multiaddr.NewMultiaddr(publisherAddr)
	require.NoError(t, err)
	publisher := peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{pubAddr},
	}

	err = r.Update(ctx, provider, publisher, cid.Undef, nil, 0)
	require.NoError(t, err)

	// Check that provider is allowed.
	require.True(t, r.Allowed(provID), "peer should be allowed")
	pinfo, allowed := r.ProviderInfo(provID)
	require.NotNil(t, pinfo)
	require.True(t, allowed)

	require.True(t, r.PublishAllowed(provID, pubID), "peer should be allowed")
	require.True(t, r.PublishAllowed(pubID, provID), "peer should be allowed")

	// Block provider and check that provider is blocked.
	require.True(t, r.BlockPeer(provID), "should have updated policy to block peer")
	require.False(t, r.Allowed(provID), "peer should be blocked")
	pinfo, allowed = r.ProviderInfo(provID)
	require.NotNil(t, pinfo)
	require.False(t, allowed)

	require.True(t, r.PublishAllowed(provID, pubID), "peer can be allowed to publish when not allowed to register")
	require.False(t, r.PublishAllowed(pubID, provID), "peer should not be allowed")

	// Allow provider and check that provider is allowed again.
	require.True(t, r.AllowPeer(provID), "should have updated policy to allow peer")
	require.True(t, r.Allowed(provID), "peer should be allowed")
	pinfo, allowed = r.ProviderInfo(provID)
	require.NotNil(t, pinfo)
	require.True(t, allowed)

	require.True(t, r.Allowed(pubID), "peer should be allowed")
	require.True(t, r.PublishAllowed(pubID, pubID), "peer should be allowed")

	require.True(t, r.BlockPeer(pubID), "should have updated policy to block peer")
	require.False(t, r.Allowed(pubID), "peer should be blocked")

	require.True(t, r.AllowPeer(pubID), "should have updated policy to allow peer")
	require.True(t, r.Allowed(pubID), "peer should be allowed")

	require.NoError(t, r.SetPolicy(config.Policy{}))

	if !r.policy.NoneAllowed() {
		t.Error("expected inaccessible policy")
	}

	err = r.SetPolicy(config.Policy{
		Allow:  true,
		Except: []string{publisherID},
	})
	require.NoError(t, err)

	require.False(t, r.Allowed(pubID), "peer should be blocked")
}

func TestPollProvider(t *testing.T) {
	cfg := config.Discovery{
		Policy: config.Policy{
			Allow:   true,
			Publish: true,
		},
		RediscoverWait: config.Duration(time.Minute),
	}

	ctx := context.Background()
	r, err := New(ctx, cfg, datastore.NewMapDatastore())
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	peerID, err := peer.Decode(limitedID)
	require.NoError(t, err)

	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)

	maddr, err := multiaddr.NewMultiaddr(minerAddr)
	require.NoError(t, err)

	prov := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}
	pub := peer.AddrInfo{
		ID: pubID,
	}
	err = r.Update(ctx, prov, pub, cid.Undef, nil, 0)
	require.NoError(t, err)

	poll := polling{
		retryAfter:      time.Minute,
		stopAfter:       time.Hour,
		deactivateAfter: time.Hour,
	}

	// Check for auto-sync after pollInterval 0.
	r.pollProviders(poll, nil)
	timeout := time.After(2 * time.Second)
	select {
	case pinfo := <-r.SyncChan():
		require.Equal(t, pinfo.AddrInfo.ID, peerID, "Wrong provider ID")
		require.Equal(t, pinfo.Publisher, pubID, "Wrong publisher ID")
		require.False(t, pinfo.Inactive(), "Expected provider not to be marked inactive")
	case <-timeout:
		t.Fatal("Expected sync channel to be written")
	}

	// Check that actions chan is not blocked by unread auto-sync channel.
	poll.retryAfter = 0
	poll.deactivateAfter = 0
	r.pollProviders(poll, nil)
	r.pollProviders(poll, nil)
	r.pollProviders(poll, nil)
	done := make(chan struct{})
	r.actions <- func() {
		close(done)
	}
	select {
	case <-done:
	case <-timeout:
		t.Fatal("actions channel blocked")
	}
	select {
	case pinfo := <-r.SyncChan():
		require.Equal(t, pinfo.AddrInfo.ID, peerID, "unexpected provider info on sync channel, expected %q got %q", peerID.String(), pinfo.AddrInfo.ID.String())
		require.True(t, pinfo.Inactive(), "Expected provider to be marked inactive")
	case <-timeout:
		t.Fatal("Expected sync channel to be written")
	}

	// Inactive provider should not be returned.
	pinfo, _ := r.ProviderInfo(peerID)
	require.NotNil(t, pinfo, "expected inactive provider to still be present")
	require.True(t, pinfo.Inactive(), "expected provider to be inactive")

	// Set stopAfter to 0 so that stopAfter will have elapsed since last
	// contact. This will make publisher appear unresponsive and polling will
	// stop.
	poll.stopAfter = 0
	r.pollProviders(poll, nil)
	// wait some time to pass poll interval (for low-res timers)
	time.Sleep(time.Second)
	r.pollProviders(poll, nil)
	r.pollProviders(poll, nil)

	// Check that provider has been removed from registry after provider
	// appeared non-responsive.
	pinfo, _ = r.ProviderInfo(peerID)
	require.Nil(t, pinfo, "expected provider to be removed from registry")

	// Check that delete provider sent over sync channel.
	select {
	case pinfo = <-r.SyncChan():
		require.Equal(t, pinfo.AddrInfo.ID, peerID, "unexpected provider info on sync channel, expected %q got %q", peerID.String(), pinfo.AddrInfo.ID.String())
		require.True(t, pinfo.Deleted(), "expected delete request for unresponsive provider")
	default:
		t.Fatal("sync channel should have deleted provider")
	}

	// This should still be ok to call even after provider is removed.
	err = r.RemoveProvider(context.Background(), peerID)
	require.NoError(t, err)
}

func TestPollProviderOverrides(t *testing.T) {
	cfg := config.Discovery{
		Policy: config.Policy{
			Allow:   true,
			Publish: true,
		},
		RediscoverWait: config.Duration(time.Minute),
	}

	ctx := context.Background()
	r, err := New(ctx, cfg, datastore.NewMapDatastore())
	t.Cleanup(func() { r.Close() })
	require.NoError(t, err)
	defer r.Close()

	peerID, err := peer.Decode(limitedID)
	require.NoError(t, err)
	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)

	maddr, err := multiaddr.NewMultiaddr(minerAddr)
	require.NoError(t, err)

	prov := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}
	pub := peer.AddrInfo{
		ID: pubID,
	}
	err = r.Update(ctx, prov, pub, cid.Undef, nil, 0)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	poll := polling{
		interval:   2 * time.Hour,
		retryAfter: time.Hour,
		stopAfter:  5 * time.Hour,
	}

	overrides := make(map[peer.ID]polling)
	overrides[peerID] = polling{
		interval:   0,
		retryAfter: time.Minute,
		stopAfter:  time.Hour,
	}

	// Check for auto-sync after pollInterval 0.
	r.pollProviders(poll, overrides)
	timeout := time.After(2 * time.Second)
	select {
	case pinfo := <-r.SyncChan():
		require.Equal(t, pinfo.AddrInfo.ID, peerID, "Wrong provider ID")
		require.Equal(t, pinfo.Publisher, pubID, "Wrong publisher ID")
	case <-timeout:
		t.Fatal("Expected sync channel to be written")
	}

	// Set stopAfter to 0 so that stopAfter will have elapsed since last
	// contact. This will make publisher appear unresponsive and polling will
	// stop.
	overrides[peerID] = polling{
		retryAfter: time.Minute,
		stopAfter:  0,
	}
	r.pollProviders(poll, overrides)
	// wait some time to pass poll interval (for low-res timers)
	time.Sleep(time.Second)
	r.pollProviders(poll, overrides)
	r.pollProviders(poll, overrides)

	// Check that provider has been removed from registry after provider
	// appeared non-responsive.
	pinfo, _ := r.ProviderInfo(peerID)
	require.Nil(t, pinfo, "expected provider to be removed from registry")

	// Check that delete provider sent over sync channel.
	select {
	case pinfo = <-r.SyncChan():
		require.Equal(t, pinfo.AddrInfo.ID, peerID, "unexpected provider info on sync channel, expected %q got %q", peerID.String(), pinfo.AddrInfo.ID.String())
		require.True(t, pinfo.Deleted(), "expected delete request for unresponsive provider")
	default:
		t.Fatal("sync channel should have deleted provider")
	}

	// Check that sync channel was not written since polling should have
	// stopped.
	select {
	case <-r.SyncChan():
		t.Fatal("sync channel should not have been written to for override peer")
	default:
	}
}

func TestRegistry_RegisterOrUpdateToleratesEmptyPublisherAddrs(t *testing.T) {
	ctx := context.Background()
	subject, err := New(ctx, config.NewDiscovery(), datastore.NewMapDatastore())
	require.NoError(t, err)
	t.Cleanup(func() { subject.Close() })

	// Register a provider first
	provId, err := peer.Decode(exceptID)
	require.NoError(t, err)
	ma, err := multiaddr.NewMultiaddr(publisherAddr)
	require.NoError(t, err)
	provider := peer.AddrInfo{
		ID:    provId,
		Addrs: []multiaddr.Multiaddr{ma},
	}
	err = subject.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err)

	// Assert that updating publisher for the registered provider with empty addrs does not panic.
	mh, err := multihash.Sum([]byte("fish"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mh)
	err = subject.Update(ctx, provider, peer.AddrInfo{ID: publisherID}, c, nil, 0)
	require.NoError(t, err)

	info, _ := subject.ProviderInfo(provId)
	require.NotNil(t, info)
	require.Nil(t, info.PublisherAddr)

	// Register a publisher that has no addresses, but publisherID is same as
	// provider. Registry should use provider's address as publisher.
	err = subject.Update(ctx, provider, peer.AddrInfo{ID: provId}, c, nil, 0)
	info, _ = subject.ProviderInfo(provId)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, info.AddrInfo.Addrs[0], info.PublisherAddr)
}

func TestFilterIPs(t *testing.T) {
	cfg := config.NewDiscovery()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	maddrLocal, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	require.NoError(t, err)
	maddrPvt, err := multiaddr.NewMultiaddr("/ip4/10.0.1.1/tcp/9999")
	require.NoError(t, err)

	provID, err := peer.Decode(limitedID)
	require.NoError(t, err)
	provAddr, err := multiaddr.NewMultiaddr("/dns4/provider.example.com/tcp/12345")
	require.NoError(t, err)

	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)
	pubAddr, err := multiaddr.NewMultiaddr("/dns4/publisher.example.com/tcp/9876")
	require.NoError(t, err)

	dstore := datastore.NewMapDatastore()
	reg, err := New(ctx, cfg, dstore)
	require.NoError(t, err)
	t.Cleanup(func() { reg.Close() })

	provider := peer.AddrInfo{
		ID:    provID,
		Addrs: []multiaddr.Multiaddr{maddrPvt, provAddr, maddrLocal},
	}
	publisher := peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{maddrPvt, pubAddr, maddrLocal},
	}
	err = reg.Update(ctx, provider, publisher, cid.Undef, nil, 0)
	require.NoError(t, err)
	reg.Close()

	cfg.FilterIPs = true
	reg, err = New(ctx, cfg, dstore)
	require.NoError(t, err)
	t.Cleanup(func() { reg.Close() })

	require.True(t, reg.FilterIPsEnabled())

	// Check that loading from datastore filtered IPs.
	pinfo, _ := reg.ProviderInfo(provID)
	require.NotNil(t, pinfo)
	require.Equal(t, 1, len(pinfo.AddrInfo.Addrs))
	require.Equal(t, provAddr, pinfo.AddrInfo.Addrs[0])
	// Expect nil because first publisher addr was private, so that was what
	// was persisted.
	require.Nil(t, pinfo.PublisherAddr)

	// Check the Update filters IPs.
	err = reg.Update(ctx, provider, publisher, cid.Undef, nil, 0)
	require.NoError(t, err)
	pinfo, _ = reg.ProviderInfo(provID)
	require.NotNil(t, pinfo)
	require.Equal(t, 1, len(pinfo.AddrInfo.Addrs))
	require.Equal(t, provAddr, pinfo.AddrInfo.Addrs[0])
	require.Equal(t, pubAddr.String(), pinfo.PublisherAddr.String())

	provider2 := peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{maddrPvt, pubAddr, maddrLocal},
	}
	// Check the Register filters IPs.
	err = reg.Update(ctx, provider2, peer.AddrInfo{}, cid.Undef, nil, 0)
	require.NoError(t, err)
	pinfo, _ = reg.ProviderInfo(pubID)
	require.NotNil(t, pinfo)
	require.Equal(t, 1, len(pinfo.AddrInfo.Addrs))
	require.Equal(t, pubAddr, pinfo.AddrInfo.Addrs[0])
}

func TestRegistry_loadPersistedProvidersFiltersNilAddrGracefully(t *testing.T) {
	ctx := context.Background()
	ds := datastore.NewMapDatastore()
	pid, err := peer.Decode("12D3KooWK7CTS7cyWi51PeNE3cTjS2F2kDCZaQVU4A5xBmb9J1do")
	require.NoError(t, err)

	err = ds.Put(ctx, peerIDToDsKey(providerKeyPath, pid), []byte(`{"PublisherAddr": null,"AddrInfo": {},"LastAdvertisement":null,"LastAdvertisementTime":"0001-01-01T00:00:00Z","Publisher":"`+pid.String()+`"}`))
	require.NoError(t, err)
	cfg := config.NewDiscovery()
	cfg.FilterIPs = true
	_, err = New(ctx, cfg, ds)
	require.NoError(t, err)
}

func TestFreezeUnfreeze(t *testing.T) {
	cfg := config.Discovery{
		Policy: config.Policy{
			Allow:   true,
			Publish: true,
		},
	}

	ctx := context.Background()
	tempDir := t.TempDir()
	dstore := datastore.NewMapDatastore()
	r, err := New(ctx, cfg, dstore, WithFreezer(tempDir, 90.0))
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	peerID, err := peer.Decode(limitedID)
	require.NoError(t, err)

	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)

	maddr, err := multiaddr.NewMultiaddr(minerAddr)
	require.NoError(t, err)

	prov := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}
	pub := peer.AddrInfo{
		ID: pubID,
	}

	mh, err := multihash.Sum([]byte("somedata"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	adCid := cid.NewCidV1(cid.Raw, mh)

	err = r.Update(ctx, prov, pub, adCid, nil, 0)
	require.NoError(t, err)

	require.False(t, r.Frozen())
	infos := r.AllProviderInfo()
	for i := range infos {
		require.False(t, infos[i].FrozenAt.Defined())
		require.True(t, infos[i].FrozenAtTime.IsZero())
	}

	require.NoError(t, r.Freeze())

	require.True(t, r.Frozen())
	infos = r.AllProviderInfo()
	for i := range infos {
		require.True(t, infos[i].FrozenAt.Defined())
		require.False(t, infos[i].FrozenAtTime.IsZero())
	}

	// Check that a new provider cannot be registered with a frozen publisher.
	peerID2, err := peer.Decode(limitedID2)
	require.NoError(t, err)
	maddr2, err := multiaddr.NewMultiaddr(minerAddr2)
	require.NoError(t, err)
	prov2 := peer.AddrInfo{
		ID:    peerID2,
		Addrs: []multiaddr.Multiaddr{maddr2},
	}
	pubID2, err := peer.Decode(limitedID3)
	require.NoError(t, err)
	pub2 := peer.AddrInfo{
		ID: pubID2,
	}
	mh, err = multihash.Sum([]byte("some-other-data"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	adCid2 := cid.NewCidV1(cid.Raw, mh)

	err = r.Update(ctx, prov2, pub2, adCid2, nil, 0)
	require.ErrorIs(t, err, ErrFrozen)

	// Stop and restart registry and check providers are still frozen.
	r.Close()
	r, err = New(ctx, cfg, dstore, WithFreezer(tempDir, 90.0))
	require.NoError(t, err)
	require.True(t, r.Frozen())
	infos = r.AllProviderInfo()
	for i := range infos {
		require.True(t, infos[i].FrozenAt.Defined())
		require.False(t, infos[i].FrozenAtTime.IsZero())
	}
	r.Close()

	unfrozen, err := Unfreeze(ctx, tempDir, 90.0, dstore)
	require.NoError(t, err)
	require.Equal(t, len(infos), len(unfrozen))
	for i := range infos {
		frozenAt, ok := unfrozen[infos[i].Publisher]
		require.True(t, ok)
		require.Equal(t, infos[i].FrozenAt, frozenAt)
	}

	r, err = New(ctx, cfg, dstore, WithFreezer(tempDir, 90.0))
	require.NoError(t, err)
	require.False(t, r.Frozen())
	infos = r.AllProviderInfo()
	for i := range infos {
		require.False(t, infos[i].FrozenAt.Defined())
		require.True(t, infos[i].FrozenAtTime.IsZero())
	}
	r.Close()

	unfrozen, err = Unfreeze(ctx, tempDir, 90.0, dstore)
	require.NoError(t, err)
	require.Zero(t, len(unfrozen))
}

func TestHandoff(t *testing.T) {
	cfg := config.Discovery{
		Policy: config.Policy{
			Allow:   true,
			Publish: true,
		},
		UseAssigner: true,
	}

	ctx := context.Background()
	tempDir := t.TempDir()
	r, err := New(ctx, cfg, datastore.NewMapDatastore(), WithFreezer(tempDir, 90.0))
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })

	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)
	pubAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	require.NoError(t, err)

	pubAddrInfo := peer.AddrInfo{
		ID:    pubID,
		Addrs: []multiaddr.Multiaddr{pubAddr},
	}

	mh, err := multihash.Sum([]byte("somedata"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	adCid := cid.NewCidV1(cid.Raw, mh)
	lastAdTime := time.Now()

	frozenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		t.Log("Frozen indexer received", req.Method, "request at", req.URL.String())
		pInfos := []model.ProviderInfo{{
			AddrInfo:              pubAddrInfo,
			LastAdvertisement:     adCid,
			LastAdvertisementTime: lastAdTime.Format(time.RFC3339),
			Publisher:             &pubAddrInfo,
			FrozenAt:              adCid,
			FrozenAtTime:          lastAdTime.Format(time.RFC3339),
		}}
		data, err := json.Marshal(pInfos)
		if err != nil {
			panic(err.Error())
		}
		writeJsonResponse(w, http.StatusOK, data)
	}))
	defer frozenServer.Close()

	frozenURL, err := url.Parse(frozenServer.URL)
	require.NoError(t, err)

	peerID, err := peer.Decode(limitedID)
	require.NoError(t, err)

	err = r.Handoff(ctx, pubID, peerID, frozenURL)
	require.NoError(t, err)

	select {
	case pinfo := <-r.SyncChan():
		require.Equal(t, pubID, pinfo.AddrInfo.ID, "Wrong provider ID")
		require.Equal(t, pubID, pinfo.Publisher, "Wrong publisher ID")
		require.Equal(t, adCid, pinfo.StopCid(), "Wrong stop ad CID")
	default:
		t.Fatal("Expected sync channel to be written")
	}

	pubs, froms, err := r.ListAssignedPeers()
	require.NoError(t, err)

	require.Equal(t, 1, len(pubs))
	require.Equal(t, 1, len(froms))

	require.Equal(t, pubID, pubs[0])
	require.Equal(t, peerID, froms[0])

	provs := r.AllProviderInfo()
	require.NoError(t, err)
	require.Equal(t, 1, len(provs))

	prov := provs[0]
	require.Equal(t, pubID, prov.AddrInfo.ID)
	require.Equal(t, pubID, prov.Publisher)

	// Freeze the indexer and check that a new publisher cannot be assigned.
	require.NoError(t, r.Freeze())
	pubID2, err := peer.Decode(limitedID3)
	require.NoError(t, err)
	require.ErrorIs(t, r.AssignPeer(pubID2), ErrFrozen)
}

func writeJsonResponse(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		http.Error(w, "", http.StatusInternalServerError)
	}
}
