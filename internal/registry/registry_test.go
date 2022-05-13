package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry/discovery"
	"github.com/ipfs/go-cid"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
)

type mockDiscoverer struct {
	discoverRsp *discovery.Discovered
}

const (
	exceptID   = "12D3KooWK7CTS7cyWi51PeNE3cTjS2F2kDCZaQVU4A5xBmb9J1do"
	limitedID  = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
	limitedID2 = "12D3KooWKSNuuq77xqnpPLnU3fq1bTQW2TwSZL2Z4QTHEYpUVzfr"

	minerDiscoAddr = "stitest999999"
	minerAddr      = "/ip4/127.0.0.1/tcp/9999"
	minerAddr2     = "/ip4/127.0.0.2/tcp/9999"

	publisherID   = "12D3KooWFNkdmdb38g4VVCGaJsKin4BzGpP4bedfxU76PF2AahoP"
	publisherAddr = "/ip4/127.0.0.3/tcp/1234"
)

var discoveryCfg = config.Discovery{
	Policy: config.Policy{
		Allow:           false,
		Except:          []string{exceptID, limitedID, limitedID2, publisherID},
		RateLimit:       false,
		RateLimitExcept: []string{limitedID, limitedID2, publisherID},
		Publish:         false,
		PublishExcept:   []string{publisherID},
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

	r, err := NewRegistry(ctx, discoveryCfg, nil, mockDiscoverer)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("created new registry")

	peerID, err := peer.Decode(limitedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3002")
	if err != nil {
		t.Fatalf("Cannot create multiaddr: %s", err)
	}
	pubID, err := peer.Decode(publisherID)
	if err != nil {
		t.Fatal("bad publisher ID:", err)
	}
	pubAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	if err != nil {
		t.Fatalf("Cannot create multiaddr: %s", err)
	}
	info := &ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
		Publisher:     pubID,
		PublisherAddr: pubAddr,
	}

	err = r.Register(ctx, info)
	if err != nil {
		t.Error("failed to register directly:", err)
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Check that 2nd call to Close is ok
	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryAllowed(t *testing.T) {
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := NewRegistry(ctx, discoveryCfg, nil, mockDiscoverer)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	t.Log("created new registry")

	peerID, err := peer.Decode(exceptID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}

	err = r.Discover(peerID, minerDiscoAddr, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("discovered mock miner", minerDiscoAddr)

	info := r.ProviderInfo(peerID)
	if info == nil {
		t.Fatal("did not get provider info for miner")
	}
	t.Log("got provider info for miner")

	if info.AddrInfo.ID != peerID {
		t.Error("did not get correct porvider id")
	}

	peerID, err = peer.Decode(limitedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3002")
	if err != nil {
		t.Fatalf("Cannot create multiaddr: %s", err)
	}
	info = &ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
	}

	err = r.Register(ctx, info)
	if err != nil {
		t.Error("failed to register directly:", err)
	}

	if !r.IsRegistered(peerID) {
		t.Error("peer is not registered")
	}

	infos := r.AllProviderInfo()
	if len(infos) != 2 {
		t.Fatal("expected 2 provider infos")
	}

	r.cleanup()
	r.actions <- func() { r.rediscoverWait = 0 }
	if len(r.discoverTimes) == 0 {
		t.Error("should not have cleaned up discovery times")
	}
	r.cleanup()
	r.actions <- func() {}
	if len(r.discoverTimes) != 0 {
		t.Error("should have cleaned up discovery times")
	}
}

func TestDiscoveryBlocked(t *testing.T) {
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID, err := peer.Decode(exceptID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}

	r, err := NewRegistry(ctx, discoveryCfg, nil, mockDiscoverer)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	t.Log("created new registry")

	r.BlockPeer(peerID)

	err = r.Discover(peerID, minerDiscoAddr, true)
	if !errors.Is(err, ErrNotAllowed) {
		t.Fatal("expected error:", ErrNotAllowed, "got:", err)
	}

	into := r.ProviderInfo(peerID)
	if into != nil {
		t.Error("should not have found provider info for miner")
	}
}

func TestDatastore(t *testing.T) {
	dataStorePath := t.TempDir()
	mockDiscoverer := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID, err := peer.Decode(limitedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	maddr, err := multiaddr.NewMultiaddr(minerAddr)
	if err != nil {
		t.Fatal("bad miner address:", err)
	}
	info1 := &ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
	}
	peerID, err = peer.Decode(limitedID2)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	maddr, err = multiaddr.NewMultiaddr(minerAddr2)
	if err != nil {
		t.Fatal("bad miner address:", err)
	}
	pubID, err := peer.Decode(publisherID)
	if err != nil {
		t.Fatal("bad publisher ID:", err)
	}
	pubAddr, err := multiaddr.NewMultiaddr(publisherAddr)
	if err != nil {
		t.Fatal("bad publisher address:", err)
	}
	info2 := &ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
		Publisher:     pubID,
		PublisherAddr: pubAddr,
	}

	// Create datastore
	dstore, err := leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRegistry(ctx, discoveryCfg, dstore, mockDiscoverer)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("created new registry with datastore")

	err = r.Register(ctx, info1)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	publisher := peer.AddrInfo{
		ID:    info2.Publisher,
		Addrs: []multiaddr.Multiaddr{pubAddr},
	}
	err = r.RegisterOrUpdate(ctx, info2.AddrInfo.ID, []string{minerAddr2}, cid.Undef, publisher)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	pinfo := r.ProviderInfo(peerID)
	if pinfo == nil {
		t.Fatal("did not find registered provider")
	}

	pinfo = r.ProviderInfo(info2.AddrInfo.ID)
	if pinfo == nil {
		t.Fatal("did not find registered provider")
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Create datastore
	dstore, err = leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err = NewRegistry(ctx, discoveryCfg, dstore, mockDiscoverer)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("re-created new registry with datastore")

	infos := r.AllProviderInfo()
	if len(infos) != 2 {
		t.Fatal("expected 2 provider infos")
	}

	for _, provInfo := range infos {
		switch provInfo.AddrInfo.ID {
		case info1.AddrInfo.ID:
			if provInfo.Publisher.Validate() == nil {
				t.Fatal("info1 should not have valid publisher")
			}
		case info2.AddrInfo.ID:
			if provInfo.Publisher != info2.Publisher {
				t.Fatal("info2 has wrong publisher ID")
			}
			if provInfo.PublisherAddr == nil {
				t.Fatal("info2 missing publisher address")
			}
			if !provInfo.PublisherAddr.Equal(info2.PublisherAddr) {
				t.Fatalf("info2 has wrong publisher ID %q, expected %q", provInfo.PublisherAddr, info2.PublisherAddr)
			}
		default:
			t.Fatalf("loaded invalid provider ID: %q", provInfo.AddrInfo.ID)
		}
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
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
	// Create datastore
	dstore, err := leveldb.NewDatastore(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRegistry(ctx, cfg, dstore, nil)
	if err != nil {
		t.Fatal(err)
	}

	peerID, err := peer.Decode(limitedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	pubID, err := peer.Decode(publisherID)
	if err != nil {
		t.Fatal("bad publisher ID:", err)
	}

	pub := peer.AddrInfo{
		ID: pubID,
	}
	err = r.RegisterOrUpdate(ctx, peerID, []string{minerAddr}, cid.Undef, pub)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	poll := polling{
		retryAfter: time.Minute,
		stopAfter:  time.Hour,
	}

	// Check for auto-sync after pollInterval 0.
	r.pollProviders(poll, nil)
	timeout := time.After(2 * time.Second)
	select {
	case pinfo := <-r.SyncChan():
		if pinfo.AddrInfo.ID != peerID {
			t.Fatal("Wrong provider ID")
		}
		if pinfo.Publisher != pubID {
			t.Fatal("Wrong publisher ID")
		}
	case <-timeout:
		t.Fatal("Expected sync channel to be written")
	}

	// Check that actions chan is not blocked by unread auto-sync channel.
	poll.retryAfter = 0
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
	case <-r.SyncChan():
	case <-timeout:
		t.Fatal("Expected sync channel to be written")
	}

	// Set stopAfter to 0 so that stopAfter will have elapsed since last
	// contact. This will make publisher appear unresponsive and polling will
	// stop.
	poll.stopAfter = 0
	r.pollProviders(poll, nil)
	r.pollProviders(poll, nil)
	r.pollProviders(poll, nil)

	// Check that publisher has been removed from provider info when publisher
	// appeared non-responsive.
	pinfo := r.ProviderInfo(peerID)
	if pinfo == nil {
		t.Fatal("did not find registered provider")
	}
	if pinfo.Publisher.Validate() == nil {
		t.Fatal("should not have valid publisher after polling stopped")
	}

	// Check that sync channel was not written since polling should have
	// stopped.
	select {
	case <-r.SyncChan():
		t.Fatal("sync channel should not have beem written to")
	default:
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
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
	// Create datastore
	dstore, err := leveldb.NewDatastore(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRegistry(ctx, cfg, dstore, nil)
	if err != nil {
		t.Fatal(err)
	}

	peerID, err := peer.Decode(limitedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	pubID, err := peer.Decode(publisherID)
	if err != nil {
		t.Fatal("bad publisher ID:", err)
	}

	pub := peer.AddrInfo{
		ID: pubID,
	}
	err = r.RegisterOrUpdate(ctx, peerID, []string{minerAddr}, cid.Undef, pub)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	r, err = NewRegistry(ctx, cfg, dstore, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = r.RegisterOrUpdate(ctx, peerID, []string{minerAddr}, cid.Undef, pub)
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
		retryAfter: time.Minute,
		stopAfter:  time.Hour,
	}

	// Check for auto-sync after pollInterval 0.
	r.pollProviders(poll, overrides)
	timeout := time.After(2 * time.Second)
	select {
	case pinfo := <-r.SyncChan():
		if pinfo.AddrInfo.ID != peerID {
			t.Fatal("Wrong provider ID")
		}
		if pinfo.Publisher != pubID {
			t.Fatal("Wrong publisher ID")
		}
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
	r.pollProviders(poll, overrides)
	r.pollProviders(poll, overrides)

	// Check that publisher has been removed from provider info when publisher
	// appeared non-responsive.
	pinfo := r.ProviderInfo(peerID)
	if pinfo == nil {
		t.Fatal("did not find registered provider")
	}
	if pinfo.Publisher.Validate() == nil {
		t.Fatal("should not have valid publisher after polling stopped")
	}

	// Check that sync channel was not written since polling should have
	// stopped.
	select {
	case <-r.SyncChan():
		t.Fatal("sync channel should not have beem written to")
	default:
	}

	err = r.Close()
	if err != nil {
		t.Fatal(err)
	}
}
