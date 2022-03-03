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
	trustedID  = "12D3KooWSG3JuvEjRkSxt93ADTjQxqe4ExbBwSkQ9Zyk1WfBaZJF"
	trustedID2 = "12D3KooWKSNuuq77xqnpPLnU3fq1bTQW2TwSZL2Z4QTHEYpUVzfr"

	minerDiscoAddr = "stitest999999"
	minerAddr      = "/ip4/127.0.0.1/tcp/9999"
	minerAddr2     = "/ip4/127.0.0.2/tcp/9999"
)

var discoveryCfg = config.Discovery{
	Policy: config.Policy{
		Allow:       false,
		Except:      []string{exceptID, trustedID, trustedID2},
		Trust:       false,
		TrustExcept: []string{trustedID, trustedID2},
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
	mockDisco := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := NewRegistry(ctx, discoveryCfg, nil, mockDisco)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("created new registry")

	peerID, err := peer.Decode(trustedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3002")
	if err != nil {
		t.Fatalf("Cannot create multiaddr: %s", err)
	}
	info := &ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
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
	mockDisco := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := NewRegistry(ctx, discoveryCfg, nil, mockDisco)
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

	info := r.ProviderInfoByAddr(minerDiscoAddr)
	if info == nil {
		t.Fatal("did not get provider info for miner")
	}
	t.Log("got provider info for miner")

	if info.AddrInfo.ID != peerID {
		t.Error("did not get correct porvider id")
	}

	peerID, err = peer.Decode(trustedID)
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

	infos := r.AllProviderInfo()
	if len(infos) != 2 {
		t.Fatal("expected 2 provider infos")
	}
}

func TestDiscoveryBlocked(t *testing.T) {
	mockDisco := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID, err := peer.Decode(exceptID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}

	r, err := NewRegistry(ctx, discoveryCfg, nil, mockDisco)
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

	into := r.ProviderInfoByAddr(minerDiscoAddr)
	if into != nil {
		t.Error("should not have found provider info for miner")
	}
}

func TestDatastore(t *testing.T) {
	dataStorePath := t.TempDir()
	mockDisco := newMockDiscoverer(t, exceptID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID, err := peer.Decode(trustedID)
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
	peerID, err = peer.Decode(trustedID2)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}
	maddr, err = multiaddr.NewMultiaddr(minerAddr2)
	if err != nil {
		t.Fatal("bad miner address:", err)
	}
	info2 := &ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		},
	}

	// Create datastore
	dstore, err := leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewRegistry(ctx, discoveryCfg, dstore, mockDisco)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("created new registry with datastore")

	err = r.Register(ctx, info1)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	err = r.RegisterOrUpdate(ctx, info2.AddrInfo.ID, []string{minerAddr2}, cid.Undef, info2.AddrInfo.ID)
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
	r, err = NewRegistry(ctx, discoveryCfg, dstore, mockDisco)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("re-created new registry with datastore")

	infos := r.AllProviderInfo()
	if len(infos) != 2 {
		t.Fatal("expected 2 provider infos")
	}

	for _, provInfo := range infos {
		if provInfo.AddrInfo.ID == info1.AddrInfo.ID {
			if err = provInfo.Publisher.Validate(); err == nil {
				t.Fatal("info1 should not have valid publisher")
			}
		} else if provInfo.AddrInfo.ID == info2.AddrInfo.ID {
			if provInfo.Publisher != info2.AddrInfo.ID {
				t.Fatal("info2 has wrong publisher")
			}
		} else {
			t.Fatalf("loaded invalid provider ID: %s", provInfo.AddrInfo.ID)
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
			Allow: true,
			Trust: true,
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

	peerID, err := peer.Decode(trustedID)
	if err != nil {
		t.Fatal("bad provider ID:", err)
	}

	err = r.RegisterOrUpdate(ctx, peerID, []string{minerAddr}, cid.Undef, peerID)
	if err != nil {
		t.Fatal("failed to register directly:", err)
	}

	retryAfter := time.Minute
	stopAfter := time.Hour

	// Check for auto-sync after pollInterval 0.
	r.pollProviders(0, retryAfter, stopAfter)
	timeout := time.After(2 * time.Second)
	select {
	case <-r.SyncChan():
	case <-timeout:
		t.Fatal("Expected sync channel to be written")
	}

	// Check that actions chan is not blocked by unread auto-sync channel.
	retryAfter = 0
	r.pollProviders(0, retryAfter, stopAfter)
	r.pollProviders(0, retryAfter, stopAfter)
	r.pollProviders(0, retryAfter, stopAfter)
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
	stopAfter = 0
	r.pollProviders(0, retryAfter, stopAfter)
	r.pollProviders(0, retryAfter, stopAfter)
	r.pollProviders(0, retryAfter, stopAfter)

	// Check that publisher has been removed from provider info when publisher
	// appeared non-responsive.
	pinfo := r.ProviderInfo(peerID)
	if pinfo == nil {
		t.Fatal("did not find registered provider")
	}
	if err = pinfo.Publisher.Validate(); err == nil {
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
