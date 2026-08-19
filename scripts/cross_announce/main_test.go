package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

func genPeerID(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		t.Fatalf("generating key pair: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("deriving peer ID: %v", err)
	}
	return id
}

func testProvider(t *testing.T, adCid cid.Cid) *model.ProviderInfo {
	t.Helper()
	providerID := genPeerID(t)
	publisherID := genPeerID(t)
	maStr := fmt.Sprintf("/ip4/127.0.0.1/tcp/4001/p2p/%s", providerID)
	maddr, _ := ma.NewMultiaddr(maStr)
	pubMaStr := fmt.Sprintf("/ip4/127.0.0.1/tcp/4002/p2p/%s", publisherID)
	pubMaddr, _ := ma.NewMultiaddr(pubMaStr)
	return &model.ProviderInfo{
		AddrInfo: peer.AddrInfo{
			ID:    providerID,
			Addrs: []ma.Multiaddr{maddr},
		},
		LastAdvertisement: adCid,
		Publisher: &peer.AddrInfo{
			ID:    publisherID,
			Addrs: []ma.Multiaddr{pubMaddr},
		},
	}
}

func testCid() cid.Cid {
	h, _ := mh.Sum([]byte("test"), uint64(mc.Identity), -1)
	return cid.NewCidV1(uint64(mc.DagCbor), h)
}

func newFindServer(providers []*model.ProviderInfo) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(providers)
	}))
}

func newAnnounceServer(status int, count *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/announce" || r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		*count++
		w.WriteHeader(status)
	}))
}

func testOptions(source, target string) options {
	return options{
		source:      source,
		target:      target,
		httpTimeout: 5 * time.Second,
	}
}

func TestAllOK(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
		testProvider(t, testCid()),
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.total != 3 {
		t.Errorf("total = %d, want 3", st.total)
	}
	if st.announced != 3 {
		t.Errorf("announced = %d, want 3", st.announced)
	}
	if st.skipped != 0 {
		t.Errorf("skipped = %d, want 0", st.skipped)
	}
	if st.notAllowed != 0 {
		t.Errorf("notAllowed = %d, want 0", st.notAllowed)
	}
	if st.failed != 0 {
		t.Errorf("failed = %d, want 0", st.failed)
	}
	if announceCount != 3 {
		t.Errorf("announce requests = %d, want 3", announceCount)
	}
}

func Test403NotAllowed(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusForbidden, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.notAllowed != 1 {
		t.Errorf("notAllowed = %d, want 1", st.notAllowed)
	}
	if st.failed != 0 {
		t.Errorf("failed = %d, want 0", st.failed)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
}

func Test500Failed(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusInternalServerError, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.failed != 1 {
		t.Errorf("failed = %d, want 1", st.failed)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
}

func TestNilPublisherSkipped(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
	}
	providers[0].Publisher = nil
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.skipped != 1 {
		t.Errorf("skipped = %d, want 1", st.skipped)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}

func TestUndefCidSkipped(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, cid.Undef),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.skipped != 1 {
		t.Errorf("skipped = %d, want 1", st.skipped)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}

func TestPIDFilter(t *testing.T) {
	p1 := testProvider(t, testCid())
	p2 := testProvider(t, testCid())
	providers := []*model.ProviderInfo{p1, p2}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.providerID = p2.AddrInfo.ID.String()

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.total != 2 {
		t.Errorf("total = %d, want 2", st.total)
	}
	if st.announced != 1 {
		t.Errorf("announced = %d, want 1", st.announced)
	}
	if announceCount != 1 {
		t.Errorf("announce requests = %d, want 1", announceCount)
	}
}

func TestCancelledContext(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
	}

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	// Serve providers from a handler that cancels the context after writing
	// the response, so ListProviders succeeds but the loop sees a cancelled
	// context on its first iteration.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	findSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/providers" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(providers)
		cancel()
	}))
	defer findSrv.Close()

	st, err := run(ctx, testOptions(findSrv.URL, announceSrv.URL))
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if st.total != 1 {
		t.Errorf("total = %d, want 1", st.total)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}

func TestDryRun(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.dryRun = true

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 2 {
		t.Errorf("announced = %d, want 2", st.announced)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}
