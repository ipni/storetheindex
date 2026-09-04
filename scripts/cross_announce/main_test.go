package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/find/model"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
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

// uniqueCid returns a distinct CID per call, for fixtures that need several
// different advertisements.
func uniqueCid(t *testing.T) cid.Cid {
	t.Helper()
	h, _ := mh.Sum([]byte(genPeerID(t).String()), uint64(mc.Identity), -1)
	return cid.NewCidV1(uint64(mc.DagCbor), h)
}

// variantCid returns a CID distinct from testCid() so source and target can
// carry different advertisements.
func variantCid() cid.Cid {
	h, _ := mh.Sum([]byte("variant"), uint64(mc.Identity), -1)
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
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/announce" || r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		*count++
		mu.Unlock()
		w.WriteHeader(status)
	}))
}

// newAnnounceCidServer records the announced CID from each request's CBOR
// body, which is what the ingest client sends.
func newAnnounceCidServer(cids *[]cid.Cid, t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/announce" || r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading announce body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var msg message.Message
		if err := msg.UnmarshalCBOR(bytes.NewReader(body)); err != nil {
			t.Errorf("decoding announce body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		*cids = append(*cids, msg.Cid)
		w.WriteHeader(http.StatusNoContent)
	}))
}

func testOptions(source, target string) options {
	return options{
		source:         source,
		target:         target,
		httpTimeout:    5 * time.Second,
		skipInactive:   true,
		skipLagging:    false,
		lagFreshWithin: time.Hour,
		// allowNoTargetFind keeps the Task 3 and Task 4 tests running now that
		// -target-find is mandatory; those tests predate the guard chain and
		// exercise selection without a target view.
		allowNoTargetFind: true,
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
	if st.deduped != 0 {
		t.Errorf("deduped = %d, want 0", st.deduped)
	}
	if st.upToDate != 0 {
		t.Errorf("upToDate = %d, want 0", st.upToDate)
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
	if st.skipped != 1 {
		t.Errorf("skipped = %d, want 1", st.skipped)
	}
	if announceCount != 1 {
		t.Errorf("announce requests = %d, want 1", announceCount)
	}
}

// TestPIDExcludedGroupCounting asserts the -pid counting rule: an excluded
// publisher group with more than one member contributes exactly one to
// skipped (the head), not one per provider. Under the wrong reading skipped
// would be 2 and the bookkeeping identity would break.
func TestPIDExcludedGroupCounting(t *testing.T) {
	// Publisher P1 holds providers A and B; publisher P2 holds provider C.
	a := testProvider(t, testCid())
	b := testProvider(t, testCid())
	b.Publisher = a.Publisher
	c := testProvider(t, testCid())
	providers := []*model.ProviderInfo{a, b, c}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	// -pid names C, so P2's head is kept and P1's head (with B deduped) is
	// excluded.
	o := testOptions(findSrv.URL, announceSrv.URL)
	o.providerID = c.AddrInfo.ID.String()

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.total != 3 {
		t.Errorf("total = %d, want 3", st.total)
	}
	if st.deduped != 1 {
		t.Errorf("deduped = %d, want 1", st.deduped)
	}
	if st.announced != 1 {
		t.Errorf("announced = %d, want 1", st.announced)
	}
	if st.skipped != 1 {
		t.Errorf("skipped = %d, want 1 (the excluded head only, not the whole group)", st.skipped)
	}
	if announceCount != 1 {
		t.Errorf("announce requests = %d, want 1", announceCount)
	}
}

func TestCancelledContext(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	// Cancel before the run so ListProviders fails on the cancelled context.
	// The point of this case is that no announce request is made once the
	// context is done.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	st, err := run(ctx, testOptions(findSrv.URL, announceSrv.URL))
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
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

func TestSharedPublisherDedupes(t *testing.T) {
	p1 := testProvider(t, testCid())
	p2 := testProvider(t, testCid())
	p2.Publisher = p1.Publisher
	providers := []*model.ProviderInfo{p1, p2}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.total != 2 {
		t.Errorf("total = %d, want 2", st.total)
	}
	if st.announced != 1 {
		t.Errorf("announced = %d, want 1", st.announced)
	}
	if st.deduped != 1 {
		t.Errorf("deduped = %d, want 1", st.deduped)
	}
	if st.skipped != 0 {
		t.Errorf("skipped = %d, want 0", st.skipped)
	}
	if announceCount != 1 {
		t.Errorf("announce requests = %d, want 1", announceCount)
	}
}

func TestNewestTimeWins(t *testing.T) {
	newer := testCid()
	older := testCid()

	pNewer := testProvider(t, newer)
	pNewer.LastAdvertisementTime = "2026-08-18T12:00:00Z"
	pOlder := testProvider(t, older)
	pOlder.LastAdvertisementTime = "2026-08-18T11:00:00Z"
	pOlder.Publisher = pNewer.Publisher
	providers := []*model.ProviderInfo{pOlder, pNewer}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announcedCids []cid.Cid
	announceSrv := newAnnounceCidServer(&announcedCids, t)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 1 {
		t.Fatalf("announced = %d, want 1", st.announced)
	}
	if len(announcedCids) != 1 || !announcedCids[0].Equals(newer) {
		t.Errorf("announced cid = %v, want %v", announcedCids, newer)
	}
}

func TestTieBreakLowestProviderID(t *testing.T) {
	ids := make([]peer.ID, 0, 8)
	for i := 0; i < 8; i++ {
		ids = append(ids, genPeerID(t))
	}
	pubID := genPeerID(t)

	var winner peer.ID
	for trial := 0; trial < 20; trial++ {
		providers := make([]*model.ProviderInfo, 0, len(ids))
		for _, id := range ids {
			p := testProvider(t, testCid())
			p.AddrInfo.ID = id
			providers = append(providers, p)
		}
		// Give every provider the same publisher and the same (empty) time so
		// the tie-break is on provider ID alone.
		pub := &peer.AddrInfo{ID: pubID, Addrs: []ma.Multiaddr{mustMultiaddr(t, pubID)}}
		for _, p := range providers {
			p.Publisher = pub
		}
		// Shuffle the slice so the tie-break cannot depend on source order.
		for i := len(providers) - 1; i > 0; i-- {
			j := i % (trial + 1)
			providers[i], providers[j] = providers[j], providers[i]
		}

		heads, skipped, deduped := groupByPublisher(providers)
		if skipped != 0 || deduped != len(ids)-1 {
			t.Fatalf("trial %d: skipped = %d, deduped = %d", trial, skipped, deduped)
		}
		if len(heads) != 1 {
			t.Fatalf("trial %d: got %d heads, want 1", trial, len(heads))
		}
		var headID peer.ID
		for _, h := range heads {
			headID = h.provider.AddrInfo.ID
		}
		if trial == 0 {
			winner = headID
		} else if headID != winner {
			t.Fatalf("trial %d: head = %s, want stable winner %s", trial, headID, winner)
		}
	}

	// The winner must be the lowest provider ID, independent of slice order.
	var lowest peer.ID
	for i, id := range ids {
		if i == 0 || id.String() < lowest.String() {
			lowest = id
		}
	}
	if winner != lowest {
		t.Errorf("winner = %s, want lowest ID %s", winner, lowest)
	}
}

func TestUpToDate(t *testing.T) {
	p := testProvider(t, testCid())
	source := []*model.ProviderInfo{p}
	target := []*model.ProviderInfo{p}
	findSrv := newFindServer(source)
	defer findSrv.Close()
	targetFindSrv := newFindServer(target)
	defer targetFindSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.targetFindURL = targetFindSrv.URL

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.upToDate != 1 {
		t.Errorf("upToDate = %d, want 1", st.upToDate)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}

func TestAncestorScenario(t *testing.T) {
	pubID := genPeerID(t)
	pubAddr := mustMultiaddr(t, pubID)
	pub := &peer.AddrInfo{ID: pubID, Addrs: []ma.Multiaddr{pubAddr}}

	cidX := testCid()
	cidY := testCid()
	cidZ := testCid()

	// Source: A at X (newer), B at Y (older).
	aSrc := testProvider(t, cidX)
	aSrc.Publisher = pub
	aSrc.LastAdvertisementTime = "2026-08-18T12:00:00Z"
	bSrc := testProvider(t, cidY)
	bSrc.Publisher = pub
	bSrc.LastAdvertisementTime = "2026-08-18T11:00:00Z"
	source := []*model.ProviderInfo{aSrc, bSrc}

	// Target: A at X (newer), B at Z (older).
	aTgt := testProvider(t, cidX)
	aTgt.Publisher = pub
	aTgt.LastAdvertisementTime = "2026-08-18T12:00:00Z"
	bTgt := testProvider(t, cidZ)
	bTgt.Publisher = pub
	bTgt.LastAdvertisementTime = "2026-08-18T11:00:00Z"
	target := []*model.ProviderInfo{aTgt, bTgt}

	findSrv := newFindServer(source)
	defer findSrv.Close()
	targetFindSrv := newFindServer(target)
	defer targetFindSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.targetFindURL = targetFindSrv.URL

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.upToDate != 1 {
		t.Errorf("upToDate = %d, want 1", st.upToDate)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}

func TestPIDAnnouncesPublisherHead(t *testing.T) {
	headCid := testCid()
	ancestorCid := testCid()

	head := testProvider(t, headCid)
	head.LastAdvertisementTime = "2026-08-18T12:00:00Z"
	other := testProvider(t, ancestorCid)
	other.Publisher = head.Publisher
	other.LastAdvertisementTime = "2026-08-18T11:00:00Z"
	providers := []*model.ProviderInfo{other, head}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announcedCids []cid.Cid
	announceSrv := newAnnounceCidServer(&announcedCids, t)
	defer announceSrv.Close()

	// -pid names the non-head provider; the head must still be announced.
	o := testOptions(findSrv.URL, announceSrv.URL)
	o.providerID = other.AddrInfo.ID.String()

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 1 {
		t.Fatalf("announced = %d, want 1", st.announced)
	}
	if len(announcedCids) != 1 || !announcedCids[0].Equals(headCid) {
		t.Errorf("announced cid = %v, want publisher head %v", announcedCids, headCid)
	}
}

func TestUndefCidNeverBecomesHead(t *testing.T) {
	// A cid.Undef provider sharing a publisher with a valid one must not win
	// the group.
	valid := testProvider(t, testCid())
	undef := testProvider(t, cid.Undef)
	undef.Publisher = valid.Publisher
	providers := []*model.ProviderInfo{undef, valid}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announcedCids []cid.Cid
	announceSrv := newAnnounceCidServer(&announcedCids, t)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.skipped != 1 {
		t.Errorf("skipped = %d, want 1", st.skipped)
	}
	if st.announced != 1 {
		t.Fatalf("announced = %d, want 1", st.announced)
	}
	if len(announcedCids) != 1 || announcedCids[0].Equals(cid.Undef) {
		t.Errorf("announced cid = %v, want the valid cid", announcedCids)
	}

	// A cid.Undef provider alone in its group is skipped, never announced.
	lone := []*model.ProviderInfo{testProvider(t, cid.Undef)}
	loneSrv := newFindServer(lone)
	defer loneSrv.Close()
	var loneCount int
	loneAnnounce := newAnnounceServer(http.StatusNoContent, &loneCount)
	defer loneAnnounce.Close()

	st, err = run(context.Background(), testOptions(loneSrv.URL, loneAnnounce.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.skipped != 1 {
		t.Errorf("skipped = %d, want 1", st.skipped)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if loneCount != 0 {
		t.Errorf("announce requests = %d, want 0", loneCount)
	}
}

// headSum is the sum of every head-level counter: each head of a surviving
// publisher group lands in exactly one of these buckets.
func headSum(st stats) int {
	return st.announced + st.upToDate + st.inactive + st.lagging + st.targetAhead + st.unverifiable + st.notAllowed + st.failed
}

func TestBookkeepingIdentity(t *testing.T) {
	providers := make([]*model.ProviderInfo, 0, 10)
	for i := 0; i < 10; i++ {
		providers = append(providers, testProvider(t, testCid()))
	}
	// Two providers share a publisher (one deduped).
	providers[1].Publisher = providers[0].Publisher
	// One has no publisher.
	providers[2].Publisher = nil
	// One has an undefined CID.
	providers[3].LastAdvertisement = cid.Undef

	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	st, err := run(context.Background(), testOptions(findSrv.URL, announceSrv.URL))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.total != 10 {
		t.Fatalf("total = %d, want 10", st.total)
	}
	// Provider-level: 2 skipped (nil publisher, undef cid), 1 deduped, 7 heads.
	if st.skipped != 2 {
		t.Errorf("skipped = %d, want 2", st.skipped)
	}
	if st.deduped != 1 {
		t.Errorf("deduped = %d, want 1", st.deduped)
	}
	heads := st.total - st.skipped - st.deduped
	if heads != 7 {
		t.Errorf("heads = %d, want 7", heads)
	}
	// Head-level: all 7 heads announced.
	if headSum(st) != heads {
		t.Errorf("head buckets sum to %d, want %d", headSum(st), heads)
	}
}

// TestBookkeepingIdentityWithGuards asserts the bookkeeping identity while the
// safety guards are firing: with a target list, several heads are dropped by
// different guards in one run, and skipped + deduped plus the sum of the
// head-level counters must still equal total.
func TestBookkeepingIdentityWithGuards(t *testing.T) {
	srcTime := time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339)
	fresh := time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339)

	// Up to date: same head on both sides.
	pubUpToDate := sharedPublisher(t)
	srcUpToDate := providerWith(t, pubUpToDate, testCid(), "2026-08-18T12:00:00Z", 0, false)
	tgtUpToDate := providerWith(t, pubUpToDate, testCid(), "2026-08-18T12:00:00Z", 0, false)

	// Target ahead: target time at or after source time, differing CIDs.
	pubAhead := sharedPublisher(t)
	srcAhead := providerWith(t, pubAhead, testCid(), "2026-08-18T11:00:00Z", 0, false)
	tgtAhead := providerWith(t, pubAhead, variantCid(), "2026-08-18T12:00:00Z", 0, false)

	// Unverifiable: target has a defined CID but no parseable timestamp.
	pubUnverifiable := sharedPublisher(t)
	srcUnverifiable := providerWith(t, pubUnverifiable, testCid(), "2026-08-18T12:00:00Z", 0, false)
	tgtUnverifiable := providerWith(t, pubUnverifiable, variantCid(), "", 0, false)

	// All-inactive group: every source provider is Inactive.
	pubInactive := sharedPublisher(t)
	srcInactive1 := providerWith(t, pubInactive, testCid(), "2026-08-18T12:00:00Z", 0, true)
	srcInactive2 := providerWith(t, pubInactive, variantCid(), "2026-08-18T11:00:00Z", 0, true)
	tgtInactive := providerWith(t, pubInactive, variantCid(), "2026-08-18T11:00:00Z", 0, false)

	// Lagging: target group has a non-zero Lag with a fresh timestamp.
	pubLagging := sharedPublisher(t)
	srcLagging := providerWith(t, pubLagging, testCid(), srcTime, 0, false)
	tgtLagging := providerWith(t, pubLagging, variantCid(), fresh, 3, false)

	// Genuinely announced: target time older than source time.
	pubAnnounced := sharedPublisher(t)
	srcAnnounced := providerWith(t, pubAnnounced, testCid(), "2026-08-18T12:00:00Z", 0, false)
	tgtAnnounced := providerWith(t, pubAnnounced, variantCid(), "2026-08-18T11:00:00Z", 0, false)

	source := []*model.ProviderInfo{
		srcUpToDate,
		srcAhead,
		srcUnverifiable,
		srcInactive1,
		srcInactive2,
		srcLagging,
		srcAnnounced,
	}
	target := []*model.ProviderInfo{
		tgtUpToDate,
		tgtAhead,
		tgtUnverifiable,
		tgtInactive,
		tgtLagging,
		tgtAnnounced,
	}

	o := testOptions("", "")
	o.skipLagging = true
	st, count, err := runWithTarget(t, source, target, o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Provider-level: 7 providers, 1 deduped (the second inactive member),
	// 6 heads.
	if st.total != 7 {
		t.Fatalf("total = %d, want 7", st.total)
	}
	if st.skipped != 0 {
		t.Errorf("skipped = %d, want 0", st.skipped)
	}
	if st.deduped != 1 {
		t.Errorf("deduped = %d, want 1", st.deduped)
	}
	heads := st.total - st.skipped - st.deduped
	if heads != 6 {
		t.Fatalf("heads = %d, want 6", heads)
	}

	// Head-level: one head per guard, plus the announced one.
	if st.upToDate != 1 {
		t.Errorf("upToDate = %d, want 1", st.upToDate)
	}
	if st.targetAhead != 1 {
		t.Errorf("targetAhead = %d, want 1", st.targetAhead)
	}
	if st.unverifiable != 1 {
		t.Errorf("unverifiable = %d, want 1", st.unverifiable)
	}
	if st.inactive != 1 {
		t.Errorf("inactive = %d, want 1", st.inactive)
	}
	if st.lagging != 1 {
		t.Errorf("lagging = %d, want 1", st.lagging)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}

	// The identity: skipped + deduped + the sum of the head-level counters
	// equals total.
	if st.skipped+st.deduped+headSum(st) != st.total {
		t.Errorf("skipped + deduped + head buckets = %d, want total %d", st.skipped+st.deduped+headSum(st), st.total)
	}
}

func mustMultiaddr(t *testing.T, id peer.ID) ma.Multiaddr {
	t.Helper()
	m, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/4002/p2p/%s", id))
	if err != nil {
		t.Fatalf("building multiaddr: %v", err)
	}
	return m
}

// providerWith builds a provider with a shared publisher and explicit
// time/lag/inactive, for the guard tests.
func providerWith(t *testing.T, pub *peer.AddrInfo, adCid cid.Cid, adTime string, lag int, inactive bool) *model.ProviderInfo {
	t.Helper()
	p := testProvider(t, adCid)
	p.Publisher = pub
	p.LastAdvertisementTime = adTime
	p.Lag = lag
	p.Inactive = inactive
	return p
}

func sharedPublisher(t *testing.T) *peer.AddrInfo {
	t.Helper()
	id := genPeerID(t)
	return &peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{mustMultiaddr(t, id)}}
}

func runWithTarget(t *testing.T, source, target []*model.ProviderInfo, o options) (stats, int, error) {
	t.Helper()
	findSrv := newFindServer(source)
	defer findSrv.Close()
	targetFindSrv := newFindServer(target)
	defer targetFindSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o.source = findSrv.URL
	o.target = announceSrv.URL
	o.targetFindURL = targetFindSrv.URL
	st, err := run(context.Background(), o)
	return st, announceCount, err
}

func TestTargetOlderAnnounces(t *testing.T) {
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), "2026-08-18T12:00:00Z", 0, false)
	tgt := providerWith(t, pub, variantCid(), "2026-08-18T11:00:00Z", 0, false)

	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, []*model.ProviderInfo{tgt}, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}
}

func TestTargetAhead(t *testing.T) {
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), "2026-08-18T11:00:00Z", 0, false)
	tgt := providerWith(t, pub, variantCid(), "2026-08-18T12:00:00Z", 0, false)

	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, []*model.ProviderInfo{tgt}, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.targetAhead != 1 {
		t.Errorf("targetAhead = %d, want 1", st.targetAhead)
	}
	if st.announced != 0 || count != 0 {
		t.Errorf("announced = %d (requests %d), want 0", st.announced, count)
	}
}

func TestEqualTimestampsTargetAhead(t *testing.T) {
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), "2026-08-18T12:00:00Z", 0, false)
	tgt := providerWith(t, pub, variantCid(), "2026-08-18T12:00:00Z", 0, false)

	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, []*model.ProviderInfo{tgt}, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.targetAhead != 1 {
		t.Errorf("targetAhead = %d, want 1", st.targetAhead)
	}
	if st.announced != 0 || count != 0 {
		t.Errorf("announced = %d (requests %d), want 0", st.announced, count)
	}
}

func TestUnverifiable(t *testing.T) {
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), "2026-08-18T12:00:00Z", 0, false)
	// Target has a defined CID but no parseable timestamp.
	tgt := providerWith(t, pub, variantCid(), "", 0, false)

	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, []*model.ProviderInfo{tgt}, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.unverifiable != 1 {
		t.Errorf("unverifiable = %d, want 1", st.unverifiable)
	}
	if st.announced != 0 || count != 0 {
		t.Errorf("announced = %d (requests %d), want 0", st.announced, count)
	}
}

func TestTargetUndefCidAnnounces(t *testing.T) {
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), "2026-08-18T12:00:00Z", 0, false)
	// Target knows the publisher but has never ingested an ad.
	tgt := providerWith(t, pub, cid.Undef, "", 0, false)

	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, []*model.ProviderInfo{tgt}, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}
	if st.unverifiable != 0 || st.targetAhead != 0 {
		t.Errorf("guard counters should be 0, got unverifiable=%d targetAhead=%d", st.unverifiable, st.targetAhead)
	}
}

func TestTargetAbsentAnnounces(t *testing.T) {
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), "2026-08-18T12:00:00Z", 0, false)

	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, nil, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}
}

func TestSkipInactive(t *testing.T) {
	pub := sharedPublisher(t)
	// Every provider in the group is inactive.
	src := providerWith(t, pub, testCid(), "2026-08-18T12:00:00Z", 0, true)
	src2 := providerWith(t, pub, variantCid(), "2026-08-18T11:00:00Z", 0, true)
	tgt := providerWith(t, pub, variantCid(), "2026-08-18T11:00:00Z", 0, false)

	o := testOptions("", "")
	o.skipInactive = true
	st, count, err := runWithTarget(t, []*model.ProviderInfo{src, src2}, []*model.ProviderInfo{tgt}, o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.inactive != 1 {
		t.Errorf("inactive = %d, want 1", st.inactive)
	}
	if st.announced != 0 || count != 0 {
		t.Errorf("announced = %d (requests %d), want 0", st.announced, count)
	}

	// One active provider in the group is reason enough to announce.
	pub2 := sharedPublisher(t)
	srcA := providerWith(t, pub2, testCid(), "2026-08-18T12:00:00Z", 0, true)
	srcB := providerWith(t, pub2, variantCid(), "2026-08-18T11:00:00Z", 0, false)
	tgt2 := providerWith(t, pub2, variantCid(), "2026-08-18T11:00:00Z", 0, false)
	st, count, err = runWithTarget(t, []*model.ProviderInfo{srcA, srcB}, []*model.ProviderInfo{tgt2}, o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.inactive != 0 {
		t.Errorf("inactive = %d, want 0", st.inactive)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}
}

func TestSkipLagging(t *testing.T) {
	// Source time is the newest of the three so the target-ahead guard does
	// not fire; only the lag guard is under test.
	srcTime := time.Now().Add(-2 * time.Minute).UTC().Format(time.RFC3339)
	fresh := time.Now().Add(-5 * time.Minute).UTC().Format(time.RFC3339)
	stale := time.Now().Add(-3 * time.Hour).UTC().Format(time.RFC3339)

	// Non-zero Lag with a fresh timestamp is dropped when -skip-lagging.
	pub := sharedPublisher(t)
	src := providerWith(t, pub, testCid(), srcTime, 0, false)
	tgt := providerWith(t, pub, variantCid(), fresh, 3, false)
	o := testOptions("", "")
	o.skipLagging = true
	st, count, err := runWithTarget(t, []*model.ProviderInfo{src}, []*model.ProviderInfo{tgt}, o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.lagging != 1 {
		t.Errorf("lagging = %d, want 1", st.lagging)
	}
	if st.announced != 0 || count != 0 {
		t.Errorf("announced = %d (requests %d), want 0", st.announced, count)
	}

	// The same group with a stale timestamp is announced (stale Lag must not
	// starve the provider).
	pub2 := sharedPublisher(t)
	src2 := providerWith(t, pub2, testCid(), srcTime, 0, false)
	tgt2 := providerWith(t, pub2, variantCid(), stale, 3, false)
	st, count, err = runWithTarget(t, []*model.ProviderInfo{src2}, []*model.ProviderInfo{tgt2}, o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.lagging != 0 {
		t.Errorf("lagging = %d, want 0", st.lagging)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}

	// And announced when -skip-lagging is left at its default (off).
	pub3 := sharedPublisher(t)
	src3 := providerWith(t, pub3, testCid(), srcTime, 0, false)
	tgt3 := providerWith(t, pub3, variantCid(), fresh, 3, false)
	st, count, err = runWithTarget(t, []*model.ProviderInfo{src3}, []*model.ProviderInfo{tgt3}, testOptions("", ""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.lagging != 0 {
		t.Errorf("lagging = %d, want 0", st.lagging)
	}
	if st.announced != 1 || count != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, count)
	}
}

func TestNoTargetFindFatal(t *testing.T) {
	src := providerWith(t, sharedPublisher(t), testCid(), "2026-08-18T12:00:00Z", 0, false)
	findSrv := newFindServer([]*model.ProviderInfo{src})
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.targetFindURL = ""
	o.allowNoTargetFind = false

	_, err := run(context.Background(), o)
	if err == nil {
		t.Fatal("expected error for missing -target-find, got nil")
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}

func TestAllowNoTargetFindAnnouncesAll(t *testing.T) {
	src := providerWith(t, sharedPublisher(t), testCid(), "2026-08-18T12:00:00Z", 0, false)
	findSrv := newFindServer([]*model.ProviderInfo{src})
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.targetFindURL = ""
	o.allowNoTargetFind = true

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != 1 || announceCount != 1 {
		t.Errorf("announced = %d (requests %d), want 1", st.announced, announceCount)
	}
}

// newAnnounceRecordServer records each announced CID, guarded by a mutex so
// concurrent workers cannot race the append.
func newAnnounceRecordServer(cids *[]cid.Cid, mu *sync.Mutex, t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/announce" || r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading announce body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var msg message.Message
		if err := msg.UnmarshalCBOR(bytes.NewReader(body)); err != nil {
			t.Errorf("decoding announce body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		*cids = append(*cids, msg.Cid)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
}

func TestConcurrentAnnounce(t *testing.T) {
	const n = 20
	providers := make([]*model.ProviderInfo, 0, n)
	wantCids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := uniqueCid(t)
		p := testProvider(t, c)
		p.LastAdvertisementTime = fmt.Sprintf("2026-08-18T12:%02d:00Z", i)
		providers = append(providers, p)
		wantCids = append(wantCids, c)
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var mu sync.Mutex
	var gotCids []cid.Cid
	announceSrv := newAnnounceRecordServer(&gotCids, &mu, t)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.concurrency = 4

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != n {
		t.Errorf("announced = %d, want %d", st.announced, n)
	}
	mu.Lock()
	got := make(map[string]int, len(gotCids))
	for _, c := range gotCids {
		got[c.String()]++
	}
	mu.Unlock()
	if len(gotCids) != n {
		t.Fatalf("announce requests = %d, want %d", len(gotCids), n)
	}
	for _, c := range wantCids {
		if got[c.String()] != 1 {
			t.Errorf("cid %s announced %d times, want exactly once", c, got[c.String()])
		}
	}
}

// TestConcurrencyClampedToSerial asserts that values below 1 degrade to a
// serial run: no two announce handler invocations may overlap.
//
// The overlap has to be forced rather than observed. A handler that answers
// immediately finishes the first announce before the second is even
// dispatched, so maxInFlight reads 1 whether the clamp works or not: against
// a mutant that clamps to 4 instead of 1, that version of this test passed
// 10 runs out of 10. Instead the first request blocks on a gate that only a
// second, overlapping request can open. A broken clamp opens it and both
// requests are provably in flight together; a working clamp cannot, because
// the single worker is parked inside the first announce, so the gate falls
// through on its timeout and maxInFlight stays 1. Detection no longer
// depends on the timeout, which only bounds how long the serial path waits.
func TestConcurrencyClampedToSerial(t *testing.T) {
	// Long enough that a parallel run would certainly have landed its second
	// request, short enough to pay twice without slowing the suite.
	const overlapWait = 500 * time.Millisecond

	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	for _, c := range []int{0, -1} {
		t.Run(fmt.Sprintf("concurrency=%d", c), func(t *testing.T) {
			var mu sync.Mutex
			var inFlight, maxInFlight, count int
			// Closed by whichever request finds itself overlapping another,
			// releasing the one that is waiting.
			gate := make(chan struct{})
			var once sync.Once
			announceSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				inFlight++
				if inFlight > maxInFlight {
					maxInFlight = inFlight
				}
				count++
				first := count == 1
				overlapping := inFlight > 1
				mu.Unlock()

				switch {
				case overlapping:
					once.Do(func() { close(gate) })
				case first:
					// Only the first request waits; a later one arriving
					// alone has already proved the run is serial, and
					// holding it too would just cost another timeout.
					select {
					case <-gate:
					case <-time.After(overlapWait):
					}
				}

				w.WriteHeader(http.StatusNoContent)
				mu.Lock()
				inFlight--
				mu.Unlock()
			}))
			defer announceSrv.Close()

			o := testOptions(findSrv.URL, announceSrv.URL)
			o.concurrency = c

			st, err := run(context.Background(), o)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if st.announced != 2 || count != 2 {
				t.Errorf("announced = %d (requests %d), want 2", st.announced, count)
			}
			if maxInFlight != 1 {
				t.Errorf("max concurrent handler invocations = %d, want 1 (serial)", maxInFlight)
			}
		})
	}
}

// TestCancelledContextReturns cancels mid-dispatch and requires run to
// return a partial summary rather than hang. It enforces the end-to-end
// contract; the channel close that makes it hold is enforced directly by
// TestDispatchCandidatesClosesChannelOnCancel, because the window in which a
// missing close strands a worker on an empty channel is narrow and this
// test does not reliably reach it. The test cancels from inside the announce
// handler, where dispatch is provably still in flight, and repeats to sample
// the scheduling.
func TestCancelledContextReturns(t *testing.T) {
	const (
		n      = 500
		rounds = 10
		// Cancel once enough announces have landed that the dispatcher is
		// well inside the candidate list, but far from its end.
		cancelAfter = 8
	)
	providers := make([]*model.ProviderInfo, 0, n)
	for i := 0; i < n; i++ {
		providers = append(providers, testProvider(t, testCid()))
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	for round := 0; round < rounds; round++ {
		ctx, cancel := context.WithCancel(context.Background())

		var mu sync.Mutex
		var served int
		announceSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			served++
			hit := served == cancelAfter
			mu.Unlock()
			if hit {
				cancel()
			}
			w.WriteHeader(http.StatusNoContent)
		}))

		o := testOptions(findSrv.URL, announceSrv.URL)
		o.concurrency = 4

		done := make(chan struct{})
		var st stats
		var runErr error
		go func() {
			st, runErr = run(ctx, o)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			// Leaves the server and goroutine behind on purpose: the process
			// is failing the suite, and Close would block on the stuck run.
			t.Fatalf("round %d: run did not return after cancellation; the candidate channel was probably not closed", round)
		}
		announceSrv.Close()
		cancel()

		if runErr == nil {
			t.Fatalf("round %d: expected error from cancelled context, got nil", round)
		}
		if st.announced >= n {
			t.Fatalf("round %d: announced = %d, want strictly below %d", round, st.announced, n)
		}
	}
}

// TestDispatchCandidatesClosesChannelOnCancel tests the dispatcher's
// contract directly, rather than through run: with a pre-cancelled context
// it must stop and close the candidate channel. The end-to-end cancellation
// test cannot catch a missing close, because the channel is sized to the
// worker count and the dispatcher keeps it full, so at the moment of
// cancellation every worker holds exactly one candidate and none is ever
// left waiting on an empty, unclosed channel.
func TestDispatchCandidatesClosesChannelOnCancel(t *testing.T) {
	const n = 10
	candidates := make([]publisherHead, n)
	for i := range candidates {
		candidates[i] = publisherHead{
			publisher: &peer.AddrInfo{ID: genPeerID(t)},
			provider:  &model.ProviderInfo{AddrInfo: peer.AddrInfo{ID: genPeerID(t)}},
		}
	}

	// The real dispatcher runs against a stub channel with a pre-cancelled
	// context, exactly as run would start it. The contract under test is the
	// one change 4 exists for: the channel is closed on every exit path.
	// With the close removed, the dispatcher exits without closing and the
	// drain below blocks until the timeout, so the test fails instead of
	// passing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	defer cancel()

	ch := make(chan publisherHead, 4)
	go dispatchCandidates(ctx, ch, candidates)

	// Drain until the close is observed. The dispatcher exits on its first
	// select, before any send, so with a correct implementation the channel
	// is empty and the first receive already sees the close.
	closed := false
	for !closed {
		select {
		case _, ok := <-ch:
			closed = !ok
		case <-time.After(5 * time.Second):
			t.Fatal("candidate channel was not closed after cancellation")
		}
	}
	// The dispatcher's exit is implied by the close: it is the only closer
	// of the channel, and its defer runs before the close.
}

// TestDispatchCandidatesStopsOnCancelWhileBlocked exercises the other exit
// path: the dispatcher blocks on a send into a full channel and must observe
// the cancellation there and exit. The buffer is filled before the
// dispatcher starts, so its first send is guaranteed to block.
//
// The test waits for the dispatcher to return before it drains the channel.
// Draining first would free buffer space and let a dispatcher missing its
// ctx.Done arm complete every send and reach the deferred close, so the
// drain alone cannot tell the two exit paths apart; the return can.
func TestDispatchCandidatesStopsOnCancelWhileBlocked(t *testing.T) {
	const n = 10
	candidates := make([]publisherHead, n)
	for i := range candidates {
		candidates[i] = publisherHead{
			publisher: &peer.AddrInfo{ID: genPeerID(t)},
			provider:  &model.ProviderInfo{AddrInfo: peer.AddrInfo{ID: genPeerID(t)}},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan publisherHead, 4)
	// Fill the buffer first: with the dispatcher not yet running, its first
	// select is a send that cannot proceed, so it is parked on the select
	// before the cancellation lands.
	for i := 0; i < cap(ch); i++ {
		ch <- candidates[i]
	}
	done := make(chan struct{})
	go func() {
		dispatchCandidates(ctx, ch, candidates)
		close(done)
	}()
	cancel()

	// The dispatcher is parked on a send into a full channel that nothing
	// drains; the ctx.Done arm is the only way out. A missing arm leaves it
	// parked and this times out.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher did not exit on cancellation while blocked on a send")
	}

	// The dispatcher returned, so its deferred close must have run. Drain
	// the four buffered candidates until the close is observed; a cancel
	// path that returns without closing blocks here.
	closed := false
	for !closed {
		select {
		case _, ok := <-ch:
			closed = !ok
		case <-time.After(5 * time.Second):
			t.Fatal("candidate channel was not closed after the dispatcher exited")
		}
	}
}

// TestPanickingHandlerRecordedAsFailed covers the transport-error path: a
// handler that panics is recovered by net/http, which aborts the response, so
// the ingest client returns an error and the worker records a failure.
func TestPanickingHandlerRecordedAsFailed(t *testing.T) {
	providers := []*model.ProviderInfo{
		testProvider(t, testCid()),
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	announceSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.concurrency = 4

	st, err := run(context.Background(), o)
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

// TestWorkerPanicRecordedAsFailed panics inside a worker itself. That path is
// unreachable through an httptest fixture, and it is the one that decides
// whether a crash costs one candidate or the whole summary: an unrecovered
// panic in a goroutine kills the process before stats.print runs, and a
// recover that skips either the result send or wg.Done hangs the run instead.
func TestWorkerPanicRecordedAsFailed(t *testing.T) {
	const n = 4
	providers := make([]*model.ProviderInfo, 0, n)
	for i := 0; i < n; i++ {
		providers = append(providers, testProvider(t, testCid()))
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	orig := announceHead
	defer func() { announceHead = orig }()
	var mu sync.Mutex
	var calls int
	announceHead = func(ctx context.Context, c *ingestclient.Client, head publisherHead) error {
		mu.Lock()
		first := calls == 0
		calls++
		mu.Unlock()
		if first {
			panic("boom")
		}
		return orig(ctx, c, head)
	}

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.concurrency = 4

	done := make(chan struct{})
	var st stats
	var err error
	go func() {
		st, err = run(context.Background(), o)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after a worker panic; the recover skipped its send or wg.Done")
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.failed != 1 {
		t.Errorf("failed = %d, want 1", st.failed)
	}
	// The panicking worker survives the panic, so every candidate is still
	// accounted for and the summary is intact.
	if st.announced != n-1 {
		t.Errorf("announced = %d, want %d", st.announced, n-1)
	}
	if headSum(st) != st.total {
		t.Errorf("head buckets sum to %d, want %d", headSum(st), st.total)
	}

	// With more candidates than workers and a panic on every call, the pool
	// must keep its full width: a recover on the goroutine body would end
	// each worker after its first panic, and the summary would silently lose
	// the candidates no worker ever reached.
	const m = 10
	providers = make([]*model.ProviderInfo, 0, m)
	for i := 0; i < m; i++ {
		providers = append(providers, testProvider(t, testCid()))
	}
	findSrv = newFindServer(providers)
	defer findSrv.Close()

	announceHead = func(ctx context.Context, c *ingestclient.Client, head publisherHead) error {
		panic("boom")
	}

	o = testOptions(findSrv.URL, announceSrv.URL)
	o.concurrency = 4

	done = make(chan struct{})
	st = stats{}
	err = nil
	go func() {
		st, err = run(context.Background(), o)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return when every announce panicked; the dispatcher is blocked with no live worker")
	}

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.failed != m {
		t.Errorf("failed = %d, want %d", st.failed, m)
	}
	if st.announced != 0 {
		t.Errorf("announced = %d, want 0", st.announced)
	}
	if headSum(st) != st.total {
		t.Errorf("head buckets sum to %d, want %d", headSum(st), st.total)
	}
}

// TestPrintDryRunInOrder asserts the ordering half of the dry-run contract
// directly: the lines are printed in the order the candidates are given,
// which is the order selectCandidates returns them. Through run that order
// is map-iteration order and not reproducible from a second call, so the
// test calls the printer with a known slice.
func TestPrintDryRunInOrder(t *testing.T) {
	const n = 10
	candidates := make([]publisherHead, n)
	for i := range candidates {
		candidates[i] = publisherHead{
			publisher: &peer.AddrInfo{ID: genPeerID(t)},
			provider:  &model.ProviderInfo{AddrInfo: peer.AddrInfo{ID: genPeerID(t)}},
		}
	}

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating pipe: %v", err)
	}
	os.Stdout = w
	announced, runErr := printDryRun(context.Background(), candidates)
	w.Close()
	os.Stdout = old
	if runErr != nil {
		t.Fatalf("unexpected error: %v", runErr)
	}
	if announced != n {
		t.Fatalf("announced = %d, want %d", announced, n)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading stdout: %v", err)
	}

	lines := strings.Split(string(out), "\n")
	var got []peer.ID
	for _, line := range lines {
		if !strings.HasPrefix(line, "[dry-run] Would announce provider ") {
			continue
		}
		rest := strings.TrimPrefix(line, "[dry-run] Would announce provider ")
		fields := strings.Fields(rest)
		if len(fields) < 1 {
			t.Fatalf("malformed dry-run line: %q", line)
		}
		mh, err := mh.FromB58String(fields[0])
		if err != nil {
			t.Fatalf("decoding provider ID %q: %v", fields[0], err)
		}
		got = append(got, peer.ID(mh))
	}
	if len(got) != n {
		t.Fatalf("dry-run lines = %d, want %d", len(got), n)
	}
	for i, id := range got {
		if id != candidates[i].provider.AddrInfo.ID {
			t.Errorf("line %d announces provider %s, want %s (the given order)", i, id, candidates[i].provider.AddrInfo.ID)
		}
	}
}

// TestDryRunPrintsEveryCandidateOnce checks the wiring through run: the
// dry-run path prints one line per candidate, each exactly once. The order
// itself is not observable through run (map-iteration order) and is covered
// by TestPrintDryRunInOrder.
func TestDryRunPrintsEveryCandidateOnce(t *testing.T) {
	const n = 10
	providers := make([]*model.ProviderInfo, 0, n)
	for i := 0; i < n; i++ {
		providers = append(providers, testProvider(t, testCid()))
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.dryRun = true
	o.concurrency = 8

	want := make(map[peer.ID]bool, n)
	for _, p := range providers {
		want[p.AddrInfo.ID] = true
	}

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating pipe: %v", err)
	}
	os.Stdout = w
	_, runErr := run(context.Background(), o)
	w.Close()
	os.Stdout = old
	if runErr != nil {
		t.Fatalf("unexpected error: %v", runErr)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading stdout: %v", err)
	}

	lines := strings.Split(string(out), "\n")
	var got []peer.ID
	for _, line := range lines {
		if !strings.HasPrefix(line, "[dry-run] Would announce provider ") {
			continue
		}
		rest := strings.TrimPrefix(line, "[dry-run] Would announce provider ")
		fields := strings.Fields(rest)
		if len(fields) < 1 {
			t.Fatalf("malformed dry-run line: %q", line)
		}
		mh, err := mh.FromB58String(fields[0])
		if err != nil {
			t.Fatalf("decoding provider ID %q: %v", fields[0], err)
		}
		got = append(got, peer.ID(mh))
	}
	if len(got) != n {
		t.Fatalf("dry-run lines = %d, want %d", len(got), n)
	}
	seen := make(map[peer.ID]int, n)
	for i, id := range got {
		if !want[id] {
			t.Errorf("line %d announces provider %s, which is not a candidate", i, id)
		}
		seen[id]++
	}
	for id := range want {
		if seen[id] != 1 {
			t.Errorf("provider %s appears %d times in the dry-run output, want exactly once", id, seen[id])
		}
	}
}

func TestDryRunConcurrentNoRequests(t *testing.T) {
	const n = 10
	providers := make([]*model.ProviderInfo, 0, n)
	for i := 0; i < n; i++ {
		providers = append(providers, testProvider(t, testCid()))
	}
	findSrv := newFindServer(providers)
	defer findSrv.Close()

	var announceCount int
	announceSrv := newAnnounceServer(http.StatusNoContent, &announceCount)
	defer announceSrv.Close()

	o := testOptions(findSrv.URL, announceSrv.URL)
	o.dryRun = true
	o.concurrency = 8

	st, err := run(context.Background(), o)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.announced != n {
		t.Errorf("announced = %d, want %d", st.announced, n)
	}
	if announceCount != 0 {
		t.Errorf("announce requests = %d, want 0", announceCount)
	}
}
