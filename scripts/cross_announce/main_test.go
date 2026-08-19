package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/announce/message"
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
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/announce" || r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		*count++
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
