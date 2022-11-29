package dagsync_test

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/storetheindex/dagsync"
	"github.com/filecoin-project/storetheindex/dagsync/httpsync"
	"github.com/filecoin-project/storetheindex/dagsync/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type httpTestEnv struct {
	srcHost    host.Host
	dstHost    host.Host
	pub        dagsync.Publisher
	sub        *dagsync.Subscriber
	srcStore   *dssync.MutexDatastore
	srcLinkSys linking.LinkSystem
	dstStore   *dssync.MutexDatastore
	pubAddr    multiaddr.Multiaddr
}

func setupPublisherSubscriber(t *testing.T, subscriberOptions []dagsync.Option) httpTestEnv {
	srcPrivKey, _, err := ic.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal("Err generating private key", err)
	}

	srcHost = test.MkTestHost(libp2p.Identity(srcPrivKey))
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcLinkSys := test.MkLinkSystem(srcStore)
	httpPub, err := httpsync.NewPublisher("127.0.0.1:0", srcLinkSys, srcHost.ID(), srcPrivKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		httpPub.Close()
	})
	pub := httpPub

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLinkSys := test.MkLinkSystem(dstStore)
	dstHost := test.MkTestHost()

	sub, err := dagsync.NewSubscriber(dstHost, dstStore, dstLinkSys, testTopic, nil, subscriberOptions...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sub.Close()
	})

	return httpTestEnv{
		srcHost:    srcHost,
		dstHost:    dstHost,
		pub:        pub,
		sub:        sub,
		srcStore:   srcStore,
		srcLinkSys: srcLinkSys,
		dstStore:   dstStore,
		pubAddr:    httpPub.Addrs()[0],
	}
}

func TestManualSync(t *testing.T) {
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blocksSeenByHook[c] = struct{}{}
		t.Log("http block hook got", c, "from", p)
	}

	te := setupPublisherSubscriber(t, []dagsync.Option{dagsync.BlockHook(blockHook)})

	rootLnk, err := test.Store(te.srcStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := te.pub.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	syncCid, err := te.sub.Sync(ctx, te.srcHost.ID(), cid.Undef, nil, te.pubAddr)
	if err != nil {
		t.Fatal(err)
	}

	if !syncCid.Equals(rootLnk.(cidlink.Link).Cid) {
		t.Fatalf("didn't get expected cid. expected %s, got %s", rootLnk, syncCid)
	}

	_, ok := blocksSeenByHook[syncCid]
	if !ok {
		t.Fatal("hook did not get", syncCid)
	}
}

func TestSyncHttpFailsUnexpectedPeer(t *testing.T) {
	te := setupPublisherSubscriber(t, nil)

	rootLnk, err := test.Store(te.srcStore, basicnode.NewString("hello world"))
	if err != nil {
		t.Fatal(err)
	}
	if err := te.pub.UpdateRoot(context.Background(), rootLnk.(cidlink.Link).Cid); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), updateTimeout)

	defer cancel()
	_, otherPubKey, err := ic.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal("failed to make another peerid")
	}
	otherPeerID, err := peer.IDFromPublicKey(otherPubKey)
	if err != nil {
		t.Fatal("failed to make another peerid")
	}

	// This fails because the head msg is signed by srcHost.ID(), but we are asking this to check if it's signed by otherPeerID.
	_, err = te.sub.Sync(ctx, otherPeerID, cid.Undef, nil, te.pubAddr)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unexpected peer") {
		t.Fatalf("expected error to contain the string 'unexpected peer', got %s", err.Error())
	}
}

func TestSyncFnHttp(t *testing.T) {
	var blockHookCalls int
	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(_ peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blockHookCalls++
		blocksSeenByHook[c] = struct{}{}
	}
	te := setupPublisherSubscriber(t, []dagsync.Option{dagsync.BlockHook(blockHook)})

	// Store the whole chain in source node
	chainLnks := test.MkChain(te.srcLinkSys, true)

	watcher, cancelWatcher := te.sub.OnSyncFinished()
	defer cancelWatcher()

	// Try to sync with a non-existing cid to chack that sync returns with err,
	// and SyncFinished watcher does not get event.
	cids, _ := test.RandomCids(1)
	ctx, syncncl := context.WithTimeout(context.Background(), time.Second)
	defer syncncl()

	var err error
	if _, err = te.sub.Sync(ctx, te.srcHost.ID(), cids[0], nil, te.pubAddr); err == nil {
		t.Fatal("expected error when no content to sync")
	}
	if !strings.Contains(err.Error(), "failed to traverse requested dag") {
		t.Fatalf("expected error to contain the string 'failed to traverse requested dag', got %s", err.Error())
	}
	syncncl()

	select {
	case <-time.After(updateTimeout):
	case <-watcher:
		t.Fatal("watcher should not receive event if sync error")
	}

	// Assert the latestSync is updated by explicit sync when cid and selector are unset.
	newHead := chainLnks[0].(cidlink.Link).Cid
	if err = te.pub.UpdateRoot(context.Background(), newHead); err != nil {
		t.Fatal(err)
	}

	lnk := chainLnks[1]

	curLatestSync := te.sub.GetLatestSync(te.srcHost.ID())

	// Sync with publisher via HTTP.
	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err := te.sub.Sync(ctx, te.srcHost.ID(), lnk.(cidlink.Link).Cid, nil, te.pubAddr)
	if err != nil {
		t.Fatal(err)
	}

	if !syncCid.Equals(lnk.(cidlink.Link).Cid) {
		t.Fatalf("sync'd cid unexpected %s vs %s", syncCid, lnk)
	}
	if _, err = te.dstStore.Get(context.Background(), datastore.NewKey(syncCid.String())); err != nil {
		t.Fatalf("data not in receiver store: %v", err)
	}
	syncncl()

	_, ok := blocksSeenByHook[lnk.(cidlink.Link).Cid]
	if !ok {
		t.Fatal("block hook did not see link cid")
	}
	if blockHookCalls != 11 {
		t.Fatalf("expected 11 block hook calls, got %d", blockHookCalls)
	}

	// Assert the latestSync is not updated by explicit sync when cid is set
	if te.sub.GetLatestSync(te.srcHost.ID()) != nil && assertLatestSyncEquals(te.sub, te.srcHost.ID(), curLatestSync.(cidlink.Link).Cid) != nil {
		t.Fatal("Sync should not update latestSync")
	}

	ctx, syncncl = context.WithTimeout(context.Background(), updateTimeout)
	defer syncncl()
	syncCid, err = te.sub.Sync(ctx, te.srcHost.ID(), cid.Undef, nil, te.pubAddr)
	if err != nil {
		t.Fatal(err)
	}
	if !syncCid.Equals(newHead) {
		t.Fatalf("sync'd cid unexpected %s vs %s", syncCid, lnk)
	}
	if _, err = te.dstStore.Get(context.Background(), datastore.NewKey(syncCid.String())); err != nil {
		t.Fatalf("data not in receiver store: %v", err)
	}
	syncncl()

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync from published update")
	case syncFin, open := <-watcher:
		if !open {
			t.Fatal("sync finished channel closed with no event")
		}
		if syncFin.Cid != newHead {
			t.Fatalf("Should have been updated to %s, got %s", newHead, syncFin.Cid)
		}
	}
	cancelWatcher()

	if err = assertLatestSyncEquals(te.sub, te.srcHost.ID(), newHead); err != nil {
		t.Fatal(err)
	}
}
