package dagsync

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipni/storetheindex/dagsync/dtsync"
	"github.com/ipni/storetheindex/dagsync/httpsync"
	"github.com/ipni/storetheindex/dagsync/test"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const (
	testTopic     = "/dagsync/testtopic"
	updateTimeout = time.Second
)

func TestAnnounceReplace(t *testing.T) {
	t.Parallel()
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost()

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	pub, err := dtsync.NewPublisher(srcHost, srcStore, srcLnkS, testTopic)
	require.NoError(t, err)
	defer pub.Close()

	sub, err := NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil)
	require.NoError(t, err)
	defer sub.Close()

	require.NoError(t, test.WaitForP2PPublisher(pub, dstHost, testTopic))

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	hnd, err := sub.getOrCreateHandler(srcHost.ID())
	require.NoError(t, err)

	// Lock mutex inside sync handler to simulate publisher blocked in graphsync.
	hnd.syncMutex.Lock()

	firstCid := chainLnks[2].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), firstCid)
	require.NoError(t, err)

	// Have the subscriber receive an announce.  This is the same as if it was
	// published by the publisher without having to wait for it to arrive.
	err = sub.Announce(context.Background(), firstCid, srcHost.ID(), srcHost.Addrs())
	require.NoError(t, err)
	t.Log("Sent announce for first CID", firstCid)

	// This first announce should start the handler goroutine and clear the
	// pending cid.
	var pendingCid cid.Cid
	require.Eventually(t, func() bool {
		hnd.qlock.Lock()
		pendingCid = hnd.pendingCid
		hnd.qlock.Unlock()
		return pendingCid == cid.Undef
	}, 2*time.Second, 100*time.Millisecond)

	// Announce two more times.
	c := chainLnks[1].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), c)
	require.NoError(t, err)

	err = sub.Announce(context.Background(), c, srcHost.ID(), srcHost.Addrs())
	require.NoError(t, err)

	t.Log("Sent announce for second CID", c)
	lastCid := chainLnks[0].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), lastCid)
	require.NoError(t, err)

	err = sub.Announce(context.Background(), lastCid, srcHost.ID(), srcHost.Addrs())
	require.NoError(t, err)
	t.Log("Sent announce for last CID", lastCid)

	// Check that the pending CID gets set to the last one announced.
	require.Eventually(t, func() bool {
		hnd.qlock.Lock()
		pendingCid = hnd.pendingCid
		hnd.qlock.Unlock()
		return pendingCid == lastCid
	}, 2*time.Second, 100*time.Millisecond)

	// Unblock the first handler goroutine
	hnd.syncMutex.Unlock()

	// Validate that sink for first CID happened.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		require.True(t, open, "event channle closed without receiving event")
		require.Equal(t, firstCid, downstream.Cid, "sync returned unexpected first cid")
		_, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
		t.Log("Received sync notification for first CID:", firstCid)
	}

	// Validate that sink for last CID happened.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher:
		require.True(t, open, "event channle closed without receiving event")
		require.Equal(t, lastCid, downstream.Cid, "sync returned unexpected last cid")
		_, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
		t.Log("Received sync notification for last CID:", lastCid)
	}

	// Validate that no additional updates happen.
	select {
	case <-time.After(3 * time.Second):
	case changeEvent, open := <-watcher:
		require.Falsef(t, open,
			"no exchange should have been performed, but got change from peer %s for cid %s",
			changeEvent.PeerID, changeEvent.Cid)
	}
}

func TestAnnounce_LearnsHttpPublisherAddr(t *testing.T) {
	// Instantiate a HTTP publisher
	pubh := test.MkTestHost()
	defer pubh.Close()
	pubds := dssync.MutexWrap(datastore.NewMapDatastore())
	publs := test.MkLinkSystem(pubds)
	pub, err := httpsync.NewPublisher("0.0.0.0:0", publs, pubh.Peerstore().PrivKey(pubh.ID()))
	require.NoError(t, err)
	defer pub.Close()

	// Store one CID at publisher
	wantOneMsg := "fish"
	oneLink, err := test.Store(pubds, basicnode.NewString(wantOneMsg))
	require.NoError(t, err)
	oneC := oneLink.(cidlink.Link).Cid

	// Store another CID at publisher
	wantAnotherMsg := "lobster"
	anotherLink, err := test.Store(pubds, basicnode.NewString(wantAnotherMsg))
	require.NoError(t, err)
	anotherC := anotherLink.(cidlink.Link).Cid

	// Instantiate a subscriber
	subh := test.MkTestHost()
	defer pubh.Close()
	subds := dssync.MutexWrap(datastore.NewMapDatastore())
	subls := test.MkLinkSystem(subds)
	sub, err := NewSubscriber(subh, subds, subls, testTopic, nil)
	require.NoError(t, err)
	defer sub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Announce one CID to the subscriber. Note that announce does a sync in the background.
	// That's why we use one cid here and another for sync so that we can concretely assert that
	// data was synced via the sync call and not via the earlier background sync via announce.
	err = sub.Announce(ctx, oneC, pubh.ID(), pub.Addrs())
	require.NoError(t, err)

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to finish")
	case <-watcher:
	}

	// Now assert that we can sync another CID because, the subscriber should have learned the
	// address of publisher via earlier announce.
	peerInfo := peer.AddrInfo{
		ID: pubh.ID(),
	}
	gotc, err := sub.Sync(ctx, peerInfo, anotherC, nil)
	require.NoError(t, err)
	require.Equal(t, anotherC, gotc)
	gotNode, err := subls.Load(ipld.LinkContext{Ctx: ctx}, anotherLink, basicnode.Prototype.String)
	require.NoError(t, err)
	gotAnotherMsg, err := gotNode.AsString()
	require.NoError(t, err)
	require.Equal(t, wantAnotherMsg, gotAnotherMsg)
}

func TestAnnounceRepublish(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost := test.MkTestHost()
	srcLnkS := test.MkLinkSystem(srcStore)

	dstHost := test.MkTestHost()

	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	dstLnkS := test.MkLinkSystem(dstStore)

	dstStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	dstLnkS2 := test.MkLinkSystem(dstStore2)
	dstHost2 := test.MkTestHost()

	topics := test.WaitForMeshWithMessage(t, testTopic, dstHost, dstHost2)

	sub2, err := NewSubscriber(dstHost2, dstStore2, dstLnkS2, testTopic, nil, Topic(topics[1]))
	require.NoError(t, err)
	defer sub2.Close()

	sub1, err := NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil, Topic(topics[0]), ResendAnnounce(true))
	require.NoError(t, err)
	defer sub1.Close()

	pub, err := dtsync.NewPublisher(srcHost, srcStore, srcLnkS, testTopic)
	require.NoError(t, err)
	defer pub.Close()
	require.NoError(t, test.WaitForP2PPublisher(pub, dstHost, testTopic))

	watcher2, cncl := sub2.OnSyncFinished()
	defer cncl()

	// Store the whole chain in source node
	chainLnks := test.MkChain(srcLnkS, true)

	firstCid := chainLnks[2].(cidlink.Link).Cid
	err = pub.SetRoot(context.Background(), firstCid)
	require.NoError(t, err)

	// Announce one CID to subscriber1.
	err = sub1.Announce(context.Background(), firstCid, srcHost.ID(), srcHost.Addrs())
	require.NoError(t, err)
	t.Log("Sent announce for first CID", firstCid)

	// Validate that sink for first CID happened on subscriber2.
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream, open := <-watcher2:
		require.True(t, open, "event channel closed without receiving event")
		require.True(t, downstream.Cid.Equals(firstCid), "sync returned unexpected first cid %s, expected %s", downstream.Cid, firstCid)
		_, err = dstStore2.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in second receiver store: %s", err)
		t.Log("Received sync notification for first CID:", firstCid)
	}
}
