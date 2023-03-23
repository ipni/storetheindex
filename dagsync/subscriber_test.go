package dagsync_test

import (
	"context"
	cryptorand "crypto/rand"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/storetheindex/announce"
	"github.com/ipni/storetheindex/announce/p2psender"
	"github.com/ipni/storetheindex/dagsync"
	"github.com/ipni/storetheindex/dagsync/dtsync"
	"github.com/ipni/storetheindex/dagsync/httpsync"
	"github.com/ipni/storetheindex/dagsync/test"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

const (
	testTopic     = "/dagsync/testtopic"
	updateTimeout = 3 * time.Second
)

type pubMeta struct {
	pub dagsync.Publisher
	h   host.Host
}

func TestScopedBlockHook(t *testing.T) {
	err := quick.Check(func(ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			ds := dssync.MutexWrap(datastore.NewMapDatastore())
			pubHost := test.MkTestHost()
			lsys := test.MkLinkSystem(ds)
			pub, err := dtsync.NewPublisher(pubHost, ds, lsys, testTopic)
			require.NoError(t, err)

			head := ll.Build(t, lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			err = pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
			require.NoError(t, err)

			subHost := test.MkTestHost()
			subDS := dssync.MutexWrap(datastore.NewMapDatastore())
			subLsys := test.MkLinkSystem(subDS)

			require.NoError(t, test.WaitForP2PPublisher(pub, subHost, testTopic))

			var calledGeneralBlockHookTimes int64
			sub, err := dagsync.NewSubscriber(subHost, subDS, subLsys, testTopic, nil, dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
				atomic.AddInt64(&calledGeneralBlockHookTimes, 1)
			}))
			require.NoError(t, err)

			var calledScopedBlockHookTimes int64
			_, err = sub.Sync(context.Background(), pubHost.ID(), cid.Undef, nil, pubHost.Addrs()[0], dagsync.ScopedBlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
				atomic.AddInt64(&calledScopedBlockHookTimes, 1)
			}))
			require.NoError(t, err)

			require.Zero(t, atomic.LoadInt64(&calledGeneralBlockHookTimes),
				"General block hook should not have been called when scoped block hook is set")

			require.Equal(t, int64(ll.Length), atomic.LoadInt64(&calledScopedBlockHookTimes),
				"Didn't call scoped block hook enough times")

			anotherLL := llBuilder{
				Length: ll.Length,
				Seed:   ll.Seed + 1,
			}.Build(t, lsys)

			err = pub.SetRoot(context.Background(), anotherLL.(cidlink.Link).Cid)
			require.NoError(t, err)

			_, err = sub.Sync(context.Background(), pubHost.ID(), cid.Undef, nil, pubHost.Addrs()[0])
			require.NoError(t, err)

			require.Equal(t, int64(ll.Length), atomic.LoadInt64(&calledGeneralBlockHookTimes),
				"General hook should have been called only in secod sync")
		})
	}, &quick.Config{
		MaxCount: 3,
	})
	require.NoError(t, err)
}

func TestSyncedCidsReturned(t *testing.T) {
	err := quick.Check(func(ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			ds := dssync.MutexWrap(datastore.NewMapDatastore())
			pubHost := test.MkTestHost()
			lsys := test.MkLinkSystem(ds)
			pub, err := dtsync.NewPublisher(pubHost, ds, lsys, testTopic)
			require.NoError(t, err)

			head := ll.Build(t, lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			err = pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
			require.NoError(t, err)

			subHost := test.MkTestHost()
			subDS := dssync.MutexWrap(datastore.NewMapDatastore())
			subLsys := test.MkLinkSystem(subDS)

			require.NoError(t, test.WaitForP2PPublisher(pub, subHost, testTopic))

			sub, err := dagsync.NewSubscriber(subHost, subDS, subLsys, testTopic, nil)
			require.NoError(t, err)

			onFinished, cancel := sub.OnSyncFinished()
			defer cancel()
			_, err = sub.Sync(context.Background(), pubHost.ID(), cid.Undef, nil, pubHost.Addrs()[0])
			require.NoError(t, err)

			finishedVal := <-onFinished
			require.Equalf(t, int(ll.Length), len(finishedVal.SyncedCids),
				"The finished value should include %d synced cids, but has %d", ll.Length, len(finishedVal.SyncedCids))

			require.Equal(t, head.(cidlink.Link).Cid, finishedVal.SyncedCids[0],
				"The latest synced cid should be the head and first in the list")
		})
	}, &quick.Config{
		MaxCount: 3,
	})
	require.NoError(t, err)
}

func TestConcurrentSync(t *testing.T) {
	err := quick.Check(func(ll llBuilder, publisherCount uint8) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			if publisherCount == 0 {
				// Empty case
				return
			}

			var publishers []pubMeta

			// limit to at most 10 concurrent publishers
			publisherCount := int(publisherCount)%10 + 1

			subHost := test.MkTestHost()

			for i := 0; i < publisherCount; i++ {
				ds := dssync.MutexWrap(datastore.NewMapDatastore())
				pubHost := test.MkTestHost()
				lsys := test.MkLinkSystem(ds)
				pub, err := dtsync.NewPublisher(pubHost, ds, lsys, testTopic)
				require.NoError(t, err)
				require.NoError(t, test.WaitForP2PPublisher(pub, subHost, testTopic))

				publishers = append(publishers, pubMeta{pub, pubHost})

				head := ll.Build(t, lsys)
				if head == nil {
					// We built an empty list. So nothing to test.
					return
				}

				err = pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
				require.NoError(t, err)
			}

			subDS := dssync.MutexWrap(datastore.NewMapDatastore())
			subLsys := test.MkLinkSystem(subDS)

			var calledTimes int64
			sub, err := dagsync.NewSubscriber(subHost, subDS, subLsys, testTopic, nil, dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
				atomic.AddInt64(&calledTimes, 1)
			}))
			require.NoError(t, err)

			wg := sync.WaitGroup{}
			// Now sync again. We shouldn't call the hook because we persisted our latestSync
			for _, pub := range publishers {
				wg.Add(1)

				go func(pub pubMeta) {
					defer wg.Done()
					_, err := sub.Sync(context.Background(), pub.h.ID(), cid.Undef, nil, pub.h.Addrs()[0])
					if err != nil {
						panic("sync failed")
					}
				}(pub)

			}

			doneChan := make(chan struct{})

			go func() {
				wg.Wait()
				close(doneChan)
			}()

			select {
			case <-time.After(20 * time.Second):
				t.Fatal("Failed to sync")
			case <-doneChan:
			}

			require.Equalf(t, int64(ll.Length)*int64(publisherCount), atomic.LoadInt64(&calledTimes),
				"Didn't call block hook for each publisher. Expected %d saw %d", int(ll.Length)*publisherCount, calledTimes)
		})
	}, &quick.Config{
		MaxCount: 3,
	})
	require.NoError(t, err)
}

func TestSync(t *testing.T) {
	err := quick.Check(func(dpsb dagsyncPubSubBuilder, ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			t.Parallel()
			pubSys := newHostSystem(t)
			subSys := newHostSystem(t)
			defer pubSys.close()
			defer subSys.close()

			calledTimes := 0
			pubAddr, pub, sub := dpsb.Build(t, testTopic, pubSys, subSys,
				[]dagsync.Option{dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					calledTimes++
				})},
			)

			head := ll.Build(t, pubSys.lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			err := pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
			require.NoError(t, err)

			_, err = sub.Sync(context.Background(), pubSys.host.ID(), cid.Undef, nil, pubAddr)
			require.NoError(t, err)
			calledTimesFirstSync := calledTimes
			latestSync := sub.GetLatestSync(pubSys.host.ID())
			require.Equal(t, head, latestSync, "Subscriber did not persist latest sync")
			// Now sync again. We shouldn't call the hook.
			_, err = sub.Sync(context.Background(), pubSys.host.ID(), cid.Undef, nil, pubAddr)
			require.NoError(t, err)
			require.Equalf(t, calledTimes, calledTimesFirstSync,
				"Subscriber called the block hook multiple times for the same sync. Expected %d, got %d", calledTimesFirstSync, calledTimes)
			require.Equal(t, int(ll.Length), calledTimes, "Subscriber did not call the block hook exactly once for each block")
		})
	}, &quick.Config{
		MaxCount: 5,
	})
	require.NoError(t, err)
}

// TestSyncWithHydratedDataStore tests what happens if we call sync when the
// subscriber datastore already has the dag.
func TestSyncWithHydratedDataStore(t *testing.T) {
	err := quick.Check(func(dpsb dagsyncPubSubBuilder, ll llBuilder) bool {
		return t.Run("Quickcheck", func(t *testing.T) {
			t.Parallel()
			pubPrivKey, _, err := crypto.GenerateEd25519Key(cryptorand.Reader)
			require.NoError(t, err)

			pubDs := dssync.MutexWrap(datastore.NewMapDatastore())
			pubSys := hostSystem{
				privKey: pubPrivKey,
				host:    test.MkTestHost(libp2p.Identity(pubPrivKey)),
				ds:      pubDs,
				lsys:    test.MkLinkSystem(pubDs),
			}
			subDs := dssync.MutexWrap(datastore.NewMapDatastore())
			subSys := hostSystem{
				host: test.MkTestHost(),
				ds:   subDs,
				lsys: test.MkLinkSystem(subDs),
			}

			calledTimes := 0
			var calledWith []cid.Cid
			pubAddr, pub, sub := dpsb.Build(t, testTopic, pubSys, subSys,
				[]dagsync.Option{dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					calledWith = append(calledWith, c)
					calledTimes++
				})},
			)

			head := ll.Build(t, pubSys.lsys)
			if head == nil {
				// We built an empty list. So nothing to test.
				return
			}

			err = pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
			require.NoError(t, err)

			// Sync once to hydrate the datastore
			// Note we set the cid we are syncing to so we don't update the latestSync.
			_, err = sub.Sync(context.Background(), pubSys.host.ID(), head.(cidlink.Link).Cid, nil, pubAddr)
			require.NoError(t, err)
			require.Equal(t, int(ll.Length), calledTimes, "Subscriber did not call the block hook exactly once for each block")
			require.Equal(t, head.(cidlink.Link).Cid, calledWith[0], "Subscriber did not call the block hook in the correct order")

			calledTimesFirstSync := calledTimes

			// Now sync again. We might call the hook because we don't have the latestSync persisted.
			_, err = sub.Sync(context.Background(), pubSys.host.ID(), cid.Undef, nil, pubAddr)
			require.NoError(t, err)
			require.GreaterOrEqual(t, calledTimes, calledTimesFirstSync, "Expected to have called block hook twice. Once for each sync.")
		})
	}, &quick.Config{
		MaxCount: 5,
	})
	require.NoError(t, err)
}

func TestRoundTripSimple(t *testing.T) {
	// Init dagsync publisher and subscriber
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost, dstHost, pub, sub := initPubSub(t, srcStore, dstStore)
	defer srcHost.Close()
	defer dstHost.Close()
	defer pub.Close()
	defer sub.Close()

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	// Update root with item
	itm := basicnode.NewString("hello world")
	lnk, err := test.Store(srcStore, itm)
	require.NoError(t, err)

	err = pub.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid)
	require.NoError(t, err)

	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propagate")
	case downstream := <-watcher:
		require.Equalf(t, lnk.(cidlink.Link).Cid, downstream.Cid,
			"sync'd cid unexpected %s vs %s", downstream.Cid, lnk)
		_, err = dstStore.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
	}
}

func TestRoundTrip(t *testing.T) {
	// Init dagsync publisher and subscriber
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost1 := test.MkTestHost()
	defer srcHost1.Close()
	srcLnkS1 := test.MkLinkSystem(srcStore1)

	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcHost2 := test.MkTestHost()
	defer srcHost2.Close()
	srcLnkS2 := test.MkLinkSystem(srcStore2)

	dstStore := dssync.MutexWrap(datastore.NewMapDatastore())
	dstHost := test.MkTestHost()
	defer dstHost.Close()
	dstLnkS := test.MkLinkSystem(dstStore)

	topics := test.WaitForMeshWithMessage(t, "testTopic", srcHost1, srcHost2, dstHost)

	p2pSender, err := p2psender.New(nil, "", p2psender.WithTopic(topics[0]))
	require.NoError(t, err)

	pub1, err := dtsync.NewPublisher(srcHost1, srcStore1, srcLnkS1, "", dtsync.WithAnnounceSenders(p2pSender))
	require.NoError(t, err)
	defer pub1.Close()

	p2pSender, err = p2psender.New(nil, "", p2psender.WithTopic(topics[1]))
	require.NoError(t, err)

	pub2, err := dtsync.NewPublisher(srcHost2, srcStore2, srcLnkS2, "", dtsync.WithAnnounceSenders(p2pSender))
	require.NoError(t, err)
	defer pub2.Close()

	blocksSeenByHook := make(map[cid.Cid]struct{})
	blockHook := func(p peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
		blocksSeenByHook[c] = struct{}{}
		t.Log("block hook got", c, "from", p)
	}

	sub, err := dagsync.NewSubscriber(dstHost, dstStore, dstLnkS, testTopic, nil, dagsync.Topic(topics[2]), dagsync.BlockHook(blockHook))
	require.NoError(t, err)
	defer sub.Close()

	watcher1, cncl1 := sub.OnSyncFinished()
	defer cncl1()
	watcher2, cncl2 := sub.OnSyncFinished()
	defer cncl2()

	// Update root on publisher one with item
	itm1 := basicnode.NewString("hello world")
	lnk1, err := test.Store(srcStore1, itm1)
	require.NoError(t, err)
	// Update root on publisher one with item
	itm2 := basicnode.NewString("hello world 2")
	lnk2, err := test.Store(srcStore2, itm2)
	require.NoError(t, err)

	err = pub1.UpdateRoot(context.Background(), lnk1.(cidlink.Link).Cid)
	require.NoError(t, err)
	t.Log("Publish 1:", lnk1.(cidlink.Link).Cid)
	waitForSync(t, "Watcher 1", dstStore, lnk1.(cidlink.Link), watcher1)
	waitForSync(t, "Watcher 2", dstStore, lnk1.(cidlink.Link), watcher2)

	err = pub2.UpdateRoot(context.Background(), lnk2.(cidlink.Link).Cid)
	require.NoError(t, err)
	t.Log("Publish 2:", lnk2.(cidlink.Link).Cid)
	waitForSync(t, "Watcher 1", dstStore, lnk2.(cidlink.Link), watcher1)
	waitForSync(t, "Watcher 2", dstStore, lnk2.(cidlink.Link), watcher2)

	require.Equal(t, 2, len(blocksSeenByHook))

	_, ok := blocksSeenByHook[lnk1.(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link1")

	_, ok = blocksSeenByHook[lnk2.(cidlink.Link).Cid]
	require.True(t, ok, "hook did not see link2")
}

func TestHttpPeerAddrPeerstore(t *testing.T) {
	pubHostSys := newHostSystem(t)
	subHostSys := newHostSystem(t)
	defer pubHostSys.close()
	defer subHostSys.close()

	pubAddr, pub, sub := dagsyncPubSubBuilder{
		IsHttp: true,
	}.Build(t, testTopic, pubHostSys, subHostSys, nil)

	ll := llBuilder{
		Length: 3,
		Seed:   1,
	}.Build(t, pubHostSys.lsys)

	// a new link on top of ll
	nextLL := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, ll)

	prevHead := ll
	head := nextLL

	err := pub.SetRoot(context.Background(), prevHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, pubAddr)
	require.NoError(t, err)

	err = pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
	require.NoError(t, err)

	// Now call sync again with no address. The subscriber should re-use the
	// previous address and succeeed.
	_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, nil)
	require.NoError(t, err)
}

func TestRateLimiter(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name   string
		isHttp bool
	}

	testCases := []testCase{
		{"DT rate limiter", false},
		{"HTTP rate limiter", true},
	}

	for _, tc := range testCases {
		isHttp := tc.isHttp
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubHostSys := newHostSystem(t)
			subHostSys := newHostSystem(t)
			defer pubHostSys.close()
			defer subHostSys.close()

			tokenEvery := 100 * time.Millisecond
			limiter := rate.NewLimiter(rate.Every(tokenEvery), 1)
			var calledTimes int64
			pubAddr, pub, sub := dagsyncPubSubBuilder{
				IsHttp: isHttp,
			}.Build(t, testTopic, pubHostSys, subHostSys, []dagsync.Option{
				dagsync.BlockHook(func(i peer.ID, c cid.Cid, _ dagsync.SegmentSyncActions) {
					atomic.AddInt64(&calledTimes, 1)
				}),
				dagsync.RateLimiter(func(publisher peer.ID) *rate.Limiter {
					return limiter
				}),
			})

			llB := llBuilder{
				Length: 5,
			}
			ll := llB.Build(t, pubHostSys.lsys)

			err := pub.SetRoot(context.Background(), ll.(cidlink.Link).Cid)
			require.NoError(t, err)

			start := time.Now()
			_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, pubAddr)
			require.NoError(t, err)
			// Minus 1 because we start with a full bucket.
			require.GreaterOrEqual(t, time.Since(start), tokenEvery*time.Duration(llB.Length-1))

			require.Equal(t, atomic.LoadInt64(&calledTimes), int64(llB.Length))
		})
	}

}

func TestBackpressureDoesntDeadlock(t *testing.T) {
	t.Parallel()
	pubHostSys := newHostSystem(t)
	subHostSys := newHostSystem(t)
	defer pubHostSys.close()
	defer subHostSys.close()

	pubAddr, pub, sub := dagsyncPubSubBuilder{}.Build(t, testTopic, pubHostSys, subHostSys, nil)

	ll := llBuilder{
		Length: 1,
		Seed:   1,
	}.Build(t, pubHostSys.lsys)

	// a new link on top of ll
	nextLL := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, ll)

	headLL := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, nextLL)

	// Purposefully not pulling from this channel yet to create backpressure
	onSyncFinishedChan, cncl := sub.OnSyncFinished()
	defer cncl()

	err := pub.SetRoot(context.Background(), ll.(cidlink.Link).Cid)
	require.NoError(t, err)

	_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, pubAddr)
	require.NoError(t, err)

	err = pub.SetRoot(context.Background(), nextLL.(cidlink.Link).Cid)
	require.NoError(t, err)

	_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, pubAddr)
	require.NoError(t, err)

	err = pub.SetRoot(context.Background(), headLL.(cidlink.Link).Cid)
	require.NoError(t, err)

	_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, pubAddr)
	require.NoError(t, err)

	head := llBuilder{
		Length: 1,
		Seed:   2,
	}.BuildWithPrev(t, pubHostSys.lsys, headLL)

	err = pub.SetRoot(context.Background(), head.(cidlink.Link).Cid)
	require.NoError(t, err)

	// This is blocked until we read from onSyncFinishedChan
	syncDoneCh := make(chan error)
	go func() {
		_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), cid.Undef, nil, pubAddr)
		syncDoneCh <- err
	}()

	specificSyncDoneCh := make(chan error)
	go func() {
		// A sleep so that the upper sync starts first
		time.Sleep(time.Second)
		sel := dagsync.ExploreRecursiveWithStopNode(selector.RecursionLimitDepth(1), nil, nil)
		_, err = sub.Sync(context.Background(), pubHostSys.host.ID(), nextLL.(cidlink.Link).Cid, sel, pubAddr)
		specificSyncDoneCh <- err
	}()

	select {
	case <-syncDoneCh:
		t.Fatal("sync should not have finished because it should be blocked by backpressure on the onSyncFinishedChan")
	case err := <-specificSyncDoneCh:
		// This sync should finish because it's not blocked by backpressure. This is
		// because it's a simple sync not trying to setLatest. This is the kind of
		// sync a user will call explicitly, so it should not be blocked by the
		// backpressure of SyncFinishedEvent (it doesn't emit a SyncFinishedEvent).
		require.NoError(t, err)
	}

	// Now pull from onSyncFinishedChan
	select {
	case <-onSyncFinishedChan:
	default:
		t.Fatal("Expected event to be ready to read from onSyncFinishedChan")
	}
	emptySyncFinishedChan(onSyncFinishedChan)

	// Now the syncDoneCh should be able to proceed
	err = <-syncDoneCh
	require.NoError(t, err)

	// So that we can close properly
	emptySyncFinishedChan(onSyncFinishedChan)
}

func emptySyncFinishedChan(ch <-chan dagsync.SyncFinished) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func waitForSync(t *testing.T, logPrefix string, store *dssync.MutexDatastore, expectedCid cidlink.Link, watcher <-chan dagsync.SyncFinished) {
	select {
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		require.Equal(t, expectedCid.Cid, downstream.Cid, "sync'd cid unexpected")
		_, err := store.Get(context.Background(), datastore.NewKey(downstream.Cid.String()))
		require.NoError(t, err, "data not in receiver store")
		t.Log(logPrefix+" got sync:", downstream.Cid)
	}

}

func TestCloseSubscriber(t *testing.T) {
	st := dssync.MutexWrap(datastore.NewMapDatastore())
	sh := test.MkTestHost()
	lsys := test.MkLinkSystem(st)

	sub, err := dagsync.NewSubscriber(sh, st, lsys, testTopic, nil)
	require.NoError(t, err)

	watcher, cncl := sub.OnSyncFinished()
	defer cncl()

	err = sub.Close()
	require.NoError(t, err)

	select {
	case _, open := <-watcher:
		require.False(t, open, "Watcher channel should have been closed")
	case <-time.After(updateTimeout):
		t.Fatal("timed out waiting for watcher to close")
	}

	err = sub.Close()
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		cncl()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(updateTimeout):
		t.Fatal("OnSyncFinished cancel func did not return after Close")
	}
}

type dagsyncPubSubBuilder struct {
	IsHttp      bool
	P2PAnnounce bool
}

type hostSystem struct {
	privKey crypto.PrivKey
	host    host.Host
	ds      datastore.Batching
	lsys    ipld.LinkSystem
}

func newHostSystem(t *testing.T) hostSystem {
	privKey, _, err := crypto.GenerateEd25519Key(cryptorand.Reader)
	require.NoError(t, err)
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	return hostSystem{
		privKey: privKey,
		host:    test.MkTestHost(libp2p.Identity(privKey)),
		ds:      ds,
		lsys:    test.MkLinkSystem(ds),
	}
}

func (h *hostSystem) close() {
	h.host.Close()
}

func (b dagsyncPubSubBuilder) Build(t *testing.T, topicName string, pubSys hostSystem, subSys hostSystem, subOpts []dagsync.Option) (multiaddr.Multiaddr, dagsync.Publisher, *dagsync.Subscriber) {
	var pub dagsync.Publisher
	var senders []announce.Sender

	if !b.P2PAnnounce {
		p2pSender, err := p2psender.New(pubSys.host, topicName)
		require.NoError(t, err)
		senders = append(senders, p2pSender)
	}
	var err error
	if b.IsHttp {
		pub, err = httpsync.NewPublisher("127.0.0.1:0", pubSys.lsys, pubSys.privKey, httpsync.WithAnnounceSenders(senders...))
		require.NoError(t, err)
		require.NoError(t, test.WaitForHttpPublisher(pub))
	} else {
		pub, err = dtsync.NewPublisher(pubSys.host, pubSys.ds, pubSys.lsys, topicName, dtsync.WithAnnounceSenders(senders...))
		require.NoError(t, err)
		require.NoError(t, test.WaitForP2PPublisher(pub, subSys.host, topicName))
	}

	sub, err := dagsync.NewSubscriber(subSys.host, subSys.ds, subSys.lsys, topicName, nil, subOpts...)
	require.NoError(t, err)

	return pub.Addrs()[0], pub, sub
}

type llBuilder struct {
	Length uint8
	Seed   int64
}

func (b llBuilder) Build(t *testing.T, lsys ipld.LinkSystem) datamodel.Link {
	return b.BuildWithPrev(t, lsys, nil)
}

func (b llBuilder) BuildWithPrev(t *testing.T, lsys ipld.LinkSystem, prev datamodel.Link) datamodel.Link {
	var linkproto = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}

	rng := rand.New(rand.NewSource(b.Seed))
	for i := 0; i < int(b.Length); i++ {
		p := basicnode.Prototype.Map
		b := p.NewBuilder()
		ma, err := b.BeginMap(2)
		require.NoError(t, err)
		eb, err := ma.AssembleEntry("Value")
		require.NoError(t, err)
		err = eb.AssignInt(int64(rng.Intn(100)))
		require.NoError(t, err)
		eb, err = ma.AssembleEntry("Next")
		require.NoError(t, err)
		if prev != nil {
			err = eb.AssignLink(prev)
			require.NoError(t, err)
		} else {
			err = eb.AssignNull()
			require.NoError(t, err)
		}
		err = ma.Finish()
		require.NoError(t, err)

		n := b.Build()

		prev, err = lsys.Store(linking.LinkContext{}, linkproto, n)
		require.NoError(t, err)
	}

	return prev
}
