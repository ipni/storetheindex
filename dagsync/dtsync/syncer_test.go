package dtsync_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/storetheindex/dagsync/dtsync"
	"github.com/filecoin-project/storetheindex/dagsync/test"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/require"
)

func TestDTSync_CallsBlockHookWhenCIDsAreFullyFoundLocally(t *testing.T) {
	const topic = "fish"
	ctx := context.Background()

	// Create a linksystem and populate it with 3 nodes.
	// The test uses the same linksystem store across both publisher and syncer.
	// This is to simulate a scenario where the DAG being synced is present on syncer's side.
	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)
	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}

	l1, err := ls.Store(ipld.LinkContext{Ctx: ctx}, lp, fluent.MustBuildMap(basicnode.Prototype.Map, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("fish").AssignString("lobster")
	}))
	require.NoError(t, err)

	l2, err := ls.Store(ipld.LinkContext{Ctx: ctx}, lp, fluent.MustBuildMap(basicnode.Prototype.Map, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("gogo").AssignString("barreleye")
		na.AssembleEntry("next").AssignLink(l1)
	}))
	require.NoError(t, err)

	l3, err := ls.Store(ipld.LinkContext{Ctx: ctx}, lp, fluent.MustBuildMap(basicnode.Prototype.Map, 2, func(na fluent.MapAssembler) {
		na.AssembleEntry("unda").AssignString("dasea")
		na.AssembleEntry("next").AssignLink(l2)
	}))
	require.NoError(t, err)

	// Start a publisher to sync from.
	pubh := test.MkTestHost()
	pub, err := dtsync.NewPublisher(pubh, dssync.MutexWrap(datastore.NewMapDatastore()), ls, topic)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pub.Close()) })

	// Set up a syncer.
	subh := test.MkTestHost()
	subh.Peerstore().AddAddrs(pubh.ID(), pubh.Addrs(), peerstore.PermanentAddrTTL)
	var gotCids []cid.Cid
	testHook := func(id peer.ID, cid cid.Cid) {
		gotCids = append(gotCids, cid)
	}

	// Use the same linksystem as the publisher for the syncer; this is to assure all the blocks being
	// synced are already present on the syncer side.
	subject, err := dtsync.NewSync(subh, dssync.MutexWrap(datastore.NewMapDatastore()), ls, testHook)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	// Sync l3 from the publisher.
	syncer := subject.NewSyncer(pubh.ID(), topic, nil)
	require.NoError(t, syncer.Sync(ctx, l3.(cidlink.Link).Cid, selectorparse.CommonSelector_ExploreAllRecursively))

	// Assert there are three synced CIDs.
	require.Len(t, gotCids, 3)

	// Assert that they are signalled in the right order.
	require.Equal(t, l3.(cidlink.Link).Cid, gotCids[0])
	require.Equal(t, l2.(cidlink.Link).Cid, gotCids[1])
	require.Equal(t, l1.(cidlink.Link).Cid, gotCids[2])
}

func TestDTSync_CallsBlockHookWhenCIDsArePartiallyFoundLocally(t *testing.T) {
	const topic = "fish"
	ctx := context.Background()

	// Create a publisher that stores 3 nodes
	var pubh host.Host
	var l1, l2, l3 ipld.Link
	substore := &memstore.Store{}
	{
		publs := cidlink.DefaultLinkSystem()
		pubstore := &memstore.Store{}
		publs.SetReadStorage(pubstore)
		publs.SetWriteStorage(pubstore)
		lp := cidlink.LinkPrototype{
			Prefix: cid.Prefix{
				Version:  1,
				Codec:    uint64(multicodec.DagJson),
				MhType:   uint64(multicodec.Sha2_256),
				MhLength: -1,
			},
		}
		var err error
		l1, err = publs.Store(ipld.LinkContext{Ctx: ctx}, lp, fluent.MustBuildMap(basicnode.Prototype.Map, 1, func(na fluent.MapAssembler) {
			na.AssembleEntry("fish").AssignString("lobster")
		}))
		require.NoError(t, err)

		l2, err = publs.Store(ipld.LinkContext{Ctx: ctx}, lp, fluent.MustBuildMap(basicnode.Prototype.Map, 2, func(na fluent.MapAssembler) {
			na.AssembleEntry("gogo").AssignString("barreleye")
			na.AssembleEntry("next").AssignLink(l1)
		}))
		require.NoError(t, err)

		l3, err = publs.Store(ipld.LinkContext{Ctx: ctx}, lp, fluent.MustBuildMap(basicnode.Prototype.Map, 2, func(na fluent.MapAssembler) {
			na.AssembleEntry("unda").AssignString("dasea")
			na.AssembleEntry("next").AssignLink(l2)
		}))
		require.NoError(t, err)

		// Copy l1 and l3 but not l2.
		// This is to simulate partial availability of a DAG on syncer side.
		for k, v := range pubstore.Bag {
			if k != l2.Binary() {
				require.NoError(t, substore.Put(ctx, k, v))
			}
		}

		// Start a publisher to sync from.
		pubh = test.MkTestHost()
		pub, err := dtsync.NewPublisher(pubh, dssync.MutexWrap(datastore.NewMapDatastore()), publs, topic)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pub.Close()) })

		// Publish head.
		require.NoError(t, pub.SetRoot(ctx, l3.(cidlink.Link).Cid))
	}

	// Set up a syncer.
	subh := test.MkTestHost()
	subh.Peerstore().AddAddrs(pubh.ID(), pubh.Addrs(), peerstore.PermanentAddrTTL)
	var gotCids []cid.Cid
	testHook := func(id peer.ID, cid cid.Cid) {
		gotCids = append(gotCids, cid)
	}

	subls := cidlink.DefaultLinkSystem()
	subls.SetReadStorage(substore)
	subls.SetWriteStorage(substore)

	// Sanity check that syncer linksystem has l1 and l3 but not l2.
	_, err := subls.Load(ipld.LinkContext{Ctx: ctx}, l1, basicnode.Prototype.Any)
	require.NoError(t, err)
	_, err = subls.Load(ipld.LinkContext{Ctx: ctx}, l3, basicnode.Prototype.Any)
	require.NoError(t, err)
	_, err = subls.Load(ipld.LinkContext{Ctx: ctx}, l2, basicnode.Prototype.Any)
	require.Error(t, err)

	subject, err := dtsync.NewSync(subh, dssync.MutexWrap(datastore.NewMapDatastore()), subls, testHook)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, subject.Close()) })

	// Sync l3 from the publisher.
	syncer := subject.NewSyncer(pubh.ID(), topic, nil)
	require.NoError(t, syncer.Sync(ctx, l3.(cidlink.Link).Cid, selectorparse.CommonSelector_ExploreAllRecursively))

	// Assert there are three synced CIDs.
	require.Len(t, gotCids, 3)

	// Assert that they are signalled in the right order.
	require.Equal(t, l3.(cidlink.Link).Cid, gotCids[0])
	require.Equal(t, l2.(cidlink.Link).Cid, gotCids[1])
	require.Equal(t, l1.(cidlink.Link).Cid, gotCids[2])
}
