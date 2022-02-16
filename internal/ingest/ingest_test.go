package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/test/typehelpers"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testProtocolID    = 0x300000
	testRetryInterval = 2 * time.Second
	testRetryTimeout  = 15 * time.Second

	testEntriesChunkCount = 3
	testEntriesChunkSize  = 15
)

var (
	ingestCfg = config.Ingest{
		PubSubTopic:             "test/ingest",
		StoreBatchSize:          256,
		SyncTimeout:             config.Duration(time.Minute),
		EntriesDepthLimit:       300,
		AdvertisementDepthLimit: 300,
	}
	rng = rand.New(rand.NewSource(1413))
)

func TestSubscribe(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost := mkTestHost()
	i, core, reg := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub, lsys := mkMockPublisher(t, pubHost, srcStore)
	defer pub.Close()
	connectHosts(t, h, pubHost)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

	// Test with two random advertisement publications.
	_, mhs, providerID := publishRandomAdv(t, i, pubHost, pub, lsys, false)

	// Check that the mhs have been indexed correctly.  Check providerID, not
	// pupHost.ID(), since provider is what was put into advertisement.
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)
	_, mhs, providerID = publishRandomAdv(t, i, pubHost, pub, lsys, false)
	// Check that the mhs have been indexed correctly.
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)

	// Test advertisement with fake signature of them.
	_, mhs, _ = publishRandomAdv(t, i, pubHost, pub, lsys, true)
	// No mhs should have been saved for related index
	for x := range mhs {
		_, b, _ := i.indexer.Get(mhs[x])
		require.False(t, b)
	}

	reg.BlockPeer(pubHost.ID())

	// Check that no advertisement is retrieved from publisher once it is no
	// longer allowed.
	c, _, _ := publishRandomIndexAndAdv(t, pub, lsys, false)
	adv, err := i.ds.Get(context.Background(), datastore.NewKey(c.String()))
	require.Error(t, err, datastore.ErrNotFound)
	require.Nil(t, adv)
}

type blockList struct {
	mu   sync.Mutex
	list map[cid.Cid]bool
}

func (b *blockList) add(c cid.Cid) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.list[c] = true
}

func (b *blockList) rm(c cid.Cid) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.list, c)
}

func TestRestartDuringSync(t *testing.T) {
	blockedReads := &blockList{list: make(map[cid.Cid]bool)}
	hitBlockedRead := make(chan cid.Cid)

	te := setupTestEnv(t, true, func(teo *testEnvOpts) {
		teo.skipIngesterCleanup = true
		teo.publisherLinkSysFn = func(ds datastore.Batching) ipld.LinkSystem {
			lsys := cidlink.DefaultLinkSystem()
			backendLsys := mkProvLinkSystem(ds)

			lsys.StorageWriteOpener = backendLsys.StorageWriteOpener
			lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
				blockedReads.mu.Lock()
				defer blockedReads.mu.Unlock()
				if blockedReads.list[l.(cidlink.Link).Cid] {
					fmt.Println("blocked read")
					hitBlockedRead <- l.(cidlink.Link).Cid
					return nil, errors.New("blocked read")
				}
				return backendLsys.StorageReadOpener(lc, l)
			}

			return lsys
		}
	})

	cCid := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)
	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.Type.Advertisement)
	cAd := adNode.(schema.Advertisement)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll UpdateRoot to be B. Sync the
	// ingester, then kill the ingester when it tries to process B but after it
	// processes A. Then we'll bring the ingester up again and update root to be
	// C, and see if we index everything (A, B, C) correctly.
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	bEntChunk, err := bAd.Entries.AsLink()
	require.NoError(t, err)
	blockedReads.add(bEntChunk.(cidlink.Link).Cid)

	bCid, err := cAd.PreviousID.AsNode().AsLink()
	require.NoError(t, err)

	ctx := context.Background()
	err = te.publisher.SetRoot(ctx, bCid.(cidlink.Link).Cid)
	require.NoError(t, err)

	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = te.ingester.Sync(sctx, te.pubHost.ID(), nil)
	require.NoError(t, err)

	// The ingester tried to sync B, but it was blocked. Now let's stop the ingester.
	<-hitBlockedRead
	aMhs := typehelpers.AllMultihashesFromAd(t, aAd, te.publisherLinkSys)
	// Check that we processed A correctly.
	checkMhsIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), aMhs)

	te.ingester.Close()
	te.ingester.host.Close()

	blockedReads.rm(bEntChunk.(cidlink.Link).Cid)

	// We should not have processed B yet.
	bMhs := typehelpers.AllMultihashesFromAd(t, bAd, te.publisherLinkSys)
	err = checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), bMhs)
	if err == nil {
		require.FailNow(t, "expected that maulthashes were not found")
	}

	//requireTrueEventually(t, func() bool { return !providesAll(t, te.ingester.indexer, te.pubHost.ID(), bMhs...) }, testRetryInterval, testRetryTimeout, "multihashes were not indexed")

	// Now we bring up the ingester again.
	ingesterHost := mkTestHost(libp2p.Identity(te.ingesterPriv))
	connectHosts(t, te.pubHost, ingesterHost)
	ingester, err := NewIngester(ingestCfg, ingesterHost, te.ingester.indexer, mkRegistry(t), te.ingester.ds)
	require.NoError(t, err)
	t.Cleanup(func() {
		ingester.Close()
	})
	te.ingester = ingester

	// And sync to C
	te.publisher.UpdateRoot(ctx, cCid.(cidlink.Link).Cid)

	end, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil)
	require.NoError(t, err)
	<-end

	allMhs := typehelpers.AllMultihashesFromAd(t, cAd, te.publisherLinkSys)
	checkMhsIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}

func TestWithDuplicatedEntryChunks(t *testing.T) {
	te := setupTestEnv(t, true)

	chainHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.Type.Advertisement)
	require.NoError(t, err)

	ctx := context.Background()

	te.publisher.SetRoot(ctx, chainHead.(cidlink.Link).Cid)

	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil)
	require.NoError(t, err)
	c := <-wait

	lcid, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, chainHead.(cidlink.Link).Cid, lcid, "synced up to %s, should have synced up to %s", c, chainHead.(cidlink.Link).Cid)

	allMhs := typehelpers.AllMultihashesFromAd(t, adNode.(schema.Advertisement), te.publisherLinkSys)
	err = checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMhs)
	if err != nil {
		require.FailNow(t, "multihashes were not indexed: %s", err)
	}

	te.Close(t)
}

func TestRmWithNoEntries(t *testing.T) {
	te := setupTestEnv(t, true)

	chainHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 2},
		},
		AddRmWithNoEntries: true,
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.Type.Advertisement)
	require.NoError(t, err)
	prevHead, err := adNode.(schema.Advertisement).PreviousID.Must().AsLink()
	require.NoError(t, err)
	prevAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, prevHead, schema.Type.Advertisement)

	ctx := context.Background()
	te.publisher.UpdateRoot(context.Background(), chainHead.(cidlink.Link).Cid)

	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil)
	require.NoError(t, err)
	<-wait
	var lcid cid.Cid
	requireTrueEventually(t, func() bool {
		lcid, err = te.ingester.GetLatestSync(te.pubHost.ID())
		require.NoError(t, err)
		return chainHead.(cidlink.Link).Cid == lcid
	}, testRetryInterval, testRetryTimeout, "Expected %s but got %s", chainHead, lcid)

	allMhs := typehelpers.AllMultihashesFromAd(t, prevAdNode.(schema.Advertisement), te.publisherLinkSys)
	// Remove the mhs from the first ad (since the last add removed this from the indexer)
	allMhs = allMhs[1:]
	checkMhsIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}
func TestSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost := mkTestHost()
	i, core, _ := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub, lsys := mkMockPublisher(t, pubHost, srcStore)
	defer pub.Close()
	connectHosts(t, h, pubHost)

	c1, mhs, providerID := publishRandomIndexAndAdv(t, pub, lsys, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The explicit sync will happen concurrently with the sycn triggered by
	// the published advertisement.  These will be serialized in the go-legs
	// handler for the provider.
	end, err := i.Sync(ctx, pubHost.ID(), nil)
	require.NoError(t, err)
	select {
	case endCid := <-end:
		// We receive the CID that we synced.
		require.Equal(t, c1, endCid)
		// Check that subscriber recorded latest sync.
		lnk := i.sub.GetLatestSync(pubHost.ID())
		lcid := lnk.(cidlink.Link).Cid
		require.Equal(t, lcid, c1)
		// Check that latest sync recorded in datastore
		requireTrueEventually(t, func() bool {
			lcid, err = i.GetLatestSync(pubHost.ID())
			require.NoError(t, err)
			return c1.Equals(lcid)
		}, testRetryInterval, testRetryTimeout, "Expected %s but got %s", c1, lcid)
	case <-ctx.Done():
		t.Fatal("sync timeout")
	}
	// Checking providerID, since that was what was put in the advertisement, not pubhost.ID()
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)
}

func TestRecursionDepthLimitsEntriesSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost := mkTestHost()
	ing, core, _ := mkIngest(t, h)
	defer core.Close()
	defer ing.Close()
	pub, lsys := mkMockPublisher(t, pubHost, srcStore)
	defer pub.Close()
	connectHosts(t, h, pubHost)

	totalChunkCount := int(ingestCfg.EntriesDepthLimit * 2)
	adCid, _, providerID := publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, false, totalChunkCount)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	end, err := ing.Sync(ctx, pubHost.ID(), nil)
	require.NoError(t, err)

	select {
	case endCid := <-end:
		// We receive the CID that we synced.
		require.Equal(t, adCid, endCid)
		// Check that subscriber recorded latest sync.
		lnk := ing.sub.GetLatestSync(pubHost.ID())
		lcid := lnk.(cidlink.Link).Cid
		require.Equal(t, lcid, adCid)
		// Check that latest sync recorded in datastore
		adNode, err := lsys.Load(linking.LinkContext{}, lnk, schema.Type.Advertisement)
		require.NoError(t, err)
		mhs := typehelpers.AllMultihashesFromAd(t, adNode.(schema.Advertisement), lsys)
		checkMhsIndexedEventually(t, ing.indexer, providerID, mhs)

		lcid, err = ing.GetLatestSync(pubHost.ID())
		for err == nil && lcid == cid.Undef {
			// May not have marked ad as processed yet, retry.
			time.Sleep(time.Second)
			if ctx.Err() != nil {
				t.Fatal("sync timeout")
			}
			lcid, err = ing.GetLatestSync(pubHost.ID())
		}

		require.NoError(t, err)
		require.Equal(t, adCid, lcid)
	case <-ctx.Done():
		t.Fatal("sync timeout")
	}

	entriesCid := getAdEntriesCid(t, srcStore, adCid)
	var mhs []multihash.Multihash
	nextChunkCid := entriesCid
	for i := 0; i < totalChunkCount; i++ {
		mhs, nextChunkCid = decodeEntriesChunk(t, srcStore, nextChunkCid)
		// If chunk depth is within limit
		if i < int(ingestCfg.EntriesDepthLimit) {
			// Assert chunk multihashes are indexed
			checkMhsIndexedEventually(t, ing.indexer, providerID, mhs)
		} else {
			// Otherwise, assert chunk multihashes are not indexed.
			requireNotIndexed(t, mhs, ing)
		}
	}

	// Assert no more chunks are left.
	require.Equal(t, cid.Undef, nextChunkCid)
}

func requireNotIndexed(t *testing.T, mhs []multihash.Multihash, ing *Ingester) {
	for _, mh := range mhs {
		_, exists, err := ing.indexer.Get(mh)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func getAdEntriesCid(t *testing.T, store datastore.Batching, ad cid.Cid) cid.Cid {
	ctx := context.TODO()
	val, err := store.Get(ctx, dsKey(ad.String()))
	require.NoError(t, err)
	nad, err := decodeIPLDNode(ad.Prefix().Codec, bytes.NewBuffer(val))
	require.NoError(t, err)
	adv, err := decodeAd(nad)
	require.NoError(t, err)
	elink, err := adv.FieldEntries().AsLink()
	require.NoError(t, err)
	return elink.(cidlink.Link).Cid
}

func decodeEntriesChunk(t *testing.T, store datastore.Batching, c cid.Cid) ([]multihash.Multihash, cid.Cid) {
	ctx := context.TODO()
	val, err := store.Get(ctx, dsKey(c.String()))
	require.NoError(t, err)
	nentries, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val))
	require.NoError(t, err)
	nb := schema.Type.EntryChunk.NewBuilder()
	err = nb.AssignNode(nentries)
	require.NoError(t, err)
	nchunk := nb.Build().(schema.EntryChunk)
	entries := nchunk.FieldEntries()
	cit := entries.ListIterator()
	var mhs []multihash.Multihash
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		require.NoError(t, err)

		mhs = append(mhs, h)
	}
	if nchunk.Next.IsAbsent() || nchunk.Next.IsNull() {
		return mhs, cid.Undef
	}

	lnk, err := nchunk.Next.AsNode().AsLink()
	require.NoError(t, err)
	return mhs, lnk.(cidlink.Link).Cid
}

func TestMultiplePublishers(t *testing.T) {
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost1 := mkTestHost()
	pubHost2 := mkTestHost()
	i, core, _ := mkIngest(t, h)
	defer core.Close()
	defer i.Close()
	pub1, lsys1 := mkMockPublisher(t, pubHost1, srcStore1)
	defer pub1.Close()
	pub2, lsys2 := mkMockPublisher(t, pubHost2, srcStore2)
	defer pub2.Close()

	// connect both providers
	connectHosts(t, h, pubHost1)
	connectHosts(t, h, pubHost2)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Test with two random advertisement publications for each of them.
	c1, mhs, providerID := publishRandomAdv(t, i, pubHost1, pub1, lsys1, false)
	wait, err := i.Sync(ctx, pubHost1.ID(), nil)
	require.NoError(t, err)
	<-wait
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)
	c2, mhs, providerID := publishRandomAdv(t, i, pubHost2, pub2, lsys2, false)
	wait, err = i.Sync(ctx, pubHost2.ID(), nil)
	require.NoError(t, err)
	<-wait
	checkMhsIndexedEventually(t, i.indexer, providerID, mhs)

	// Check that subscriber recorded latest sync.
	lnk := i.sub.GetLatestSync(pubHost1.ID())
	lcid := lnk.(cidlink.Link).Cid
	require.Equal(t, lcid, c1)
	lnk = i.sub.GetLatestSync(pubHost2.ID())
	lcid = lnk.(cidlink.Link).Cid
	require.Equal(t, lcid, c2)

	lcid, err = i.GetLatestSync(pubHost1.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c1)
	lcid, err = i.GetLatestSync(pubHost2.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c2)
}

func mkTestHost(opts ...libp2p.Option) host.Host {
	opts = append(opts, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	h, _ := libp2p.New(opts...)
	return h
}

// Make new indexer engine
func mkIndexer(t *testing.T, withCache bool) *engine.Engine {
	valueStore, err := storethehash.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var resultCache cache.Interface
	if withCache {
		resultCache = radixcache.New(100000)
	}
	return engine.New(resultCache, valueStore)
}

func mkRegistry(t *testing.T) *registry.Registry {
	discoveryCfg := config.Discovery{
		Policy: config.Policy{
			Allow: true,
			Trust: true,
		},
		PollInterval:   config.Duration(time.Minute),
		RediscoverWait: config.Duration(time.Minute),
	}
	reg, err := registry.NewRegistry(context.Background(), discoveryCfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func mkProvLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, dsKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, dsKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}
func mkMockPublisher(t *testing.T, h host.Host, store datastore.Batching) (legs.Publisher, ipld.LinkSystem) {
	lsys := mkProvLinkSystem(store)
	ls, err := dtsync.NewPublisher(h, store, lsys, ingestCfg.PubSubTopic)
	require.NoError(t, err)
	return ls, lsys
}

func mkIngest(t *testing.T, h host.Host) (*Ingester, *engine.Engine, *registry.Registry) {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	reg := mkRegistry(t)
	core := mkIndexer(t, true)
	ing, err := NewIngester(ingestCfg, h, core, reg, store)
	require.NoError(t, err)
	return ing, core, reg
}

func connectHosts(t *testing.T, srcHost, dstHost host.Host) {
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
}

func newRandomLinkedList(t *testing.T, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash) {
	out := []multihash.Multihash{}
	mhs := util.RandomMultihashes(testEntriesChunkSize, rng)
	out = append(out, mhs...)
	nextLnk, _, err := schema.NewLinkedListOfMhs(lsys, mhs, nil)
	require.NoError(t, err)
	for i := 1; i < size; i++ {
		mhs := util.RandomMultihashes(testEntriesChunkSize, rng)
		nextLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, nextLnk)
		require.NoError(t, err)
		out = append(out, mhs...)
	}
	return nextLnk, out
}

func publishRandomIndexAndAdv(t *testing.T, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash, peer.ID) {
	return publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, fakeSig, testEntriesChunkCount)
}

func publishRandomIndexAndAdvWithEntriesChunkCount(t *testing.T, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool, eChunkCount int) (cid.Cid, []multihash.Multihash, peer.ID) {

	mhs := util.RandomMultihashes(1, rng)
	priv, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	p, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	ctxID := []byte("test-context-id")
	metadata := v0.Metadata{
		ProtocolID: testProtocolID,
		Data:       mhs[0],
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	mhsLnk, mhs := newRandomLinkedList(t, lsys, eChunkCount)
	_, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, mhsLnk, ctxID, metadata, false, p.String(), addrs)
	if fakeSig {
		_, advLnk, err = schema.NewAdvertisementWithFakeSig(lsys, priv, nil, mhsLnk, ctxID, metadata, false, p.String(), addrs)
	}
	require.NoError(t, err)
	lnk, err := advLnk.AsLink()
	require.NoError(t, err)
	err = pub.UpdateRoot(context.Background(), lnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	return lnk.(cidlink.Link).Cid, mhs, p
}

func checkMhsIndexedEventually(t *testing.T, ix indexer.Interface, p peer.ID, mhs []multihash.Multihash) {
	requireTrueEventually(t, func() bool { return providesAll(t, ix, p, mhs...) }, testRetryInterval, testRetryTimeout, "multihashes were not indexed")
}

func providesAll(t *testing.T, ix indexer.Interface, p peer.ID, mhs ...multihash.Multihash) bool {
	err := checkAllIndexed(ix, p, mhs)
	if err != nil {
		t.Logf("err: %v", err)
		return false
	}
	return true
}

func checkAllIndexed(ix indexer.Interface, p peer.ID, mhs []multihash.Multihash) error {
	for _, mh := range mhs {
		values, exists, err := ix.Get(mh)
		if err != nil {
			return err
		}
		if !exists {
			return errors.New("index not found")
		}
		var found bool
		for _, v := range values {
			if v.ProviderID == p {
				found = true
				break
			}
		}
		if !found {
			return errors.New("index not found for provider")
		}
	}
	return nil
}

func publishRandomAdv(t *testing.T, i *Ingester, pubHost host.Host, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash, peer.ID) {
	c, mhs, providerID := publishRandomIndexAndAdv(t, pub, lsys, fakeSig)

	if !fakeSig {
		requireTrueEventually(t, func() bool {
			has, err := i.ds.Has(context.Background(), datastore.NewKey(c.String()))
			return err == nil && has
		}, 3*time.Second, 21*time.Second, "expected advertisement with ID %s was not received", c)
	}

	// Check if advertisement in datastore.
	adv, err := i.ds.Get(context.Background(), datastore.NewKey(c.String()))
	if !fakeSig {
		require.NoError(t, err, "err getting %s", c.String())
		require.NotNil(t, adv)
	} else {
		// If the signature is invalid is should not be stored.
		require.Nil(t, adv)
	}
	if !fakeSig {
		lnk := i.sub.GetLatestSync(pubHost.ID())
		lcid := lnk.(cidlink.Link).Cid
		require.Equal(t, lcid, c)
	}

	// Check if latest sync updated.
	lcid, err := i.GetLatestSync(pubHost.ID())
	require.NoError(t, err)

	// If fakeSig Cids should not be saved.
	if !fakeSig {
		require.Equal(t, lcid, c)
	}
	return c, mhs, providerID
}

func requireTrueEventually(t *testing.T, attempt func() bool, interval time.Duration, timeout time.Duration, msgAndArgs ...interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if attempt() {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "timed out awaiting eventual success", msgAndArgs...)
			return
		case <-ticker.C:
		}
	}
}

type testEnv struct {
	publisher        legs.Publisher
	pubHost          host.Host
	pubStore         datastore.Batching
	publisherPriv    crypto.PrivKey
	ingesterPriv     crypto.PrivKey
	publisherLinkSys ipld.LinkSystem
	ingester         *Ingester
	ingesterHost     host.Host
	core             *engine.Engine
	reg              *registry.Registry
	skipIngCleanup   bool
}

type testEnvOpts struct {
	publisherLinkSysFn  func(ds datastore.Batching) ipld.LinkSystem
	skipIngesterCleanup bool
}

func (te *testEnv) Close(t *testing.T) {
	err := te.publisher.Close()
	if err != nil {
		t.Errorf("Error closing publisher: %s", err)
	}
	rm := te.pubHost.Network().ResourceManager()
	err = te.pubHost.Close()
	if err != nil {
		t.Errorf("Error closing publisher host: %s", err)
	}
	err = rm.Close()
	if err != nil {
		t.Errorf("Error closing publisher host resource manager: %s", err)
	}
	if !te.skipIngCleanup {
		err = te.ingester.Close()
		if err != nil {
			t.Errorf("Error closing ingester: %s", err)
		}
	}
	err = te.core.Close()
	if err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
	err = te.reg.Close()
	if err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	rm = te.ingesterHost.Network().ResourceManager()
	err = te.ingesterHost.Close()
	if err != nil {
		t.Errorf("Error closing ingester host: %s", err)
	}
	err = rm.Close()
	if err != nil {
		t.Errorf("Error closing ingester host resource manager: %s", err)
	}
}

func setupTestEnv(t *testing.T, shouldConnectHosts bool, opts ...func(*testEnvOpts)) *testEnv {
	testOpt := &testEnvOpts{}
	for _, f := range opts {
		f(testOpt)
	}

	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())

	ingesterPriv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	ingesterHost := mkTestHost(libp2p.Identity(ingesterPriv))
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	pubHost := mkTestHost(libp2p.Identity(priv))
	i, core, reg := mkIngest(t, ingesterHost)

	//t.Cleanup(func() {
	//	core.Close()
	//	if !testOpt.skipIngesterCleanup {
	//		i.Close()
	//	}
	//})

	var lsys ipld.LinkSystem
	if testOpt.publisherLinkSysFn != nil {
		lsys = testOpt.publisherLinkSysFn(srcStore)
	} else {
		lsys = mkProvLinkSystem(srcStore)
	}

	pub, err := dtsync.NewPublisher(pubHost, srcStore, lsys, ingestCfg.PubSubTopic)
	require.NoError(t, err)

	if shouldConnectHosts {
		connectHosts(t, ingesterHost, pubHost)
	}

	require.NoError(t, err)

	te := &testEnv{
		publisher:        pub,
		publisherPriv:    priv,
		pubHost:          pubHost,
		pubStore:         srcStore,
		publisherLinkSys: lsys,
		ingester:         i,
		ingesterHost:     ingesterHost,
		ingesterPriv:     ingesterPriv,
		core:             core,
		reg:              reg,
		skipIngCleanup:   testOpt.skipIngesterCleanup,
	}

	t.Cleanup(func() {
		te.Close(t)
	})

	return te
}
