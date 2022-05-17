package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
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
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

const (
	testRetryInterval = 2 * time.Second
	testRetryTimeout  = 15 * time.Second

	testEntriesChunkCount = 3
	testEntriesChunkSize  = 15
)

var (
	ingestCfg = config.Ingest{
		PubSubTopic: "test/ingest",
		RateLimit: config.RateLimit{
			Apply:           true,
			BlocksPerSecond: 100,
			BurstSize:       1000,
		},
		StoreBatchSize:    256,
		SyncTimeout:       config.Duration(time.Minute),
		IngestWorkerCount: 1,
	}
	rng = rand.New(rand.NewSource(1413))
)

func TestSubscribe(t *testing.T) {
	te := setupTestEnv(t, true)

	// Check that we sync with an ad chain
	adHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	ctx := context.Background()
	err := te.publisher.UpdateRoot(ctx, adHead.(cidlink.Link).Cid)
	require.NoError(t, err)
	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	<-wait
	mhs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), mhs))

	// Check that we sync if the publisher gives us another provider instead.
	someOtherProviderPriv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	adHead = typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 4},
		}}.Build(t, te.publisherLinkSys, someOtherProviderPriv)
	err = te.publisher.UpdateRoot(ctx, adHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	wait, err = te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	<-wait

	someOtherProvider, err := peer.IDFromPrivateKey(someOtherProviderPriv)
	require.NoError(t, err)
	mhs = typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, someOtherProvider, mhs))

	// Check that we don't ingest from ads that aren't signed.
	someOtherProviderPriv, _, err = test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	adHead = typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 5},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 6},
		}}.BuildWithFakeSig(t, te.publisherLinkSys, someOtherProviderPriv)
	mhs = typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)

	// No mhs should have been saved for related index because the ad wasn't signed
	for x := range mhs {
		_, b, _ := te.ingester.indexer.Get(mhs[x])
		require.False(t, b)
	}

	// Check that we don't ingest from blocked peers
	someOtherProviderPriv, _, err = test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	adHead = typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 7},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 8},
		}}.Build(t, te.publisherLinkSys, someOtherProviderPriv)

	someOtherProvider, err = peer.IDFromPrivateKey(someOtherProviderPriv)
	require.NoError(t, err)

	te.reg.BlockPeer(te.pubHost.ID())
	te.reg.BlockPeer(someOtherProvider)

	err = te.publisher.UpdateRoot(ctx, adHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	// We are manually syncing here to not rely on the pubsub mechanism inside a test.
	// This will fetch the add and put it into our datastore, but will not process it.
	wait, err = te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	<-wait

	mhs = typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	// Check that we don't have any MHs for this blocked provider
	for x := range mhs {
		_, b, _ := te.ingester.indexer.Get(mhs[x])
		require.False(t, b)
	}
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

func failBlockedRead() (io.Reader, error) {
	return nil, errors.New("blocked read")
}

func blockableLinkSys(afterBlock func() (io.Reader, error)) (opt func(teo *testEnvOpts), blockedReads *blockList, hitBlockedRead chan cid.Cid) {
	blockedReads = &blockList{list: make(map[cid.Cid]bool)}
	hitBlockedRead = make(chan cid.Cid)
	return func(teo *testEnvOpts) {
			teo.publisherLinkSysFn = func(ds datastore.Batching) ipld.LinkSystem {
				lsys := cidlink.DefaultLinkSystem()
				backendLsys := mkProvLinkSystem(ds)

				lsys.StorageWriteOpener = backendLsys.StorageWriteOpener
				lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
					blockedReads.mu.Lock()
					isBlocked := blockedReads.list[l.(cidlink.Link).Cid]
					blockedReads.mu.Unlock()
					if isBlocked {
						fmt.Println("blocked read")
						hitBlockedRead <- l.(cidlink.Link).Cid
						return afterBlock()
					}
					return backendLsys.StorageReadOpener(lc, l)
				}

				return lsys
			}
		},
		blockedReads,
		hitBlockedRead
}

func TestFailDuringResync(t *testing.T) {
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(failBlockedRead)
	te := setupTestEnv(t, true, blockableLsysOpt)
	adHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := te.publisher.SetRoot(ctx, adHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	allMHs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	allAds := typehelpers.AllAds(t, typehelpers.AdFromLink(t, adHead, te.publisherLinkSys), te.publisherLinkSys)
	prevAd := allAds[1]
	blockedReads.add(prevAd.Entries.(cidlink.Link).Cid)

	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 1, false)
	require.NoError(t, err)
	c, ok := <-wait
	require.True(t, ok)
	require.Equal(t, adHead.(cidlink.Link).Cid, c)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[1:]))
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[0:1]))

	// resync. We'll fail when we are processing head ad
	wait, err = te.ingester.Sync(ctx, te.pubHost.ID(), nil, 2, true)
	require.NoError(t, err)
	<-hitBlockedRead
	<-wait
	// We failed to index the first ad
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[0:1]))
	// We still have the mhs from the head ad.
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[1:]))

	latestSync, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, adHead.(cidlink.Link).Cid, latestSync)

	// Now we'll resync again and we should succeed.
	blockedReads.rm(prevAd.Entries.(cidlink.Link).Cid)
	wait, err = te.ingester.Sync(ctx, te.pubHost.ID(), nil, 2, true)
	require.NoError(t, err)
	<-wait
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs))
}

func TestRestartDuringSync(t *testing.T) {
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(failBlockedRead)

	te := setupTestEnv(t, true, blockableLsysOpt, func(teo *testEnvOpts) {
		teo.skipIngesterCleanup = true
	})

	cCid := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll UpdateRoot to be B. Sync the
	// ingester, then kill the ingester when it tries to process B but after it
	// processes A. Then we'll bring the ingester up again and update root to be
	// C, and see if we index everything (A, B, C) correctly.
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	blockedReads.add(bAd.Entries.(cidlink.Link).Cid)

	bCid := *cAd.PreviousID

	ctx := context.Background()
	err = te.publisher.SetRoot(ctx, bCid.(cidlink.Link).Cid)
	require.NoError(t, err)

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = te.ingester.Sync(sctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)

	// The ingester tried to sync B, but it was blocked. Now let's stop the ingester.
	<-hitBlockedRead
	te.ingester.Close()
	te.ingester.host.Close()

	aMhs := typehelpers.AllMultihashesFromAdChain(t, aAd, te.publisherLinkSys)
	// Check that we processed A correctly.
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), aMhs))

	blockedReads.rm(bAd.Entries.(cidlink.Link).Cid)

	// We should not have processed B yet.
	bMhs := typehelpers.AllMultihashesFromAdChain(t, bAd, te.publisherLinkSys)
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), bMhs), "bMHs should not be indexed yet")

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
	err = te.publisher.UpdateRoot(ctx, cCid.(cidlink.Link).Cid)
	require.NoError(t, err)

	end, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	<-end

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMhs))
}

func TestFailDuringSync(t *testing.T) {
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(func() (io.Reader, error) {
		r := strings.NewReader("This causes data transfer to fail")
		return r, nil
	})

	te := setupTestEnv(t, true, blockableLsysOpt)

	cAdBuilder := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1}, // A
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2}, // B
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3}, // C
		}}

	cCid := cAdBuilder.Build(t, te.publisherLinkSys, te.publisherPriv)
	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll UpdateRoot to be B. Sync the
	// ingester, then fail the entries sync when the ingester tries to process B
	// but after it processes A. Then we'll run a sync with head==C and verify if
	// we've processed everything correctly
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	blockedReads.add(bAd.Entries.(cidlink.Link).Cid)

	bCid := (*cAd.PreviousID)
	require.NoError(t, err)

	ctx := context.Background()
	err = te.publisher.SetRoot(ctx, bCid.(cidlink.Link).Cid)
	require.NoError(t, err)

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = te.ingester.Sync(sctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)

	// The ingester tried to sync B, but it was blocked. Now let's stop the ingester.
	<-hitBlockedRead

	aMhs := typehelpers.AllMultihashesFromAdChain(t, aAd, te.publisherLinkSys)
	// Check that we processed A correctly.
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), aMhs))

	blockedReads.rm(bAd.Entries.(cidlink.Link).Cid)

	// We should not have processed B yet.
	bMhs := typehelpers.AllMultihashesFromAdChain(t, bAd, te.publisherLinkSys)
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), bMhs), "bMHs should not be indexed yet")

	// And sync to C
	err = te.publisher.UpdateRoot(ctx, cCid.(cidlink.Link).Cid)
	require.NoError(t, err)

	end, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	require.Equal(t, cCid.(cidlink.Link).Cid, <-end)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMhs))
}

func TestIngestDoesNotSkipAdIfFirstTryFailed(t *testing.T) {
	// Use this to block the second ingest step from happening while we verify the above
	afterBlockedRead := make(chan struct{})

	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(func() (io.Reader, error) {
		afterBlockedRead <- struct{}{}
		r := strings.NewReader("This causes data transfer to fail")
		return r, nil
	})

	te := setupTestEnv(t, true, blockableLsysOpt)

	// Disable the ingester getting sync finished events, we'll manually run the
	// ingest loop for ease of testing
	te.ingester.cancelOnSyncFinished()
	te.ingester.cancelOnSyncFinished = func() {}

	cAdBuilder := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1}, // A
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2}, // B
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3}, // C
		}}

	cCid := cAdBuilder.Build(t, te.publisherLinkSys, te.publisherPriv)

	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll UpdateRoot to be B. Sync the
	// ingester, then fail the entries sync when the ingester tries to process B
	// but after it processes A. Then we'll run a sync with head==C and verify if
	// we've processed everything correctly
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	bEntChunk := bAd.Entries
	blockedReads.add(bAd.Entries.(cidlink.Link).Cid)

	bCid := *cAd.PreviousID
	aCid := *bAd.PreviousID

	ctx := context.Background()
	err = te.publisher.SetRoot(ctx, cCid.(cidlink.Link).Cid)
	require.NoError(t, err)

	syncFinishedCh, cncl := te.ingester.sub.OnSyncFinished()
	defer cncl()

	// Note that this doesn't start an ingest on the indexer because we removed
	// the OnSyncFinished hook above.
	_, err = te.ingester.sub.Sync(ctx, te.pubHost.ID(), cid.Undef, nil, nil)
	require.NoError(t, err)
	syncFinishedEvent := <-syncFinishedCh

	ingesterOnSyncFin, cnclIngesterSyncFin := te.ingester.onAdProcessed(te.pubHost.ID())
	defer cnclIngesterSyncFin()

	go func() {
		te.ingester.runIngestStep(syncFinishedEvent)
	}()

	<-hitBlockedRead

	go func() {
		te.ingester.runIngestStep(syncFinishedEvent)
	}()

	blockedReads.rm(bEntChunk.(cidlink.Link).Cid)

	aMhs := typehelpers.AllMultihashesFromAdChain(t, aAd, te.publisherLinkSys)
	// Check that we processed A correctly.
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), aMhs))

	// We should not have processed B yet.
	bMhs := typehelpers.AllMultihashesFromAdChain(t, bAd, te.publisherLinkSys)
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), bMhs), "bMHs should not be indexed yet")

	<-afterBlockedRead

	adProcessedEvent := <-ingesterOnSyncFin
	require.NoError(t, adProcessedEvent.err)
	require.Equal(t, aCid.(cidlink.Link).Cid, adProcessedEvent.adCid)

	adProcessedEvent = <-ingesterOnSyncFin
	// B failed to be processed (because we blocked it)
	require.Error(t, adProcessedEvent.err)
	require.Equal(t, bCid.(cidlink.Link).Cid, adProcessedEvent.adCid)

	// The second worker now runs and we see that it processes both b and c
	adProcessedEvent = <-ingesterOnSyncFin
	require.NoError(t, adProcessedEvent.err)
	require.Equal(t, bCid.(cidlink.Link).Cid, adProcessedEvent.adCid)

	adProcessedEvent = <-ingesterOnSyncFin
	require.NoError(t, adProcessedEvent.err)
	require.Equal(t, cCid.(cidlink.Link).Cid, adProcessedEvent.adCid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMhs))
}

func TestWithDuplicatedEntryChunks(t *testing.T) {
	te := setupTestEnv(t, true)

	chainHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	t.Log("Head is", chainHead)
	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(adNode)
	require.NoError(t, err)

	ctx := context.Background()

	err = te.publisher.SetRoot(ctx, chainHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	c := <-wait

	lcid, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, chainHead.(cidlink.Link).Cid, lcid, "synced up to %s, should have synced up to %s", c, chainHead.(cidlink.Link).Cid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, ad, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMhs))

	te.Close(t)
}

func TestSyncWithDepth(t *testing.T) {
	te := setupTestEnv(t, true)

	chainHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 2},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(adNode)
	require.NoError(t, err)

	ctx := context.Background()

	err = te.publisher.SetRoot(ctx, chainHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 1, false)
	require.NoError(t, err)
	c, ok := <-wait
	require.True(t, ok)

	lcid, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, chainHead.(cidlink.Link).Cid, lcid, "synced up to %s, should have synced up to %s", c, chainHead.(cidlink.Link).Cid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, ad, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), []multihash.Multihash{allMhs[1]}))
	requireNotIndexed(t, []multihash.Multihash{allMhs[0]}, te.ingester.indexer)

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

	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(adNode)
	require.NoError(t, err)

	require.NotNil(t, ad.PreviousID)
	prevAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, *ad.PreviousID, schema.AdvertisementPrototype)
	require.NoError(t, err)
	prevAd, err := schema.UnwrapAdvertisement(prevAdNode)
	require.NoError(t, err)

	ctx := context.Background()
	err = te.publisher.UpdateRoot(context.Background(), chainHead.(cidlink.Link).Cid)
	require.NoError(t, err)

	wait, err := te.ingester.Sync(ctx, te.pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	<-wait
	var lcid cid.Cid
	requireTrueEventually(t, func() bool {
		lcid, err = te.ingester.GetLatestSync(te.pubHost.ID())
		require.NoError(t, err)
		return chainHead.(cidlink.Link).Cid == lcid
	}, testRetryInterval, testRetryTimeout, "Expected %s but got %s", chainHead, lcid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, prevAd, te.publisherLinkSys)
	// Remove the mhs from the first ad (since the last add removed this from the indexer)
	allMhs = allMhs[1:]
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMhs))
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
	end, err := i.Sync(ctx, pubHost.ID(), nil, 0, false)
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
	require.NoError(t, checkAllIndexed(i.indexer, providerID, mhs))

	// Test that we finish this sync even if we're already at the latest
	end, err = i.Sync(ctx, pubHost.ID(), nil, 0, false)
	require.NoError(t, err)
	_, ok := <-end
	require.True(t, ok)

	fmt.Println("Testing final resync")
	// Test that we finish this sync even if we have a limit
	end, err = i.Sync(ctx, pubHost.ID(), nil, 1, true)
	require.NoError(t, err)
	_, ok = <-end
	require.True(t, ok)
}

func TestReSyncWithDepth(t *testing.T) {
	te := setupTestEnv(t, false)
	adHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 2},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	err := te.publisher.SetRoot(context.Background(), adHead.(cidlink.Link).Cid)
	require.NoError(t, err)
	wait, err := te.ingester.Sync(context.Background(), te.pubHost.ID(), te.pubHost.Addrs()[0], 1, false)
	require.NoError(t, err)
	<-wait
	allMHs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[1:]))
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[0:1]))

	// When not resync, check that nothing beyond the latest is synced.
	wait, err = te.ingester.Sync(context.Background(), te.pubHost.ID(), te.pubHost.Addrs()[0], 0, false)
	require.NoError(t, err)
	<-wait
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[1:]))
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[0:1]))

	// When resync with greater depth, check that everything in synced.
	wait, err = te.ingester.Sync(context.Background(), te.pubHost.ID(), te.pubHost.Addrs()[0], 0, true)
	require.NoError(t, err)
	<-wait
	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs))
}

func TestSkipEarlierAdsIfAlreadyProcessedLaterAd(t *testing.T) {
	te := setupTestEnv(t, false)
	adHead := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 2},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 3},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)
	allAdLinks := typehelpers.AllAdLinks(t, adHead, te.publisherLinkSys)
	aLink := allAdLinks[0]
	bLink := allAdLinks[1]
	cLink := allAdLinks[2]
	allMHs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)

	err := te.publisher.SetRoot(context.Background(), bLink.(cidlink.Link).Cid)
	require.NoError(t, err)
	wait, err := te.ingester.Sync(context.Background(), te.pubHost.ID(), te.pubHost.Addrs()[0], 0, false)
	require.NoError(t, err)
	<-wait

	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[0:2]))
	require.Error(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs[2:]))

	err = te.ingester.sub.SetLatestSync(te.pubHost.ID(), aLink.(cidlink.Link).Cid)
	require.NoError(t, err)
	err = te.publisher.SetRoot(context.Background(), cLink.(cidlink.Link).Cid)
	require.NoError(t, err)
	wait, err = te.ingester.Sync(context.Background(), te.pubHost.ID(), te.pubHost.Addrs()[0], 0, false)
	require.NoError(t, err)
	<-wait

	require.NoError(t, checkAllIndexed(te.ingester.indexer, te.pubHost.ID(), allMHs))
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

	const entriesDepth = 10

	totalChunkCount := int(entriesDepth * 2)

	// Replace ingester entries selector with on that has a much smapper limit,
	// for testing.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	ing.entriesSel = ssb.ExploreRecursive(selector.RecursionLimitDepth(entriesDepth),
		ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Next", ssb.ExploreRecursiveEdge()) // Next field in EntryChunk
		})).Node()

	adCid, _, providerID := publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, false, totalChunkCount)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	end, err := ing.Sync(ctx, pubHost.ID(), nil, 0, false)
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
		adNode, err := lsys.Load(linking.LinkContext{}, lnk, schema.AdvertisementPrototype)
		require.NoError(t, err)
		ad, err := schema.UnwrapAdvertisement(adNode)
		require.NoError(t, err)

		mhs := typehelpers.AllMultihashesFromAdChainDepth(t, ad, lsys, entriesDepth)
		require.NoError(t, checkAllIndexed(ing.indexer, providerID, mhs))

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
		if i < entriesDepth {
			// Assert chunk multihashes are indexed
			require.NoError(t, checkAllIndexed(ing.indexer, providerID, mhs))
		} else {
			// Otherwise, assert chunk multihashes are not indexed.
			requireNotIndexed(t, mhs, ing.indexer)
		}
	}

	// Assert no more chunks are left.
	require.Equal(t, cid.Undef, nextChunkCid)
}

func requireNotIndexed(t *testing.T, mhs []multihash.Multihash, ix indexer.Interface) {
	for _, mh := range mhs {
		_, exists, err := ix.Get(mh)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

func getAdEntriesCid(t *testing.T, store datastore.Batching, ad cid.Cid) cid.Cid {
	ctx := context.TODO()
	val, err := store.Get(ctx, dsKey(ad.String()))
	require.NoError(t, err)
	nad, err := decodeIPLDNode(ad.Prefix().Codec, bytes.NewBuffer(val), schema.AdvertisementPrototype)
	require.NoError(t, err)
	adv, err := schema.UnwrapAdvertisement(nad)
	require.NoError(t, err)
	return adv.Entries.(cidlink.Link).Cid
}

func decodeEntriesChunk(t *testing.T, store datastore.Batching, c cid.Cid) ([]multihash.Multihash, cid.Cid) {
	ctx := context.TODO()
	val, err := store.Get(ctx, dsKey(c.String()))
	require.NoError(t, err)
	nentries, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), schema.EntryChunkPrototype)
	require.NoError(t, err)

	ec, err := schema.UnwrapEntryChunk(nentries)
	require.NoError(t, err)

	if ec.Next == nil {
		return ec.Entries, cid.Undef
	}

	return ec.Entries, (*ec.Next).(cidlink.Link).Cid
}

func TestMultiplePublishers(t *testing.T) {
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	h := mkTestHost()
	pubHost1Priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	pubHost1 := mkTestHost(libp2p.Identity(pubHost1Priv))
	pubHost2Priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	pubHost2 := mkTestHost(libp2p.Identity(pubHost2Priv))
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

	ctx := context.Background()

	// Test with two random advertisement publications for each of them.
	c1 := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3},
		}}.Build(t, lsys1, pubHost1Priv)
	err = pub1.UpdateRoot(ctx, c1.(cidlink.Link).Cid)
	require.NoError(t, err)
	mhs := typehelpers.AllMultihashesFromAdLink(t, c1, lsys1)
	wait, err := i.Sync(ctx, pubHost1.ID(), nil, 0, false)
	require.NoError(t, err)
	<-wait

	require.NoError(t, checkAllIndexed(i.indexer, pubHost1.ID(), mhs))

	c2 := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 1},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 2},
			{ChunkCount: 10, EntriesPerChunk: 10, EntriesSeed: 3},
		}}.Build(t, lsys2, pubHost2Priv)
	err = pub2.UpdateRoot(ctx, c2.(cidlink.Link).Cid)
	require.NoError(t, err)
	mhs = typehelpers.AllMultihashesFromAdLink(t, c2, lsys2)

	wait, err = i.Sync(ctx, pubHost2.ID(), nil, 0, false)
	require.NoError(t, err)
	<-wait

	require.NoError(t, checkAllIndexed(i.indexer, pubHost2.ID(), mhs))

	// Check that subscriber recorded latest sync.
	lnk := i.sub.GetLatestSync(pubHost1.ID())
	require.Equal(t, lnk, c1)
	lnk = i.sub.GetLatestSync(pubHost2.ID())
	require.Equal(t, lnk, c2)

	lcid, err := i.GetLatestSync(pubHost1.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c1.(cidlink.Link).Cid)

	lcid, err = i.GetLatestSync(pubHost2.ID())
	require.NoError(t, err)
	require.Equal(t, lcid, c2.(cidlink.Link).Cid)
}

func TestRateLimitConfig(t *testing.T) {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	defer store.Close()
	reg := mkRegistry(t)
	defer reg.Close()
	core := mkIndexer(t, true)
	defer core.Close()
	pubHost := mkTestHost()
	h := mkTestHost()

	cfg := ingestCfg
	ingester, err := NewIngester(cfg, h, core, reg, store)
	require.NoError(t, err)
	limiter := ingester.getRateLimiter(pubHost.ID())
	require.NotNil(t, limiter)
	require.Equal(t, limiter.Limit(), rate.Limit(ingestCfg.RateLimit.BlocksPerSecond))
	ingester.Close()

	cfg.RateLimit.Apply = false
	ingester, err = NewIngester(cfg, h, core, reg, store)
	require.NoError(t, err)
	limiter = ingester.getRateLimiter(pubHost.ID())
	require.NotNil(t, limiter)
	require.Equal(t, limiter.Limit(), rate.Inf)
	ingester.Close()

	cfg.RateLimit.Apply = true
	cfg.RateLimit.BlocksPerSecond = 0
	ingester, err = NewIngester(cfg, h, core, reg, store)
	require.NoError(t, err)
	limiter = ingester.getRateLimiter(pubHost.ID())
	require.NotNil(t, limiter)
	require.Equal(t, limiter.Limit(), rate.Inf)
	ingester.Close()
}

func mkTestHost(opts ...libp2p.Option) host.Host {
	// 10x Faster than the default identity option in libp2p.New
	var defaultIdentity libp2p.Option = func(cfg *libp2p.Config) error {
		if cfg.PeerKey == nil {
			priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
			if err != nil {
				return err
			}
			cfg.PeerKey = priv
		}

		return nil
	}
	opts = append(opts, defaultIdentity)

	opts = append(opts, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
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
			Allow:   true,
			Publish: true,
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
	var out []multihash.Multihash
	var nextLnk *ipld.Link
	for i := 0; i < size; i++ {
		mhs := util.RandomMultihashes(testEntriesChunkSize, rng)
		chunk := &schema.EntryChunk{
			Entries: mhs,
			Next:    nextLnk,
		}
		node, err := chunk.ToNode()
		require.NoError(t, err)
		lnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
		out = append(out, mhs...)
		nextLnk = &lnk
	}
	return *nextLnk, out
}

func publishRandomIndexAndAdv(t *testing.T, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool) (cid.Cid, []multihash.Multihash, peer.ID) {
	return publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, fakeSig, testEntriesChunkCount)
}

func publishRandomIndexAndAdvWithEntriesChunkCount(t *testing.T, pub legs.Publisher, lsys ipld.LinkSystem, fakeSig bool, eChunkCount int) (cid.Cid, []multihash.Multihash, peer.ID) {

	priv, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	p, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	mhsLnk, mhs := newRandomLinkedList(t, lsys, eChunkCount)

	adv := &schema.Advertisement{
		Provider:  p.String(),
		Addresses: addrs,
		Entries:   mhsLnk,
		ContextID: ctxID,
		Metadata:  metadata,
	}
	if !fakeSig {
		err := adv.Sign(priv)
		require.NoError(t, err)
	}

	node, err := adv.ToNode()
	require.NoError(t, err)
	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	err = pub.UpdateRoot(context.Background(), advLnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	return advLnk.(cidlink.Link).Cid, mhs, p
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
