package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/p2psender"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	dstest "github.com/ipni/go-libipni/dagsync/test"
	schema "github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/mautil"
	"github.com/ipni/go-libipni/test"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/test/typehelpers"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	p2ptest "github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testRetryInterval = 100 * time.Millisecond
	testRetryTimeout  = 5 * time.Second

	testEntriesChunkCount = 3
	testEntriesChunkSize  = 15
)

var (
	defaultTestIngestConfig = config.Ingest{
		AdvertisementDepthLimit: 100,
		EntriesDepthLimit:       100,
		IngestWorkerCount:       1,
		PubSubTopic:             "test/ingest",
		SyncTimeout:             config.Duration(time.Minute),
		SyncSegmentDepthLimit:   1, // By default run all tests using segmented sync.
	}
	rng = rand.New(rand.NewSource(1413))
)

func TestSubscribe(t *testing.T) {
	te := setupTestEnv(t, true)

	// Check that we sync with an ad chain
	adHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)
	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	ctx := context.Background()
	_, err := te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)
	mhs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)

	// Check that we sync if the publisher gives us another provider instead.
	someOtherProviderPriv, _, err := p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	adHead = typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 3},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 4},
		}}.Build(t, te.publisherLinkSys, someOtherProviderPriv)
	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)

	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	someOtherProvider, err := peer.IDFromPrivateKey(someOtherProviderPriv)
	require.NoError(t, err)
	mhs = typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, someOtherProvider, mhs)

	// Check that we don't ingest from ads that aren't signed.
	someOtherProviderPriv, _, err = p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	adHead = typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 5},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 6},
		}}.BuildWithFakeSig(t, te.publisherLinkSys, someOtherProviderPriv)
	mhs = typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)

	// No mhs should have been saved for related index because the ad wasn't signed
	for x := range mhs {
		_, b, _ := te.ingester.indexer.Get(mhs[x])
		require.False(t, b)
	}

	// Check that we don't ingest from blocked peers
	someOtherProviderPriv, _, err = p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	adHead = typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 7},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 8},
		}}.Build(t, te.publisherLinkSys, someOtherProviderPriv)

	someOtherProvider, err = peer.IDFromPrivateKey(someOtherProviderPriv)
	require.NoError(t, err)

	te.reg.BlockPeer(te.pubHost.ID())
	te.reg.BlockPeer(someOtherProvider)

	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)

	// We are manually syncing here to not rely on the pubsub mechanism inside a test.
	// This will fetch the add and put it into our datastore, but will not process it.
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	var aiErr adIngestError
	require.ErrorAs(t, err, &aiErr)
	require.ErrorIs(t, err, registry.ErrNotAllowed)

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

type errReader struct{}

func (e *errReader) Read([]byte) (int, error) {
	return 0, errors.New("blocked read")
}

func failBlockedRead() (io.Reader, error) {
	// Returning an error here will cause a "content not found" graphsync error
	// and the ad will be skipped without failing the sync. So, return an
	// io.Reader that will return an error on calling Read.
	return &errReader{}, nil
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
						if afterBlock != nil {
							return afterBlock()
						}
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
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)

	allMHs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	allAds := typehelpers.AllAds(t, typehelpers.AdFromLink(t, adHead, te.publisherLinkSys), te.publisherLinkSys)
	prevAd := allAds[1]
	blockedReads.add(prevAd.Entries.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	c, err := te.ingester.Sync(ctx, peerInfo, 1, false)
	require.NoError(t, err)
	require.Equal(t, adHead.(cidlink.Link).Cid, c)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs[1:])
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), allMHs[0:1])

	// resync. We'll fail when we are processing head ad
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		_, err := te.ingester.Sync(ctx, peerInfo, 2, true)
		require.Error(t, err)
	}()
	<-hitBlockedRead
	<-wait
	// We failed to index the first ad
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), allMHs[0:1])
	// We still have the mhs from the head ad.
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs[1:])

	pinfo, ok := te.reg.ProviderInfo(te.pubHost.ID())
	require.True(t, ok)
	require.Equal(t, 1, pinfo.Lag)

	latestSync, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, adHead.(cidlink.Link).Cid, latestSync)

	// Now we'll resync again and we should succeed.
	blockedReads.rm(prevAd.Entries.(cidlink.Link).Cid)
	_, err = te.ingester.Sync(ctx, peerInfo, 2, true)
	require.NoError(t, err)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs)
}

func TestFirstAdMissingAddrs(t *testing.T) {
	te := setupTestEnv(t, true)

	cCid := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 1, Addrs: []string{}},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	te.publisher.SetRoot(cCid.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	ctx := context.Background()
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}

func TestRestartDuringSync(t *testing.T) {
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(failBlockedRead)

	te := setupTestEnv(t, true, blockableLsysOpt)

	cCid := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 2},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 3},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll SetRoot to be B. Sync the
	// ingester, then kill the ingester when it tries to process B but after it
	// processes A. Then we'll bring the ingester up again and update root to be
	// C, and see if we index everything (A, B, C) correctly.
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	blockedReads.add(bAd.Entries.(cidlink.Link).Cid)

	bCid := cAd.PreviousID

	ctx := context.Background()
	te.publisher.SetRoot(bCid.(cidlink.Link).Cid)

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	go func() {
		_, err := te.ingester.Sync(sctx, peerInfo, 0, false)
		// Error may be a number of different errors depending on where in the
		// sync process the service is closed. So, just check that there is an
		// error.
		require.Error(t, err)
	}()

	// The ingester tried to sync B, but it was blocked. Now let's stop the ingester.
	<-hitBlockedRead
	te.ingester.Close()
	te.ingester.host.Close()

	aMhs := typehelpers.AllMultihashesFromAdChain(t, aAd, te.publisherLinkSys)
	// Check that we processed A correctly.
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), aMhs)

	blockedReads.rm(bAd.Entries.(cidlink.Link).Cid)

	// We should not have processed B yet.
	bMhs := typehelpers.AllMultihashesFromAd(t, bAd, te.publisherLinkSys)
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), bMhs, "bMHs should not be indexed yet")

	// Now we bring up the ingester again.
	ingesterHost := dstest.MkTestHost(t, libp2p.Identity(te.ingesterPriv))
	connectHosts(t, te.pubHost, ingesterHost)
	ingester, err := NewIngester(defaultTestIngestConfig, ingesterHost, te.ingester.indexer, mkRegistry(t), te.ingester.ds, te.ingester.dsTmp)
	require.NoError(t, err)
	t.Cleanup(func() {
		ingester.Close()
	})
	te.ingester = ingester

	// And sync to C
	te.publisher.SetRoot(cCid.(cidlink.Link).Cid)
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}

func TestFailDuringSync(t *testing.T) {
	t.Parallel()
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(failBlockedRead)

	te := setupTestEnv(t, true, blockableLsysOpt)

	cAdBuilder := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 1}, // A
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 2},                  // B
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 3}, // C
		}}

	cCid := cAdBuilder.Build(t, te.publisherLinkSys, te.publisherPriv)
	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll SetRoot to be B. Sync the
	// ingester, then fail the entries sync when the ingester tries to process B
	// but after it processes A. Then we'll run a sync with head==C and verify if
	// we've processed everything correctly
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	aMhs := typehelpers.AllMultihashesFromAd(t, aAd, te.publisherLinkSys)
	bMhs := typehelpers.AllMultihashesFromAd(t, bAd, te.publisherLinkSys)

	blockedReads.add(bAd.Entries.(cidlink.Link).Cid)

	bCid := cAd.PreviousID
	require.NoError(t, err)

	te.publisher.SetRoot(bCid.(cidlink.Link).Cid)

	sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	go func() {
		_, err = te.ingester.Sync(sctx, peerInfo, 0, false)
		require.Error(t, err)
		// Should see last error since cannot ingest ad entries.
		pInfo, _ := te.reg.ProviderInfo(te.pubHost.ID())
		require.NotEmpty(t, pInfo.LastError)
		t.Log("Last Error:", pInfo.LastError)
	}()
	// The ingester tried to sync B, but it was blocked. Now let's stop the ingester.
	<-hitBlockedRead

	// Check that we processed A correctly.
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), aMhs)

	// We should not have processed B yet.
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), bMhs)

	blockedReads.rm(bAd.Entries.(cidlink.Link).Cid)
	// And sync to C
	te.publisher.SetRoot(cCid.(cidlink.Link).Cid)

	endCid, err := te.ingester.Sync(sctx, peerInfo, 0, false)
	require.NoError(t, err)
	require.Equal(t, cCid.(cidlink.Link).Cid, endCid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}

func TestIngestDoesNotSkipAdIfFirstTryFailed(t *testing.T) {
	// Use this to block the second ingest step from happening while we verify the above
	afterBlockedRead := make(chan struct{})

	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(func() (io.Reader, error) {
		afterBlockedRead <- struct{}{}
		return failBlockedRead()
	})

	te := setupTestEnv(t, true, blockableLsysOpt)

	// Disable the ingester getting sync finished events, we'll manually run the
	// ingest loop for ease of testing
	te.ingester.cancelOnSyncFinished()
	te.ingester.cancelOnSyncFinished = func() {}
	chainQueue := make(chan dagsync.SyncFinished, 1)
	te.ingester.syncFinishedEvents = chainQueue
	te.ingester.workerPoolSize = 0
	te.ingester.RunWorkers(1)

	cAdBuilder := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 1}, // A
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 2},                  // B
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 3}, // C
		}}

	cCid := cAdBuilder.Build(t, te.publisherLinkSys, te.publisherPriv)

	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// We have a chain of 3 ads, A<-B<-C. We'll SetRoot to be B. Sync the
	// ingester, then fail the entries sync when the ingester tries to process B
	// but after it processes A. Then we'll run a sync with head==C and verify if
	// we've processed everything correctly
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	aAd := allAds[2]
	bAd := allAds[1]
	require.Equal(t, allAds[0], cAd)

	bEntChunk := bAd.Entries
	blockedReads.add(bAd.Entries.(cidlink.Link).Cid)

	bCid := cAd.PreviousID
	aCid := bAd.PreviousID

	ctx := context.Background()
	te.publisher.SetRoot(cCid.(cidlink.Link).Cid)

	syncFinishedCh, cncl := te.ingester.sub.OnSyncFinished()
	defer cncl()

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	// Note that this doesn't start an ingest on the indexer because we removed
	// the OnSyncFinished hook above.
	_, err = te.ingester.sub.SyncAdChain(ctx, peerInfo)
	require.NoError(t, err)
	syncFinishedEvent := <-syncFinishedCh

	ingesterOnSyncFin, cnclIngesterSyncFin := te.ingester.onAdProcessed(te.pubHost.ID())
	defer cnclIngesterSyncFin()

	go func() {
		chainQueue <- syncFinishedEvent
	}()

	<-hitBlockedRead

	go func() {
		chainQueue <- syncFinishedEvent
	}()

	blockedReads.rm(bEntChunk.(cidlink.Link).Cid)

	aMhs := typehelpers.AllMultihashesFromAdChain(t, aAd, te.publisherLinkSys)
	// Check that we processed A correctly.
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), aMhs)

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
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}

func TestWithDuplicatedEntryChunks(t *testing.T) {
	te := setupTestEnv(t, true)

	chainHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 1},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	t.Log("Head is", chainHead)
	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(adNode)
	require.NoError(t, err)

	te.publisher.SetRoot(chainHead.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	c, err := te.ingester.Sync(context.Background(), peerInfo, 0, false)
	require.NoError(t, err)

	lcid, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, chainHead.(cidlink.Link).Cid, lcid, "synced up to %s, should have synced up to %s", c, chainHead.(cidlink.Link).Cid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, ad, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)
}

func TestSyncWithDepth(t *testing.T) {
	te := setupTestEnv(t, true)

	chainHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 2},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(adNode)
	require.NoError(t, err)

	te.publisher.SetRoot(chainHead.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	c, err := te.ingester.Sync(context.Background(), peerInfo, 1, false)
	require.NoError(t, err)

	lcid, err := te.ingester.GetLatestSync(te.pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, chainHead.(cidlink.Link).Cid, lcid, "synced up to %s, should have synced up to %s", c, chainHead.(cidlink.Link).Cid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, ad, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), []multihash.Multihash{allMhs[1]})
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), []multihash.Multihash{allMhs[0]})
}

func TestFreeze(t *testing.T) {
	te := setupTestEnv(t, true)

	// Check that we sync with an ad chain
	adHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)
	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	_, err := te.ingester.Sync(context.Background(), peerInfo, 0, false)
	require.NoError(t, err)
	mhs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)

	require.NoError(t, te.reg.Freeze())
	require.True(t, te.reg.Frozen())

	provs := te.reg.AllProviderInfo()
	require.Equal(t, 1, len(provs))
	prov := provs[0]
	require.Equal(t, te.pubHost.ID(), prov.Publisher)
	require.Equal(t, adHead.(cidlink.Link).Cid, prov.LastAdvertisement)

	prevAdCid := adHead.(cidlink.Link).Cid
	adHead = typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 11},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 222},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)

	ctx := context.Background()
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	provs = te.reg.AllProviderInfo()
	require.Equal(t, 1, len(provs))
	prov = provs[0]
	require.Equal(t, te.pubHost.ID(), prov.Publisher)
	require.Equal(t, prevAdCid, prov.FrozenAt)
	require.Equal(t, adHead.(cidlink.Link).Cid, prov.LastAdvertisement)

	// Check that none of the post-freeze multihashes were ingested.
	mhs = typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	for x := range mhs {
		_, b, _ := te.ingester.indexer.Get(mhs[x])
		require.False(t, b)
	}

	rmAdCid := publishRemovalAd(t, te.publisher, te.publisherLinkSys, false, te.pubHost.ID(), te.publisherPriv)
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	// Check that last advertisement matches last removal.
	provs = te.reg.AllProviderInfo()
	require.Equal(t, 1, len(provs))
	prov = provs[0]
	require.Equal(t, rmAdCid, prov.LastAdvertisement)
}

type coreWrap struct {
	indexer.Interface
	mhs []multihash.Multihash
}

func (c *coreWrap) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	c.mhs = append(c.mhs, mhs...)
	return c.Interface.Put(value, mhs...)
}

func TestRmWithNoEntries(t *testing.T) {
	te := setupTestEnv(t, true)
	cw := &coreWrap{
		Interface: te.ingester.indexer,
	}
	te.ingester.indexer = cw

	chainHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 2},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 3},
		},
		AddRmWithNoEntries: true,
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	adNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, chainHead, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(adNode)
	require.NoError(t, err)

	require.NotNil(t, ad.PreviousID)
	prevAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, ad.PreviousID, schema.AdvertisementPrototype)
	require.NoError(t, err)
	prevAd, err := schema.UnwrapAdvertisement(prevAdNode)
	require.NoError(t, err)

	te.publisher.SetRoot(chainHead.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	ctx := context.Background()
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)
	var lcid cid.Cid
	require.Eventually(t, func() bool {
		lcid, err = te.ingester.GetLatestSync(te.pubHost.ID())
		require.NoError(t, err)
		return chainHead.(cidlink.Link).Cid == lcid
	}, testRetryTimeout, testRetryInterval, "Expected %s but got %s", chainHead, lcid)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, prevAd, te.publisherLinkSys)

	first := allMhs[0]

	// Remove the mhs from the first ad (since the last add removed this from the indexer)
	allMhs = allMhs[1:]
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)

	// Check that first multihash was never ingested in the first place,
	// indicating it was skipped.
	var found bool
	for _, mh := range cw.mhs {
		if bytes.Equal(mh, first) {
			found = true
		}
	}
	require.False(t, found)
}

func TestSync(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost := dstest.MkTestHost(t)
	i, _ := mkIngest(t, h)
	pub, lsys := mkMockPublisher(t, pubHost, h, srcStore)
	connectHosts(t, h, pubHost)

	c1, mhs, providerID, privKey := publishRandomIndexAndAdv(t, pub, lsys, false, nil, cid.Undef)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The explicit sync will happen concurrently with the sycn triggered by
	// the published advertisement. These will be serialized in the dagsync
	// handler for the provider.
	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	endCid, err := i.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	// We receive the CID that we synced.
	require.Equal(t, c1, endCid)
	// Check that subscriber recorded latest sync.
	lnk := i.sub.GetLatestSync(pubHost.ID())
	lcid := lnk.(cidlink.Link).Cid
	require.Equal(t, lcid, c1)
	// Check that latest sync recorded in datastore
	require.Eventually(t, func() bool {
		lcid, err = i.GetLatestSync(pubHost.ID())
		require.NoError(t, err)
		return c1.Equals(lcid)
	}, testRetryTimeout, testRetryInterval, "Expected %s but got %s", c1, lcid)

	// Checking providerID, since that was what was put in the advertisement, not pubhost.ID()
	requireIndexedEventually(t, i.indexer, providerID, mhs)

	// Test that we finish this sync even if we're already at the latest
	_, err = i.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	fmt.Println("Testing final resync")
	// Test that we finish this sync even if we have a limit
	_, err = i.Sync(ctx, peerInfo, 1, true)
	require.NoError(t, err)

	publishRemovalAd(t, pub, lsys, false, providerID, privKey)
	_, err = i.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)
}

func TestSyncInternalError(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost := dstest.MkTestHost(t)
	ing, reg := mkIngest(t, h)
	pub, lsys := mkMockPublisher(t, pubHost, h, srcStore)
	connectHosts(t, h, pubHost)

	c1, _, providerID, _ := publishRandomIndexAndAdv(t, pub, lsys, false, nil, cid.Undef)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	errVS := newErrorValueStore()
	ing.indexer = engine.New(errVS)

	// The explicit sync will happen concurrently with the sycn triggered by
	// the published advertisement. These will be serialized in the dagsync
	// handler for the provider.
	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	_, err := ing.Sync(ctx, peerInfo, 0, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errInternal)
	require.ErrorContains(t, err, errVS.err.Error())
	t.Log("Sync error:", err)

	// Check that subscriber did ot record latest sync.
	lnk := ing.sub.GetLatestSync(pubHost.ID())
	lcid := lnk.(cidlink.Link).Cid
	require.Equal(t, c1, lcid)

	provs := reg.AllProviderInfo()
	require.NotZero(t, len(provs))
	p := provs[0]
	require.Equal(t, providerID, p.AddrInfo.ID)
	require.Contains(t, p.LastError, errInternal.Error())
	require.NotContains(t, p.LastError, errVS.err.Error())
	t.Log("Last Error:", p.LastError)
}

func testSyncWithExtendedProviders(t *testing.T,
	testFunc func(crypto.PrivKey, crypto.PubKey, peer.ID, *registry.Registry, linking.LinkSystem, host.Host, *Ingester, dagsync.Publisher)) {
	privKey, pubKey, err := p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	providerID, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost := dstest.MkTestHost(t)
	ingester, reg := mkIngest(t, h)
	pub, lsys := mkMockPublisher(t, pubHost, h, srcStore)
	connectHosts(t, h, pubHost)

	testFunc(privKey, pubKey, providerID, reg, lsys, pubHost, ingester, pub)
}

func TestSyncWithContextualExtendedProviders(t *testing.T) {
	testSyncWithExtendedProviders(t, func(privKey crypto.PrivKey,
		pubKey crypto.PubKey,
		providerID peer.ID,
		reg *registry.Registry,
		lsys linking.LinkSystem,
		pubHost host.Host,
		ingester *Ingester, pub dagsync.Publisher) {

		adv1, _, mhs1 := publishAdvWithExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, cid.Undef, "test-context-id", nil, false)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		syncIngester(t, ctx, ingester, providerID, pubHost, mhs1)

		pInfo, _ := reg.ProviderInfo(providerID)
		extendedProviders := pInfo.ExtendedProviders
		require.NotNil(t, extendedProviders)
		require.Equal(t, 0, len(extendedProviders.Providers))
		require.Equal(t, 1, len(extendedProviders.ContextualProviders))
		verifyContextualProviders(t, extendedProviders, adv1, reg)
	})
}

func TestSyncWithChainLevelExtendedProviders(t *testing.T) {
	testSyncWithExtendedProviders(t, func(privKey crypto.PrivKey,
		pubKey crypto.PubKey,
		providerID peer.ID,
		reg *registry.Registry,
		lsys linking.LinkSystem,
		pubHost host.Host,
		ingester *Ingester, pub dagsync.Publisher) {

		adv1, _, mhs1 := publishAdvWithExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, cid.Undef, "", nil, false)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		syncIngester(t, ctx, ingester, providerID, pubHost, mhs1)

		pInfo, _ := reg.ProviderInfo(providerID)
		extendedProviders := pInfo.ExtendedProviders
		require.NotNil(t, extendedProviders)
		require.Equal(t, len(extendedProviders.Providers), len(extendedProviders.Providers))
		require.Equal(t, 0, len(extendedProviders.ContextualProviders))
		verifyChainLevelProviders(t, extendedProviders, adv1, reg)
	})
}

func TestSyncExtendedProvidersShouldBeOverridednOnEachAd(t *testing.T) {
	testSyncWithExtendedProviders(t, func(privKey crypto.PrivKey,
		pubKey crypto.PubKey,
		providerID peer.ID,
		reg *registry.Registry,
		lsys linking.LinkSystem,
		pubHost host.Host,
		ingester *Ingester, pub dagsync.Publisher) {

		_, ad1Cid, mhs1 := publishAdvWithExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, cid.Undef, "test-context-id", nil, false)
		adv2, _, mhs2 := publishAdvWithExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, ad1Cid, "", nil, false)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		syncIngester(t, ctx, ingester, providerID, pubHost, mhs1, mhs2)

		// there should be no extended providers from ad1, only from ad2
		pInfo, _ := reg.ProviderInfo(providerID)
		extendedProviders := pInfo.ExtendedProviders
		require.NotNil(t, extendedProviders)
		require.Equal(t, len(extendedProviders.Providers), len(extendedProviders.Providers))
		require.Equal(t, 0, len(extendedProviders.ContextualProviders))
		verifyChainLevelProviders(t, extendedProviders, adv2, reg)
	})
}

func TestSyncSupplyEmpyExtendedProvidersActsAsRemove(t *testing.T) {
	testSyncWithExtendedProviders(t, func(privKey crypto.PrivKey,
		pubKey crypto.PubKey,
		providerID peer.ID,
		reg *registry.Registry,
		lsys linking.LinkSystem,
		pubHost host.Host,
		ingester *Ingester, pub dagsync.Publisher) {

		_, ad1Cid, mhs1 := publishAdvWithExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, cid.Undef, "test-context-id", nil, false)
		_, _, mhs2 := publishAdWithEmptyExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, ad1Cid, "", nil)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		syncIngester(t, ctx, ingester, providerID, pubHost, mhs1, mhs2)

		// there should be no extended providers
		pInfo, _ := reg.ProviderInfo(providerID)
		extendedProviders := pInfo.ExtendedProviders
		require.NotNil(t, extendedProviders)
		require.Equal(t, 0, len(extendedProviders.Providers))
		require.Equal(t, 0, len(extendedProviders.ContextualProviders))
	})
}

func TestSyncNilExtendedProvidersDontOverrideTheExistingOnes(t *testing.T) {
	testSyncWithExtendedProviders(t, func(privKey crypto.PrivKey,
		pubKey crypto.PubKey,
		providerID peer.ID,
		reg *registry.Registry,
		lsys linking.LinkSystem,
		pubHost host.Host,
		ingester *Ingester, pub dagsync.Publisher) {

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		ad1, ad1Cid, mhs1 := publishAdvWithExtendedProviders(t, providerID, privKey, pubKey, pub, lsys, cid.Undef, "", nil, false)
		syncIngester(t, ctx, ingester, providerID, pubHost, mhs1)
		_, mhs2, pID, _ := publishRandomIndexAndAdv(t, pub, lsys, false, nil, ad1Cid)
		syncIngester(t, ctx, ingester, pID, pubHost, mhs2)

		// there should be no extended providers
		pInfo, _ := reg.ProviderInfo(providerID)
		extendedProviders := pInfo.ExtendedProviders
		require.NotNil(t, extendedProviders)
		require.Equal(t, len(ad1.ExtendedProvider.Providers), len(extendedProviders.Providers))
		require.Equal(t, 0, len(extendedProviders.ContextualProviders))
	})
}

func syncIngester(t *testing.T, ctx context.Context, ingester *Ingester, providerID peer.ID, pubHost host.Host, mhs ...[]multihash.Multihash) {
	t.Helper()
	peerInfo := peer.AddrInfo{
		ID:    pubHost.ID(),
		Addrs: pubHost.Addrs(),
	}
	_, err := ingester.Sync(ctx, peerInfo, -1, false)
	require.NoError(t, err)
	for _, mms := range mhs {
		requireIndexedEventually(t, ingester.indexer, providerID, mms)
	}
}

func verifyContextualProviders(t *testing.T, extendedProviders *registry.ExtendedProviders, adv *schema.Advertisement, reg *registry.Registry) {
	t.Helper()
	contextualProviders := extendedProviders.ContextualProviders[string(adv.ContextID)]
	require.NotNil(t, contextualProviders)
	require.Equal(t, adv.ContextID, contextualProviders.ContextID)
	require.Equal(t, adv.ExtendedProvider.Override, contextualProviders.Override)
	require.Equal(t, len(adv.ExtendedProvider.Providers), len(contextualProviders.Providers))

	contextualProviderByID := map[peer.ID]registry.ExtendedProviderInfo{}
	for _, p := range contextualProviders.Providers {
		contextualProviderByID[p.PeerID] = p
	}

	providerID, err := peer.Decode(adv.Provider)
	require.NoError(t, err)

	verifyExtendedProviders(t, providerID, contextualProviderByID, adv.ExtendedProvider.Providers, reg)
}

func verifyChainLevelProviders(t *testing.T, extendedProviders *registry.ExtendedProviders, adv *schema.Advertisement, reg *registry.Registry) {
	t.Helper()
	providerID, err := peer.Decode(adv.Provider)
	require.NoError(t, err)

	chainLevelProviderByID := map[peer.ID]registry.ExtendedProviderInfo{}
	for _, p := range extendedProviders.Providers {
		chainLevelProviderByID[p.PeerID] = p
	}
	verifyExtendedProviders(t, providerID, chainLevelProviderByID, adv.ExtendedProvider.Providers, reg)
}

func verifyExtendedProviders(t *testing.T, adProviderID peer.ID, providerInfosMap map[peer.ID]registry.ExtendedProviderInfo, adExtendedProviders []schema.Provider, reg *registry.Registry) {
	t.Helper()
	for _, ep := range adExtendedProviders {
		peerID, err := peer.Decode(ep.ID)
		require.NoError(t, err)

		epInfo := providerInfosMap[peerID]
		require.NotNil(t, epInfo)

		require.Equal(t, peerID, epInfo.PeerID)
		addr := epInfo.Addrs[0].String()
		fmt.Println(addr)
		maddrs, err := mautil.StringsToMultiaddrs(ep.Addresses)
		require.NoError(t, err)
		require.Equal(t, maddrs, epInfo.Addrs)
		require.Equal(t, ep.Metadata, epInfo.Metadata)
	}
}

func TestSyncTooLargeMetadata(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost := dstest.MkTestHost(t)
	i, _ := mkIngest(t, h)
	pub, lsys := mkMockPublisher(t, pubHost, h, srcStore)
	connectHosts(t, h, pubHost)

	metadata := make([]byte, schema.MaxMetadataLen*2)
	copy(metadata, []byte("too-long"))

	publishRandomIndexAndAdv(t, pub, lsys, false, metadata, cid.Undef)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The explicit sync will happen concurrently with the sycn triggered by
	// the published advertisement. These will be serialized in the dagsync
	// handler for the provider.
	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	endCid, err := i.Sync(ctx, peerInfo, 0, false)
	require.ErrorContains(t, err, errBadAdvert.Error())

	// We receive the CID that we synced.
	require.Equal(t, cid.Undef, endCid)
	lcid := cid.Undef

	// Check that subscriber recorded latest sync.
	lnk := i.sub.GetLatestSync(pubHost.ID())
	if lnk != nil {
		lcid = lnk.(cidlink.Link).Cid
	}
	require.Equal(t, cid.Undef, lcid)
}

func TestSyncSkipNoMetadata(t *testing.T) {
	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost := dstest.MkTestHost(t)
	i, reg := mkIngest(t, h)
	pub, lsys := mkMockPublisher(t, pubHost, h, srcStore)
	connectHosts(t, h, pubHost)

	// Test ad that has no entries and no metadata.
	adCid, _, providerID, _ := publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, false, 0, []byte{}, cid.Undef)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	endCid, err := i.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	// We receive the CID that we synced.
	require.Equal(t, adCid, endCid)
	lcid := cid.Undef

	// Check that subscriber recorded latest sync.
	lnk := i.sub.GetLatestSync(pubHost.ID())
	if lnk != nil {
		lcid = lnk.(cidlink.Link).Cid
	}
	require.Equal(t, adCid, lcid)

	pInfo, found := reg.ProviderInfo(providerID)
	require.True(t, found)
	require.Equal(t, adCid, pInfo.LastAdvertisement)

	// Test ad that has entries and no metadata.
	adCid, _, providerID, _ = publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, false, 10, []byte{}, cid.Undef)
	endCid, err = i.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)
	require.Equal(t, adCid, endCid)

	// Even though the ad was malformed, processing it completed and indexer
	// can continue processing later ads in the chain. Check that the ad was
	// processed.
	pInfo, found = reg.ProviderInfo(providerID)
	require.True(t, found)
	require.Equal(t, adCid, pInfo.LastAdvertisement)
}

func TestReSyncWithDepth(t *testing.T) {
	te := setupTestEnv(t, false)
	adHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 2},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	te.publisher.SetRoot(adHead.(cidlink.Link).Cid)
	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	_, err := te.ingester.Sync(context.Background(), peerInfo, 1, false)
	require.NoError(t, err)
	allMHs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs[1:])
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), allMHs[0:1])

	// When not resync, check that nothing beyond the latest is synced.
	_, err = te.ingester.Sync(context.Background(), peerInfo, 0, false)
	require.NoError(t, err)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs[1:])
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), allMHs[0:1])

	// When resync with greater depth, check that everything in synced.
	_, err = te.ingester.Sync(context.Background(), peerInfo, 0, true)
	require.NoError(t, err)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs)
}

func TestSkipEarlierAdsIfAlreadyProcessedLaterAd(t *testing.T) {
	te := setupTestEnv(t, false)
	adHead := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 2},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 3},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)
	allAdLinks := typehelpers.AllAdLinks(t, adHead, te.publisherLinkSys)
	aLink := allAdLinks[0]
	bLink := allAdLinks[1]
	cLink := allAdLinks[2]
	allMHs := typehelpers.AllMultihashesFromAdLink(t, adHead, te.publisherLinkSys)
	ctx := context.Background()
	te.publisher.SetRoot(bLink.(cidlink.Link).Cid)
	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	_, err := te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs[0:2])
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), allMHs[2:])

	err = te.ingester.sub.SetLatestSync(te.pubHost.ID(), aLink.(cidlink.Link).Cid)
	require.NoError(t, err)
	te.publisher.SetRoot(cLink.(cidlink.Link).Cid)
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMHs)
}

func TestRecursionDepthLimitsEntriesSync(t *testing.T) {
	const entriesDepth = 10

	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost := dstest.MkTestHost(t)

	// make ingester with much smaller entries depth limit.
	ingCfg := defaultTestIngestConfig
	ingCfg.EntriesDepthLimit = entriesDepth
	ing, _ := mkIngestWithConfig(t, h, ingCfg)
	pub, lsys := mkMockPublisher(t, pubHost, h, srcStore)
	connectHosts(t, h, pubHost)

	totalChunkCount := entriesDepth * 2

	adCid, _, providerID, _ := publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, false, totalChunkCount, nil, cid.Undef)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	peerInfo := peer.AddrInfo{
		ID:    pub.ID(),
		Addrs: pub.Addrs(),
	}
	endCid, err := ing.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

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
	requireIndexedEventually(t, ing.indexer, providerID, mhs)

	lcid, err = ing.GetLatestSync(pubHost.ID())
	for err == nil && lcid == cid.Undef {
		// May not have marked ad as processed yet, retry.
		time.Sleep(time.Second)
		require.NoError(t, ctx.Err(), "sync timeout")
		lcid, err = ing.GetLatestSync(pubHost.ID())
	}

	require.NoError(t, err)
	require.Equal(t, adCid, lcid)

	entriesCid := getAdEntriesCid(t, srcStore, adCid)
	nextChunkCid := entriesCid
	for i := 0; i < totalChunkCount; i++ {
		mhs, nextChunkCid = decodeEntriesChunk(t, srcStore, nextChunkCid)
		// If chunk depth is within limit
		// Note that 1 is added to the entries depth because the entries sync process peaks the
		// first node in order to detect the type of entries, i.e. HAMT vs EntryChunk chain.
		// Therefore, the max entries traversal depth is 1 plus the configured max.
		if i < entriesDepth+1 {
			// Assert chunk multihashes are indexed
			requireIndexedEventually(t, ing.indexer, providerID, mhs)
		} else {
			// Otherwise, assert chunk multihashes are not indexed.
			requireNotIndexed(t, ing.indexer, providerID, mhs)
		}
	}

	// Assert no more chunks are left.
	require.Equal(t, cid.Undef, nextChunkCid)
}

func requireNotIndexed(t *testing.T, ix indexer.Interface, p peer.ID, mhs []multihash.Multihash, msgAndArgs ...interface{}) {
	t.Helper()
	for _, mh := range mhs {
		vs, exists, err := ix.Get(mh)
		require.NoError(t, err, "failed to get index for mh to check whether it is indexed")
		if exists {
			for _, v := range vs {
				require.NotEqual(t, p.String(), v.ProviderID.String(), msgAndArgs...)
			}
		}
	}
}

func getAdEntriesCid(t *testing.T, store datastore.Batching, ad cid.Cid) cid.Cid {
	ctx := context.TODO()
	val, err := store.Get(ctx, datastore.NewKey(ad.String()))
	require.NoError(t, err)
	nad, err := decodeIPLDNode(ad.Prefix().Codec, bytes.NewBuffer(val), schema.AdvertisementPrototype)
	require.NoError(t, err)
	adv, err := schema.UnwrapAdvertisement(nad)
	require.NoError(t, err)
	return adv.Entries.(cidlink.Link).Cid
}

func decodeEntriesChunk(t *testing.T, store datastore.Batching, c cid.Cid) ([]multihash.Multihash, cid.Cid) {
	ctx := context.TODO()
	val, err := store.Get(ctx, datastore.NewKey(c.String()))
	require.NoError(t, err)
	nentries, err := decodeIPLDNode(c.Prefix().Codec, bytes.NewBuffer(val), schema.EntryChunkPrototype)
	require.NoError(t, err)

	ec, err := schema.UnwrapEntryChunk(nentries)
	require.NoError(t, err)

	if ec.Next == nil {
		return ec.Entries, cid.Undef
	}

	return ec.Entries, ec.Next.(cidlink.Link).Cid
}

func TestMultiplePublishers(t *testing.T) {
	srcStore1 := dssync.MutexWrap(datastore.NewMapDatastore())
	srcStore2 := dssync.MutexWrap(datastore.NewMapDatastore())
	h := dstest.MkTestHost(t)
	pubHost1, pubHost1Priv := dstest.MkTestHostPK(t)
	pubHost2, pubHost2Priv := dstest.MkTestHostPK(t)

	i, _ := mkIngest(t, h)
	pub1, lsys1 := mkMockPublisher(t, pubHost1, h, srcStore1)
	pub2, lsys2 := mkMockPublisher(t, pubHost2, h, srcStore2)

	// connect both providers
	connectHosts(t, h, pubHost1)
	connectHosts(t, h, pubHost2)

	ctx := context.Background()

	// Test with two random advertisement publications for each of them.
	headAd1 := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 2},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 3},
		}}.Build(t, lsys1, pubHost1Priv)
	headAd1Cid := headAd1.(cidlink.Link).Cid

	pub1.SetRoot(headAd1Cid)
	mhs := typehelpers.AllMultihashesFromAdLink(t, headAd1, lsys1)
	peerInfo1 := peer.AddrInfo{
		ID:    pub1.ID(),
		Addrs: pub1.Addrs(),
	}
	gotC1, err := i.Sync(ctx, peerInfo1, 0, false)
	require.NoError(t, err)
	require.Equal(t, headAd1Cid, gotC1, "expected latest synced cid to match head of ad chain")

	requireIndexedEventually(t, i.indexer, pubHost1.ID(), mhs)

	headAd2 := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 100, Seed: 2},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 10, Seed: 3},
		}}.Build(t, lsys2, pubHost2Priv)
	headAd2Cid := headAd2.(cidlink.Link).Cid

	pub2.SetRoot(headAd2Cid)
	mhs = typehelpers.AllMultihashesFromAdLink(t, headAd2, lsys2)

	peerInfo2 := peer.AddrInfo{
		ID:    pub2.ID(),
		Addrs: pub2.Addrs(),
	}
	gotC2, err := i.Sync(ctx, peerInfo2, 0, false)
	require.NoError(t, err)
	require.Equal(t, headAd2Cid, gotC2, "expected latest synced cid to match head of ad chain")

	requireIndexedEventually(t, i.indexer, pubHost2.ID(), mhs)

	// Assert that the latest processed ad cid eventually matches the expected cid.
	require.Eventually(t, func() bool {
		gotLatestSync, err := i.GetLatestSync(pubHost1.ID())
		require.NoError(t, err)
		return headAd1Cid.Equals(gotLatestSync)
	}, testRetryTimeout, testRetryInterval, "Expected latest processed ad cid to be headAd1 for publisher 1.")
	require.Eventually(t, func() bool {
		gotLatestSync, err := i.GetLatestSync(pubHost2.ID())
		require.NoError(t, err)
		return headAd2Cid.Equals(gotLatestSync)
	}, testRetryTimeout, testRetryInterval, "Expected latest processed ad cid to be headAd2 for publisher 2.")

	// Assert that getting the latest synced from dagsync publisher matches the
	// latest processed.
	gotLink1 := i.sub.GetLatestSync(pubHost1.ID())
	require.Equal(t, gotLink1, headAd1)
	gotLink2 := i.sub.GetLatestSync(pubHost2.ID())
	require.Equal(t, gotLink2, headAd2)
}

func TestAnnounceIsDeferredWhenProcessingAd(t *testing.T) {
	t.Parallel()
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(nil)
	te := setupTestEnv(t, true, blockableLsysOpt)

	headLink := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 2},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 3},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 4},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)
	headCid := headLink.(cidlink.Link).Cid
	ads := typehelpers.AllAdLinks(t, headLink, te.publisherLinkSys)
	mhs := typehelpers.AllMultihashesFromAdLink(t, headLink, te.publisherLinkSys)
	pubAddrInfo := te.pubHost.Peerstore().PeerInfo(te.pubHost.ID())

	te.publisher.SetRoot(headCid)

	// Block syncing of head ad entries
	headAd := typehelpers.AdFromLink(t, headLink, te.publisherLinkSys)
	blockedReads.add(headAd.Entries.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}

	wait := make(chan cid.Cid, 1)
	go func() {
		syncCid, err := te.ingester.Sync(context.Background(), peerInfo, 0, false)
		require.NoError(t, err)
		wait <- syncCid
	}()

	// Assert that all multihashes except the head multihash are indexed eventually
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs[:3])
	// Asset that the head ad multihash is not indexed.
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), mhs[3:])

	require.Equal(t, 1, int(te.ingester.workersActive.Load()))

	// Announce an ad CID and assert that call to announce is deferred since
	// we have blocked the processing.
	ad2Cid := ads[2].(cidlink.Link).Cid
	// Blocked in sync handler.
	err := te.ingester.Announce(context.Background(), ad2Cid, pubAddrInfo)
	require.NoError(t, err)
	// Verify sync has not completed.
	require.Eventually(t, func() bool {
		te.ingester.syncInProgressMu.Lock()
		_, found := te.ingester.syncInProgress[pubAddrInfo.ID]
		te.ingester.syncInProgressMu.Unlock()
		return found
	}, testRetryTimeout, testRetryInterval)

	ad3Cid := ads[3].(cidlink.Link).Cid
	// Blocked waiting for previous async ad chain sync.
	err = te.ingester.Announce(context.Background(), ad3Cid, pubAddrInfo)
	require.NoError(t, err)

	// Unblock the processing and assert that everything is indexed.
	<-hitBlockedRead
	require.Equal(t, headCid, <-wait)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)

	require.Eventually(t, func() bool {
		te.ingester.syncInProgressMu.Lock()
		_, found := te.ingester.syncInProgress[te.pubHost.ID()]
		te.ingester.syncInProgressMu.Unlock()
		return !found
	}, testRetryTimeout, testRetryInterval, "Expected the pending announce to have been processed")
}

func TestAnnounceIsNotDeferredOnNoInProgressIngest(t *testing.T) {
	t.Parallel()
	te := setupTestEnv(t, true)
	headLink := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 10, EntriesPerChunk: 7, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 55, Seed: 2},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 4, EntriesPerChunk: 12, Seed: 3},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)
	headCid := headLink.(cidlink.Link).Cid
	mhs := typehelpers.AllMultihashesFromAdLink(t, headLink, te.publisherLinkSys)
	pubAddrInfo := te.pubHost.Peerstore().PeerInfo(te.pubHost.ID())

	// Announce the head ad CID.
	err := te.ingester.Announce(context.Background(), headCid, pubAddrInfo)
	require.NoError(t, err)

	// Assert that all multihashes in ad chain are indexed eventually, since Announce triggers
	// a background sync and should eventually process the ads.
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)

	require.Eventually(t, func() bool {
		te.ingester.syncInProgressMu.Lock()
		_, found := te.ingester.syncInProgress[te.pubHost.ID()]
		te.ingester.syncInProgressMu.Unlock()
		return !found
	}, testRetryTimeout, testRetryInterval)

	require.Eventually(t, func() bool {
		return te.ingester.workersActive.Load() == 0
	}, testRetryTimeout, testRetryInterval)
}

func TestAnnounceArrivedJustBeforeEntriesProcessingStartsDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(nil)
	te := setupTestEnv(t, true, blockableLsysOpt)
	headLink := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1}, // 0: A <- tail
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 2},                  // 1: B
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 3}, // 2: C
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 4}, // 3: D <- head
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)
	headCid := headLink.(cidlink.Link).Cid
	ads := typehelpers.AllAdLinks(t, headLink, te.publisherLinkSys)
	mhs := typehelpers.AllMultihashesFromAdLink(t, headLink, te.publisherLinkSys)
	pubAddrInfo := te.pubHost.Peerstore().PeerInfo(te.pubHost.ID())

	// Block Ad C which should block the sync triggered by publisher.SetRoot + announce.Send
	adCCid := ads[2].(cidlink.Link).Cid
	blockedReads.add(adCCid)

	// Publish announce message on publisher side, which should trigger a background sync that
	// gets blocked when attempting to sync ad C.
	p2pSender, err := p2psender.New(te.pubHost, defaultTestIngestConfig.PubSubTopic)
	require.NoError(t, err)
	defer p2pSender.Close()
	te.publisher.SetRoot(adCCid)
	err = announce.Send(context.Background(), adCCid, te.publisher.Addrs(), p2pSender)
	require.NoError(t, err)

	// Assert that there is no announce pending processing since no explicit announce was made to
	// storetheindex ingester.
	te.ingester.syncInProgressMu.Lock()
	_, found := te.ingester.syncInProgress[te.pubHost.ID()]
	te.ingester.syncInProgressMu.Unlock()
	require.False(t, found)

	// This sleep is required to make sure that the messages arrive to the blocking channel in the intended order
	time.Sleep(time.Second)

	// Block head ad which should block explicit Announce call made to the ingester.
	blockedReads.add(headCid)

	// Make an explicit announcement of head ad while ad sync triggered by
	// publisher.UpdateRoot is still blocked. Note that the background handling
	// of the announce should get blocked since headCid is also in the block
	// list.
	err = te.ingester.Announce(context.Background(), headCid, pubAddrInfo)
	require.NoError(t, err)

	// Unblock the sync triggered by publisher.UpdateRoot which should:
	// 1. cause the ad chain C->B->A to be downloaded.
	// 2. work to be assigned to the ingest worker which should not be processed until the
	//    blocked background sync started by the explicit announce is unblocked.
	<-hitBlockedRead

	// Assert that no multihashes are indexed since entries processing should be blocked.
	requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), mhs)

	// Now unblock the background sync started by the explicit announce, which casues the head
	// ad (D) to also be downloaded, and should eventually get indexed
	<-hitBlockedRead

	// Now that there is nothing blocked anymore, and the ingest has learn about all the ads,
	// i,e, C->B->A through publisher.UpdateRoot, and D through explicit announce, all the entries
	// should get indexed eventually.
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)
}

func TestGetEntryDataFromCar(t *testing.T) {
	tempDir := t.TempDir()
	cfgWithMirror := defaultTestIngestConfig
	cfgWithMirror.AdvertisementMirror.Read = true
	cfgWithMirror.AdvertisementMirror.Write = true
	cfgWithMirror.AdvertisementMirror.Storage.Type = "local"
	cfgWithMirror.AdvertisementMirror.Storage.Local.BasePath = tempDir
	cfgWithMirror.AdvertisementMirror.Retrieval.Type = "local"
	cfgWithMirror.AdvertisementMirror.Retrieval.Local.BasePath = tempDir

	te := setupTestEnv(t, true, func(optCfg *testEnvOpts) {
		optCfg.ingestConfig = &cfgWithMirror
	})

	cCid := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 5, EntriesPerChunk: 10, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 5, EntriesPerChunk: 10, Seed: 1},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 5, EntriesPerChunk: 10, Seed: 2},
		}}.Build(t, te.publisherLinkSys, te.publisherPriv)

	cAdNode, err := te.publisherLinkSys.Load(linking.LinkContext{}, cCid, schema.AdvertisementPrototype)
	require.NoError(t, err)
	cAd, err := schema.UnwrapAdvertisement(cAdNode)
	require.NoError(t, err)

	// Chain of 3 ads, A<-B<-C, here head is C.
	allAds := typehelpers.AllAds(t, cAd, te.publisherLinkSys)
	rootAd := allAds[len(allAds)-1]
	rootAd.Addresses = nil

	ctx := context.Background()
	te.publisher.SetRoot(cCid.(cidlink.Link).Cid)

	peerInfo := peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
	_, err = te.ingester.Sync(ctx, peerInfo, 0, false)
	require.NoError(t, err)

	allMhs := typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)

	dir, err := os.Open(tempDir)
	require.NoError(t, err)
	names, err := dir.Readdirnames(-1)
	dir.Close()
	require.NoError(t, err)
	var carCount, headCount int
	carSuffix := carstore.CarFileSuffix
	if cfgWithMirror.AdvertisementMirror.Compress == carstore.Gzip {
		carSuffix += carstore.GzipFileSuffix
	}
	for _, name := range names {
		if strings.HasSuffix(name, carSuffix) {
			carCount++
		} else if strings.HasSuffix(name, carstore.HeadFileSuffix) {
			headCount++
		}
	}
	require.Equal(t, 3, carCount)
	require.Equal(t, 1, headCount)

	// Read and read CAR file.
	adBlock, err := te.ingester.mirror.read(ctx, cCid.(cidlink.Link).Cid, false)
	require.NoError(t, err)
	require.NotZero(t, len(adBlock.Data))

	entriesCid := cAd.Entries.(cidlink.Link).Cid

	// Check that the data is a valid advertisement.
	adv, err := adBlock.Advertisement()
	require.NoError(t, err)
	require.Equal(t, entriesCid, adv.Entries.(cidlink.Link).Cid)

	// Check the first entries block looks correct.
	require.NotNil(t, adBlock.Entries)
	entBlock := <-adBlock.Entries
	require.NoError(t, entBlock.Err)
	require.Equal(t, entriesCid, entBlock.Cid)
	require.NotZero(t, len(entBlock.Data))

	// Check that the data is a valid EntryChunk.
	chunk, err := entBlock.EntryChunk()
	require.NoError(t, err)
	require.Equal(t, 10, len(chunk.Entries))

	// Check for expected number of entries blocks.
	count := 1
	for entBlock = range adBlock.Entries {
		require.NoError(t, entBlock.Err)
		require.NotZero(t, len(entBlock.Data))
		require.True(t, entBlock.Cid.Defined())
		count++
	}
	require.Equal(t, 5, count)

	// Do a resync and see that multihashes are pulled from CAR files.
	_, err = te.ingester.Sync(ctx, peerInfo, 0, true)
	require.NoError(t, err)

	allMhs = typehelpers.AllMultihashesFromAdChain(t, cAd, te.publisherLinkSys)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)

	require.Equal(t, 3*5*10, int(te.ingester.MultihashesFromMirror()))
}

// Make new indexer engine
func mkIndexer(t *testing.T, withCache bool) *engine.Engine {
	return engine.New(memory.New())
}

func mkRegistry(t *testing.T) *registry.Registry {
	discoveryCfg := config.Discovery{
		Policy: config.Policy{
			Allow:   true,
			Publish: true,
		},
	}
	reg, err := registry.New(context.Background(), discoveryCfg, nil)
	require.NoError(t, err)
	return reg
}

func mkProvLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}
func mkMockPublisher(t *testing.T, pubHost, testHost host.Host, store datastore.Batching) (dagsync.Publisher, ipld.LinkSystem) {
	lsys := mkProvLinkSystem(store)
	privKey := pubHost.Peerstore().PrivKey(pubHost.ID())
	pub, err := ipnisync.NewPublisher(lsys, privKey, ipnisync.WithStreamHost(pubHost), ipnisync.WithHeadTopic(defaultTestIngestConfig.PubSubTopic))
	require.NoError(t, err)
	t.Cleanup(func() {
		pub.Close()
	})
	return pub, lsys
}

func mkIngest(t *testing.T, h host.Host) (*Ingester, *registry.Registry) {
	return mkIngestWithConfig(t, h, defaultTestIngestConfig)
}

func mkIngestWithConfig(t *testing.T, h host.Host, cfg config.Ingest) (*Ingester, *registry.Registry) {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	tmpStore := dssync.MutexWrap(datastore.NewMapDatastore())
	reg := mkRegistry(t)
	core := mkIndexer(t, true)
	ing, err := NewIngester(cfg, h, core, reg, store, tmpStore)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, ing.Close())
		reg.Close()
		require.NoError(t, core.Close())
	})

	return ing, reg
}

func connectHosts(t *testing.T, srcHost, dstHost host.Host) {
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)
}

func newRandomLinkedList(t *testing.T, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash) {
	var out []multihash.Multihash
	var nextLnk ipld.Link
	for i := 0; i < size; i++ {
		mhs := test.RandomMultihashes(testEntriesChunkSize)
		chunk := &schema.EntryChunk{
			Entries: mhs,
			Next:    nextLnk,
		}
		node, err := chunk.ToNode()
		require.NoError(t, err)
		lnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
		out = append(out, mhs...)
		nextLnk = lnk
	}
	return nextLnk, out
}

func publishRandomIndexAndAdv(t *testing.T, pub dagsync.Publisher, lsys ipld.LinkSystem, fakeSig bool, metadata []byte, prevAdCid cid.Cid) (cid.Cid, []multihash.Multihash, peer.ID, crypto.PrivKey) {
	return publishRandomIndexAndAdvWithEntriesChunkCount(t, pub, lsys, fakeSig, testEntriesChunkCount, metadata, prevAdCid)
}

func publishRandomIndexAndAdvWithEntriesChunkCount(t *testing.T, pub dagsync.Publisher, lsys ipld.LinkSystem, fakeSig bool, eChunkCount int, metadata []byte, prevAdCid cid.Cid) (cid.Cid, []multihash.Multihash, peer.ID, crypto.PrivKey) {
	priv, pubKey, err := p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	p, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	ctxID := []byte("test-context-id")
	if metadata == nil {
		metadata = []byte("test-metadata")
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	adv := &schema.Advertisement{
		Provider:  p.String(),
		Addresses: addrs,
		ContextID: ctxID,
		Metadata:  metadata,
	}
	if prevAdCid.Defined() {
		adv.PreviousID = cidlink.Link{Cid: prevAdCid}
	}
	var mhs []multihash.Multihash
	if eChunkCount == 0 {
		adv.Entries = schema.NoEntries
	} else {
		adv.Entries, mhs = newRandomLinkedList(t, lsys, eChunkCount)
	}

	if !fakeSig {
		err := adv.Sign(priv)
		require.NoError(t, err)
	}
	node, err := adv.ToNode()
	require.NoError(t, err)
	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	pub.SetRoot(advLnk.(cidlink.Link).Cid)
	return advLnk.(cidlink.Link).Cid, mhs, p, priv
}

// publishAdvWithExtendedProviders generates an advertisement similarly to other helper functions howveer with extended providers added on top
func publishAdvWithExtendedProviders(t *testing.T,
	provider peer.ID,
	privKey crypto.PrivKey,
	pubKey crypto.PubKey,
	pub dagsync.Publisher,
	lsys ipld.LinkSystem,
	prevAdId cid.Cid,
	contextId string,
	metadata []byte,
	extProvOverride bool) (*schema.Advertisement, cid.Cid, []multihash.Multihash) {

	eChunkCount := rng.Int()%15 + 1
	extProvsNum := rng.Int()%5 + 1

	if metadata == nil {
		metadata = []byte("test-metadata")
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	mhsLnk, mhs := newRandomLinkedList(t, lsys, eChunkCount)

	adv := &schema.Advertisement{
		Provider:  provider.String(),
		Addresses: addrs,
		Entries:   mhsLnk,
		ContextID: []byte(contextId),
		Metadata:  metadata,
		ExtendedProvider: &schema.ExtendedProvider{
			Providers: []schema.Provider{},
			Override:  extProvOverride,
		},
	}

	if prevAdId != cid.Undef {
		adv.PreviousID = cidlink.Link{Cid: prevAdId}
	}

	// Generating extended providers
	epKeys := map[string]crypto.PrivKey{}
	for i := 0; i < extProvsNum; i++ {
		epPriv, epPub, err := p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
		require.NoError(t, err)
		epID, err := peer.IDFromPublicKey(epPub)
		require.NoError(t, err)

		epKeys[epID.String()] = epPriv
		adv.ExtendedProvider.Providers = append(adv.ExtendedProvider.Providers, schema.Provider{
			ID:        epID.String(),
			Addresses: test.RandomAddrs(1),
			Metadata:  []byte(fmt.Sprintf("test-metadata-%d", i)),
		})
	}

	// Appending the top level provider
	adv.ExtendedProvider.Providers = append(adv.ExtendedProvider.Providers, schema.Provider{
		ID:        provider.String(),
		Addresses: addrs,
		Metadata:  []byte{},
	})
	epKeys[provider.String()] = privKey

	err := adv.SignWithExtendedProviders(privKey, func(s string) (crypto.PrivKey, error) {
		if key, ok := epKeys[s]; ok {
			return key, nil
		}
		return nil, fmt.Errorf("pk not found")
	})
	require.NoError(t, err)

	node, err := adv.ToNode()
	require.NoError(t, err)
	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	pub.SetRoot(advLnk.(cidlink.Link).Cid)
	return adv, advLnk.(cidlink.Link).Cid, mhs
}

func publishAdWithEmptyExtendedProviders(t *testing.T,
	provider peer.ID,
	privKey crypto.PrivKey,
	pubKey crypto.PubKey,
	pub dagsync.Publisher,
	lsys ipld.LinkSystem,
	prevAdId cid.Cid,
	contextId string,
	metadata []byte) (*schema.Advertisement, cid.Cid, []multihash.Multihash) {

	eChunkCount := rng.Int()%15 + 1

	if metadata == nil {
		metadata = []byte("test-metadata")
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	mhsLnk, mhs := newRandomLinkedList(t, lsys, eChunkCount)

	adv := &schema.Advertisement{
		Provider:  provider.String(),
		Addresses: addrs,
		Entries:   mhsLnk,
		ContextID: []byte(contextId),
		Metadata:  metadata,
		ExtendedProvider: &schema.ExtendedProvider{
			Providers: []schema.Provider{},
			Override:  false,
		},
	}

	if prevAdId != cid.Undef {
		adv.PreviousID = cidlink.Link{Cid: prevAdId}
	}

	err := adv.SignWithExtendedProviders(privKey, func(s string) (crypto.PrivKey, error) {
		panic("should not be called")
	})
	require.NoError(t, err)

	node, err := adv.ToNode()
	require.NoError(t, err)
	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	pub.SetRoot(advLnk.(cidlink.Link).Cid)
	return adv, advLnk.(cidlink.Link).Cid, mhs
}

func publishRemovalAd(t *testing.T, pub dagsync.Publisher, lsys ipld.LinkSystem, fakeSig bool, providerID peer.ID, priv crypto.PrivKey) cid.Cid {
	ctxID := []byte("test-context-id")
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	adv := &schema.Advertisement{
		Provider:  providerID.String(),
		Addresses: addrs,
		Entries:   schema.NoEntries,
		ContextID: ctxID,
		Metadata:  nil,
		IsRm:      true,
	}
	if !fakeSig {
		err := adv.Sign(priv)
		require.NoError(t, err)
	}

	node, err := adv.ToNode()
	require.NoError(t, err)
	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)
	pub.SetRoot(advLnk.(cidlink.Link).Cid)
	return advLnk.(cidlink.Link).Cid
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

func requireIndexedEventually(t *testing.T, ix indexer.Interface, p peer.ID, mhs []multihash.Multihash) {
	t.Helper()
	require.Eventually(t, func() bool {
		return checkAllIndexed(ix, p, mhs) == nil
	}, testRetryTimeout, testRetryInterval, "Expected all multihashes from %s to have been indexed eventually", p.String())
}

func requireTrueEventually(t *testing.T, attempt func() bool, interval time.Duration, timeout time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	require.Eventually(t, attempt, timeout, interval, msgAndArgs...)
}

type testEnv struct {
	publisher        dagsync.Publisher
	pubHost          host.Host
	pubStore         datastore.Batching
	publisherPriv    crypto.PrivKey
	ingesterPriv     crypto.PrivKey
	publisherLinkSys ipld.LinkSystem
	ingester         *Ingester
	ingesterHost     host.Host
	reg              *registry.Registry
}

type testEnvOpts struct {
	publisherLinkSysFn func(ds datastore.Batching) ipld.LinkSystem
	ingestConfig       *config.Ingest
}

func setupTestEnv(t *testing.T, shouldConnectHosts bool, opts ...func(*testEnvOpts)) *testEnv {
	testOpt := &testEnvOpts{}
	for _, f := range opts {
		f(testOpt)
	}

	if testOpt.ingestConfig == nil {
		testOpt.ingestConfig = &defaultTestIngestConfig
	}

	srcStore := dssync.MutexWrap(datastore.NewMapDatastore())

	ingesterHost, ingesterPriv := dstest.MkTestHostPK(t)
	t.Cleanup(func() {
		ingesterHost.Network().ResourceManager().Close()
	})
	pubHost, priv := dstest.MkTestHostPK(t)
	t.Cleanup(func() {
		pubHost.Network().ResourceManager().Close()
	})

	i, reg := mkIngestWithConfig(t, ingesterHost, *testOpt.ingestConfig)

	var lsys ipld.LinkSystem
	if testOpt.publisherLinkSysFn != nil {
		lsys = testOpt.publisherLinkSysFn(srcStore)
	} else {
		lsys = mkProvLinkSystem(srcStore)
	}

	pub, err := ipnisync.NewPublisher(lsys, priv, ipnisync.WithStreamHost(pubHost), ipnisync.WithHeadTopic(defaultTestIngestConfig.PubSubTopic))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pub.Close())
	})

	if shouldConnectHosts {
		connectHosts(t, ingesterHost, pubHost)
	}

	te := &testEnv{
		publisher:        pub,
		publisherPriv:    priv,
		pubHost:          pubHost,
		pubStore:         srcStore,
		publisherLinkSys: lsys,
		ingester:         i,
		ingesterHost:     ingesterHost,
		ingesterPriv:     ingesterPriv,
		reg:              reg,
	}

	return te
}

type errorValueStore struct {
	err error
}

func newErrorValueStore() *errorValueStore {
	return &errorValueStore{
		err: errors.New("dead value store"),
	}
}
func (vs *errorValueStore) Get(_ multihash.Multihash) ([]indexer.Value, bool, error) {
	return nil, false, vs.err
}
func (vs *errorValueStore) Put(_ indexer.Value, _ ...multihash.Multihash) error    { return vs.err }
func (vs *errorValueStore) Remove(_ indexer.Value, _ ...multihash.Multihash) error { return vs.err }
func (vs *errorValueStore) RemoveProvider(_ context.Context, _ peer.ID) error      { return vs.err }
func (vs *errorValueStore) RemoveProviderContext(_ peer.ID, _ []byte) error        { return vs.err }
func (vs *errorValueStore) Size() (int64, error)                                   { return 0, vs.err }
func (vs *errorValueStore) Flush() error                                           { return vs.err }
func (vs *errorValueStore) Close() error                                           { return vs.err }
func (vs *errorValueStore) Iter() (indexer.Iterator, error)                        { return nil, vs.err }
func (vs *errorValueStore) Stats() (*indexer.Stats, error)                         { return nil, vs.err }
