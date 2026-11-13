package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/dagsync"
	schema "github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/internal/registry"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	p2ptest "github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// mixedAd describes one advertisement of a chain built by buildMixedChain.
type mixedAd struct {
	// key signs the ad, and its peer ID is the ad's provider.
	key       crypto.PrivKey
	contextID string
	// rm makes the ad a removal of its context, with no entries.
	rm bool
}

// builtAd is an advertisement stored by buildMixedChain.
type builtAd struct {
	cid      cid.Cid
	ad       *schema.Advertisement
	provider peer.ID
	// mhs are the multihashes in the ad's entries.
	mhs []multihash.Multihash
}

// buildMixedChain stores a chain of advertisements, oldest first, each signed
// by its own provider key, and returns them in the same order.
func buildMixedChain(t *testing.T, lsys ipld.LinkSystem, ads []mixedAd) []builtAd {
	built := make([]builtAd, 0, len(ads))
	var prev ipld.Link
	for _, ma := range ads {
		provider, err := peer.IDFromPrivateKey(ma.key)
		require.NoError(t, err)
		ad := schema.Advertisement{
			PreviousID: prev,
			Provider:   provider.String(),
			Addresses:  []string{"/ip4/127.0.0.1/tcp/9999"},
			Entries:    schema.NoEntries,
			ContextID:  []byte(ma.contextID),
			Metadata:   []byte("test-metadata"),
			IsRm:       ma.rm,
		}
		var mhs []multihash.Multihash
		if !ma.rm {
			mhs = random.Multihashes(5)
			chunk, err := schema.EntryChunk{Entries: mhs}.ToNode()
			require.NoError(t, err)
			ad.Entries, err = lsys.Store(ipld.LinkContext{}, schema.Linkproto, chunk)
			require.NoError(t, err)
		}
		require.NoError(t, ad.Sign(ma.key))
		node, err := ad.ToNode()
		require.NoError(t, err)
		prev, err = lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
		built = append(built, builtAd{cid: prev.(cidlink.Link).Cid, ad: &ad, provider: provider, mhs: mhs})
	}
	return built
}

// drainBlockedReads consumes the notifications of blocked reads until the test
// ends, so a blocked read never waits on the test.
func drainBlockedReads(t *testing.T, hit <-chan cid.Cid) {
	go func() {
		for {
			select {
			case <-hit:
			case <-t.Context().Done():
				return
			}
		}
	}()
}

func randomPrivKey(t *testing.T) crypto.PrivKey {
	priv, _, err := p2ptest.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	return priv
}

func publisherAddrInfo(te *testEnv) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    te.publisher.ID(),
		Addrs: te.publisher.Addrs(),
	}
}

// requireAdIndexedEventually waits until every multihash of the ad is indexed
// for its provider.
func requireAdIndexedEventually(t *testing.T, te *testEnv, ba builtAd) {
	t.Helper()
	require.NotEmpty(t, ba.mhs)
	requireIndexedEventually(t, te.ingester.indexer, ba.provider, ba.mhs)
}

// A removal by one provider must not skip another provider's ad for the same
// context ID, since a removal deletes only its own provider's context.
func TestMixedChainRemovalSkipsOnlyOwnProvider(t *testing.T) {
	te := setupTestEnv(t, true)

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "shared"},
		{key: randomPrivKey(t), contextID: "shared", rm: true},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	c, err := te.ingester.Sync(t.Context(), publisherAddrInfo(te), 0, false, cid.Undef)
	require.NoError(t, err)
	require.Equal(t, head, c)
	requireAdIndexedEventually(t, te, chain[0])
}

// A removal skips the older ad of its own provider for the same context ID:
// the ad is marked processed without its entries being fetched. Another
// provider's ad for that context ID is still indexed.
func TestMixedChainRemovalSkipsOwnOlderAd(t *testing.T) {
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(failBlockedRead)
	// Count the reads of blocked blocks. The publisher sends each one on an
	// unbuffered channel, so keep receiving until the test environment is
	// closed: this cleanup is registered first, so it runs last.
	var blockedReadCount atomic.Int32
	stopCounting := make(chan struct{})
	t.Cleanup(func() { close(stopCounting) })
	go func() {
		for {
			select {
			case <-hitBlockedRead:
				blockedReadCount.Add(1)
			case <-stopCounting:
				return
			}
		}
	}()
	te := setupTestEnv(t, true, blockableLsysOpt)

	keyB := randomPrivKey(t)
	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "shared"},
		{key: keyB, contextID: "shared"},
		{key: keyB, contextID: "shared", rm: true},
	})
	// Fetching the entries of B's older ad, whose context the removal
	// deletes, fails with a transient error, so the sync fails unless the ad
	// is skipped.
	blockedReads.add(chain[1].ad.Entries.(cidlink.Link).Cid)
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	c, err := te.ingester.Sync(t.Context(), publisherAddrInfo(te), 0, false, cid.Undef)
	require.NoError(t, err)
	require.Equal(t, head, c)
	requireAdIndexedEventually(t, te, chain[0])
	state, err := te.ingester.GetAdState(t.Context(), chain[1].cid)
	require.NoError(t, err)
	require.True(t, state.Processed)
	require.False(t, state.Skipped)
	require.Zero(t, blockedReadCount.Load())
}

func requireNoneProcessed(t *testing.T, ing *Ingester, ads []builtAd) {
	t.Helper()
	for i, ba := range ads {
		state, err := ing.GetAdState(t.Context(), ba.cid)
		require.NoError(t, err)
		require.False(t, state.Processed, "ad %d of the chain is marked processed", i)
	}
}

// requireChainIngestedAfterNewAd publishes an ad on top of a chain that failed,
// syncs it, and requires the whole chain to be ingested. The sync fetches only
// the new ad, and ingestion walks from it through the older ads, which are not
// processed.
func requireChainIngestedAfterNewAd(t *testing.T, te *testEnv, head cid.Cid, chain []builtAd) {
	t.Helper()
	newHead, mhs, provider, _ := publishRandomIndexAndAdv(t, te.publisher, te.publisherLinkSys, false, nil, head)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.NoError(t, err)
	require.Equal(t, newHead, c)
	for _, ba := range chain {
		requireAdIndexedEventually(t, te, ba)
	}
	requireIndexedEventually(t, te.ingester.indexer, provider, mhs)
}

// A transient error on the oldest ad of one provider must stop the whole chain,
// so that no newer ad of another provider is marked processed in front of it.
func TestMixedChainStopsAtTransientError(t *testing.T) {
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(failBlockedRead)
	te := setupTestEnv(t, true, blockableLsysOpt)
	drainBlockedReads(t, hitBlockedRead)

	provB := randomPrivKey(t)
	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: provB, contextID: "b1"},
		{key: te.publisherPriv, contextID: "a2"},
		{key: provB, contextID: "b2"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)
	blockedReads.add(chain[0].ad.Entries.(cidlink.Link).Cid)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	requireNoneProcessed(t, te.ingester, chain)

	blockedReads.rm(chain[0].ad.Entries.(cidlink.Link).Cid)
	requireChainIngestedAfterNewAd(t, te, head, chain)
}

// A chain stops at the first ad of a provider that another chain is ingesting
// for longer than the chain waits, and reports the failure to Sync and as the
// last error of the chain's providers. The ads before that run stay processed,
// and the chain holds no provider after it stops.
func TestMixedChainBusyProviderFailsSync(t *testing.T) {
	te := setupTestEnv(t, true)
	// The wait itself is tested in a synctest bubble by TestProviderClaimer*.
	// Here the chain gives up at once.
	te.ingester.providerBusyWait = 0

	provB := randomPrivKey(t)
	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: provB, contextID: "b1"},
		{key: te.publisherPriv, contextID: "a2"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	// Another chain holds provider B.
	otherChain, releaseOther := te.ingester.providerClaimer(t.Context())
	require.NoError(t, otherChain(chain[1].provider))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorContains(t, err, "busy")
	state, err := te.ingester.GetAdState(t.Context(), chain[0].cid)
	require.NoError(t, err)
	require.True(t, state.Processed)
	requireNoneProcessed(t, te.ingester, chain[1:])
	// Provider A is registered by its ingested ad. Provider B is not
	// registered yet, so its last error is not recorded.
	info, ok := te.reg.ProviderInfo(chain[0].provider)
	require.True(t, ok)
	require.Contains(t, info.LastError, "busy")

	// The chain released provider A when it reached B's run.
	check, releaseCheck := te.ingester.providerClaimer(t.Context())
	require.NoError(t, check(chain[0].provider))
	releaseCheck()

	releaseOther()
	requireChainIngestedAfterNewAd(t, te, head, chain)
}

// failingDatastore replaces the result of Get for one key, after letting a
// number of reads of the key through. By default it fails with an error that
// is not datastore.ErrNotFound.
type failingDatastore struct {
	datastore.Batching
	mu       sync.Mutex
	failGet  datastore.Key
	passGets int
	getVal   []byte
	getErr   error
}

func (ds *failingDatastore) setFailGet(k datastore.Key) {
	ds.setGetResult(k, nil, errors.New("injected datastore read error"))
}

func (ds *failingDatastore) setGetResult(k datastore.Key, val []byte, err error) {
	ds.setGetResultAfter(k, 0, val, err)
}

// setGetResultAfter lets the next passGets reads of k through, and replaces
// the result of the reads after them.
func (ds *failingDatastore) setGetResultAfter(k datastore.Key, passGets int, val []byte, err error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.failGet = k
	ds.passGets = passGets
	ds.getVal = val
	ds.getErr = err
}

func (ds *failingDatastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	ds.mu.Lock()
	fail := key == ds.failGet
	if fail && ds.passGets > 0 {
		ds.passGets--
		fail = false
	}
	val, err := ds.getVal, ds.getErr
	ds.mu.Unlock()
	if fail {
		return val, err
	}
	return ds.Batching.Get(ctx, key)
}

// runWorkersByHand stops the ingester from taking sync events from dagsync and
// returns the queue its single worker reads them from instead.
func runWorkersByHand(te *testEnv) chan<- dagsync.SyncFinished {
	te.ingester.cancelOnSyncFinished()
	te.ingester.cancelOnSyncFinished = func() {}
	chainQueue := make(chan dagsync.SyncFinished, 1)
	te.ingester.syncFinishedEvents = chainQueue
	te.ingester.workerPoolSize = 0
	te.ingester.RunWorkers(1)
	return chainQueue
}

// syncChain fetches the publisher's chain with dagsync only, and returns the
// event that would start its ingestion.
func syncChain(t *testing.T, te *testEnv) dagsync.SyncFinished {
	syncFinishedCh, cncl := te.ingester.sub.OnSyncFinished()
	defer cncl()
	_, err := te.ingester.sub.SyncAdChain(t.Context(), publisherAddrInfo(te))
	require.NoError(t, err)
	return <-syncFinishedCh
}

// waitChainEvent returns the terminal event of the chain with the given head:
// an error event for that head, or the event for the head ad itself.
func waitChainEvent(t *testing.T, events <-chan adProcessedEvent, head cid.Cid) adProcessedEvent {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case event := <-events:
			if event.headAdCid != head {
				continue
			}
			if event.err != nil || event.adCid == head {
				return event
			}
		case <-timeout:
			require.FailNow(t, "no terminal event for the chain")
		}
	}
}

// An ad that the walk loaded but that cannot be loaded again when it is
// ingested must stop the chain there, whatever the error, and not be marked
// processed. Once the ad can be read, the chain is ingested.
func TestChainStopsAtAdLoadErrorWhenIngesting(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "read error", err: errors.New("injected datastore read error")},
		{name: "missing", err: datastore.ErrNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := setupTestEnv(t, true)
			chainQueue := runWorkersByHand(te)
			failDS := &failingDatastore{Batching: te.ingester.dsTmp}
			te.ingester.dsTmp = failDS

			chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
				{key: te.publisherPriv, contextID: "a1"},
				{key: te.publisherPriv, contextID: "a2"},
			})
			head := chain[len(chain)-1].cid
			te.publisher.SetRoot(head)

			syncFinished := syncChain(t, te)
			events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
			defer cnclEvents()

			// The walk reads the oldest ad once; its ingestion reads it again.
			failDS.setGetResultAfter(datastore.NewKey(chain[0].cid.String()), 1, nil, tc.err)
			chainQueue <- syncFinished
			event := waitChainEvent(t, events, head)
			require.ErrorContains(t, event.err, "cannot load advertisement")
			require.Equal(t, chain[0].cid, event.adCid)
			requireNoneProcessed(t, te.ingester, chain)

			failDS.setGetResult(datastore.Key{}, nil, nil)
			chainQueue <- syncFinished
			require.NoError(t, waitChainEvent(t, events, head).err)
			for _, ba := range chain {
				requireAdIndexedEventually(t, te, ba)
			}
		})
	}
}

// A datastore read error in the middle of the walk must stop the chain before
// any ad is ingested. Ingesting the newer ads would mark them processed in front
// of the older ads that were never read, and later syncs would stop there.
func TestChainStopsAtDatastoreReadError(t *testing.T) {
	te := setupTestEnv(t, true)

	// Run the ingest loop by hand, to inject the read error after the sync.
	chainQueue := runWorkersByHand(te)
	failDS := &failingDatastore{Batching: te.ingester.dsTmp}
	te.ingester.dsTmp = failDS

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a2"},
		{key: te.publisherPriv, contextID: "a3"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	syncFinished := syncChain(t, te)
	events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
	defer cnclEvents()

	failDS.setFailGet(datastore.NewKey(chain[1].cid.String()))
	chainQueue <- syncFinished
	event := waitChainEvent(t, events, head)
	require.Error(t, event.err)
	require.Equal(t, chain[1].cid, event.adCid)
	requireNoneProcessed(t, te.ingester, chain)

	failDS.setFailGet(datastore.Key{})
	chainQueue <- syncFinished
	event = waitChainEvent(t, events, head)
	require.NoError(t, event.err)
	for _, ba := range chain {
		requireAdIndexedEventually(t, te, ba)
	}
}

// An ad that is not in the datastore ends the walk, as at the end of a
// depth-limited sync, and the newer ads collected before it are ingested. The
// ad and the older ones are not.
func TestChainWalkEndsAtMissingAd(t *testing.T) {
	te := setupTestEnv(t, true)
	chainQueue := runWorkersByHand(te)
	failDS := &failingDatastore{Batching: te.ingester.dsTmp}
	te.ingester.dsTmp = failDS

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a2"},
		{key: te.publisherPriv, contextID: "a3"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	syncFinished := syncChain(t, te)
	events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
	defer cnclEvents()

	failDS.setGetResult(datastore.NewKey(chain[1].cid.String()), nil, datastore.ErrNotFound)
	chainQueue <- syncFinished
	require.NoError(t, waitChainEvent(t, events, head).err)
	requireAdIndexedEventually(t, te, chain[2])
	requireNoneProcessed(t, te.ingester, chain[:2])
}

// An ad whose block in the temporary datastore no longer matches its CID,
// whether it still decodes or not, fails the chain without ingesting anything
// and is deleted. The next sync fetches it from the publisher again, and the
// chain is ingested.
func TestChainRefetchesCorruptAd(t *testing.T) {
	for _, tc := range []struct {
		name string
		// corrupt returns the bytes stored in place of the ad of chain[1].
		corrupt func(t *testing.T, te *testEnv, chain []builtAd) []byte
	}{
		{
			name: "decodes",
			corrupt: func(t *testing.T, te *testEnv, chain []builtAd) []byte {
				// A valid ad, but another one.
				val, err := te.ingester.dsTmp.Get(t.Context(), datastore.NewKey(chain[0].cid.String()))
				require.NoError(t, err)
				return val
			},
		},
		{
			name: "does not decode",
			corrupt: func(*testing.T, *testEnv, []builtAd) []byte {
				return []byte("not an advertisement")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := setupTestEnv(t, true)
			chainQueue := runWorkersByHand(te)

			chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
				{key: te.publisherPriv, contextID: "a1"},
				{key: te.publisherPriv, contextID: "a2"},
				{key: te.publisherPriv, contextID: "a3"},
			})
			head := chain[len(chain)-1].cid
			te.publisher.SetRoot(head)

			syncFinished := syncChain(t, te)
			events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
			defer cnclEvents()

			key := datastore.NewKey(chain[1].cid.String())
			require.NoError(t, te.ingester.dsTmp.Put(t.Context(), key, tc.corrupt(t, te, chain)))
			chainQueue <- syncFinished
			event := waitChainEvent(t, events, head)
			require.ErrorIs(t, event.err, errCorruptBlock)
			require.Equal(t, chain[1].cid, event.adCid)
			requireNoneProcessed(t, te.ingester, chain)
			has, err := te.ingester.dsTmp.Has(t.Context(), key)
			require.NoError(t, err)
			require.False(t, has)

			// The worker set the stop node back, so this sync fetches the
			// deleted ad again.
			chainQueue <- syncChain(t, te)
			require.NoError(t, waitChainEvent(t, events, head).err)
			for _, ba := range chain {
				requireAdIndexedEventually(t, te, ba)
			}
		})
	}
}

// A failed read of an ad's processed state during the walk must stop the chain
// before any ad is ingested.
func TestChainStopsAtMarkerReadError(t *testing.T) {
	te := setupTestEnv(t, true)
	chainQueue := runWorkersByHand(te)
	failDS := &failingDatastore{Batching: te.ingester.ds}
	te.ingester.ds = failDS

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a2"},
		{key: te.publisherPriv, contextID: "a3"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	syncFinished := syncChain(t, te)
	events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
	defer cnclEvents()

	failDS.setFailGet(datastore.NewKey(adProcessedPrefix + chain[1].cid.String()))
	chainQueue <- syncFinished
	event := waitChainEvent(t, events, head)
	require.Error(t, event.err)
	require.Equal(t, chain[1].cid, event.adCid)
	failDS.setFailGet(datastore.Key{})
	requireNoneProcessed(t, te.ingester, chain)
}

// A failed write of an ad's processed marker must stop the chain, so that no
// newer ad is marked processed in front of the unmarked one.
func TestChainStopsAtMarkerWriteError(t *testing.T) {
	te := setupTestEnv(t, true)

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a2"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	// Fail only the processed marker of the oldest ad. Its unprocessed marker,
	// written during the walk, goes through.
	markerKey := datastore.NewKey(adProcessedPrefix + chain[0].cid.String())
	te.ingester.ds = &markerWriteFailer{Batching: te.ingester.ds, key: markerKey, marker: adMarkerProcessed}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorContains(t, err, "cannot mark advertisement processed")
	requireNoneProcessed(t, te.ingester, chain)
}

// markerWriteFailer fails a Put of one key when the value is the given ad
// marker, and lets every other write through.
type markerWriteFailer struct {
	datastore.Batching
	mu     sync.Mutex
	key    datastore.Key
	marker byte
}

func (ds *markerWriteFailer) setKey(k datastore.Key) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.key = k
}

func (ds *markerWriteFailer) Put(ctx context.Context, key datastore.Key, value []byte) error {
	ds.mu.Lock()
	fail := key == ds.key && len(value) != 0 && value[0] == ds.marker
	ds.mu.Unlock()
	if fail {
		return errors.New("injected marker write error")
	}
	return ds.Batching.Put(ctx, key, value)
}

// A chain that stops on an error records it as the last error of each
// registered provider with an ad the chain did not ingest, so the poller syncs
// the publisher again even after recent contact. A provider whose ads were
// ingested before the failure keeps no error. When none of those providers is
// registered, a registered provider of the publisher gets the error. A later
// successful ad clears its provider's error.
func TestChainErrorSetsProviderLastError(t *testing.T) {
	const (
		provA = 0 // the publisher's own provider
		provB = 1
	)
	for _, tc := range []struct {
		name string
		// marker fails the processed marker write of the ad at failAt, and
		// otherwise the walk's read of it fails.
		marker bool
		failAt int
		// wantErr has the error after the failure, wantClean has none.
		wantErr   int
		wantClean int
	}{
		{name: "marker write error at B1", marker: true, failAt: 2, wantErr: provB, wantClean: provA},
		{name: "marker write error at A1", marker: true, failAt: 1, wantErr: provA, wantClean: -1},
		// B has no ingested ad yet, so is not registered, and the error goes
		// to the publisher's provider.
		{name: "walk read error at B1", failAt: 2, wantErr: provA, wantClean: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := setupTestEnv(t, true)
			chainQueue := runWorkersByHand(te)
			// Install both failers, inactive, before any chain runs.
			readFailer := &failingDatastore{Batching: te.ingester.dsTmp}
			markerFailer := &markerWriteFailer{Batching: te.ingester.ds, marker: adMarkerProcessed}
			te.ingester.dsTmp = readFailer
			te.ingester.ds = markerFailer
			events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
			defer cnclEvents()

			keyB := randomPrivKey(t)
			chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
				{key: te.publisherPriv, contextID: "a0"},
				{key: te.publisherPriv, contextID: "a1"},
				{key: keyB, contextID: "b1"},
				{key: keyB, contextID: "b2"},
			})
			providers := []peer.ID{chain[0].provider, chain[2].provider}
			lastError := func(p int) (string, bool) {
				info, ok := te.reg.ProviderInfo(providers[p])
				if !ok {
					return "", false
				}
				return info.LastError, true
			}

			// Ingest the first ad, so that provider A is registered.
			te.publisher.SetRoot(chain[0].cid)
			chainQueue <- syncChain(t, te)
			require.NoError(t, waitChainEvent(t, events, chain[0].cid).err)

			head := chain[len(chain)-1].cid
			te.publisher.SetRoot(head)
			syncFinished := syncChain(t, te)

			failed := chain[tc.failAt].cid
			if tc.marker {
				markerFailer.setKey(datastore.NewKey(adProcessedPrefix + failed.String()))
			} else {
				readFailer.setFailGet(datastore.NewKey(failed.String()))
			}
			chainQueue <- syncFinished
			require.Error(t, waitChainEvent(t, events, head).err)
			requireNoneProcessed(t, te.ingester, chain[tc.failAt:])

			// The last error names the failed ad, without the details of the
			// internal error.
			lastErr, ok := lastError(tc.wantErr)
			require.True(t, ok)
			require.Equal(t, adLastError(failed, errInternal).Error(), lastErr)
			if tc.wantClean >= 0 {
				lastErr, ok = lastError(tc.wantClean)
				require.True(t, ok)
				require.Empty(t, lastErr)
			}

			markerFailer.setKey(datastore.Key{})
			readFailer.setFailGet(datastore.Key{})
			chainQueue <- syncFinished
			require.NoError(t, waitChainEvent(t, events, head).err)
			// B's ads were ingested, so its error, if any, is cleared.
			lastErr, ok = lastError(provB)
			require.True(t, ok)
			require.Empty(t, lastErr)
		})
	}
}

// A failed write of the marker of an ad skipped because a later ad removes its
// context must stop the chain like any other marker write error.
func TestChainStopsAtRemovedContextMarkerWriteError(t *testing.T) {
	te := setupTestEnv(t, true)

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a1", rm: true},
	})
	te.publisher.SetRoot(chain[len(chain)-1].cid)

	markerKey := datastore.NewKey(adProcessedPrefix + chain[0].cid.String())
	te.ingester.ds = &markerWriteFailer{Batching: te.ingester.ds, key: markerKey, marker: adMarkerProcessed}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorContains(t, err, "cannot mark advertisement processed")
	requireNoneProcessed(t, te.ingester, chain)
}

// A failed write of the marker of an ad skipped for a permanent error must stop
// the chain like any other marker write error.
func TestChainStopsAtSkippedMarkerWriteError(t *testing.T) {
	// A read error from the publisher's link system fails the first entry
	// with a 500, which skips the ad permanently when skip500EntsErr is set.
	blockableLsysOpt, blockedReads, hitBlockedRead := blockableLinkSys(func(datamodel.Link, func(linking.LinkContext, datamodel.Link) (io.Reader, error)) (io.Reader, error) {
		return nil, errors.New("entries not found")
	})
	te := setupTestEnv(t, true, blockableLsysOpt)
	drainBlockedReads(t, hitBlockedRead)
	te.ingester.skip500EntsErr.Store(true)

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a2"},
	})
	te.publisher.SetRoot(chain[len(chain)-1].cid)
	blockedReads.add(chain[0].ad.Entries.(cidlink.Link).Cid)

	markerKey := datastore.NewKey(adProcessedPrefix + chain[0].cid.String())
	te.ingester.ds = &markerWriteFailer{Batching: te.ingester.ds, key: markerKey, marker: adMarkerSkipped}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorContains(t, err, "cannot mark advertisement skipped")
	requireNoneProcessed(t, te.ingester, chain)
}

// A chain with nothing to ingest still sends its terminal event, so a waiting
// Sync returns. Each case runs in its own environment, so the terminal event is
// the only event the chain can send.
func TestTerminalEventForChainWithNothingToIngest(t *testing.T) {
	for _, tc := range []struct {
		name string
		// prepare changes what the chain finds, and returns the event that
		// starts its ingestion.
		prepare func(t *testing.T, te *testEnv, syncFinished dagsync.SyncFinished, head cid.Cid) dagsync.SyncFinished
	}{
		{
			name: "head already processed",
			prepare: func(t *testing.T, te *testEnv, syncFinished dagsync.SyncFinished, head cid.Cid) dagsync.SyncFinished {
				require.NoError(t, te.ingester.MarkAdProcessed(te.pubHost.ID(), head))
				return syncFinished
			},
		},
		{
			name: "nothing fetched",
			prepare: func(_ *testing.T, _ *testEnv, syncFinished dagsync.SyncFinished, _ cid.Cid) dagsync.SyncFinished {
				syncFinished.Count = 0
				return syncFinished
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := setupTestEnv(t, true)
			chainQueue := runWorkersByHand(te)

			chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
				{key: te.publisherPriv, contextID: "a1"},
			})
			head := chain[0].cid
			te.publisher.SetRoot(head)

			syncFinished := tc.prepare(t, te, syncChain(t, te), head)
			events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
			defer cnclEvents()

			chainQueue <- syncFinished
			require.NoError(t, waitChainEvent(t, events, head).err)
		})
	}
}

func newClaimTestIngester() *Ingester {
	return &Ingester{
		providersBusy:    make(map[peer.ID]chan struct{}),
		providerBusyWait: time.Minute,
	}
}

func busyProviders(ing *Ingester) []peer.ID {
	ing.providersBusyMu.Lock()
	defer ing.providersBusyMu.Unlock()
	return slices.Sorted(maps.Keys(ing.providersBusy))
}

func TestProviderClaimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ing := newClaimTestIngester()
		a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
		claim, release := ing.providerClaimer(t.Context())
		other, releaseOther := ing.providerClaimer(t.Context())

		require.NoError(t, claim(a))
		require.NoError(t, claim(a))
		require.Equal(t, []peer.ID{a}, busyProviders(ing))
		require.ErrorIs(t, other(a), errProviderBusy)

		// Moving to another provider releases the one held.
		require.NoError(t, claim(b))
		require.Equal(t, []peer.ID{b}, busyProviders(ing))
		require.NoError(t, other(a))
		require.Equal(t, []peer.ID{a, b}, busyProviders(ing))

		// A busy provider fails the claim after the wait, and the one held is
		// released before the wait.
		require.NoError(t, other(c))
		require.ErrorIs(t, claim(c), errProviderBusy)
		require.Equal(t, []peer.ID{c}, busyProviders(ing))

		// After a failed claim, nothing is held, so release releases nothing.
		release()
		require.Equal(t, []peer.ID{c}, busyProviders(ing))

		require.NoError(t, claim(a))
		release()
		require.Equal(t, []peer.ID{c}, busyProviders(ing))

		releaseOther()
		require.Empty(t, busyProviders(ing))
	})
}

// A claim of a busy provider waits until the provider is released, holding no
// claim while it waits, and then claims it.
func TestProviderClaimerWaitsForRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ing := newClaimTestIngester()
		a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
		other, releaseOther := ing.providerClaimer(t.Context())
		require.NoError(t, other(b))
		third, releaseThird := ing.providerClaimer(t.Context())
		require.NoError(t, third(c))

		claim, release := ing.providerClaimer(t.Context())
		require.NoError(t, claim(a))
		done := make(chan error, 1)
		go func() {
			done <- claim(b)
		}()
		synctest.Wait()
		require.Empty(t, done)
		require.Equal(t, []peer.ID{b, c}, busyProviders(ing), "a released while waiting")

		// Another provider released does not end the wait.
		releaseThird()
		synctest.Wait()
		require.Empty(t, done)

		start := time.Now()
		time.Sleep(time.Second)
		releaseOther()
		require.NoError(t, <-done)
		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, []peer.ID{b}, busyProviders(ing))
		release()
	})
}

// A claim of a busy provider gives up after providerBusyWait, or when its
// context ends.
func TestProviderClaimerGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ing := newClaimTestIngester()
		a := peer.ID("a")
		other, _ := ing.providerClaimer(t.Context())
		require.NoError(t, other(a))

		start := time.Now()
		claim, _ := ing.providerClaimer(t.Context())
		require.ErrorIs(t, claim(a), errProviderBusy)
		require.Equal(t, ing.providerBusyWait, time.Since(start))

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start = time.Now()
		claim, _ = ing.providerClaimer(ctx)
		require.ErrorIs(t, claim(a), context.DeadlineExceeded)
		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, []peer.ID{a}, busyProviders(ing))
	})
}

// freezeAfterPut freezes the indexer right after the multihashes of one ad are
// put into the value store.
type freezeAfterPut struct {
	indexer.Interface
	reg     *registry.Registry
	trigger multihash.Multihash
}

func (ix *freezeAfterPut) Put(value indexer.Value, mhs ...multihash.Multihash) error {
	if err := ix.Interface.Put(value, mhs...); err != nil {
		return err
	}
	if slices.ContainsFunc(mhs, func(mh multihash.Multihash) bool { return bytes.Equal(mh, ix.trigger) }) {
		return ix.reg.Freeze()
	}
	return nil
}

// A freeze while a chain is being ingested applies from the next ad of the
// chain on: those ads are marked frozen and their multihashes are not indexed.
func TestChainFreezeAppliesFromNextAd(t *testing.T) {
	te := setupTestEnv(t, true)

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: te.publisherPriv, contextID: "a2"},
		{key: te.publisherPriv, contextID: "a3"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)
	te.ingester.indexer = &freezeAfterPut{
		Interface: te.ingester.indexer,
		reg:       te.reg,
		trigger:   chain[0].mhs[0],
	}

	c, err := te.ingester.Sync(t.Context(), publisherAddrInfo(te), 0, false, cid.Undef)
	require.NoError(t, err)
	require.Equal(t, head, c)
	require.True(t, te.reg.Frozen())

	requireAdIndexedEventually(t, te, chain[0])
	state, err := te.ingester.GetAdState(t.Context(), chain[0].cid)
	require.NoError(t, err)
	require.True(t, state.Processed)
	require.False(t, state.Frozen)
	for _, ba := range chain[1:] {
		state, err := te.ingester.GetAdState(t.Context(), ba.cid)
		require.NoError(t, err)
		require.True(t, state.Processed)
		require.True(t, state.Frozen)
		requireNotIndexed(t, te.ingester.indexer, ba.provider, ba.mhs)
	}
}

// A chain writes a provider's mirror head only after it claims the provider.
// A chain that stops before a provider's run, here at a busy provider, does
// not replace the head written by the chain that ingests that provider.
func TestMirrorHeadWrittenOnlyForClaimedProviders(t *testing.T) {
	te, _ := setupMirrorTestEnv(t)
	// The chain gives up at once on the busy provider.
	te.ingester.providerBusyWait = 0

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: randomPrivKey(t), contextID: "b1"},
	})
	provA, provB := chain[0].provider, chain[1].provider
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	// Another chain holds provider B and wrote B's head.
	otherChain, releaseOther := te.ingester.providerClaimer(t.Context())
	require.NoError(t, otherChain(provB))
	defer releaseOther()
	otherHead := random.Cids(1)[0]
	_, err := te.ingester.mirror.writeHead(t.Context(), otherHead, provB)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorIs(t, err, errProviderBusy)

	headA, err := te.ingester.mirror.mainCarReader.ReadHead(t.Context(), provA)
	require.NoError(t, err)
	require.Equal(t, chain[0].cid, headA)
	headB, err := te.ingester.mirror.mainCarReader.ReadHead(t.Context(), provB)
	require.NoError(t, err)
	require.Equal(t, otherHead, headB)
}

// An entry chunk changed in the temporary datastore after it was stored fails
// the ad as a transient error, not as a permanent one, so the ad is not
// skipped. The chunk is deleted, and the next attempt fetches it again from
// the publisher and indexes the ad.
func TestCorruptEntryChunkIsRefetched(t *testing.T) {
	te := setupTestEnv(t, true)
	chainQueue := runWorkersByHand(te)
	failDS := &failingDatastore{Batching: te.ingester.dsTmp}
	te.ingester.dsTmp = failDS

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
	})
	head := chain[0].cid
	te.publisher.SetRoot(head)
	syncFinished := syncChain(t, te)
	events, cnclEvents := te.ingester.onAdProcessed(te.pubHost.ID())
	defer cnclEvents()

	// Reads of the chunk return changed bytes, as if it changed after the
	// entries sync stored it.
	chunkKey := datastore.NewKey(chain[0].ad.Entries.(cidlink.Link).Cid.String())
	failDS.setGetResult(chunkKey, []byte("not an entry chunk"), nil)
	chainQueue <- syncFinished
	require.ErrorIs(t, waitChainEvent(t, events, head).err, errCorruptBlock)
	state, err := te.ingester.GetAdState(t.Context(), head)
	require.NoError(t, err)
	require.False(t, state.Processed)
	require.False(t, state.Skipped)
	has, err := te.ingester.dsTmp.Has(t.Context(), chunkKey)
	require.NoError(t, err)
	require.False(t, has)

	failDS.setGetResult(datastore.Key{}, nil, nil)
	chainQueue <- syncChain(t, te)
	require.NoError(t, waitChainEvent(t, events, head).err)
	requireAdIndexedEventually(t, te, chain[0])
}
