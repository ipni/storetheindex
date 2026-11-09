package ingest

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
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

// A chain stops at the first ad of a provider that another chain is ingesting,
// and reports the failure to Sync. The ads before that run stay processed, and
// the chain holds no provider after it stops.
func TestMixedChainBusyProviderFailsSync(t *testing.T) {
	te := setupTestEnv(t, true)

	provB := randomPrivKey(t)
	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: provB, contextID: "b1"},
		{key: te.publisherPriv, contextID: "a2"},
	})
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	// Another chain holds provider B.
	otherChain := te.ingester.providerClaimer()
	require.True(t, otherChain(chain[1].provider))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorContains(t, err, "busy")
	state, err := te.ingester.GetAdState(t.Context(), chain[0].cid)
	require.NoError(t, err)
	require.True(t, state.Processed)
	requireNoneProcessed(t, te.ingester, chain[1:])

	// The chain released provider A when it reached B's run.
	check := te.ingester.providerClaimer()
	require.True(t, check(chain[0].provider))
	require.True(t, check(""))

	require.True(t, otherChain(""))
	requireChainIngestedAfterNewAd(t, te, head, chain)
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

func TestProviderClaimer(t *testing.T) {
	ing := &Ingester{providersBusy: make(map[peer.ID]struct{})}
	a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
	busy := func() []peer.ID {
		return slices.Sorted(maps.Keys(ing.providersBusy))
	}
	claim := ing.providerClaimer()
	other := ing.providerClaimer()

	require.True(t, claim(a))
	require.True(t, claim(a))
	require.Equal(t, []peer.ID{a}, busy())
	require.False(t, other(a))

	// Moving to another provider releases the one held.
	require.True(t, claim(b))
	require.Equal(t, []peer.ID{b}, busy())
	require.True(t, other(a))
	require.Equal(t, []peer.ID{a, b}, busy())

	// A busy provider fails the claim, and the one held is released.
	require.True(t, other(c))
	require.False(t, claim(c))
	require.Equal(t, []peer.ID{c}, busy())

	// After a failed claim, nothing is held, so the empty ID releases nothing.
	require.True(t, claim(""))
	require.Equal(t, []peer.ID{c}, busy())

	require.True(t, claim(a))
	require.True(t, claim(""))
	require.Equal(t, []peer.ID{c}, busy())

	require.True(t, other(""))
	require.Empty(t, busy())
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

	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "a1"},
		{key: randomPrivKey(t), contextID: "b1"},
	})
	provA, provB := chain[0].provider, chain[1].provider
	head := chain[len(chain)-1].cid
	te.publisher.SetRoot(head)

	// Another chain holds provider B and wrote B's head.
	otherChain := te.ingester.providerClaimer()
	require.True(t, otherChain(provB))
	defer otherChain("")
	otherHead := random.Cids(1)[0]
	_, err := te.ingester.mirror.writeHead(t.Context(), otherHead, provB)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = te.ingester.Sync(ctx, publisherAddrInfo(te), 0, false, cid.Undef)
	require.ErrorContains(t, err, "busy")

	headA, err := te.ingester.mirror.mainCarReader.ReadHead(t.Context(), provA)
	require.NoError(t, err)
	require.Equal(t, chain[0].cid, headA)
	headB, err := te.ingester.mirror.mainCarReader.ReadHead(t.Context(), provB)
	require.NoError(t, err)
	require.Equal(t, otherHead, headB)
}
