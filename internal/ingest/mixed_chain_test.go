package ingest

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	schema "github.com/ipni/go-libipni/ingest/schema"
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
	te := setupTestEnv(t, true, blockableLsysOpt)
	go func() {
		for {
			select {
			case <-hitBlockedRead:
			case <-t.Context().Done():
				return
			}
		}
	}()

	keyB := randomPrivKey(t)
	chain := buildMixedChain(t, te.publisherLinkSys, []mixedAd{
		{key: te.publisherPriv, contextID: "shared"},
		{key: keyB, contextID: "shared"},
		{key: keyB, contextID: "shared", rm: true},
	})
	// Fetching the entries of B's removed ad fails with a transient error,
	// so the sync fails unless the ad is skipped.
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
}
