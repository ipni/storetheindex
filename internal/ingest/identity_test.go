package ingest

import (
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-test/random"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/carstore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// None of the identity blocks in these tests are stored at the publisher. An
// identity CID holds its block, so the indexer must not request it.

func TestIngestIdentityEntries(t *testing.T) {
	te := setupTestEnv(t, true)

	mhs := random.Multihashes(1)
	entriesLink := identityLink(t, multicodec.DagJson, entryChunkNode(t, mhs, nil))
	adCid := storeTestAd(t, te, entriesLink, nil)

	syncTestAd(t, te, adCid)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)
}

func TestIngestIdentityHamtRoot(t *testing.T) {
	te := setupTestEnv(t, true)

	// A HAMT with one multihash is a single root node, small enough for an
	// identity CID in dag-cbor.
	mhs := random.Multihashes(1)
	hb := hamt.NewBuilder(hamt.Prototype{BitWidth: 3, BucketSize: 1}).WithLinking(te.publisherLinkSys, schema.Linkproto)
	ma, err := hb.BeginMap(0)
	require.NoError(t, err)
	for _, mh := range mhs {
		require.NoError(t, ma.AssembleKey().AssignBytes(mh))
		require.NoError(t, ma.AssembleValue().AssignBool(true))
	}
	require.NoError(t, ma.Finish())
	root := identityLink(t, multicodec.DagCbor, hb.Build().(*hamt.Node).Substrate())
	require.LessOrEqual(t, identityDigestSize(t, root), ipnisync.MaxIdentityDigestSize)
	adCid := storeTestAd(t, te, root, nil)

	syncTestAd(t, te, adCid)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)
	state, err := te.ingester.GetAdState(t.Context(), adCid)
	require.NoError(t, err)
	require.True(t, state.Processed)
	require.False(t, state.Skipped)
}

func TestIngestIdentityEntriesRootWithLinkedChunks(t *testing.T) {
	te, _ := setupMirrorTestEnv(t)

	// An identity root links to a chain of hashed chunks that the publisher
	// serves.
	var nextLink ipld.Link
	var hashedCids []cid.Cid
	var allMhs []multihash.Multihash
	for range 3 {
		mhs := random.Multihashes(5)
		var err error
		nextLink, err = te.publisherLinkSys.Store(ipld.LinkContext{}, schema.Linkproto, entryChunkNode(t, mhs, nextLink))
		require.NoError(t, err)
		hashedCids = append([]cid.Cid{nextLink.(cidlink.Link).Cid}, hashedCids...)
		allMhs = append(allMhs, mhs...)
	}
	rootMhs := random.Multihashes(1)
	allMhs = append(allMhs, rootMhs...)
	// In dag-json, one multihash and a Next link are larger than the size
	// limit, so this root uses dag-cbor.
	entriesLink := identityLink(t, multicodec.DagCbor, entryChunkNode(t, rootMhs, nextLink))
	adCid := storeTestAd(t, te, entriesLink, nil)

	syncTestAd(t, te, adCid)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), allMhs)

	// The mirror CAR holds the whole chain in order, starting with the identity
	// root.
	adBlock, _, err := te.ingester.mirror.readMain(t.Context(), adCid, false)
	require.NoError(t, err)
	defer func() { _ = adBlock.Close() }()
	var carCids []cid.Cid
	for entBlock := range adBlock.Entries {
		require.NoError(t, entBlock.Err)
		carCids = append(carCids, entBlock.Cid)
	}
	require.Equal(t, append([]cid.Cid{entriesLink.(cidlink.Link).Cid}, hashedCids...), carCids)
}

func TestIngestIdentityEntriesFromCar(t *testing.T) {
	te, mainDir := setupMirrorTestEnv(t)

	mhs := random.Multihashes(1)
	entriesLink := identityLink(t, multicodec.DagJson, entryChunkNode(t, mhs, nil))
	adCid := storeTestAd(t, te, entriesLink, nil)

	syncTestAd(t, te, adCid)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)
	require.Zero(t, te.ingester.MultihashesFromMirror())
	require.Len(t, listCarFiles(t, mainDir, carstore.CarFileSuffix), 1)

	// A resync reads the entries from the Main CAR, which is complete
	before := carUnusableMetricCount(t, "main", mainDir, "")
	_, err := te.ingester.Sync(t.Context(), testPeerInfo(te), 0, true, cid.Undef)
	require.NoError(t, err)
	requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), mhs)
	require.EqualValues(t, 1, te.ingester.MultihashesFromMirror())
	require.Equal(t, before, carUnusableMetricCount(t, "main", mainDir, ""))
}

func TestIngestIdentityEntriesOverSizeLimitInCar(t *testing.T) {
	// A CAR written elsewhere, such as by an indexer without the size limit,
	// may hold an oversized identity block. Ingest from the CAR applies the
	// same limit as a sync from the publisher.
	t.Run("root checked before reading the CAR", func(t *testing.T) {
		// The Entries link is checked before the mirror is read, so the CAR
		// holding the oversized block is never opened. The ad is refused the
		// same way as one synced from the publisher.
		te, _ := setupMirrorTestEnv(t)
		oversized, oversizedMhs := oversizedEntriesLink(t)
		adCid := storeTestAd(t, te, oversized, nil)
		writeTestCar(t, te, adCid, oversized)

		syncTestAd(t, te, adCid)
		requireSkippedMalformed(t, te, adCid)
		requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), oversizedMhs)
	})

	t.Run("next", func(t *testing.T) {
		te, _ := setupMirrorTestEnv(t)
		oversized, oversizedMhs := oversizedEntriesLink(t)
		rootMhs := random.Multihashes(1)
		rootLink, err := te.publisherLinkSys.Store(ipld.LinkContext{}, schema.Linkproto, entryChunkNode(t, rootMhs, oversized))
		require.NoError(t, err)
		adCid := storeTestAd(t, te, rootLink, nil)
		writeTestCar(t, te, adCid, rootLink, oversized)

		syncTestAd(t, te, adCid)
		requireSkippedMalformed(t, te, adCid)
		// Multihashes from the chunk before the oversized link stay indexed
		requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), rootMhs)
		requireNotIndexed(t, te.ingester.indexer, te.pubHost.ID(), oversizedMhs)
	})
}

func TestIngestIdentityEntriesOverSizeLimit(t *testing.T) {
	// Four multihashes encode to more than ipnisync.MaxIdentityDigestSize bytes
	oversized := func(t *testing.T) ipld.Link {
		lnk := identityLink(t, multicodec.DagCbor, entryChunkNode(t, random.Multihashes(4), nil))
		require.Greater(t, identityDigestSize(t, lnk), ipnisync.MaxIdentityDigestSize)
		return lnk
	}

	t.Run("root", func(t *testing.T) {
		te := setupTestEnv(t, true)
		adCid := storeTestAd(t, te, oversized(t), nil)
		syncTestAd(t, te, adCid)
		requireSkippedMalformed(t, te, adCid)
	})

	t.Run("next", func(t *testing.T) {
		te := setupTestEnv(t, true)
		rootMhs := random.Multihashes(1)
		rootLink, err := te.publisherLinkSys.Store(ipld.LinkContext{}, schema.Linkproto, entryChunkNode(t, rootMhs, oversized(t)))
		require.NoError(t, err)
		adCid := storeTestAd(t, te, rootLink, nil)
		syncTestAd(t, te, adCid)
		requireSkippedMalformed(t, te, adCid)
		// Multihashes from the chunk before the oversized link stay indexed
		requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), rootMhs)
	})

	t.Run("next with mirror", func(t *testing.T) {
		// With a writable mirror, synced chunks stay in dsTmp until the ad is
		// mirrored. A skipped ad is not mirrored, so its data is removed.
		te, mainDir := setupMirrorTestEnv(t)
		rootMhs := random.Multihashes(1)
		rootLink, err := te.publisherLinkSys.Store(ipld.LinkContext{}, schema.Linkproto, entryChunkNode(t, rootMhs, oversized(t)))
		require.NoError(t, err)
		adCid := storeTestAd(t, te, rootLink, nil)
		syncTestAd(t, te, adCid)
		requireSkippedMalformed(t, te, adCid)
		requireIndexedEventually(t, te.ingester.indexer, te.pubHost.ID(), rootMhs)

		for _, c := range []cid.Cid{adCid, rootLink.(cidlink.Link).Cid} {
			has, err := te.ingester.dsTmp.Has(t.Context(), datastore.NewKey(c.String()))
			require.NoError(t, err)
			require.False(t, has, c)
		}
		require.NoFileExists(t, filepath.Join(mainDir, te.ingester.mirror.mainCarWriter.CarPath(adCid)))
	})
}

func TestIngestIdentityAdvertisement(t *testing.T) {
	// A signed advertisement is larger than ipnisync.MaxIdentityDigestSize, so
	// it cannot be an identity CID, wherever it is in the chain.
	t.Run("signed over size limit", func(t *testing.T) {
		te := setupTestEnv(t, true)
		firstCid := storeTestAd(t, te, schema.NoEntries, nil)
		node, err := newTestAd(t, te, schema.NoEntries, cidlink.Link{Cid: firstCid}).ToNode()
		require.NoError(t, err)
		idAdLink := identityLink(t, multicodec.DagCbor, node)
		require.Greater(t, identityDigestSize(t, idAdLink), ipnisync.MaxIdentityDigestSize)

		t.Run("head", func(t *testing.T) {
			te.publisher.SetRoot(idAdLink.(cidlink.Link).Cid)
			_, err := te.ingester.Sync(t.Context(), testPeerInfo(te), 0, false, cid.Undef)
			require.ErrorIs(t, err, ipnisync.ErrIdentityTooLarge)
		})

		t.Run("in chain", func(t *testing.T) {
			headCid := storeTestAd(t, te, schema.NoEntries, idAdLink)
			te.publisher.SetRoot(headCid)
			_, err := te.ingester.Sync(t.Context(), testPeerInfo(te), 0, false, cid.Undef)
			require.ErrorIs(t, err, ipnisync.ErrIdentityTooLarge)
		})
	})

	// An identity advertisement within the size limit passes through the same
	// signature verification as a fetched one.
	t.Run("unsigned within size limit", func(t *testing.T) {
		te := setupTestEnv(t, true)
		// A peer ID that is a sha2-256 multihash, like that of an RSA key, is
		// short enough for the ad to fit within the size limit.
		providerID, err := peer.IDFromBytes(random.Multihashes(1)[0])
		require.NoError(t, err)
		emptyLink := identityLink(t, multicodec.Raw, nil)
		unsigned := schema.Advertisement{
			Provider: providerID.String(),
			Entries:  emptyLink,
			IsRm:     true,
		}
		node, err := unsigned.ToNode()
		require.NoError(t, err)
		idAdLink := identityLink(t, multicodec.DagCbor, node)
		require.LessOrEqual(t, identityDigestSize(t, idAdLink), ipnisync.MaxIdentityDigestSize)

		headCid := storeTestAd(t, te, schema.NoEntries, idAdLink)
		te.publisher.SetRoot(headCid)
		_, err = te.ingester.Sync(t.Context(), testPeerInfo(te), 0, false, cid.Undef)
		require.ErrorIs(t, err, errInvalidAdvertSignature)
	})
}

// oversizedEntriesLink returns an identity link to an EntryChunk whose digest
// is larger than ipnisync.MaxIdentityDigestSize, and the chunk's multihashes.
func oversizedEntriesLink(t *testing.T) (ipld.Link, []multihash.Multihash) {
	mhs := random.Multihashes(4)
	lnk := identityLink(t, multicodec.DagJson, entryChunkNode(t, mhs, nil))
	require.Greater(t, identityDigestSize(t, lnk), ipnisync.MaxIdentityDigestSize)
	return lnk, mhs
}

// writeTestCar writes the Main CAR for an ad stored at the publisher, with the
// given entry blocks, through the ingester's mirror writer.
func writeTestCar(t *testing.T, te *testEnv, adCid cid.Cid, entries ...ipld.Link) {
	ctx := t.Context()
	for _, lnk := range append([]ipld.Link{cidlink.Link{Cid: adCid}}, entries...) {
		data, err := te.publisherLinkSys.LoadRaw(ipld.LinkContext{Ctx: ctx}, lnk)
		if err != nil {
			// An identity block is not stored at the publisher
			dmh, decErr := multihash.Decode(lnk.(cidlink.Link).Hash())
			require.NoError(t, decErr)
			require.EqualValues(t, multihash.IDENTITY, dmh.Code)
			data = dmh.Digest
		}
		require.NoError(t, te.ingester.dsTmp.Put(ctx, datastore.NewKey(lnk.String()), data))
	}
	_, err := te.ingester.mirror.write(ctx, adCid, false, false)
	require.NoError(t, err)
}

// identityLink returns an identity CID link that holds node encoded with
// codec. A nil node gives an empty raw block.
func identityLink(t *testing.T, codec multicodec.Code, node ipld.Node) ipld.Link {
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(codec),
		MhType:   multihash.IDENTITY,
		MhLength: -1,
	}}
	if node == nil {
		c, err := lp.Sum(nil)
		require.NoError(t, err)
		return cidlink.Link{Cid: c}
	}
	lsys := cidlink.DefaultLinkSystem()
	lnk, err := lsys.ComputeLink(lp, node)
	require.NoError(t, err)
	return lnk
}

func identityDigestSize(t *testing.T, lnk ipld.Link) int {
	dmh, err := multihash.Decode(lnk.(cidlink.Link).Hash())
	require.NoError(t, err)
	require.EqualValues(t, multihash.IDENTITY, dmh.Code)
	return len(dmh.Digest)
}

func requireSkippedMalformed(t *testing.T, te *testEnv, adCid cid.Cid) {
	state, err := te.ingester.GetAdState(t.Context(), adCid)
	require.NoError(t, err)
	require.True(t, state.Processed)
	require.True(t, state.Skipped)
	require.Contains(t, state.SkipReason, "malformedErr")
}
