package ingest

import (
	"context"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/test/typehelpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestIngester_IngestsMixedEntriesTypeSuccessfully(t *testing.T) {
	ctx := context.Background()
	te := setupTestEnv(t, true)
	defer te.Close(t)

	// Generate an ad chain with mixed entries type and different shape HAMT.
	headAd := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomHamtEntryBuilder{BucketSize: 3, BitWidth: 5, MultihashCount: 500, Seed: 1},
			typehelpers.RandomHamtEntryBuilder{BucketSize: 1, BitWidth: 3, MultihashCount: 500, Seed: 2},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 13, EntriesPerChunk: 100, Seed: 3},
			typehelpers.RandomHamtEntryBuilder{BucketSize: 10, BitWidth: 8, MultihashCount: 810, Seed: 4},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 5},
			typehelpers.RandomHamtEntryBuilder{BucketSize: 1, BitWidth: 3, MultihashCount: 1, Seed: 6},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)

	// Set the head on publisher.
	headAdCid := headAd.(cidlink.Link).Cid
	te.publisher.SetRoot(headAdCid)

	// Extract the list of all multihashes in the ad chain.
	mhs := typehelpers.AllMultihashesFromAdLink(t, headAd, te.publisherLinkSys)
	require.Len(t, mhs, 500+500+13*100+810+1+1) // Sanity check the total expected number of multihashes.

	subject := te.ingester

	pubInfo := peer.AddrInfo{
		ID: te.publisher.ID(),
	}
	// Trigger a sync.
	gotHeadAd, err := subject.Sync(ctx, pubInfo, 0, false)
	require.NoError(t, err)
	require.Equal(t, headAdCid, gotHeadAd, "Expected latest synced cid to match head of ad chain")

	// Assert all indices are processed eventually
	requireTrueEventually(t, func() bool {
		return checkAllIndexed(subject.indexer, pubInfo.ID, mhs) == nil
	}, testRetryInterval, testRetryTimeout, "Expected all multihashes to have been indexed eventually")

	// Assert All ads are processed eventually
	requireTrueEventually(t, func() bool {
		latestSync, err := subject.GetLatestSync(pubInfo.ID)
		require.NoError(t, err)
		return latestSync.Equals(headAdCid)
	}, testRetryInterval, testRetryTimeout, "Expected all ads from publisher to have been indexed eventually")

	// Assert multihash indices correspond to the single expected provider.
	for _, mh := range mhs {
		gotIdx, b, err := subject.indexer.Get(mh)
		require.NoError(t, err)
		require.True(t, b)
		require.Equal(t, 1, len(gotIdx))
		require.Equal(t, pubInfo.ID, gotIdx[0].ProviderID)
	}
}
