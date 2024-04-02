package ingest

import (
	"context"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/test/typehelpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestInvalidMultihashesAreNotIngested(t *testing.T) {
	te := setupTestEnv(t, true)

	headAd := typehelpers.RandomAdBuilder{
		EntryBuilders: []typehelpers.EntryBuilder{
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 1, WithInvalidMultihashes: true},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 2, WithInvalidMultihashes: false},
			typehelpers.RandomEntryChunkBuilder{ChunkCount: 1, EntriesPerChunk: 1, Seed: 3, WithInvalidMultihashes: true},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 4, WithInvalidMultihashes: false},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 5, WithInvalidMultihashes: true},
			typehelpers.RandomHamtEntryBuilder{MultihashCount: 1, Seed: 6, WithInvalidMultihashes: false},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)
	headAdCid := headAd.(cidlink.Link).Cid
	ctx := context.Background()
	te.publisher.SetRoot(headAdCid)
	mhs := typehelpers.AllMultihashesFromAdLink(t, headAd, te.publisherLinkSys)
	require.Len(t, mhs, 6) // 6 ads; one mh each.
	validMhs := []multihash.Multihash{mhs[1], mhs[3], mhs[5]}
	invalidMhs := []multihash.Multihash{mhs[0], mhs[2], mhs[4]}

	subject := te.ingester

	pubInfo := peer.AddrInfo{
		ID: te.publisher.ID(),
	}
	gotHeadAd, err := subject.Sync(ctx, pubInfo, 0, false)
	require.NoError(t, err)

	require.Equal(t, headAdCid, gotHeadAd, "Expected latest synced cid to match head of ad chain")

	require.Eventually(t, func() bool {
		return checkAllIndexed(subject.indexer, pubInfo.ID, validMhs) == nil
	}, testRetryTimeout, testRetryInterval, "Expected only valid multihashes to be indexed")

	require.Eventually(t, func() bool {
		latestSync, err := subject.GetLatestSync(pubInfo.ID)
		require.NoError(t, err)
		return latestSync.Equals(headAdCid)
	}, testRetryTimeout, testRetryInterval, "Expected all ads from publisher to have been indexed")

	// Assert valid multihash indices correspond to the expected provider.
	for _, mh := range validMhs {
		gotIdx, b, err := subject.indexer.Get(mh)
		require.NoError(t, err)
		require.True(t, b)
		require.Equal(t, 1, len(gotIdx))
		require.Equal(t, pubInfo.ID, gotIdx[0].ProviderID)
	}

	// Assert invalid multihashes are not indexed.
	for _, mh := range invalidMhs {
		_, b, err := subject.indexer.Get(mh)
		require.NoError(t, err)
		require.False(t, b)
	}
}
