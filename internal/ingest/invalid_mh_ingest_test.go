package ingest

import (
	"context"
	"testing"

	"github.com/filecoin-project/storetheindex/test/typehelpers"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestInvalidMultihashesAreNotIngested(t *testing.T) {
	te := setupTestEnv(t, true)
	defer te.Close(t)

	headAd := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: []typehelpers.RandomEntryChunkBuilder{
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 1, WithInvalidMultihashes: true},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 2, WithInvalidMultihashes: false},
			{ChunkCount: 1, EntriesPerChunk: 1, EntriesSeed: 3, WithInvalidMultihashes: true},
		},
	}.Build(t, te.publisherLinkSys, te.publisherPriv)
	headAdCid := headAd.(cidlink.Link).Cid
	ctx := context.Background()
	err := te.publisher.SetRoot(ctx, headAdCid)
	require.NoError(t, err)
	mhs := typehelpers.AllMultihashesFromAdLink(t, headAd, te.publisherLinkSys)

	providerID := te.pubHost.ID()
	subject := te.ingester

	wait, err := subject.Sync(ctx, providerID, nil, 0, false)
	require.NoError(t, err)
	gotHeadAd := <-wait

	require.Equal(t, headAdCid, gotHeadAd, "Expected latest synced cid to match head of ad chain")

	requireTrueEventually(t, func() bool {
		return checkAllIndexed(subject.indexer, providerID, mhs[1:2]) == nil
	}, testRetryInterval, testRetryTimeout, "Expected only valid multihashes to be indexed")

	requireTrueEventually(t, func() bool {
		latestSync, err := subject.GetLatestSync(providerID)
		require.NoError(t, err)
		return latestSync.Equals(headAdCid)
	}, testRetryInterval, testRetryTimeout, "Expected all ads from publisher to have been indexed")

	_, b, err := subject.indexer.Get(mhs[0])
	require.NoError(t, err)
	require.False(t, b)

	get, b, err := subject.indexer.Get(mhs[1])
	require.NoError(t, err)
	require.True(t, b)
	require.Equal(t, 1, len(get))
	require.Equal(t, providerID, get[0].ProviderID)

	_, b, err = subject.indexer.Get(mhs[2])
	require.NoError(t, err)
	require.False(t, b)
}
