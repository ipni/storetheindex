package ingest

import (
	"context"
	"math/rand"
	"testing"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/test/typehelpers"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/stretchr/testify/require"
)

func TestAdsSyncedViaSegmentsAreProcessed(t *testing.T) {
	cfg := config.NewIngest()
	cfg.PubSubTopic = defaultTestIngestConfig.PubSubTopic
	cfg.SyncSegmentDepthLimit = 7

	te := setupTestEnv(t, true, func(opts *testEnvOpts) {
		opts.ingestConfig = &cfg
	})
	defer te.Close(t)
	rng := rand.New(rand.NewSource(1413))
	var cb []typehelpers.RandomEntryChunkBuilder
	for i := 0; i < 50; i++ {
		chunkCount := rng.Int31n(100)
		ePerChunk := rng.Int31n(100)
		cb = append(cb, typehelpers.RandomEntryChunkBuilder{ChunkCount: uint8(uint32(chunkCount)), EntriesPerChunk: uint8(ePerChunk), EntriesSeed: rng.Int63()})
	}

	headAd := typehelpers.RandomAdBuilder{
		EntryChunkBuilders: cb,
	}.Build(t, te.publisherLinkSys, te.publisherPriv)
	headAdCid := headAd.(cidlink.Link).Cid

	ctx := context.Background()
	err := te.publisher.UpdateRoot(ctx, headAdCid)
	require.NoError(t, err)
	mhs := typehelpers.AllMultihashesFromAdLink(t, headAd, te.publisherLinkSys)

	providerID := te.pubHost.ID()
	subject := te.ingester

	wait, err := subject.Sync(ctx, providerID, nil, 0, false)
	require.NoError(t, err)
	gotHeadAd := <-wait
	require.Equal(t, headAdCid, gotHeadAd, "Expected latest synced cid to match head of ad chain")

	requireTrueEventually(t, func() bool {
		return checkAllIndexed(subject.indexer, providerID, mhs) == nil
	}, testRetryInterval, testRetryTimeout, "Expected all ads from publisher to have been indexed.")

	requireTrueEventually(t, func() bool {
		latestSync, err := subject.GetLatestSync(providerID)
		require.NoError(t, err)
		return latestSync.Equals(headAdCid)
	}, testRetryInterval, testRetryTimeout, "Expected all ads from publisher to have been indexed.")
}
