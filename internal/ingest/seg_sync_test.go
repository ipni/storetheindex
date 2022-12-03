package ingest

import (
	"context"
	"math/rand"
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/test/typehelpers"
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
	var cb []typehelpers.EntryBuilder
	for i := 0; i < 50; i++ {
		chunkCount := rng.Int31n(50)
		ePerChunk := rng.Int31n(50)
		seed := rng.Int63()
		kindChunk := rng.Float32() > 0.5 // Flip a coin to decide what kind of entries to generate.
		if kindChunk {
			cb = append(cb, typehelpers.RandomEntryChunkBuilder{ChunkCount: uint8(chunkCount), EntriesPerChunk: uint8(ePerChunk), Seed: seed})
		} else {
			cb = append(cb, typehelpers.RandomHamtEntryBuilder{MultihashCount: uint32(chunkCount * ePerChunk), Seed: seed})
		}
	}

	headAd := typehelpers.RandomAdBuilder{
		EntryBuilders: cb,
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
