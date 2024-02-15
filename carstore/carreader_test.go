package carstore_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-datastore"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/filestore"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	const entBlockCount = 5

	dstore := datastore.NewMapDatastore()
	metadata := []byte("car-test-metadata")

	carDir := t.TempDir()
	fileStore, err := filestore.NewLocal(carDir)
	require.NoError(t, err)
	carw, err := carstore.NewWriter(dstore, fileStore, carstore.WithCompress(testCompress))
	require.NoError(t, err)

	adLink, ad, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, nil, dstore)
	adCid := adLink.(cidlink.Link).Cid
	entriesCid := ad.Entries.(cidlink.Link).Cid

	ctx := context.Background()

	// Create car file
	_, err = carw.Write(ctx, adCid, false, false)
	require.NoError(t, err)

	// Create reader.
	carr, err := carstore.NewReader(fileStore, carstore.WithCompress(testCompress))
	require.NoError(t, err)

	// Read and read CAR file.
	adBlock, err := carr.Read(ctx, adCid, false)
	require.NoError(t, err)
	require.NotZero(t, len(adBlock.Data))

	// Check that the data is a valid advertisement.
	adv, err := adBlock.Advertisement()
	require.NoError(t, err)
	require.Equal(t, entriesCid, adv.Entries.(cidlink.Link).Cid)

	// Check the first entries block looks correct.
	require.NotNil(t, adBlock.Entries)
	entBlock := <-adBlock.Entries
	require.NoError(t, entBlock.Err)
	require.NotZero(t, len(entBlock.Data))
	require.Equal(t, entriesCid, entBlock.Cid)

	// Check that the data is a valid EntryChunk.
	chunk, err := entBlock.EntryChunk()
	require.NoError(t, err)
	require.Equal(t, testEntriesChunkSize, len(chunk.Entries))

	// Check for expected number of entries blocks.
	count := 1
	for entBlock = range adBlock.Entries {
		require.NoError(t, entBlock.Err)
		require.NotZero(t, len(entBlock.Data))
		require.True(t, entBlock.Cid.Defined())
		count++
	}
	require.Equal(t, entBlockCount, count)

	// Read CAR file, skipping entries.
	adBlock, err = carr.Read(ctx, adCid, true)
	require.NoError(t, err)
	require.Nil(t, adBlock.Entries)

	// Read CAR file and cancel context after reading first entries block.
	xctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adBlock, err = carr.Read(xctx, adCid, false)
	require.NoError(t, err)

	count = 0
	for entBlock = range adBlock.Entries {
		count++
		cancel()
	}
	require.ErrorIs(t, entBlock.Err, context.Canceled)

	peerID, err := peer.Decode("12D3KooWLjeDyvuv7rbfG2wWNvWn7ybmmU88PirmSckuqCgXBAph")
	require.NoError(t, err)
	_, err = carw.WriteHead(ctx, adCid, peerID)
	require.NoError(t, err)

	headCid, err := carr.ReadHead(ctx, peerID)
	require.NoError(t, err)
	require.Equal(t, adCid, headCid)
}
