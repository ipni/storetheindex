package schema_test

import (
	"math/rand"
	"strings"
	"testing"

	stischema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/stretchr/testify/require"
)

func Test_OldSchemaIsCompatibleWithNewStructs(t *testing.T) {
	oldSchema, err := ipld.LoadSchemaBytes([]byte(`
		type EntryChunk struct {
			Entries List_Bytes
			Next optional Link_EntryChunk
		}
		
		type Link_EntryChunk &EntryChunk
		type List_String [String]
		type List_Bytes [Bytes]
		
		type Advertisement struct {
			PreviousID optional Link_Advertisement
			Provider String
			Addresses List_String
			Signature Bytes
			Entries Link
			ContextID Bytes
			Metadata Bytes
			IsRm Bool
		}
		
		type Link_Advertisement &Advertisement`))
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(1413))
	ad := generateAdvertisement(rng)
	oldAdType := oldSchema.TypeByName("Advertisement")
	nodeFromOldAdType := bindnode.Wrap(ad, oldAdType).Representation()

	newAd, err := stischema.UnwrapAdvertisement(nodeFromOldAdType)
	require.NoError(t, err)
	require.Equal(t, ad, newAd)

	chunk := generateEntryChunk(rng)
	oldChunkType := oldSchema.TypeByName("EntryChunk")
	nodeFromOldChunkType := bindnode.Wrap(chunk, oldChunkType).Representation()

	newChunk, err := stischema.UnwrapEntryChunk(nodeFromOldChunkType)
	require.NoError(t, err)
	require.Equal(t, chunk, newChunk)
}

func TestAdvertisement_Serde(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ad := generateAdvertisement(rng)
	gotNode, err := ad.ToNode()
	require.NoError(t, err)

	gotAd1, err := stischema.UnwrapAdvertisement(gotNode)
	require.NoError(t, err)
	require.Equal(t, ad, gotAd1)

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)

	node, err := ad.ToNode()
	require.NoError(t, err)
	lnk, err := ls.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	gotNode2, err := ls.Load(ipld.LinkContext{}, lnk, stischema.AdvertisementPrototype)
	require.NoError(t, err)
	gotAd, err := stischema.UnwrapAdvertisement(gotNode2)
	require.NoError(t, err)
	require.Equal(t, ad, gotAd)
}

func TestEntryChunk_Serde(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	chunk := generateEntryChunk(rng)
	_, err := chunk.ToNode()
	require.NoError(t, err)

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)

	cNode, err := chunk.ToNode()
	require.NoError(t, err)
	lnk, err := ls.Store(ipld.LinkContext{}, stischema.Linkproto, cNode)
	require.NoError(t, err)
	require.NotNil(t, lnk)

	gotCNode, err := ls.Load(ipld.LinkContext{}, lnk, stischema.EntryChunkPrototype)
	require.NoError(t, err)
	gotChunk, err := stischema.UnwrapEntryChunk(gotCNode)
	require.NoError(t, err)
	require.Equal(t, chunk, gotChunk)
}

func Test_MismatchingNodeIsError(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	c := generateEntryChunk(rng)
	chunkNode, err := c.ToNode()
	require.NoError(t, err)

	ad, err := stischema.UnwrapAdvertisement(chunkNode)
	require.Nil(t, ad)
	require.NotNil(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "faild to convert node prototype"))

	a := generateAdvertisement(rng)
	adNode, err := a.ToNode()
	require.NoError(t, err)

	chunk, err := stischema.UnwrapEntryChunk(adNode)
	require.Nil(t, chunk)
	require.NotNil(t, err)
	require.True(t, strings.HasPrefix(err.Error(), "faild to convert node prototype"))
}

func generateAdvertisement(rng *rand.Rand) *stischema.Advertisement {
	mhs := util.RandomMultihashes(7, rng)
	prev := ipld.Link(cidlink.Link{Cid: cid.NewCidV1(cid.Raw, mhs[0])})
	return &stischema.Advertisement{
		PreviousID: &prev,
		Provider:   mhs[1].String(),
		Addresses: []string{
			mhs[2].String(),
		},
		Entries:   cidlink.Link{Cid: cid.NewCidV1(cid.Raw, mhs[3])},
		ContextID: mhs[4],
		Metadata:  mhs[5],
		Signature: mhs[6],
		IsRm:      false,
	}
}

func generateEntryChunk(rng *rand.Rand) *stischema.EntryChunk {
	mhs := util.RandomMultihashes(100, rng)
	next := ipld.Link(cidlink.Link{Cid: cid.NewCidV1(cid.Raw, mhs[0])})
	return &stischema.EntryChunk{
		Entries: mhs,
		Next:    &next,
	}
}
