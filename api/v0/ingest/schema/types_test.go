package schema_test

import (
	_ "embed"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	ipldSchema "github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	stischema "github.com/ipni/storetheindex/api/v0/ingest/schema"
	"github.com/ipni/storetheindex/test/util"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// This struct captures Advertisement before ExtendedProviders have been added.
// It's needed to make sure that changes to Advertisement struct are backward compatible.
type OldAdvertisement struct {
	PreviousID ipld.Link
	Provider   string
	Addresses  []string
	Signature  []byte
	Entries    ipld.Link
	ContextID  []byte
	Metadata   []byte
	IsRm       bool
}

// testSchema mimics the logic of schema.go. The test requires that to be able to load both old and new schemas in the same time.
type testSchema struct {
	noEntries              cidlink.Link
	linkproto              cidlink.LinkPrototype
	advertisementPrototype ipldSchema.TypedPrototype
	entryChunkPrototype    ipldSchema.TypedPrototype
	typeSystem             *ipldSchema.TypeSystem
}

func createTestSchema(t *testing.T, schemaBytes []byte, adInterfacePtr interface{}) *testSchema {
	ts := &testSchema{
		linkproto: cidlink.LinkPrototype{
			Prefix: cid.Prefix{
				Version:  1,
				Codec:    uint64(multicodec.DagJson),
				MhType:   uint64(multicodec.Sha2_256),
				MhLength: -1,
			},
		},
	}

	typeSystem, err := ipld.LoadSchemaBytes(schemaBytes)
	require.NoError(t, err)

	ts.typeSystem = typeSystem
	ts.advertisementPrototype = bindnode.Prototype(adInterfacePtr, typeSystem.TypeByName("Advertisement"))
	ts.entryChunkPrototype = bindnode.Prototype((*stischema.EntryChunk)(nil), typeSystem.TypeByName("EntryChunk"))

	// Define NoEntries as the CID of a sha256 hash of nil.
	m, err := multihash.Sum(nil, multihash.SHA2_256, 16)
	if err != nil {
		panic(fmt.Errorf("failed to sum NoEntries multihash: %w", err))
	}
	ts.noEntries = cidlink.Link{Cid: cid.NewCidV1(cid.Raw, m)}
	return ts
}

// oldSchema defines Advertisemnt schema before ExtendedProviders have been added
var oldSchema = []byte(`
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

	type Link_Advertisement &Advertisement
`)

//go:embed schema.ipldsch
var newSchema []byte

func TestOldAdsCanBeReadWithNewStructs(t *testing.T) {
	oldSchema := createTestSchema(t, oldSchema, (*OldAdvertisement)(nil))

	rng := rand.New(rand.NewSource(1413))

	mhs := util.RandomMultihashes(7, rng)
	prev := ipld.Link(cidlink.Link{Cid: cid.NewCidV1(cid.Raw, mhs[0])})
	oldAd := &OldAdvertisement{
		PreviousID: prev,
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

	oldAdType := oldSchema.typeSystem.TypeByName("Advertisement")
	nodeFromOldAdType := bindnode.Wrap(oldAd, oldAdType).Representation()

	newAd, err := stischema.UnwrapAdvertisement(nodeFromOldAdType)
	require.NoError(t, err)

	// we have to compare manually field by field as old struct doesn't have ExtendedProvider
	require.Equal(t, oldAd.PreviousID, newAd.PreviousID)
	require.Equal(t, oldAd.Provider, newAd.Provider)
	require.Equal(t, oldAd.Addresses, newAd.Addresses)
	require.Equal(t, oldAd.Signature, newAd.Signature)
	require.Equal(t, oldAd.Entries, newAd.Entries)
	require.Equal(t, oldAd.ContextID, newAd.ContextID)
	require.Equal(t, oldAd.Metadata, newAd.Metadata)
	require.Equal(t, oldAd.IsRm, newAd.IsRm)
	require.Nil(t, newAd.ExtendedProvider)

	chunk := generateEntryChunk(rng)
	oldChunkType := oldSchema.typeSystem.TypeByName("EntryChunk")
	nodeFromOldChunkType := bindnode.Wrap(chunk, oldChunkType).Representation()

	newChunk, err := stischema.UnwrapEntryChunk(nodeFromOldChunkType)
	require.NoError(t, err)
	require.Equal(t, chunk, newChunk)
}

func TestNewAdsCanBeReadWithOldStructs(t *testing.T) {
	oldSchema := createTestSchema(t, oldSchema, (*OldAdvertisement)(nil))
	newSchema := createTestSchema(t, newSchema, (*stischema.Advertisement)(nil))

	rng := rand.New(rand.NewSource(1413))

	mhs := util.RandomMultihashes(7, rng)
	prev := ipld.Link(cidlink.Link{Cid: cid.NewCidV1(cid.Raw, mhs[0])})
	newAd := &stischema.Advertisement{
		PreviousID: prev,
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

	newAdType := newSchema.typeSystem.TypeByName("Advertisement")
	nodeFromNewAdType := bindnode.Wrap(newAd, newAdType).Representation()

	oldAd, err := oldUnwrap(nodeFromNewAdType, oldSchema.advertisementPrototype)
	require.NoError(t, err)

	// we have to compare manually field by field as old struct doesn't have ExtendedProvider
	require.Equal(t, oldAd.PreviousID, newAd.PreviousID)
	require.Equal(t, oldAd.Provider, newAd.Provider)
	require.Equal(t, oldAd.Addresses, newAd.Addresses)
	require.Equal(t, oldAd.Signature, newAd.Signature)
	require.Equal(t, oldAd.Entries, newAd.Entries)
	require.Equal(t, oldAd.ContextID, newAd.ContextID)
	require.Equal(t, oldAd.Metadata, newAd.Metadata)
	require.Equal(t, oldAd.IsRm, newAd.IsRm)
	require.Nil(t, newAd.ExtendedProvider)

	chunk := generateEntryChunk(rng)
	newChunkType := oldSchema.typeSystem.TypeByName("EntryChunk")
	nodeFromNewChunkType := bindnode.Wrap(chunk, newChunkType).Representation()

	newChunk, err := stischema.UnwrapEntryChunk(nodeFromNewChunkType)
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
		PreviousID: prev,
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
		Next:    next,
	}
}

func oldUnwrap(node ipld.Node, oldAdPrototype ipldSchema.TypedPrototype) (*OldAdvertisement, error) {
	if node.Prototype() != oldAdPrototype {
		adBuilder := oldAdPrototype.NewBuilder()
		err := adBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = adBuilder.Build()
	}

	unwrapped := bindnode.Unwrap(node)
	ad, ok := unwrapped.(*OldAdvertisement)
	if !ok || ad == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.Advertisement")
	}
	return ad, nil
}
