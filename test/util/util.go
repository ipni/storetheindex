package util

import (
	"fmt"
	"math/rand"
	"testing"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testProtocolID = 0x300000
)

func RandomMultihashes(n int, rng *rand.Rand) []multihash.Multihash {
	prefix := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}

	mhashes := make([]multihash.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n+16)
		rng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			panic(err.Error())
		}
		mhashes[i] = c.Hash()
	}
	return mhashes
}

type RandomAdBuilder struct {
	// FakeSig bool

	EntryChunkBuilders []RandomEntryChunkBuilder
	Seed               int64
}

func (b RandomAdBuilder) Build(t *testing.T, lsys ipld.LinkSystem, signingKey crypto.PrivKey) datamodel.Link {
	if len(b.EntryChunkBuilders) == 0 {
		return nil
	}

	// Limit chain to be at most 256 links
	b.EntryChunkBuilders = b.EntryChunkBuilders[:len(b.EntryChunkBuilders)%256]

	mhs := RandomMultihashes(1, rand.New(rand.NewSource(b.Seed)))

	p, err := peer.IDFromPrivateKey(signingKey)
	require.NoError(t, err)

	metadata := v0.Metadata{
		ProtocolID: testProtocolID,
		Data:       mhs[0],
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	var headLink schema.Link_Advertisement

	for i, ecb := range b.EntryChunkBuilders {
		ctxID := []byte("test-context-id-" + fmt.Sprint(i))
		ec := ecb.Build(t, lsys)
		if ec == nil {
			continue
		}

		_, headLink, err = schema.NewAdvertisementWithLink(lsys, signingKey, headLink, ec, ctxID, metadata, false, p.String(), addrs)

		// if b.FakeSig {
		// 	_, headLink, err = schema.NewAdvertisementWithFakeSig(lsys, signingKey, headLink, ec, ctxID, metadata, false, p.String(), addrs)
		// }
		require.NoError(t, err)
	}
	lnk, err := headLink.AsLink()
	require.NoError(t, err)
	return lnk
}

type RandomEntryChunkBuilder struct {
	ChunkCount      uint8
	EntriesPerChunk uint8
	EntriesSeed     int64
}

func (b RandomEntryChunkBuilder) Build(t *testing.T, lsys ipld.LinkSystem) datamodel.Link {
	var headLink datamodel.Link = nil
	prng := rand.New(rand.NewSource(b.EntriesSeed))

	for i := 0; i < int(b.ChunkCount); i++ {
		mhs := RandomMultihashes(int(b.EntriesPerChunk), prng)
		var err error
		headLink, _, err = schema.NewLinkedListOfMhs(lsys, mhs, headLink)
		require.NoError(t, err)
	}

	return headLink
}

func AllMultihashesFromAd(t *testing.T, ad schema.Advertisement, lsys ipld.LinkSystem) []multihash.Multihash {
	var out []multihash.Multihash

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem: lsys,
			// LinkVisitOnlyOnce: true,
			LinkTargetNodePrototypeChooser: func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
				return basicnode.Prototype.Any, nil
			},
		},
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	exploreEntriesRecursively := func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Entries",
			ssb.ExploreRecursive(selector.RecursionLimitDepth(0xff),
				ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
					// In the EntryChunk
					efsb.Insert("Entries", ssb.ExploreAll(ssb.Matcher()))
					// Recurse with "Next"
					efsb.Insert("Next", ssb.ExploreRecursiveEdge())
				})))
	}
	sel, err := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID",
				ssb.ExploreRecursive(selector.RecursionLimitDepth(0xff),
					ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
						efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
						exploreEntriesRecursively(efsb)
					})))
			exploreEntriesRecursively(efsb)
		}).Selector()
	require.NoError(t, err)

	err = progress.WalkMatching(
		ad,
		sel,
		func(p traversal.Progress, n datamodel.Node) error {
			b, err := n.AsBytes()
			if err != nil {
				return err
			}
			_, mh, err := multihash.MHFromBytes(b)
			if err != nil {
				return err
			}
			out = append(out, mh)
			return nil
		})
	require.NoError(t, err)

	return out
}
