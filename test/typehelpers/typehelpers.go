package typehelpers

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-test/random"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/ingest/schema"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var globalSeed atomic.Int64

type RandomAdBuilder struct {
	EntryBuilders      []EntryBuilder
	Seed               int64
	AddRmWithNoEntries bool
}

func (b RandomAdBuilder) Build(t *testing.T, lsys ipld.LinkSystem, signingKey crypto.PrivKey) datamodel.Link {
	return b.build(t, lsys, signingKey, false)
}

func (b RandomAdBuilder) BuildWithFakeSig(t *testing.T, lsys ipld.LinkSystem, signingKey crypto.PrivKey) datamodel.Link {
	return b.build(t, lsys, signingKey, true)
}

func (b RandomAdBuilder) build(t *testing.T, lsys ipld.LinkSystem, signingKey crypto.PrivKey, fakeSig bool) datamodel.Link {
	if len(b.EntryBuilders) == 0 {
		return nil
	}

	// Limit chain to be at most 256 links
	b.EntryBuilders = b.EntryBuilders[:len(b.EntryBuilders)%256]

	p, err := peer.IDFromPrivateKey(signingKey)
	require.NoError(t, err)

	metadata := []byte("test-metadata")
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	var headLink datamodel.Link

	for i, ecb := range b.EntryBuilders {
		ctxID := []byte("test-context-id-" + fmt.Sprint(i))
		ec := ecb.Build(t, lsys)
		if ec == nil {
			continue
		}

		ad := schema.Advertisement{
			Provider:  p.String(),
			Addresses: addrs,
			Entries:   ec,
			ContextID: ctxID,
			Metadata:  metadata,
		}
		ecbAddrs := ecb.GetAddrs()
		if ecbAddrs != nil {
			ad.Addresses = ecbAddrs
		}

		ad.PreviousID = headLink

		if !fakeSig {
			err := ad.Sign(signingKey)
			require.NoError(t, err)
		}

		node, err := ad.ToNode()
		require.NoError(t, err)
		headLink, err = lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
	}

	if b.AddRmWithNoEntries {
		// This will just remove all things in the first ad block.
		ctxID := []byte("test-context-id-" + fmt.Sprint(0))

		ad := schema.Advertisement{
			PreviousID: headLink,
			Provider:   p.String(),
			Addresses:  addrs,
			Entries:    schema.NoEntries,
			ContextID:  ctxID,
			Metadata:   metadata,
			IsRm:       true,
		}

		if !fakeSig {
			err := ad.Sign(signingKey)
			require.NoError(t, err)
		}

		node, err := ad.ToNode()
		require.NoError(t, err)
		headLink, err = lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
	}

	require.NoError(t, err)
	return headLink
}

type EntryBuilder interface {
	Build(t *testing.T, lsys ipld.LinkSystem) datamodel.Link
	GetAddrs() []string
}

var _ EntryBuilder = (*RandomEntryChunkBuilder)(nil)

type RandomEntryChunkBuilder struct {
	ChunkCount             uint8
	EntriesPerChunk        uint8
	Seed                   int64
	WithInvalidMultihashes bool
	Addrs                  []string
}

func (b RandomEntryChunkBuilder) GetAddrs() []string {
	return b.Addrs
}

func (b RandomEntryChunkBuilder) Build(t *testing.T, lsys ipld.LinkSystem) datamodel.Link {
	var headLink ipld.Link
	for i := 0; i < int(b.ChunkCount); i++ {
		var mhs []multihash.Multihash
		if b.WithInvalidMultihashes {
			for j := uint8(0); j < b.EntriesPerChunk; j++ {
				badmh := multihash.Multihash(fmt.Sprintf("invalid mh %d", globalSeed.Add(1)))
				mhs = append(mhs, badmh)
			}
		} else {
			mhs = random.Multihashes(int(b.EntriesPerChunk))
		}

		var err error
		chunk := schema.EntryChunk{
			Next:    headLink,
			Entries: mhs,
		}
		node, err := chunk.ToNode()
		require.NoError(t, err)
		headLink, err = lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
	}

	return headLink
}

var _ EntryBuilder = (*RandomHamtEntryBuilder)(nil)

type RandomHamtEntryBuilder struct {
	BucketSize             int
	BitWidth               int
	MultihashCount         uint32
	Seed                   int64
	WithInvalidMultihashes bool
	Addrs                  []string
}

func (b RandomHamtEntryBuilder) GetAddrs() []string {
	return b.Addrs
}

func (b RandomHamtEntryBuilder) Build(t *testing.T, lsys ipld.LinkSystem) datamodel.Link {
	hb := hamt.NewBuilder(hamt.Prototype{
		BitWidth:   b.BitWidth,
		BucketSize: b.BucketSize,
	}).WithLinking(lsys, schema.Linkproto)

	ma, err := hb.BeginMap(0)
	require.NoError(t, err)
	for i := 0; i < int(b.MultihashCount); i++ {
		data := fmt.Sprintf("invalid mh %d", globalSeed.Add(1))
		var mh multihash.Multihash
		if b.WithInvalidMultihashes {
			mh = multihash.Multihash(data)
		} else {
			mh, err = multihash.Sum([]byte(data), multihash.SHA2_256, -1)
			require.NoError(t, err)
		}
		require.NoError(t, ma.AssembleKey().AssignBytes(mh))
		require.NoError(t, ma.AssembleValue().AssignBool(true))
	}
	require.NoError(t, ma.Finish())
	hn := hb.Build().(*hamt.Node).Substrate()

	link, err := lsys.Store(ipld.LinkContext{Ctx: context.TODO()}, schema.Linkproto, hn)
	require.NoError(t, err)
	return link
}

func AllMultihashesFromAdChain(t *testing.T, ad *schema.Advertisement, lsys ipld.LinkSystem) []multihash.Multihash {
	return AllMultihashesFromAdChainDepth(t, ad, lsys, 0)
}

func AllMultihashesFromAdChainDepth(t *testing.T, ad *schema.Advertisement, lsys ipld.LinkSystem, entriesDepth int) []multihash.Multihash {
	progress := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem: lsys,
			// LinkVisitOnlyOnce: true,
			LinkTargetNodePrototypeChooser: func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
				return basicnode.Prototype.Any, nil
			},
		},
	}

	var rLimit selector.RecursionLimit
	if entriesDepth < 1 {
		rLimit = selector.RecursionLimitNone()
	} else {
		rLimit = selector.RecursionLimitDepth(int64(entriesDepth))
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	exploreEntriesRecursively := func(efsb builder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Entries",
			ssb.ExploreUnion(
				// EntryChunk
				ssb.ExploreRecursive(rLimit,
					ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
						// In the EntryChunk
						efsb.Insert("Entries", ssb.ExploreAll(ssb.Matcher()))
						// Recurse with "Next"
						efsb.Insert("Next", ssb.ExploreRecursiveEdge())
					}),
				),
				// Select entries node itself, which can be a link (in ad), or HAMT root node.
				// Because both fields in EntryChunk and ad are called "Entries", the former has
				// []Bytes as value and the latter has a link as value.
				// TODO: improve once selector builder API is rich enough to allow us to conditionally explore.
				ssb.Matcher(),
			),
		)
	}
	sel, err := ssb.ExploreFields(
		func(efsb builder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID",
				ssb.ExploreRecursive(selector.RecursionLimitNone(),
					ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
						efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
						exploreEntriesRecursively(efsb)
					})))
			exploreEntriesRecursively(efsb)
		}).Selector()
	require.NoError(t, err)

	adNode, err := ad.ToNode()
	require.NoError(t, err)

	var out []multihash.Multihash
	err = progress.WalkMatching(
		adNode,
		sel,
		multihashCollector(lsys, &out))
	require.NoError(t, err)
	return out
}

func multihashCollector(lsys ipld.LinkSystem, out *[]multihash.Multihash) func(p traversal.Progress, n datamodel.Node) error {
	return func(p traversal.Progress, n datamodel.Node) error {
		b, err := n.AsBytes()
		if err != nil {
			if h, _ := n.LookupByString("hamt"); h != nil {
				rootNode, err := lsys.Load(ipld.LinkContext{Ctx: context.TODO()}, p.LastBlock.Link, hamt.HashMapRootPrototype)
				if err != nil {
					return err
				}
				root, ok := bindnode.Unwrap(rootNode).(*hamt.HashMapRoot)
				if !ok {
					return fmt.Errorf("expected HAMT root node; got %v", rootNode)
				}
				node := hamt.Node{
					HashMapRoot: *root,
				}.WithLinking(lsys, schema.Linkproto)

				it := node.MapIterator()
				for !it.Done() {
					k, _, err := it.Next()
					if err != nil {
						return err
					}
					s, err := k.AsString()
					if err != nil {
						return err
					}
					*out = append(*out, []byte(s))
				}
				return nil
			}
			if e, _ := n.LookupByString("Entries"); e != nil {
				// Ignore selector matching Entries link in an ad or Entries field in EntryChunk.
				return nil
			}
			return err
		}
		*out = append(*out, b)
		return nil
	}
}

func AllMultihashesFromAd(t *testing.T, ad *schema.Advertisement, lsys ipld.LinkSystem) []multihash.Multihash {
	progress := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem: lsys,
			LinkTargetNodePrototypeChooser: func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
				return basicnode.Prototype.Any, nil
			},
		},
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel, err := ssb.ExploreUnion(
		ssb.ExploreFields(
			func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Entries",
					ssb.ExploreRecursive(selector.RecursionLimitNone(),
						ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
							efsb.Insert("Next", ssb.ExploreRecursiveEdge())
						})),
				)
			}),
		// Select entries node itself, which can be a link (in ad), or HAMT root node.
		// Because both fields in EntryChunk and ad are called "Entries", the former has
		// []Bytes as value and the latter has a link as value.
		// TODO: improve once selector builder API is rich enough to allow us to conditionally explore.
		ssb.ExploreFields(
			func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Entries", ssb.Matcher())
			}),
	).Selector()
	require.NoError(t, err)

	adNode, err := ad.ToNode()
	require.NoError(t, err)

	var out []multihash.Multihash
	err = progress.WalkMatching(
		adNode,
		sel,
		multihashCollector(lsys, &out))
	require.NoError(t, err)
	return out
}

func AllAds(t *testing.T, ad *schema.Advertisement, lsys ipld.LinkSystem) []*schema.Advertisement {
	var out []*schema.Advertisement
	progress := traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem: lsys,
			LinkTargetNodePrototypeChooser: func(l datamodel.Link, lc linking.LinkContext) (datamodel.NodePrototype, error) {
				return schema.AdvertisementPrototype, nil
			},
		},
	}

	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	sel, err := ssb.ExploreUnion(
		ssb.Matcher(),
		ssb.ExploreFields(
			func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert("PreviousID",
					ssb.ExploreRecursive(selector.RecursionLimitDepth(0xff),
						ssb.ExploreUnion(
							ssb.Matcher(),
							ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
								efsb.Insert("PreviousID", ssb.ExploreUnion(ssb.ExploreRecursiveEdge()))
							}))))

			})).Selector()
	require.NoError(t, err)

	adNode, err := ad.ToNode()
	require.NoError(t, err)

	err = progress.WalkMatching(
		adNode,
		sel,
		func(p traversal.Progress, n datamodel.Node) error {
			if !n.IsAbsent() {
				ad, err := schema.UnwrapAdvertisement(n)
				require.NoError(t, err)
				out = append(out, ad)
			}
			return nil
		})
	require.NoError(t, err)

	return out
}

func AllMultihashesFromAdLink(t *testing.T, adLink datamodel.Link, lsys ipld.LinkSystem) []multihash.Multihash {
	ad := AdFromLink(t, adLink, lsys)
	return AllMultihashesFromAdChain(t, ad, lsys)
}

func AdFromLink(t *testing.T, adLink datamodel.Link, lsys ipld.LinkSystem) *schema.Advertisement {
	node, err := lsys.Load(linking.LinkContext{}, adLink, schema.AdvertisementPrototype)
	require.NoError(t, err)
	ad, err := schema.UnwrapAdvertisement(node)
	require.NoError(t, err)
	return ad
}

// AllAdLinks returns a list of all ad cids for a given chain. Latest last
func AllAdLinks(t *testing.T, head datamodel.Link, lsys ipld.LinkSystem) []datamodel.Link {
	out := []datamodel.Link{head}
	ad := AdFromLink(t, head, lsys)
	for ad.PreviousID != nil {
		out = append(out, ad.PreviousID)
		ad = AdFromLink(t, ad.PreviousID, lsys)
	}

	// Flip order so the latest is last
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return out
}
