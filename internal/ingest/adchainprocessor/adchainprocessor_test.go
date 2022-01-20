package adchainprocessor

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"testing/quick"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
)

// TestApplyAtVariousHeights makes sure the process only processes each block
// once, even if the caller to `Run` passes an older head by accident/race.
func TestApplyAtVariousHeights(t *testing.T) {
	err := quick.Check(func(numberOfMhChunksInEachAdv []uint8, headIdxToApply []uint8) bool {
		if len(numberOfMhChunksInEachAdv) == 0 {
			return true
		}

		return t.Run("quickcheck", func(t *testing.T) {
			te := setupTestEnv(t)
			p := te.p
			ctx := context.Background()

			advLink, err := util.RandomAdvChain(te.provKey, te.mi.lsys, numberOfMhChunksInEachAdv)
			require.NoError(t, err)
			var allAdCids []cid.Cid

			adCid := advLink.ToCid()
			for {
				ad, err := te.mi.GetAd(ctx, adCid)
				require.NoError(t, err)
				allAdCids = append(allAdCids, adCid)
				if ad.PreviousID.IsAbsent() {
					break
				}
				l, err := ad.PreviousID.AsNode().AsLink()
				require.NoError(t, err)
				adCid = l.(cidlink.Link).Cid
			}

			// Randomly apply different parts of the adv chain. It should not reapply
			// anything it already has.
			for _, idx := range headIdxToApply {
				headToApply := allAdCids[idx%uint8(len(allAdCids))]
				err = p.Run(context.Background(), headToApply)
				if err != nil {
					t.Fatal("Processor error:", err)
				}
			}

			// Finally, apply the head so we can check if we've applied the whole chain once.
			err = p.Run(context.Background(), advLink.ToCid())
			if err != nil {
				t.Fatal("Processor error:", err)
			}

			sumBlocks := expectedNumberOfIndexedBlocks(numberOfMhChunksInEachAdv)

			te.mi.mutex.Lock()
			defer te.mi.mutex.Unlock()
			if sumBlocks != len(te.mi.indexedBlocks) {
				t.Fatalf("Expected %d blocks to be indexed, but got %d", sumBlocks, len(te.mi.indexedBlocks))
			}
		})

	}, &quick.Config{
		MaxCount: 30,
	})
	require.NoError(t, err)
}

func TestMultipleAdsWithEntryChunks(t *testing.T) {
	err := quick.Check(func(numberOfMhChunksInEachAdv []uint8) bool {
		if len(numberOfMhChunksInEachAdv) == 0 {
			return true
		}

		return t.Run("quickcheck", func(t *testing.T) {
			te := setupTestEnv(t)
			p := te.p

			advLink, err := util.RandomAdvChain(te.provKey, te.mi.lsys, numberOfMhChunksInEachAdv)
			require.NoError(t, err)

			// Also check if we get this notification
			ch, cncl := p.OnAllAdApplied()
			defer cncl()

			err = p.Run(context.Background(), advLink.ToCid())
			if err != nil {
				t.Fatal("Processor error:", err)
			}

			select {
			case head := <-ch:
				if head.Head != advLink.ToCid() {
					t.Fatal("Didn't process to head")
				}
			default:
				t.Fatal("Didn't get notification")
			}

			te.mi.mutex.Lock()
			defer te.mi.mutex.Unlock()
			sumBlocks := expectedNumberOfIndexedBlocks(numberOfMhChunksInEachAdv)

			if sumBlocks != len(te.mi.indexedBlocks) {
				t.Fatalf("Expected %d blocks to be indexed, but got %d", sumBlocks, len(te.mi.indexedBlocks))
			}
		})

	}, &quick.Config{
		MaxCount: 30,
	})
	require.NoError(t, err)
}

type indexBlock struct {
	adCid      cid.Cid
	ad         schema.Advertisement
	provider   peer.ID
	entryChunk ipld.Node
}

type processedUpToMeta struct {
	headCid     cid.Cid
	blockHeight uint
}

type MockIngester struct {
	mutex         sync.Mutex
	ds            datastore.Batching
	lsys          ipld.LinkSystem
	processedUpTo map[peer.ID]processedUpToMeta
	indexedBlocks []indexBlock
}

func newMockIngester() *MockIngester {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := util.NewProvLinkSystem(ds)
	return &MockIngester{
		processedUpTo: make(map[peer.ID]processedUpToMeta),
		ds:            ds,
		lsys:          lsys,
	}
}

func (mi *MockIngester) GetAd(ctx context.Context, c cid.Cid) (schema.Advertisement, error) {
	// Get data corresponding to the block.
	val, err := mi.GetBlock(ctx, c)
	if err != nil {
		log.Errorw("Error while fetching the node from datastore", "err", err)
	}
	if err != nil {
		return nil, err
	}

	nb := schema.Type.Advertisement.NewBuilder()
	err = dagjson.Decode(nb, bytes.NewReader(val))
	if err != nil {
		return nil, err
	}
	n := nb.Build()
	adv, ok := n.(schema.Advertisement)
	if !ok {
		return nil, errors.New("type assertion failed for advertisement")
	}

	return adv, nil
}

func (mi *MockIngester) GetBlock(ctx context.Context, c cid.Cid) ([]byte, error) {
	return mi.ds.Get(ctx, datastore.NewKey(c.String()))
}

func (mi *MockIngester) DeleteBlock(ctx context.Context, c cid.Cid) error {
	// We won't delete the block here because we don't have a way to get it back. A real implementation can get it back via syncdag.
	return nil
}

func (mi *MockIngester) GetProcessedUpTo(ctx context.Context, provider peer.ID) (cid.Cid, uint, error) {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()
	m, ok := mi.processedUpTo[provider]
	if !ok {
		return cid.Undef, 0, nil
	}
	return m.headCid, m.blockHeight, nil
}

func (mi *MockIngester) PutProcessedUpTo(ctx context.Context, p peer.ID, c cid.Cid, blockHeight uint) error {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()
	mi.processedUpTo[p] = processedUpToMeta{
		headCid:     c,
		blockHeight: blockHeight,
	}
	return nil
}
func (mi *MockIngester) SyncDag(ctx context.Context, peerID peer.ID, c cid.Cid, sel ipld.Node) (cid.Cid, error) {
	return c, nil
}

func (mi *MockIngester) IndexContentBlock(adCid cid.Cid, ad schema.Advertisement, provider peer.ID, entryChunk ipld.Node) error {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()
	mi.indexedBlocks = append(mi.indexedBlocks, indexBlock{
		adCid:      adCid,
		ad:         ad,
		provider:   provider,
		entryChunk: entryChunk,
	})
	return nil
}

func expectedNumberOfIndexedBlocks(numberOfMhChunksInEachAdv []uint8) int {
	sumBlocks := 0
	for _, b := range numberOfMhChunksInEachAdv {
		if b == 0 {
			// even if the ad didn't have and entry chunks it still gets indexed.
			sumBlocks++
		} else {
			sumBlocks += int(b)
		}
	}
	return sumBlocks
}

type testEnv struct {
	p       *Processor
	mi      *MockIngester
	provKey crypto.PrivKey
}

func setupTestEnv(t *testing.T) *testEnv {
	provKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	provider, err := peer.IDFromPrivateKey(provKey)
	require.NoError(t, err)

	mi := newMockIngester()

	p := NewUpdateProcessor(provider, mi)
	t.Cleanup(func() { p.Close() })
	return &testEnv{p: p, mi: mi, provKey: provKey}
}
