package adchainprocessor

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/require"
)

func TestIt(t *testing.T) {
	err := quick.Check(func(numberOfMhChunksInEachAdv []uint8) bool {
		t.Log("Testing with", numberOfMhChunksInEachAdv)
		if len(numberOfMhChunksInEachAdv) == 0 {
			return true
		}

		provKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		provider, err := peer.IDFromPrivateKey(provKey)
		require.NoError(t, err)

		mi := newMockIngester()

		p := NewUpdateProcessor(provider, mi, func() {})

		processorErr := make(chan error)
		go func() {
			processorErr <- p.Run()
		}()
		defer p.Close()

		advLink, err := util.RandomAdvChain(provKey, mi.lsys, numberOfMhChunksInEachAdv)
		require.NoError(t, err)

		p.QueueUpdate(advLink.ToCid())

		ch, cncl := p.OnAllAdApplied()
		defer cncl()

		select {
		case head := <-ch:
			if head.Head != advLink.ToCid() {
				t.Log("Didn't process to head")
				return false
			}
		case err = <-processorErr:
			t.Log("Processor error:", err)
			return false
		case <-time.After(3 * time.Second):
			t.Log("Timed out waiting for all advertisements to be applied")
			return false
		}

		mi.mutex.Lock()
		defer mi.mutex.Unlock()
		sumBlocks := 0
		for _, b := range numberOfMhChunksInEachAdv {
			if b == 0 {
				sumBlocks++
			} else {
				sumBlocks += int(b)
			}
		}

		if sumBlocks != len(mi.indexedBlocks) {
			t.Logf("Expected %d blocks to be indexed, but got %d", sumBlocks, len(mi.indexedBlocks))
			return false
		}
		return true

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

type MockIngester struct {
	mutex         sync.Mutex
	ds            datastore.Batching
	lsys          ipld.LinkSystem
	processedUpTo map[peer.ID]cid.Cid
	indexedBlocks []indexBlock
}

func newMockIngester() *MockIngester {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := util.NewProvLinkSystem(ds)
	return &MockIngester{
		processedUpTo: make(map[peer.ID]cid.Cid),
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
	b, err := mi.ds.Get(ctx, datastore.NewKey(c.String()))
	if err != nil {
		fmt.Println("Error while fetching the node from datastore", err)
		time.Sleep(1 * time.Second)
		b, err = mi.ds.Get(ctx, datastore.NewKey(c.String()))
		fmt.Println("Error 2 while fetching the node from datastore", err)
	}
	return b, err
}

func (mi *MockIngester) DeleteBlock(ctx context.Context, c cid.Cid) error {
	// We won't delete the block here because we don't have a way to get it back. A real implementation can get it back via syncdag.
	return nil
}

func (mi *MockIngester) GetProcessedUpTo(ctx context.Context, provider peer.ID) (cid.Cid, error) {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()
	return mi.processedUpTo[provider], nil
}

func (mi *MockIngester) PutProcessedUpTo(ctx context.Context, p peer.ID, c cid.Cid) error {
	mi.mutex.Lock()
	defer mi.mutex.Unlock()
	mi.processedUpTo[p] = c
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
