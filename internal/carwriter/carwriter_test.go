package carwriter_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	car "github.com/ipld/go-car/v2"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/api/v0/ingest/schema"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/ipni/storetheindex/internal/carwriter"
	"github.com/ipni/storetheindex/internal/filestore"
	"github.com/ipni/storetheindex/test/util"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testEntriesChunkCount = 3
	testEntriesChunkSize  = 15
)

var rng = rand.New(rand.NewSource(1413))

func TestWrite(t *testing.T) {
	const entBlockCount = 5

	dstore := datastore.NewMapDatastore()
	metadata := []byte("car-test-metadata")

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	fileStore, err := filestore.New(cfg)
	require.NoError(t, err)
	carw := carwriter.New(dstore, fileStore)

	adCid, ad, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, dstore)
	entriesCid := ad.Entries.(cidlink.Link).Cid

	// Check that datastore has ad and entries CID before reading to car.
	ok, err := dstore.Has(context.Background(), datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = dstore.Has(context.Background(), datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.True(t, ok)

	// Test that car file is created.
	adCarPath, entCarPath, err := carw.Write(context.Background(), adCid, false)
	require.NoError(t, err)
	require.True(t, fsutil.FileExists(filepath.Join(carDir, adCarPath)))
	require.True(t, fsutil.FileExists(filepath.Join(carDir, entCarPath)))
	t.Log("Created advertisement CAR file:", adCarPath)
	t.Log("Created entries CAR file:", entCarPath)

	// Read CAR file and see that it has expected contents.
	_, r, err := fileStore.Get(context.Background(), adCarPath)
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	reader := bytes.NewReader(buf.Bytes())
	acbs, err := carblockstore.NewReadOnly(reader, nil)
	require.NoError(t, err)

	// Check that ad block is present.
	blk, err := acbs.Get(context.Background(), adCid)
	require.NoError(t, err, "failed to get ad block from car file")
	require.NotNil(t, blk)

	// Read CAR file and see that it has expected contents.
	_, r, err = fileStore.Get(context.Background(), entCarPath)
	require.NoError(t, err)
	buf.Reset()
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	reader.Reset(buf.Bytes())
	ecbs, err := carblockstore.NewReadOnly(reader, nil)
	require.NoError(t, err)

	// Check that first entries block is present.
	blk, err = ecbs.Get(context.Background(), entriesCid)
	require.NoError(t, err, "failed to get ad entried block from car file")
	require.NotNil(t, blk)

	// Check that the CAR is iterable.
	reader.Reset(buf.Bytes())
	cr, err := car.NewReader(reader)
	require.NoError(t, err)
	defer cr.Close()

	idxReader, err := cr.IndexReader()
	require.NoError(t, err)
	require.NotNil(t, idxReader, "CAR has no index")

	idx, err := carindex.ReadFrom(idxReader)
	require.NoError(t, err)

	codec := idx.Codec()
	t.Log("CAR codec:", codec)
	require.Equal(t, multicodec.CarMultihashIndexSorted, codec, "CAR index not iterable, wrong codec")
	itIdx, ok := idx.(carindex.IterableIndex)
	require.True(t, ok, "expected CAR index to implement index.IterableIndex interface")

	offset, err := carindex.GetFirst(itIdx, entriesCid)
	require.NoError(t, err)
	require.NotZero(t, offset)

	// Check that there are 5 chunks stored.
	var count int
	err = itIdx.ForEach(func(mh multihash.Multihash, offset uint64) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, entBlockCount, count)

	// Check that ad and entries block are no longer in datastore.
	ok, err = dstore.Has(context.Background(), datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = dstore.Has(context.Background(), datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestWriteToExistingAdCar(t *testing.T) {
	const entBlockCount = 1

	dstore := datastore.NewMapDatastore()
	metadata := []byte("car-test-metadata")

	adCid, ad, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, dstore)
	entriesCid := ad.Entries.(cidlink.Link).Cid

	// Check that datastore has ad and entries CID before reading to car.
	ok, err := dstore.Has(context.Background(), datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = dstore.Has(context.Background(), datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.True(t, ok)

	carDir := t.TempDir()
	adCarPath := filepath.Join(carDir, adCid.String()) + "_adv.car"
	f, err := os.Create(adCarPath)
	require.NoError(t, err)
	f.Close()

	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}
	fileStore, err := filestore.New(cfg)
	require.NoError(t, err)

	carw := carwriter.New(dstore, fileStore)

	adCarPath, entCarPath, err := carw.Write(context.Background(), adCid, false)
	require.NoError(t, err)

	// Check that ad car file was not written to.
	fileInfo, err := fileStore.Head(context.Background(), adCarPath)
	require.NoError(t, err)
	require.Zero(t, fileInfo.Size)

	// Check that entries car file was written to.
	fileInfo, err = fileStore.Head(context.Background(), entCarPath)
	require.NoError(t, err)
	require.NotZero(t, fileInfo.Size)

	// Check that ad and entries block are no longer in datastore.
	ok, err = dstore.Has(context.Background(), datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = dstore.Has(context.Background(), datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestWriteExistingAdsInStore(t *testing.T) {
	const entBlockCount = 5

	dstore := datastore.NewMapDatastore()
	metadata := []byte("car-test-metadata")

	adCid, ad, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, dstore)
	entriesCid := ad.Entries.(cidlink.Link).Cid

	// Check that datastore has ad and entries CID before reading to car.
	ok, err := dstore.Has(context.Background(), datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = dstore.Has(context.Background(), datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.True(t, ok)

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}
	fileStore, err := filestore.New(cfg)
	require.NoError(t, err)

	carw := carwriter.New(dstore, fileStore)

	countChan := carw.WriteExisting(context.Background())
	n := <-countChan
	require.Equal(t, 1, n)
	require.True(t, fsutil.FileExists(filepath.Join(carDir, adCid.String())+"_adv.car"))
	require.True(t, fsutil.FileExists(filepath.Join(carDir, entriesCid.String())+"_mhs.car"))

	// Check that ad and entries block are no longer in datastore.
	ok, err = dstore.Has(context.Background(), datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = dstore.Has(context.Background(), datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
}

func newRandomLinkedList(t *testing.T, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash) {
	var out []multihash.Multihash
	var nextLnk ipld.Link
	for i := 0; i < size; i++ {
		mhs := util.RandomMultihashes(testEntriesChunkSize, rng)
		chunk := &schema.EntryChunk{
			Entries: mhs,
			Next:    nextLnk,
		}
		node, err := chunk.ToNode()
		require.NoError(t, err)
		lnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
		require.NoError(t, err)
		out = append(out, mhs...)
		nextLnk = lnk
	}
	return nextLnk, out
}

func mkProvLinkSystem(ds datastore.Datastore) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func storeRandomIndexAndAd(t *testing.T, eChunkCount int, metadata []byte, dstore datastore.Datastore) (cid.Cid, *schema.Advertisement, []multihash.Multihash, peer.ID, crypto.PrivKey) {
	lsys := mkProvLinkSystem(dstore)

	priv, pubKey, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)

	p, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)

	ctxID := []byte("test-context-id")
	if metadata == nil {
		metadata = []byte("test-metadata")
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	adv := &schema.Advertisement{
		Provider:  p.String(),
		Addresses: addrs,
		ContextID: ctxID,
		Metadata:  metadata,
	}
	var mhs []multihash.Multihash
	if eChunkCount == 0 {
		adv.Entries = schema.NoEntries
	} else {
		adv.Entries, mhs = newRandomLinkedList(t, lsys, eChunkCount)
	}

	err = adv.Sign(priv)
	require.NoError(t, err)

	node, err := adv.ToNode()
	require.NoError(t, err)

	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)

	return advLnk.(cidlink.Link).Cid, adv, mhs, p, priv
}
