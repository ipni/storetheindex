package carstore_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"io/fs"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-test/random"
	car "github.com/ipld/go-car/v2"
	carblockstore "github.com/ipld/go-car/v2/blockstore"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/filestore"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testCompress = carstore.Gzip

	testEntriesChunkCount = 3
	testEntriesChunkSize  = 15
)

func TestWrite(t *testing.T) {
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

	// Check that datastore has ad and entries CID before reading to car.
	ok, err := dstore.Has(ctx, datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = dstore.Has(ctx, datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.True(t, ok)

	// Test that car file is created.
	carInfo, err := carw.Write(ctx, adCid, false, false)
	require.NoError(t, err)
	require.NotNil(t, carInfo)
	headInfo, err := fileStore.Head(ctx, carInfo.Path)
	require.NoError(t, err)
	require.Equal(t, carInfo.Path, headInfo.Path)
	require.Equal(t, carInfo.Size, headInfo.Size)
	t.Log("Created advertisement CAR file:", carInfo.Path)

	// Read CAR file and see that it has expected contents.
	_, r, err := fileStore.Get(ctx, carInfo.Path)
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	err = r.Close()
	require.NoError(t, err)

	if carw.Compression() == carstore.Gzip {
		gzr, err := gzip.NewReader(&buf)
		require.NoError(t, err)
		var ungzBuf bytes.Buffer
		_, err = io.Copy(&ungzBuf, gzr)
		require.NoError(t, err)
		gzr.Close()
		buf = ungzBuf
	}

	reader := bytes.NewReader(buf.Bytes())

	cbs, err := carblockstore.NewReadOnly(reader, nil)
	require.NoError(t, err)

	// Check that ad block is present.
	blk, err := cbs.Get(ctx, adCid)
	require.NoError(t, err, "failed to get ad block from car file")
	require.NotNil(t, blk)

	// Check that first entries block is present.
	blk, err = cbs.Get(ctx, entriesCid)
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

	// Check that there is 1 ad and 5 entries chunks stored.
	var count int
	err = itIdx.ForEach(func(mh multihash.Multihash, offset uint64) error {
		count++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1+entBlockCount, count)

	// Check that ad and entries block are no longer in datastore.
	ok, err = dstore.Has(ctx, datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = dstore.Has(ctx, datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestWriteToExistingAdCar(t *testing.T) {
	const entBlockCount = 1

	dstore := datastore.NewMapDatastore()
	metadata := []byte("car-test-metadata")

	adLink, ad, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, nil, dstore)
	adCid := adLink.(cidlink.Link).Cid
	entriesCid := ad.Entries.(cidlink.Link).Cid

	ctx := context.Background()

	// Check that datastore has ad and entries CID before reading to car.
	ok, err := dstore.Has(ctx, datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = dstore.Has(ctx, datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.True(t, ok)

	fileStore, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	fileName := adCid.String() + carstore.CarFileSuffix
	if testCompress == carstore.Gzip {
		fileName += carstore.GzipFileSuffix
	}

	_, err = fileStore.Put(ctx, fileName, nil)
	require.NoError(t, err)

	carw, err := carstore.NewWriter(dstore, fileStore, carstore.WithCompress(testCompress))
	require.NoError(t, err)

	carInfo, err := carw.Write(ctx, adCid, false, true)
	require.ErrorIs(t, err, fs.ErrExist)
	require.Zero(t, carInfo.Size)

	// Check that ad car file was not written to.
	fileInfo, err := fileStore.Head(ctx, carInfo.Path)
	require.NoError(t, err)
	require.Zero(t, fileInfo.Size)

	// Check that ad and entries block are no longer in datastore.
	ok, err = dstore.Has(ctx, datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = dstore.Has(ctx, datastore.NewKey(entriesCid.String()))
	require.NoError(t, err)
	require.False(t, ok)
}

func TestWriteChain(t *testing.T) {
	const entBlockCount = 5

	dstore := datastore.NewMapDatastore()
	metadata := []byte("car-test-metadata")

	carDir := t.TempDir()
	fileStore, err := filestore.NewLocal(carDir)
	require.NoError(t, err)
	carw, err := carstore.NewWriter(dstore, fileStore, carstore.WithCompress(testCompress))
	require.NoError(t, err)

	adLink1, _, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, nil, dstore)
	adLink2, _, _, _, _ := storeRandomIndexAndAd(t, entBlockCount, metadata, adLink1, dstore)
	adCid2 := adLink2.(cidlink.Link).Cid

	ctx := context.Background()

	count, err := carw.WriteChain(ctx, adCid2, false)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	// Test that car file is created.
	fileCh, errCh := fileStore.List(ctx, "", false)
	infos := make([]*filestore.File, 0, 2)
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 2, len(infos))
}

func newRandomLinkedList(t *testing.T, lsys ipld.LinkSystem, size int) (ipld.Link, []multihash.Multihash) {
	var out []multihash.Multihash
	var nextLnk ipld.Link
	for range size {
		mhs := random.Multihashes(testEntriesChunkSize)
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

func storeRandomIndexAndAd(t *testing.T, eChunkCount int, metadata []byte, prevLink ipld.Link, dstore datastore.Datastore) (ipld.Link, *schema.Advertisement, []multihash.Multihash, peer.ID, crypto.PrivKey) {
	lsys := mkProvLinkSystem(dstore)

	p, priv, _ := random.Identity()

	ctxID := []byte("test-context-id")
	if metadata == nil {
		metadata = []byte("test-metadata")
	}
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	adv := &schema.Advertisement{
		Provider:   p.String(),
		Addresses:  addrs,
		ContextID:  ctxID,
		Metadata:   metadata,
		PreviousID: prevLink,
	}
	var mhs []multihash.Multihash
	if eChunkCount == 0 {
		adv.Entries = schema.NoEntries
	} else {
		adv.Entries, mhs = newRandomLinkedList(t, lsys, eChunkCount)
	}

	err := adv.Sign(priv)
	require.NoError(t, err)

	node, err := adv.ToNode()
	require.NoError(t, err)

	advLnk, err := lsys.Store(ipld.LinkContext{}, schema.Linkproto, node)
	require.NoError(t, err)

	return advLnk, adv, mhs, p, priv
}
