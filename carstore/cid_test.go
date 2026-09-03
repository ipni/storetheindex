package carstore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipni/storetheindex/filestore"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestVerifyCID(t *testing.T) {
	data := []byte("hello")
	c, err := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256}.Sum(data)
	require.NoError(t, err)
	require.NoError(t, verifyCID(c, data))

	err = verifyCID(c, []byte("other"))
	require.ErrorIs(t, err, ErrUnusable)

	err = verifyCID(cid.Undef, data)
	require.ErrorIs(t, err, ErrUnusable)
}

func TestReadRejectsWrongFirstBlock(t *testing.T) {
	adData := []byte("ad-payload")
	other := []byte("other-payload")
	adCid, err := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256}.Sum(adData)
	require.NoError(t, err)
	otherCid, err := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256}.Sum(other)
	require.NoError(t, err)

	ctx := context.Background()
	fileStore := putTestCar(t, adCid, []testCarBlock{
		{otherCid, other},
		{adCid, adData},
	})

	carr, err := NewReader(fileStore)
	require.NoError(t, err)
	_, err = carr.Read(ctx, adCid, true)
	require.ErrorIs(t, err, ErrUnusable)
}

func TestReadRejectsAdvertisementHashMismatch(t *testing.T) {
	adData := []byte("ad-payload")
	adCid, err := cid.V1Builder{Codec: cid.Raw, MhType: multihash.SHA2_256}.Sum(adData)
	require.NoError(t, err)

	ctx := context.Background()
	fileStore := putTestCar(t, adCid, []testCarBlock{{adCid, []byte("not-the-ad")}})

	carr, err := NewReader(fileStore)
	require.NoError(t, err)
	_, err = carr.Read(ctx, adCid, true)
	require.ErrorIs(t, err, ErrUnusable)
}

type testCarBlock struct {
	cid  cid.Cid
	data []byte
}

func putTestCar(t *testing.T, root cid.Cid, blocks []testCarBlock) filestore.Interface {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tmp.car")
	f, err := os.Create(path)
	require.NoError(t, err)
	carStore, err := carstorage.NewWritable(f, []cid.Cid{root})
	require.NoError(t, err)
	for _, b := range blocks {
		require.NoError(t, carStore.Put(context.Background(), b.cid.KeyString(), b.data))
	}
	require.NoError(t, carStore.Finalize())
	require.NoError(t, f.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	fileStore, err := filestore.NewLocal(dir)
	require.NoError(t, err)
	carr, err := NewReader(fileStore)
	require.NoError(t, err)
	_, err = fileStore.Put(context.Background(), carr.CarPath(root), bytes.NewReader(data))
	require.NoError(t, err)
	return fileStore
}
