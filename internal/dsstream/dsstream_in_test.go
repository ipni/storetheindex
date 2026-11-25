package dsstream

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/ipni/go-indexer-core/importer/storeutil"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/stretchr/testify/require"
)

// testRawEntries returns the datastore's keys and values
func testRawEntries(t *testing.T, ds *leveldb.Datastore) []importer.Entry {
	t.Helper()
	it := ds.DB.NewIterator(nil, nil)
	defer it.Release()
	var out []importer.Entry
	for it.Next() {
		out = append(out, importer.Entry{Key: bytes.Clone(it.Key()), Value: bytes.Clone(it.Value())})
	}
	require.NoError(t, it.Error())
	return out
}

// testWithPrefixes returns the entries of each kind with their datastore keys,
// sorted by key
func testWithPrefixes(t *testing.T, byKind map[importer.Kind][]importer.Entry) []importer.Entry {
	var out []importer.Entry
	for kind, entries := range byKind {
		prefix, err := kindPrefix(kind)
		require.NoError(t, err)
		for _, e := range entries {
			out = append(out, importer.Entry{Key: append([]byte(prefix), e.Key...), Value: e.Value})
		}
	}
	slices.SortFunc(out, func(a, b importer.Entry) int { return bytes.Compare(a.Key, b.Key) })
	return out
}

func TestDatastoreToDatastore(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	srcDS := openTestDatastore(t, t.TempDir())
	want := fillTestDatastore(t, srcDS)
	src := NewSource(srcDS)

	dstDir := filepath.Join(t.TempDir(), "dst")
	dstDS := openTestDatastore(t, dstDir)
	dst := NewTarget(dstDS)
	for _, kind := range testKinds {
		importertest.Copy(t, rng, src, dst, kind, 16<<10, 4)
	}
	require.NoError(t, dstDS.Close())

	// The copy survives a reopen and holds only the sent entries
	dstDS = openTestDatastore(t, dstDir)
	require.Equal(t, testWithPrefixes(t, want), testRawEntries(t, dstDS))
}

// testEntriesOf yields entries, then err when it is not nil
func testEntriesOf(err error, entries ...importer.Entry) iter.Seq2[importer.Entry, error] {
	return func(yield func(importer.Entry, error) bool) {
		for _, e := range entries {
			if !yield(e, nil) {
				return
			}
		}
		if err != nil {
			yield(importer.Entry{}, err)
		}
	}
}

func TestTargetRefuses(t *testing.T) {
	ds := openTestDatastore(t, t.TempDir())
	dst := NewTarget(ds)
	ctx := t.Context()
	for _, tc := range []struct {
		name  string
		kind  importer.Kind
		entry importer.Entry
	}{
		{"empty key", importer.KindProviders, importer.Entry{Key: nil, Value: []byte("v")}},
		{"key with a slash", importer.KindSync, importer.Entry{Key: []byte("a/b"), Value: []byte("v")}},
		{"key of a slash", importer.KindAdSkipReason, importer.Entry{Key: []byte("/"), Value: []byte("v")}},
		{"empty value", importer.KindAdSkipReason, importer.Entry{Key: []byte("k"), Value: nil}},
		{"unprocessed marker", importer.KindAdProcessed, importer.Entry{Key: []byte("k"), Value: []byte{0}}},
		{"resync marker", importer.KindAdProcessed, importer.Entry{Key: []byte("k"), Value: testMarker(dskey.AdMarkerResync, 1)}},
		{"unknown marker", importer.KindAdProcessed, importer.Entry{Key: []byte("k"), Value: []byte{4}}},
		{"empty marker", importer.KindAdProcessed, importer.Entry{Key: []byte("k"), Value: nil}},
		{"unknown kind", importer.KindRecords, importer.Entry{Key: []byte("k"), Value: []byte("v")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := dst.Write(ctx, tc.kind, testEntriesOf(nil, tc.entry))
			require.True(t, importer.IsPermanent(err), "%v", err)
		})
	}
	require.Empty(t, testRawEntries(t, ds), "a refused entry was written")

	t.Run("entries error", func(t *testing.T) {
		boom := errors.New("boom")
		err := dst.Write(ctx, importer.KindSync, testEntriesOf(boom, importer.Entry{Key: []byte("k"), Value: []byte("v")}))
		require.ErrorIs(t, err, boom)
		require.False(t, importer.IsPermanent(err))
	})
	t.Run("cancelled part way", func(t *testing.T) {
		// The first entry fills a batch, which is committed, and the context
		// is cancelled before the second
		big := bytes.Repeat([]byte("v"), storeutil.CommitBytes)
		cctx, cancel := context.WithCancel(ctx)
		entries := func(yield func(importer.Entry, error) bool) {
			if yield(importer.Entry{Key: []byte("a"), Value: big}, nil) {
				cancel()
				yield(importer.Entry{Key: []byte("b"), Value: []byte("v")}, nil)
			}
		}
		err := dst.Write(cctx, importer.KindProviders, entries)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, importer.IsPermanent(err))
		require.Equal(t, []importer.Entry{{Key: []byte(providersPrefix + "a"), Value: big}}, testRawEntries(t, ds))
	})
}

func TestTargetRewrites(t *testing.T) {
	ds := openTestDatastore(t, t.TempDir())
	dst := NewTarget(ds)
	ctx := t.Context()
	part := []importer.Entry{
		{Key: []byte("a"), Value: testMarker(dskey.AdMarkerProcessed, 1)},
		{Key: []byte("b"), Value: testMarker(dskey.AdMarkerSkipped, 2)},
	}
	require.NoError(t, dst.Write(ctx, importer.KindAdProcessed, testEntriesOf(nil, part...)))
	first := testRawEntries(t, ds)
	require.Len(t, first, 2)
	require.NoError(t, dst.Write(ctx, importer.KindAdProcessed, testEntriesOf(nil, part...)))
	require.NoError(t, dst.Sync())
	require.Equal(t, first, testRawEntries(t, ds), "writing a part again changes the datastore")
}

func TestTargetClosedDatastore(t *testing.T) {
	ds := openTestDatastore(t, t.TempDir())
	require.NoError(t, ds.Close())

	// A target's write error is permanent
	err := NewTarget(ds).Write(t.Context(), importer.KindSync, testEntriesOf(nil, importer.Entry{Key: []byte("k"), Value: []byte("v")}))
	require.True(t, importer.IsPermanent(err), "%v", err)
}
