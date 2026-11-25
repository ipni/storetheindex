package dsstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/freeze"
	"github.com/stretchr/testify/require"
	goleveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var testKinds = []importer.Kind{importer.KindAdProcessed, importer.KindAdSkipReason, importer.KindSync, importer.KindProviders}

func openTestDatastore(t *testing.T, dir string) *leveldb.Datastore {
	t.Helper()
	ds, err := leveldb.NewDatastore(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

func setTestFrozenCheckBytes(t *testing.T, n int) {
	old := frozenCheckBytes
	frozenCheckBytes = n
	t.Cleanup(func() { frozenCheckBytes = old })
}

// testMarker returns a marker of status, with the time it was written for
// every other ad, as the ingester writes it, and without for the rest, as
// legacy markers are
func testMarker(status byte, i int) []byte {
	if i%2 == 0 {
		return []byte{status}
	}
	return binary.LittleEndian.AppendUint64([]byte{status}, uint64(1_700_000_000_000_000+i))
}

// fillTestDatastore writes ad markers of every status, skip reasons, /sync
// entries and provider records into ds, with keys just outside each kind's
// prefix and keys the source skips. Part of the data is compacted into
// tables and part stays in the memtable. It returns the entries the source
// sends, by kind.
func fillTestDatastore(t *testing.T, ds *leveldb.Datastore) map[importer.Kind][]importer.Entry {
	ctx := t.Context()
	want := make(map[importer.Kind][]importer.Entry)
	put := func(key string, value []byte) {
		require.NoError(t, ds.DB.Put([]byte(key), value, nil))
	}
	sent := func(kind importer.Kind, key string, value []byte) {
		prefix, err := kindPrefix(kind)
		require.NoError(t, err)
		put(prefix+key, value)
		want[kind] = append(want[kind], importer.Entry{Key: []byte(key), Value: value})
	}

	ads := random.Cids(4000)
	status := make(map[cid.Cid]byte, len(ads))
	b, err := ds.Batch(ctx)
	require.NoError(t, err)
	for i, ad := range ads {
		s := byte(i % 4)
		status[ad] = s
		key := dskey.AdProcessedPrefix + ad.String()
		require.NoError(t, b.Put(ctx, datastore.RawKey(key), testMarker(s, i)))
		if s == dskey.AdMarkerProcessed || s == dskey.AdMarkerSkipped {
			want[importer.KindAdProcessed] = append(want[importer.KindAdProcessed], importer.Entry{Key: []byte(ad.String()), Value: testMarker(s, i)})
		}
	}
	require.NoError(t, b.Commit(ctx))
	require.NoError(t, ds.DB.CompactRange(util.Range{}))

	for i, ad := range ads[:400] {
		if status[ad] == dskey.AdMarkerSkipped {
			sent(importer.KindAdSkipReason, ad.String(), fmt.Appendf(nil, "reason %d", i))
		}
	}
	// A publisher whose latest ad is marked for resync is not sent
	pubs := random.Peers(40)
	for i, pub := range pubs[:36] {
		ad := ads[i]
		if status[ad] == dskey.AdMarkerResync {
			put(dskey.SyncPrefix+pub.String(), ad.Bytes())
		} else {
			sent(importer.KindSync, pub.String(), ad.Bytes())
		}
	}
	// An ad with no marker, and a value that is not a CID, are sent
	sent(importer.KindSync, pubs[36].String(), random.Cids(1)[0].Bytes())
	sent(importer.KindSync, pubs[37].String(), []byte("not a cid"))
	for i, p := range random.Peers(30) {
		sent(importer.KindProviders, p.String(), fmt.Appendf(nil, `{"provider":%d}`, i))
	}

	// Keys the source skips: an empty value, and the prefix alone
	put(dskey.AdSkipReasonPrefix+ads[1].String(), nil)
	put(providersPrefix, []byte("prefix alone"))
	// Keys just outside each prefix, and other keys of the datastore
	for _, prefix := range []string{dskey.AdProcessedPrefix, dskey.AdSkipReasonPrefix, dskey.SyncPrefix, providersPrefix} {
		base := prefix[:len(prefix)-1]
		put(base, []byte("x"))
		put(base+".", []byte("x"))
		put(base+"0", []byte("x"))
		put(base+"0a", []byte("x"))
	}
	put("/adF/"+ads[0].String(), []byte{1})
	put("/registry/pinfo", []byte("x"))
	put("/registry/assignment/x", []byte("x"))

	for _, entries := range want {
		slices.SortFunc(entries, func(a, b importer.Entry) int { return bytes.Compare(a.Key, b.Key) })
	}
	return want
}

// requireTestPartSizes checks that no part opened with a size hint holds more
// than twice the hint, plus its largest entry, by which a read overshoots.
func requireTestPartSizes(t *testing.T, parts []importertest.Part) {
	t.Helper()
	for _, p := range parts {
		if p.Hint == 0 {
			continue
		}
		var largest uint64
		for _, e := range p.Entries {
			largest = max(largest, uint64(len(e.Key)+len(e.Value)))
		}
		require.LessOrEqual(t, p.Bytes(), 2*p.Hint+largest, "a part far above its size hint")
	}
}

func TestSourceParts(t *testing.T) {
	// Small frozen checks, so that a part takes many of them
	setTestFrozenCheckBytes(t, 4<<10)
	rng := rand.New(rand.NewPCG(1, 2))
	ds := openTestDatastore(t, t.TempDir())
	want := fillTestDatastore(t, ds)
	src := NewSource(ds)

	for _, kind := range testKinds {
		parts := importertest.ReadParts(t, rng, src, kind, 16<<10)
		require.Equal(t, want[kind], importertest.Entries(parts), kind)
		requireTestPartSizes(t, parts)
		if kind == importer.KindAdProcessed {
			require.Greater(t, len(parts), 5, "markers are cut in parts")
		}
	}
}

func TestSourceFrozen(t *testing.T) {
	setTestFrozenCheckBytes(t, 64)
	ctx := t.Context()
	ds := openTestDatastore(t, t.TempDir())
	for _, c := range random.Cids(20) {
		require.NoError(t, ds.Put(ctx, datastore.NewKey(dskey.AdProcessedPrefix+c.String()), []byte{dskey.AdMarkerProcessed}))
	}
	src := NewSource(ds)

	// Frozen part way through a part: the batches after it are refused
	_, entries, err := src.OpenStream(ctx, importer.KindAdProcessed, nil, []byte{0xff}, 0)
	require.NoError(t, err)
	n := 0
	for _, err = range entries {
		if n == 0 {
			freezer, ferr := freeze.New([]string{t.TempDir()}, 99, ds, func() error { return nil })
			require.NoError(t, ferr)
			require.NoError(t, freezer.Freeze())
			freezer.Close()
		}
		n++
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, errFrozen)
	require.Less(t, n, 20, "a frozen source sent the whole part")

	// Frozen when a part is opened
	for _, kind := range testKinds {
		_, _, err = src.OpenStream(ctx, kind, nil, nil, 0)
		require.ErrorIs(t, err, errFrozen)
		require.True(t, importer.IsPermanent(err))
	}
}

func TestSourceRefuses(t *testing.T) {
	ds := openTestDatastore(t, t.TempDir())
	src := NewSource(ds)
	ctx := t.Context()
	for _, tc := range []struct {
		name       string
		kind       importer.Kind
		start, end []byte
	}{
		{"unknown kind", importer.KindRecords, nil, nil},
		{"end equals start", importer.KindSync, []byte("a"), []byte("a")},
		{"end before start", importer.KindSync, []byte("b"), []byte("a")},
		{"empty end", importer.KindSync, nil, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := src.OpenStream(ctx, tc.kind, tc.start, tc.end, 0)
			require.True(t, importer.IsPermanent(err), "%v", err)
		})
	}

	t.Run("cancelled", func(t *testing.T) {
		require.NoError(t, ds.Put(ctx, datastore.NewKey(dskey.SyncPrefix+"p"), random.Cids(1)[0].Bytes()))
		cctx, cancel := context.WithCancel(ctx)
		_, entries, err := src.OpenStream(cctx, importer.KindSync, nil, nil, 0)
		require.NoError(t, err)
		cancel()
		n := 0
		for _, err := range entries {
			require.ErrorIs(t, err, context.Canceled)
			n++
		}
		require.Equal(t, 1, n)
	})
}

// testFailingStorage fails to open LevelDB's tables while fail is set
type testFailingStorage struct {
	storage.Storage
	fail atomic.Bool
}

var errTestInjected = errors.New("injected")

func (s *testFailingStorage) Open(fd storage.FileDesc) (storage.Reader, error) {
	if fd.Type == storage.TypeTable && s.fail.Load() {
		return nil, errTestInjected
	}
	return s.Storage.Open(fd)
}

// TestSourceReadError checks that a failed read of the markers fails an open
// part when it is opened, and a closed part when it is read, with a
// transient error.
func TestSourceReadError(t *testing.T) {
	stor := &testFailingStorage{Storage: storage.NewMemStorage()}
	db, err := goleveldb.Open(stor, nil)
	require.NoError(t, err)
	for _, c := range random.Cids(10) {
		require.NoError(t, db.Put([]byte(dskey.AdProcessedPrefix+c.String()), []byte{dskey.AdMarkerProcessed}, nil))
	}
	require.NoError(t, db.CompactRange(util.Range{}))
	require.NoError(t, db.Close())

	// The frozen key is outside the table's keys, so only reads of the
	// markers open the table.
	db, err = goleveldb.Open(stor, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	stor.fail.Store(true)
	src := NewSource(&leveldb.Datastore{DB: db})
	ctx := t.Context()

	_, _, err = src.OpenStream(ctx, importer.KindAdProcessed, nil, nil, 0)
	require.ErrorIs(t, err, errTestInjected)
	require.False(t, importer.IsPermanent(err))

	_, entries, err := src.OpenStream(ctx, importer.KindAdProcessed, nil, []byte{0xff}, 0)
	require.NoError(t, err)
	for _, err = range entries {
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, errTestInjected)
	require.False(t, importer.IsPermanent(err))
}

func TestSourceClosedDatastore(t *testing.T) {
	ds := openTestDatastore(t, t.TempDir())
	require.NoError(t, ds.Close())

	// A source's read error is transient
	_, _, err := NewSource(ds).OpenStream(t.Context(), importer.KindSync, nil, nil, 0)
	require.Error(t, err)
	require.False(t, importer.IsPermanent(err))
}
