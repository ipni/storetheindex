package migrate

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/dsstream"
	"github.com/stretchr/testify/require"
)

const testToken = "test-token"

// testStores are an import's source and destination stores
type testStores struct {
	srcDS, dstDS     *leveldb.Datastore
	srcVals, dstVals indexer.Interface
	// source is a client of a sender of every kind of the source stores,
	// with small parts
	source importer.Source
}

func newTestStores(t *testing.T) *testStores {
	t.Helper()
	s := &testStores{
		srcDS:   testDatastore(t),
		dstDS:   testDatastore(t),
		srcVals: testValueStore(t),
		dstVals: testValueStore(t),
	}
	fillTestDatastore(t, s.srcDS)
	fillTestValueStore(t, s.srcVals)
	s.source = testSourceClient(t, s.srcDS, s.srcVals)
	return s
}

// testSourceClient returns a client of a sender of every kind of ds and
// vals
func testSourceClient(t *testing.T, ds *leveldb.Datastore, vals indexer.Interface) importer.Source {
	sources := make(map[importer.Kind]importer.Source)
	for _, kind := range dsstream.Kinds {
		sources[kind] = dsstream.NewSource(ds)
	}
	for _, kind := range pebble.Kinds {
		sources[kind] = vals.(importer.Source)
	}
	return testClient(t, sources)
}

func testDatastore(t *testing.T) *leveldb.Datastore {
	ds, err := leveldb.NewDatastore(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

func testValueStore(t *testing.T) indexer.Interface {
	s, err := pebble.New(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fillTestDatastore puts markers, skip reasons, syncs and provider records
// into ds
func fillTestDatastore(t *testing.T, ds *leveldb.Datastore) {
	t.Helper()
	put := func(key string, value []byte) {
		require.NoError(t, ds.DB.Put([]byte(key), value, nil))
	}
	for i, c := range random.Cids(2000) {
		status := dskey.AdMarkerProcessed
		if i%10 == 0 {
			status = dskey.AdMarkerSkipped
			put(dskey.AdSkipReasonPrefix+c.String(), fmt.Appendf(nil, "reason %d", i))
		}
		put(dskey.AdProcessedPrefix+c.String(), []byte{status})
		if i%5 == 0 {
			put(dskey.SyncPrefix+fmt.Sprintf("publisher-%04d", i), c.Bytes())
			put(dskey.ProviderInfoPath+fmt.Sprintf("/provider-%04d", i), fmt.Appendf(nil, `{"provider":%d}`, i))
		}
	}
}

// fillTestValueStore puts values of several providers and contexts into s
func fillTestValueStore(t *testing.T, s indexer.Interface) {
	t.Helper()
	for i, p := range random.Peers(4) {
		for c := range 4 {
			v := indexer.Value{
				ProviderID:    p,
				ContextID:     fmt.Appendf(nil, "context-%d-%d", i, c),
				MetadataBytes: fmt.Appendf(nil, "metadata-%d-%d", i, c),
			}
			require.NoError(t, s.Put(v, random.Multihashes(300)...))
		}
	}
	require.NoError(t, s.Flush())
}

// testClient returns a client of a sender of sources, with parts of about
// 16 KiB
func testClient(t *testing.T, sources map[importer.Kind]importer.Source) importer.Source {
	t.Helper()
	h, err := importhttp.NewSender(sources, testToken, importhttp.WithPartBytes(16<<10))
	require.NoError(t, err)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := importhttp.NewClient(srv.URL, testToken, importhttp.WithHTTPClient(srv.Client()))
	require.NoError(t, err)
	return c
}

// requireImported checks that every kind of the destination stores holds
// the source's entries
func (s *testStores) requireImported(t *testing.T) {
	t.Helper()
	rng := rand.New(rand.NewPCG(1, 2))
	entries := func(src importer.Source, kind importer.Kind) []importer.Entry {
		return importertest.Entries(importertest.ReadParts(t, rng, src, kind, 0))
	}
	for _, kind := range dsstream.Kinds {
		want := entries(dsstream.NewSource(s.srcDS), kind)
		require.NotEmpty(t, want, kind)
		require.Equal(t, want, entries(dsstream.NewSource(s.dstDS), kind), kind)
	}
	for _, kind := range pebble.Kinds {
		want := entries(s.srcVals.(importer.Source), kind)
		require.NotEmpty(t, want, kind)
		require.Equal(t, want, entries(s.dstVals.(importer.Source), kind), kind)
	}
}

// testFailingSource fails kind permanently once it has opened it after
// times
type testFailingSource struct {
	importer.Source
	kind  importer.Kind
	after int64
	opens atomic.Int64
}

func (s *testFailingSource) OpenStream(ctx context.Context, kind importer.Kind, start, end []byte, hint uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	if kind == s.kind && s.opens.Add(1) > s.after {
		return nil, nil, importer.PermanentError(errors.New("stopped"))
	}
	return s.Source.OpenStream(ctx, kind, start, end, hint)
}

func TestRun(t *testing.T) {
	s := newTestStores(t)
	require.NoError(t, Run(t.Context(), s.dstDS, s.source, s.dstVals.(importer.Target),
		importer.WithWorkers(4), importer.WithStagger(0)))
	s.requireImported(t)

	st, err := LoadState(t.Context(), s.dstDS)
	require.NoError(t, err)
	require.False(t, st.Started.IsZero())
	for _, step := range []Step{st.AdProcessed, st.AdSkipReason, st.Sync, st.Providers, st.Records, st.ContextMap, st.Multihashes} {
		require.Equal(t, Step{Done: true}, step)
	}
}

// TestRunResumes stops an import part way through a kind, and checks that
// the next run skips the kinds done, resumes the kind from its progress and
// finishes the import
func TestRunResumes(t *testing.T) {
	s := newTestStores(t)
	failing := &testFailingSource{Source: s.source, kind: importer.KindAdProcessed, after: 3}
	err := Run(t.Context(), s.dstDS, failing, s.dstVals.(importer.Target), importer.WithWorkers(1), importer.WithStagger(0))
	require.ErrorContains(t, err, "stopped")
	st, err := LoadState(t.Context(), s.dstDS)
	require.NoError(t, err)
	require.False(t, st.AdProcessed.Done)
	progress := st.AdProcessed.Progress
	require.NotNil(t, progress, "the kind stored its progress")

	// The kind resumes from its progress: its first open starts there
	resumed := &testFirstStart{Source: s.source}
	require.NoError(t, Run(t.Context(), s.dstDS, resumed, s.dstVals.(importer.Target), importer.WithWorkers(4), importer.WithStagger(0)))
	require.Equal(t, progress, resumed.first)
	s.requireImported(t)

	// A done import opens nothing
	none := &testFailingSource{Source: s.source, kind: importer.KindAdProcessed}
	require.NoError(t, Run(t.Context(), s.dstDS, none, s.dstVals.(importer.Target)))
}

// testFirstStart records the start of the first part opened
type testFirstStart struct {
	importer.Source
	first  []byte
	opened atomic.Bool
}

func (s *testFirstStart) OpenStream(ctx context.Context, kind importer.Kind, start, end []byte, hint uint64) ([]byte, iter.Seq2[importer.Entry, error], error) {
	if s.opened.CompareAndSwap(false, true) {
		s.first = start
	}
	return s.Source.OpenStream(ctx, kind, start, end, hint)
}

func TestLoadStateNone(t *testing.T) {
	st, err := LoadState(t.Context(), testDatastore(t))
	require.NoError(t, err)
	require.Equal(t, State{}, st)
}

// testStateFailing is a datastore whose reads or writes of the import
// state fail
type testStateFailing struct {
	*leveldb.Datastore
	getErr, putErr error
}

func (d testStateFailing) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	if key.String() == dskey.MigrateState && d.getErr != nil {
		return nil, d.getErr
	}
	return d.Datastore.Get(ctx, key)
}

func (d testStateFailing) Put(ctx context.Context, key datastore.Key, value []byte) error {
	if key.String() == dskey.MigrateState && d.putErr != nil {
		return d.putErr
	}
	return d.Datastore.Put(ctx, key, value)
}

// testSyncFailing is a target whose Sync fails after ok of them succeed
type testSyncFailing struct {
	importer.Target
	ok    int64
	syncs atomic.Int64
}

func (d *testSyncFailing) Sync() error {
	if d.syncs.Add(1) > d.ok {
		return errors.New("sync failed")
	}
	return d.Target.Sync()
}

func TestRunErrors(t *testing.T) {
	s := newTestStores(t)
	ctx := t.Context()
	opts := []importer.ManagerOption{importer.WithStagger(0)}

	t.Run("state does not decode", func(t *testing.T) {
		ds := testDatastore(t)
		require.NoError(t, ds.Put(ctx, datastore.NewKey(dskey.MigrateState), []byte("not json")))
		require.ErrorContains(t, Run(ctx, ds, s.source, s.dstVals.(importer.Target), opts...), "decode")
	})
	t.Run("state cannot be read", func(t *testing.T) {
		ds := testStateFailing{Datastore: testDatastore(t), getErr: errors.New("disk read")}
		require.ErrorContains(t, Run(ctx, ds, s.source, s.dstVals.(importer.Target), opts...), "disk read")
	})
	t.Run("state cannot be stored", func(t *testing.T) {
		ds := testStateFailing{Datastore: testDatastore(t), putErr: errors.New("disk full")}
		require.ErrorContains(t, Run(ctx, ds, s.source, s.dstVals.(importer.Target), opts...), "disk full")
	})
	t.Run("target sync fails at a progress", func(t *testing.T) {
		// Records and the context map are one part each here, synced at
		// their end; the multihashes report progress after their first
		// part
		dst := &testSyncFailing{Target: testValueStore(t).(importer.Target), ok: 2}
		require.ErrorContains(t, Run(ctx, testDatastore(t), s.source, dst, opts...), "sync failed")
	})
	t.Run("target sync fails at the end of a kind", func(t *testing.T) {
		// An empty kind is one part, with no progress before its end
		dst := &testSyncFailing{Target: testValueStore(t).(importer.Target)}
		require.ErrorContains(t, Run(ctx, testDatastore(t), testSourceClient(t, s.srcDS, testValueStore(t)), dst, opts...), "sync failed")
	})
}

func TestCheckFinished(t *testing.T) {
	s := newTestStores(t)
	ctx := t.Context()
	require.NoError(t, CheckFinished(ctx, s.dstDS), "no import has started")

	failing := &testFailingSource{Source: s.source, kind: importer.KindRecords}
	require.Error(t, Run(ctx, s.dstDS, failing, s.dstVals.(importer.Target), importer.WithStagger(0)))
	require.ErrorIs(t, CheckFinished(ctx, s.dstDS), ErrUnfinished)

	require.NoError(t, Run(ctx, s.dstDS, s.source, s.dstVals.(importer.Target), importer.WithStagger(0)))
	require.NoError(t, CheckFinished(ctx, s.dstDS))

	bad := testDatastore(t)
	require.NoError(t, bad.Put(ctx, datastore.NewKey(dskey.MigrateState), []byte("not json")))
	require.ErrorContains(t, CheckFinished(ctx, bad), "decode")
}
