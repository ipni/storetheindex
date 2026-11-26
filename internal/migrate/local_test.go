package migrate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/dsstream"
	"github.com/stretchr/testify/require"
)

// TestRecheckSync checks that the re-check drops the copied /sync entries
// whose ad the source has since marked for resync, keeps those that moved
// on, and does nothing once done
func TestRecheckSync(t *testing.T) {
	ctx := t.Context()
	src, dst := testDatastore(t), testDatastore(t)
	ads := random.Cids(4)
	put := func(ds interface {
		Put(context.Context, datastore.Key, []byte) error
	}, key string, value []byte) {
		require.NoError(t, ds.Put(ctx, datastore.NewKey(key), value))
	}
	for _, c := range ads {
		put(src, dskey.AdProcessedPrefix+c.String(), []byte{dskey.AdMarkerProcessed})
	}
	// p0 is unchanged, p1's ad is now marked for resync on the source, p2
	// moved on to a newer ad, and p3 is no longer sent at all
	put(dst, dskey.SyncPrefix+"p0", ads[0].Bytes())
	put(dst, dskey.SyncPrefix+"p1", ads[1].Bytes())
	put(dst, dskey.SyncPrefix+"p2", ads[2].Bytes())
	put(dst, dskey.SyncPrefix+"p3", ads[3].Bytes())
	put(src, dskey.SyncPrefix+"p0", ads[0].Bytes())
	put(src, dskey.SyncPrefix+"p1", ads[1].Bytes())
	put(src, dskey.AdProcessedPrefix+ads[1].String(), []byte{dskey.AdMarkerResync})
	put(src, dskey.SyncPrefix+"p2", ads[3].Bytes())

	sources := make(map[importer.Kind]importer.Source)
	for _, kind := range dsstream.Kinds {
		sources[kind] = dsstream.NewSource(src)
	}
	r := &run{ds: dst, state: newState(), opts: []importer.ManagerOption{importer.WithStagger(0)}}
	require.NoError(t, r.recheckSync(ctx, testClient(t, sources)))
	require.True(t, r.state.step(stepSyncRecheck).Done)

	for publisher, want := range map[string][]byte{"p0": ads[0].Bytes(), "p1": nil, "p2": ads[2].Bytes(), "p3": nil} {
		got, err := dst.Get(ctx, datastore.NewKey(dskey.SyncPrefix+publisher))
		if want == nil {
			require.ErrorIs(t, err, datastore.ErrNotFound, publisher)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, want, got, publisher)
	}
	st, err := LoadState(ctx, dst)
	require.NoError(t, err)
	require.True(t, st.step(stepSyncRecheck).Done, "the step is stored done")

	// A done re-check reads nothing
	none := &testFailingSource{Source: testClient(t, sources), kind: importer.KindSync}
	require.NoError(t, r.recheckSync(ctx, none))

	// A source that cannot be read stops the step
	r.state.step(stepSyncRecheck).Done = false
	require.ErrorContains(t, r.recheckSync(ctx, none), "again")
}

// TestStateDone checks that an import is done only when every one of its
// steps is, and that CheckFinished refuses it until then
func TestStateDone(t *testing.T) {
	ctx := t.Context()
	all := newState()
	all.Started = time.Now()
	for i := range all.Steps {
		all.Steps[i].Done = true
	}
	require.True(t, all.Done())
	for _, name := range stepNames {
		st := State{Started: all.Started, Steps: slices.Clone(all.Steps)}
		st.step(name).Done = false
		require.False(t, st.Done(), name)

		ds := testDatastore(t)
		require.NoError(t, (&run{ds: ds, state: st}).save(ctx))
		require.ErrorIs(t, CheckFinished(ctx, ds), ErrUnfinished, name)
	}
	ds := testDatastore(t)
	require.NoError(t, (&run{ds: ds, state: all}).save(ctx))
	require.NoError(t, CheckFinished(ctx, ds))
}

// testFailingDatastore fails the queries, batches or batch operations of a
// datastore
type testFailingDatastore struct {
	*leveldb.Datastore
	queryErr, resultErr, batchErr, deleteErr, commitErr error
}

func (d testFailingDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	if d.resultErr != nil {
		sent := false
		return query.ResultsFromIterator(q, query.Iterator{Next: func() (query.Result, bool) {
			if sent {
				return query.Result{}, false
			}
			sent = true
			return query.Result{Error: d.resultErr}, true
		}}), nil
	}
	return d.Datastore.Query(ctx, q)
}

func (d testFailingDatastore) Batch(ctx context.Context) (datastore.Batch, error) {
	if d.batchErr != nil {
		return nil, d.batchErr
	}
	b, err := d.Datastore.Batch(ctx)
	return testFailingBatch{Batch: b, deleteErr: d.deleteErr, commitErr: d.commitErr}, err
}

type testFailingBatch struct {
	datastore.Batch
	deleteErr, commitErr error
}

func (b testFailingBatch) Delete(ctx context.Context, key datastore.Key) error {
	if b.deleteErr != nil {
		return b.deleteErr
	}
	return b.Batch.Delete(ctx, key)
}

func (b testFailingBatch) Commit(ctx context.Context) error {
	if b.commitErr != nil {
		return b.commitErr
	}
	return b.Batch.Commit(ctx)
}

// TestRecheckSyncErrors checks that a failure to list or drop the copied
// /sync entries stops the re-check, which is then not done
func TestRecheckSyncErrors(t *testing.T) {
	ctx := t.Context()
	src := testDatastore(t)
	sources := make(map[importer.Kind]importer.Source)
	for _, kind := range dsstream.Kinds {
		sources[kind] = dsstream.NewSource(src)
	}
	client := testClient(t, sources)
	fail := errors.New("disk")
	for name, ds := range map[string]testFailingDatastore{
		"query":  {queryErr: fail},
		"result": {resultErr: fail},
		"batch":  {batchErr: fail},
		"delete": {deleteErr: fail},
		"commit": {commitErr: fail},
	} {
		t.Run(name, func(t *testing.T) {
			ds.Datastore = testDatastore(t)
			// A copied entry that the source does not send, so it is dropped
			require.NoError(t, ds.Put(ctx, datastore.NewKey(dskey.SyncPrefix+"p0"), []byte("ad")))
			r := &run{ds: ds, state: newState(), opts: []importer.ManagerOption{importer.WithStagger(0)}}
			require.ErrorIs(t, r.recheckSync(ctx, client), fail)
			require.False(t, r.state.step(stepSyncRecheck).Done)
		})
	}
}

// TestRunRecheckFails checks that an import whose re-check fails stops
func TestRunRecheckFails(t *testing.T) {
	ctx := t.Context()
	ds := testDatastore(t)
	r := &run{ds: ds, state: newState()}
	r.state.Started = time.Now()
	for _, name := range []string{stepAdProcessed, stepAdSkipReason, stepSync, stepProviders} {
		r.state.step(name).Done = true
	}
	require.NoError(t, r.save(ctx))
	s := newTestStores(t)
	failing := &testFailingSource{Source: s.source, kind: importer.KindSync}
	require.ErrorContains(t, New(ds, failing, s.dstVals.(importer.Target), true, importer.WithStagger(0)).Run(ctx), "again")
}

func TestKeySet(t *testing.T) {
	k := &keySet{keys: make(map[string]struct{})}
	err := k.Write(t.Context(), importer.KindSync, func(yield func(importer.Entry, error) bool) {
		if yield(importer.Entry{Key: []byte("a")}, nil) {
			yield(importer.Entry{}, errors.New("cut off"))
		}
	})
	require.ErrorContains(t, err, "cut off")
	require.Contains(t, k.keys, "a")
	require.NoError(t, k.Sync())
}

// testValidator is a value store whose ValidateRecords counts through
// positions "1" to "4", with one deletion each, and fails at position
// failAt, where "" is the first
type testValidator struct {
	importer.Target
	froms  []string
	failAt string
}

func (v *testValidator) ValidateRecords(_ context.Context, from []byte, limit int) ([]byte, bool, int, error) {
	if limit != validateBatch {
		return nil, false, 0, fmt.Errorf("limit %d", limit)
	}
	v.froms = append(v.froms, string(from))
	if string(from) == v.failAt {
		return nil, false, 0, errors.New("disk read")
	}
	var i int
	if from != nil {
		_, _ = fmt.Sscan(string(from), &i)
	}
	if i == 4 {
		return nil, true, 1, nil
	}
	return fmt.Append(nil, i+1), false, 1, nil
}

func TestValidateRecordsStep(t *testing.T) {
	ctx := t.Context()
	ds := testDatastore(t)
	r := &run{ds: ds, state: newState()}

	// A failure part way keeps the position stored before it
	v := &testValidator{failAt: "3"}
	require.ErrorContains(t, r.validateRecords(ctx, v, true), "disk read")
	require.Equal(t, []string{"", "1", "2", "3"}, v.froms)
	st, err := LoadState(ctx, ds)
	require.NoError(t, err)
	require.Equal(t, Step{Name: stepValidate, Progress: []byte("3")}, *st.step(stepValidate))

	// The next run resumes from the stored position
	r = &run{ds: ds, state: st}
	v = &testValidator{failAt: "never"}
	require.NoError(t, r.validateRecords(ctx, v, true))
	require.Equal(t, []string{"3", "4"}, v.froms)
	st, err = LoadState(ctx, ds)
	require.NoError(t, err)
	require.Equal(t, Step{Name: stepValidate, Done: true}, *st.step(stepValidate))

	// A done step validates nothing
	require.NoError(t, r.validateRecords(ctx, v, true))
	require.Len(t, v.froms, 2)

	// A value store that cannot validate is imported without the check
	r = &run{ds: testDatastore(t), state: newState()}
	require.NoError(t, r.validateRecords(ctx, &keySet{keys: make(map[string]struct{})}, true))
	require.True(t, r.state.step(stepValidate).Done)

	// A check that is turned off part way is done without resuming
	ds = testDatastore(t)
	r = &run{ds: ds, state: newState()}
	r.state.step(stepValidate).Progress = []byte("2")
	v = &testValidator{failAt: "never"}
	require.NoError(t, r.validateRecords(ctx, v, false))
	require.Empty(t, v.froms)
	st, err = LoadState(ctx, ds)
	require.NoError(t, err)
	require.Equal(t, Step{Name: stepValidate, Done: true}, *st.step(stepValidate))

	// A failure to store the position stops the step
	failing := testStateFailing{Datastore: testDatastore(t), putErr: errors.New("disk full")}
	r = &run{ds: failing, state: newState()}
	require.ErrorContains(t, r.validateRecords(ctx, &testValidator{failAt: "never"}, true), "disk full")
}

// TestRunValidateFails checks that an import whose records cannot be
// validated stops before the multihashes
func TestRunValidateFails(t *testing.T) {
	s := newTestStores(t)
	ctx := t.Context()
	ds := testDatastore(t)
	v := &testValidator{Target: s.dstVals.(importer.Target), failAt: ""}
	require.ErrorContains(t, New(ds, s.source, v, true, importer.WithStagger(0)).Run(ctx), "disk read")
	st, err := LoadState(ctx, ds)
	require.NoError(t, err)
	require.True(t, st.step(stepContextMap).Done)
	require.False(t, st.step(stepValidate).Done)
	require.False(t, st.step(stepMultihashes).Done)
}

// TestRunWithoutValidate checks that an import without validation copies
// everything and never validates
func TestRunWithoutValidate(t *testing.T) {
	s := newTestStores(t)
	ctx := t.Context()
	ds := testDatastore(t)
	v := &testValidator{Target: s.dstVals.(importer.Target), failAt: ""}
	require.NoError(t, New(ds, s.source, v, false, importer.WithStagger(0)).Run(ctx))
	require.Empty(t, v.froms)
	require.NoError(t, CheckFinished(ctx, ds))
}
