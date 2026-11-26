package migrate

import (
	"context"
	"iter"
	"testing"
	"time"

	"github.com/ipni/go-indexer-core/importer"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// testGatedTarget is a value store whose writes of records wait until gate
// closes, once entered is signalled
type testGatedTarget struct {
	importer.Target
	entered chan struct{}
	gate    chan struct{}
}

func (g *testGatedTarget) Write(ctx context.Context, kind importer.Kind, entries iter.Seq2[importer.Entry, error]) error {
	if kind == importer.KindRecords {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-g.gate
	}
	return g.Target.Write(ctx, kind, entries)
}

// testGauge returns the last value of the view of measure name with tags
func testGauge(t *testing.T, name string, tags ...tag.Tag) float64 {
	t.Helper()
	rows, err := view.RetrieveData(name)
	require.NoError(t, err)
	for _, row := range rows {
		if len(row.Tags) == len(tags) && (len(tags) == 0 || row.Tags[0] == tags[0]) {
			return row.Data.(*view.LastValueData).Value
		}
	}
	require.FailNow(t, "no row", "%s %v", name, tags)
	return 0
}

func TestImportStatus(t *testing.T) {
	require.NoError(t, view.Register(Views...))
	t.Cleanup(func() { view.Unregister(Views...) })
	s := newTestStores(t)
	ctx := t.Context()
	vs := &testGatedTarget{Target: s.dstVals.(importer.Target), entered: make(chan struct{}, 1), gate: make(chan struct{})}
	im := New(testDatastore(t), s.source, vs, true, importer.WithWorkers(4), importer.WithStagger(0))

	// Before Run, nothing has started
	st := im.Status()
	require.True(t, st.Started.IsZero())
	require.Len(t, st.Steps, 9)
	require.Equal(t, StepStatus{Name: "adProcessed", State: "pending"}, st.Steps[0])
	require.Nil(t, st.Running)
	require.NoError(t, im.SetWorkers(3), "a number set before Run applies to its kinds")

	errc := make(chan error, 1)
	go func() { errc <- im.Run(ctx) }()
	select {
	case <-vs.entered:
	case err := <-errc:
		require.FailNow(t, "the import ended before the records", "%v", err)
	}

	st = im.Status()
	require.False(t, st.Started.IsZero())
	want := map[string]string{
		"adProcessed": "done", "adSkipReason": "done", "sync": "done", "providers": "done", "syncRecheck": "done",
		"records": "running", "contextMap": "pending", "validate": "pending", "multihashes": "pending",
	}
	for _, step := range st.Steps {
		require.Equal(t, want[step.Name], step.State, step.Name)
	}
	require.NotNil(t, st.Running)
	require.Equal(t, importer.KindRecords, st.Running.Kind)
	require.Equal(t, 3, st.Running.Workers)
	require.Positive(t, st.Running.PartsInFlight)
	require.EqualValues(t, 1, testGauge(t, "migrate/step_state", tag.Tag{Key: stepTag, Value: "records"}))
	require.EqualValues(t, 3, testGauge(t, "migrate/workers"))

	// The number of workers applies to the running kind
	require.NoError(t, im.SetWorkers(2))
	require.Equal(t, 2, im.Status().Running.Workers)
	require.EqualValues(t, 2, testGauge(t, "migrate/workers"))
	require.Error(t, im.SetWorkers(-1))
	require.Error(t, im.SetWorkers(importer.MaxWorkers+1))

	close(vs.gate)
	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		require.FailNow(t, "the import did not finish")
	}
	st = im.Status()
	require.Nil(t, st.Running)
	for _, step := range st.Steps {
		require.Equal(t, "done", step.State, step.Name)
		require.EqualValues(t, 2, testGauge(t, "migrate/step_state", tag.Tag{Key: stepTag, Value: step.Name}))
	}
}
