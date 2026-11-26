package migrate

import (
	"context"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// stepTag is the name of an import's step
var stepTag, _ = tag.NewKey("step")

// The import's state gauges. Each is recorded when the state it shows
// changes, and when an import loads its state on start.
var (
	stepStateMeasure = stats.Int64("migrate/step_state", "State of each step of the import into this indexer: 0 pending, 1 running, 2 done", stats.UnitDimensionless)
	workersMeasure   = stats.Int64("migrate/workers", "Number of workers set for the import's kinds; 0 means paused", stats.UnitDimensionless)
)

// Views are the OpenCensus views of an import's state
var Views = []*view.View{
	{Measure: stepStateMeasure, Aggregation: view.LastValue(), TagKeys: []tag.Key{stepTag}},
	{Measure: workersMeasure, Aggregation: view.LastValue()},
}

// recordSteps records the state of each step of st
func recordSteps(st State) {
	for i, step := range st.Steps {
		var v int64
		switch stepState(st, i) {
		case "running":
			v = 1
		case "done":
			v = 2
		}
		err := stats.RecordWithTags(context.Background(), []tag.Mutator{tag.Upsert(stepTag, step.Name)}, stepStateMeasure.M(v))
		if err != nil {
			log.Errorw("Cannot record the step state", "err", err)
		}
	}
}

// recordWorkers records the number of workers set
func recordWorkers(n int) {
	stats.Record(context.Background(), workersMeasure.M(int64(n)))
}
