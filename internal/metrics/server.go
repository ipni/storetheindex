package metrics

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	"contrib.go.opencensus.io/exporter/prometheus"
	promclient "github.com/prometheus/client_golang/prometheus"
)

// Global Tags
var (
	Version, _ = tag.NewKey("version")

	Method, _ = tag.NewKey("method")
)

// Measures
var (
	FindLatency   = stats.Float64("find/latency", "Time to respond to a find request", stats.UnitMilliseconds)
	IngestChange  = stats.Int64("ingest/change", "Number of ingest triggers received", stats.UnitDimensionless)
	ProviderCount = stats.Int64("provider/count", "Number of know (registered) providers", stats.UnitDimensionless)
	SyncLatency   = stats.Float64("ingest/synclatency", "Time for sync to complete", stats.UnitMilliseconds)
)

// Views
var (
	findLatencyView = &view.View{
		Measure:     FindLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
	ingestChangeView = &view.View{
		Measure:     IngestChange,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Method},
	}
	providerView = &view.View{
		Measure:     ProviderCount,
		Aggregation: view.LastValue(),
	}
	syncLatencyView = &view.View{
		Measure:     SyncLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
)

var log = logging.Logger("indexer/metrics")

// Start creates an HTTP router for serving metric info
func Start(views []*view.View) http.Handler {
	// Register default views
	err := view.Register(findLatencyView, ingestChangeView, providerView, syncLatencyView)
	if err != nil {
		log.Errorf("cannot register metrics default views: %s", err)
	}
	// Register other views
	err = view.Register(views...)
	if err != nil {
		log.Errorf("cannot register metrics views: %s", err)
	}
	registry, ok := promclient.DefaultRegisterer.(*promclient.Registry)
	if !ok {
		log.Warnf("failed to export default prometheus registry; some metrics will be unavailable; unexpected type: %T", promclient.DefaultRegisterer)
	}
	exporter, err := prometheus.NewExporter(prometheus.Options{
		Registry:  registry,
		Namespace: "storetheindex",
	})
	if err != nil {
		log.Errorf("could not create the prometheus stats exporter: %v", err)
	}

	return exporter
}
