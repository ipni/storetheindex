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

	Method, _          = tag.NewKey("method")
	AdIngestErrType, _ = tag.NewKey("ErrorType")
)

// Measures
var (
	FindLatency              = stats.Float64("find/latency", "Time to respond to a find request", stats.UnitMilliseconds)
	IngestChange             = stats.Int64("ingest/change", "Number of syncAdEntries started", stats.UnitDimensionless)
	AdSyncedCount            = stats.Int64("ingest/adsync", "Number of syncAdEntries completed successfully", stats.UnitDimensionless)
	AdIngestError            = stats.Int64("ingest/adingesterror", "Number of errors encountered during syncAdEntries", stats.UnitDimensionless)
	EntryChunkAlreadyPresent = stats.Int64("ingest/entryChunkAlreadyPresent", "Number of times we early return an ad ingest because we already have the entryChunk. This is an anamoly and would happen when an indexer crashes while processing an entry chunk.", stats.UnitDimensionless)
	AdIngestLatency          = stats.Float64("ingest/adsynclatency", "latency of syncAdEntries completed successfully", stats.UnitDimensionless)
	ProviderCount            = stats.Int64("provider/count", "Number of known (registered) providers", stats.UnitDimensionless)
	EntriesSyncLatency       = stats.Float64("ingest/entriessynclatency", "How long it took to sync an Ad's entries", stats.UnitMilliseconds)
)

// Views
var (
	findLatencyView = &view.View{
		Measure:     FindLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
	adIngestLatencyView = &view.View{
		Measure:     AdIngestLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
	ingestChangeView = &view.View{
		Measure:     IngestChange,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Method},
	}
	adSyncCountView = &view.View{
		Measure:     AdSyncedCount,
		Aggregation: view.Count(),
	}
	adIngestErrorView = &view.View{
		Measure:     AdIngestError,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{AdIngestErrType},
	}
	entryChunkAlreadyPresentView = &view.View{
		Measure:     EntryChunkAlreadyPresent,
		Aggregation: view.Count(),
	}
	providerView = &view.View{
		Measure:     ProviderCount,
		Aggregation: view.LastValue(),
	}
	entriesSyncLatencyView = &view.View{
		Measure:     EntriesSyncLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
)

var log = logging.Logger("indexer/metrics")

// Start creates an HTTP router for serving metric info
func Start(views []*view.View) http.Handler {
	// Register default views
	err := view.Register(
		adIngestErrorView,
		adIngestLatencyView,
		adSyncCountView,
		entriesSyncLatencyView,
		entryChunkAlreadyPresentView,
		findLatencyView,
		ingestChangeView,
		providerView,
	)

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
