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
	ErrKind, _ = tag.NewKey("errKind")
	Method, _  = tag.NewKey("method")
	Found, _   = tag.NewKey("found")
	Version, _ = tag.NewKey("version")
)

// Measures
var (
	FindLatency          = stats.Float64("find/latency", "Time to respond to a find request", stats.UnitMilliseconds)
	IngestChange         = stats.Int64("ingest/change", "Number of syncAdEntries started", stats.UnitDimensionless)
	AdIngestLatency      = stats.Float64("ingest/adsynclatency", "latency of syncAdEntries completed successfully", stats.UnitDimensionless)
	AdIngestErrorCount   = stats.Int64("ingest/adingestError", "Number of errors encountered while processing an ad", stats.UnitDimensionless)
	AdIngestQueued       = stats.Int64("ingest/adingestqueued", "Number of queued advertisements", stats.UnitDimensionless)
	AdIngestBacklog      = stats.Int64("ingest/adbacklog", "Queued backlog of adverts", stats.UnitDimensionless)
	AdIngestActive       = stats.Int64("ingest/adactive", "Active ingest workers", stats.UnitDimensionless)
	AdIngestSuccessCount = stats.Int64("ingest/adingestSuccess", "Number of successful ad ingest", stats.UnitDimensionless)
	AdIngestSkippedCount = stats.Int64("ingest/adingestSkipped", "Number of ads skipped during ingest", stats.UnitDimensionless)
	AdLoadError          = stats.Int64("ingest/adLoadError", "Number of times an ad failed to load", stats.UnitDimensionless)
	ProviderCount        = stats.Int64("provider/count", "Number of known (registered) providers", stats.UnitDimensionless)
	EntriesSyncLatency   = stats.Float64("ingest/entriessynclatency", "How long it took to sync an Ad's entries", stats.UnitMilliseconds)
	MhStoreNanoseconds   = stats.Int64("ingest/mhstorenanoseconds", "Average nanoseconds to store one multihash", stats.UnitDimensionless)
	IndexCount           = stats.Int64("provider/indexCount", "Number of indexes stored for all providers", stats.UnitDimensionless)
)

// Views
var (
	findLatencyView = &view.View{
		Measure:     FindLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
		TagKeys:     []tag.Key{Method, Found},
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
	providerView = &view.View{
		Measure:     ProviderCount,
		Aggregation: view.LastValue(),
	}
	entriesSyncLatencyView = &view.View{
		Measure:     EntriesSyncLatency,
		Aggregation: view.Distribution(0, 1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 1000, 2000, 5000),
	}
	adIngestError = &view.View{
		Measure:     AdIngestErrorCount,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{ErrKind},
	}
	adIngestQueued = &view.View{
		Measure:     AdIngestQueued,
		Aggregation: view.Count(),
	}
	adIngestBacklog = &view.View{
		Measure:     AdIngestBacklog,
		Aggregation: view.Distribution(0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024),
	}
	adIngestActive = &view.View{
		Measure:     AdIngestActive,
		Aggregation: view.Distribution(0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024),
	}
	adIngestSuccess = &view.View{
		Measure:     AdIngestSuccessCount,
		Aggregation: view.Count(),
	}
	adIngestSkipped = &view.View{
		Measure:     AdIngestSkippedCount,
		Aggregation: view.Count(),
	}
	adLoadError = &view.View{
		Measure:     AdLoadError,
		Aggregation: view.Count(),
	}
	mhStoreNanosecondsView = &view.View{
		Measure:     MhStoreNanoseconds,
		Aggregation: view.LastValue(),
	}
	indexCountView = &view.View{
		Measure:     IndexCount,
		Aggregation: view.LastValue(),
	}
)

var log = logging.Logger("indexer/metrics")

// Start creates an HTTP router for serving metric info
func Start(views []*view.View) http.Handler {
	// Register default views
	err := view.Register(
		findLatencyView,
		ingestChangeView,
		providerView,
		entriesSyncLatencyView,
		adIngestLatencyView,
		adIngestError,
		adIngestQueued,
		adIngestBacklog,
		adIngestActive,
		adIngestSkipped,
		adIngestSuccess,
		adLoadError,
		mhStoreNanosecondsView,
		indexCountView,
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
