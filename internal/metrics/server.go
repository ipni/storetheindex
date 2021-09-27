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
	IngestCIDs    = stats.Int64("ingest/cids", "Number of CIDs ingested", stats.UnitDimensionless)
	IngestChange  = stats.Int64("ingest/change", "Number of ingest triggers received", stats.UnitDimensionless)
	ProviderCount = stats.Int64("provider/count", "Number of know (registered) providers", stats.UnitDimensionless)
)

// Views
var (
	IngestCIDView = &view.View{
		Measure:     IngestCIDs,
		Aggregation: view.Count(),
	}
	IngestChangeView = &view.View{
		Measure:     IngestChange,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{Method},
	}
	ProviderView = &view.View{
		Measure:     ProviderCount,
		Aggregation: view.Count(),
	}
)

var DefaultViews = []*view.View{
	IngestCIDView,
	IngestChangeView,
	ProviderView,
}

var log = logging.Logger("metrics")

// Start creates an HTTP router for serving metric info
func Start() http.Handler {
	registry, ok := promclient.DefaultRegisterer.(*promclient.Registry)
	if !ok {
		log.Warnf("failed to export default prometheus registry; some metrics will be unavailable; unexpected type: %T", promclient.DefaultRegisterer)
	}
	exporter, err := prometheus.NewExporter(prometheus.Options{
		Registry:  registry,
		Namespace: "lotus",
	})
	if err != nil {
		log.Errorf("could not create the prometheus stats exporter: %v", err)
	}

	return exporter
}
