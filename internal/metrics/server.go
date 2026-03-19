package metrics

import (
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	stiNS = "storetheindex"

	errKindTag = "errKind"
	foundTag   = "found"
)

// Measures
var (
	FindLatency = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: stiNS,
			Name:      "find_latency",
			Help:      "Time to respond to a find request in milliseconds",
		},
		[]string{foundTag},
	)

	AdIngestLatency = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: stiNS,
			Name:      "ingest_ad_sync_latency",
			Help:      "Time to ingest an ad milliseconds",
		},
	)

	AdIngestErrorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: stiNS,
			Name:      "ingest_ad_ingest_error",
			Help:      "Number of errors encountered while processing an ad",
		},
		[]string{errKindTag},
	)

	AdIngestActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: stiNS,
			Name:      "ingest_ad_active",
			Help:      "Active ingest workers",
		},
	)

	AdIngestSuccessCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: stiNS,
			Name:      "ingest_ad_success",
			Help:      "Number of successful ad ingests",
		},
	)

	AdIngestSkippedCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: stiNS,
			Name:      "ingest_ad_skipped",
			Help:      "Number of ads skipped during ingest",
		},
	)

	ProviderCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: stiNS,
			Name:      "provider_count",
			Help:      "Number of known (registered) providers",
		},
	)

	EntriesSyncLatency = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: stiNS,
			Name:      "ingest_entries_sync_latency",
			Help:      "Time to sync an ad's multihashes in milliseconds",
		},
	)

	PercentUsage = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: stiNS,
			Name:      "percent_usage",
			Help:      "Percent usage of storage available in value store",
		},
	)

	NonRemoveAdCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: stiNS,
			Name:      "ingest_nonremove_ad_count",
			Help:      "Number of non-removal advertisements",
		},
	)

	RemoveAdCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: stiNS,
			Name:      "ingest_remove_ad_count",
			Help:      "Number of removal advertisements",
		},
	)
)

var log = logging.Logger("indexer/metrics")

// Start creates an HTTP router for serving metric info
func Start() http.Handler {
	coremetrics.PromRegistry.MustRegister(
		FindLatency, AdIngestLatency, AdIngestErrorCount, AdIngestActive, AdIngestSuccessCount,
		AdIngestSkippedCount, ProviderCount, EntriesSyncLatency, PercentUsage,
		NonRemoveAdCount, RemoveAdCount,
	)

	// Bitswap metrics (via go-metrics-interface) register with the default
	// registry, so we gather from both our custom registry and the default.
	gatherers := prometheus.Gatherers{
		coremetrics.PromRegistry,
		prometheus.DefaultGatherer,
	}

	mux := http.NewServeMux()

	// Register prometheus metrics endpoint
	mux.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))

	return mux
}
