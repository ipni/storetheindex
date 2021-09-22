package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds exported metrics.
var Metrics = prometheus.NewRegistry()

// The individual metrics being tracked within storetheindex
var (
	IngestCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_counter",
		Help: "Ingest Change events received",
	}, []string{"method"})

	ProviderCounter = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "provider_registry",
		Help: "Known (registered) providers",
	})
)

// Start creates an HTTP router for serving metric info
func Start() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(Metrics, promhttp.HandlerOpts{Registry: Metrics}))
	return mux
}
