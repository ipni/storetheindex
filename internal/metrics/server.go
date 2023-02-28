package metrics

import (
	"context"
	"net"
	"net/http"

	"github.com/cockroachdb/pebble"
	"github.com/ipni/go-indexer-core/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Sti  *stiMetrics
	Core *metrics.Metrics
	s    *http.Server
)

func Start(metricsAddr string, pebbleMetrcisProvider func() *pebble.Metrics) error {
	var err error
	Core, err = metrics.New(pebbleMetrcisProvider)
	if err != nil {
		return err
	}

	Sti, err = newStiMetrics()
	if err != nil {
		return err
	}

	s = &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux(),
	}

	err = Sti.Start()
	if err != nil {
		return err
	}

	err = Core.Start()
	if err != nil {
		return err
	}

	mln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}

	go func() { _ = s.Serve(mln) }()

	log.Infow("Metrics server started", "addr", mln.Addr())
	return nil

}

func Shutdown(ctx context.Context) {
	err := Sti.Shutdwon()
	if err != nil {
		log.Warnw("Error shutting down sti metrics", "err", err)
		err = nil
	}
	err = Core.Shutdown()
	if err != nil {
		log.Warnw("Error shutting down core metrics", "err", err)
		err = nil
	}
	err = s.Shutdown(ctx)
	if err != nil {
		log.Warnw("Error shutting down the metrics server", "err", err)
		err = nil
	}
}

func metricsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
