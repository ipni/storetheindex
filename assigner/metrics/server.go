package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/internal/metrics/pprof"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var log = logging.Logger("assigner/metrics")

const defaultShutdownTimeout = 30 * time.Second

// Server serves Prometheus metrics and pprof endpoints.
type Server struct {
	server   *http.Server
	listener net.Listener
}

func New(listen string) (*Server, error) {
	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/metrics/", promhttp.Handler())
	mux.Handle("/debug/pprof/", pprof.WithProfile())

	return &Server{
		server: &http.Server{
			Handler:      mux,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		listener: l,
	}, nil
}

func (s *Server) Start() error {
	log.Infow("metrics server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}
