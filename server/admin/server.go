package admin

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	coremetrics "github.com/ipni/go-indexer-core/metrics"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/metrics/pprof"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("indexer/admin")

type Server struct {
	cancel          context.CancelFunc
	handler         *adminHandler
	listener        net.Listener
	server          *http.Server
	shutdownTimeout time.Duration
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, id peer.ID, indexer indexer.Interface, ingester *ingest.Ingester, reg *registry.Registry, reloadErrChan chan<- chan error, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	server := &http.Server{
		Handler:      mux,
		WriteTimeout: opts.writeTimeout,
		ReadTimeout:  opts.readTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())
	h := newHandler(ctx, id, indexer, ingester, reg, reloadErrChan)

	s := &Server{
		cancel:          cancel,
		handler:         h,
		listener:        l,
		server:          server,
		shutdownTimeout: opts.shutdownTimeout,
	}

	// Admin routes
	mux.HandleFunc("DELETE /removeprovider/", h.removeProvider)
	mux.HandleFunc("PUT /freeze", h.freeze)
	mux.HandleFunc("GET /status", h.status)
	mux.HandleFunc("GET /healthcheck", h.healthCheckHandler)
	mux.HandleFunc("POST /importproviders", h.importProviders)
	mux.HandleFunc("POST /reloadconfig", h.reloadConfig)
	mux.HandleFunc("PUT /markadprocessed/", h.markAdProcessed)

	// Ingester routes
	mux.HandleFunc("PUT /ingest/allow/", h.allowPeer)
	mux.HandleFunc("PUT /ingest/block/", h.blockPeer)
	mux.HandleFunc("POST /ingest/sync/", h.handlePostSyncs)
	mux.HandleFunc("GET /ingest/sync/", h.handleGetSyncs)

	// Assignment routes
	mux.HandleFunc("POST /ingest/assign/", h.assignPeer)
	mux.HandleFunc("GET /ingest/assigned", h.listAssignedPeers)
	mux.HandleFunc("POST /ingest/handoff/", h.handoffPeer)
	mux.HandleFunc("PUT /ingest/unassign/", h.unassignPeer)
	mux.HandleFunc("GET /ingest/preferred", h.listPreferredPeers)

	// Metrics routes
	mux.Handle("/metrics/", metrics.Start(slices.Concat(
		coremetrics.DefaultViews,
		coremetrics.PebbleViews,
		coremetrics.MeteringViews,
		coremetrics.MeteringProviderViews,
	)))
	mux.Handle("/debug/pprof/", pprof.WithProfile())

	// Telemetry routes
	mux.HandleFunc("GET /telemetry/providers", h.listTelemetry)
	mux.HandleFunc("GET /telemetry/providers/", h.getTelemetry)

	// Metering routes
	mux.HandleFunc("GET /metering", h.meteringStats)
	mux.HandleFunc("GET /metering/providers", h.meteringProviders)
	mux.HandleFunc("GET /metering/providers/{providerID}", h.meteringProvider)
	mux.HandleFunc("GET /metering/scan", h.meteringScan)
	mux.HandleFunc("POST /metering/scan", h.meteringTriggerScan)
	mux.HandleFunc("DELETE /metering/scan", h.meteringCancelScan)
	mux.HandleFunc("GET /metering/scan/{providerID}", h.meteringProviderScan)

	// Config routes
	mux.HandleFunc("POST /config/log/level", setLogLevel)
	mux.HandleFunc("GET /config/log/subsystems", listLogSubSystems)

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("admin http server shutdown")
	s.cancel() // stop any sync in progress
	s.handler.pendingSyncs.Wait()

	ctx := context.Background()
	if s.shutdownTimeout > 0 {
		tctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
		ctx = tctx
	}

	return s.server.Shutdown(ctx)
}
