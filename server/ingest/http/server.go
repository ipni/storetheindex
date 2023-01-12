package httpingestserver

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
)

var log = logging.Logger("indexer/ingest")

type Server struct {
	server *http.Server
	l      net.Listener
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.l.Addr().String())
}

func New(listen string, indexer indexer.Interface, ingester *ingest.Ingester, registry *registry.Registry, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)
	server := &http.Server{
		Handler:      r,
		WriteTimeout: opts.writeTimeout,
		ReadTimeout:  opts.readTimeout,
	}
	s := &Server{server, l}

	h := newHandler(indexer, ingester, registry)

	// Advertisement routes
	r.HandleFunc("/ingest/announce", h.announce).Methods(http.MethodPut)

	// Discovery
	r.HandleFunc("/discover", h.discoverProvider).Methods(http.MethodPost)

	// Registration routes
	r.HandleFunc("/register", h.registerProvider).Methods(http.MethodPost)
	r.HandleFunc("/register/{providerid}", h.removeProvider).Methods(http.MethodDelete)
	return s, nil
}

func (s *Server) Start() error {
	log.Infow("ingest http server listening", "listen_addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Close() error {
	log.Info("ingest http server shutdown")
	return s.server.Shutdown(context.Background())
}
