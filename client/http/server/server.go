package httpserver

import (
	"context"
	"net"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/client"
	httpclient "github.com/filecoin-project/storetheindex/client/http/client"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("httpserver")

type Server struct {
	server *http.Server
	l      net.Listener
	engine *indexer.Engine
}

// Endpoint returns the endpoint of the protocol server.
func (s *Server) Endpoint() client.Endpoint {
	return httpclient.HTTPEndpoint("http://" + s.l.Addr().String())
}

func New(listen string, e *indexer.Engine, options ...ServerOption) (*Server, error) {
	var cfg serverConfig
	if err := cfg.apply(append([]ServerOption{serverDefaults}, options...)...); err != nil {
		return nil, err
	}
	var err error

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)
	server := &http.Server{
		Handler:      r,
		WriteTimeout: cfg.apiWriteTimeout,
		ReadTimeout:  cfg.apiReadTimeout,
	}
	s := &Server{server, l, e}

	// Set protocol handlers
	// Import routes
	r.HandleFunc("/import/manifest/{minerid}", s.ImportManifestHandler).Methods("POST")
	r.HandleFunc("/import/cidlist/{minerid}", s.ImportCidListHandler).Methods("POST")

	// Client routes
	r.HandleFunc("/cid/{cid}", s.GetSingleCidHandler).Methods("GET")
	r.HandleFunc("/cid", s.GetBatchCidHandler).Methods("POST")

	// Admin routes
	r.HandleFunc("/healthcheck", s.HealthCheckHandler).Methods("GET")

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("api listening", "addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
