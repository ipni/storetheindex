package carmirror

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/filestore"
)

var log = logging.Logger("indexer/carmirror")

const pathPrefix = "/carmirror/"

type Server struct {
	server          *http.Server
	listener        net.Listener
	shutdownTimeout time.Duration
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, fs filestore.Interface, options ...Option) (*Server, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	handler, err := filestore.NewHTTPHandler(fs)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	s := &Server{
		server: &http.Server{
			Handler: mux,
		},
		listener:        l,
		shutdownTimeout: opts.shutdownTimeout,
	}

	mux.Handle(pathPrefix, http.StripPrefix("/carmirror", handler))

	return s, nil
}

func (s *Server) Start() error {
	log.Infow("carmirror http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("carmirror http server shutdown")

	ctx := context.Background()
	if s.shutdownTimeout > 0 {
		tctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
		ctx = tctx
	}

	return s.server.Shutdown(ctx)
}
