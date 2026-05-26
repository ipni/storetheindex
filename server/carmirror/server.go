package carmirror

import (
	"context"
	"fmt"
	"net"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/filestore"
)

var log = logging.Logger("indexer/carmirror")

const pathPrefix = "/carmirror/"

type Server struct {
	server   *http.Server
	listener net.Listener
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func New(listen string, fs filestore.Interface) (*Server, error) {
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
		listener: l,
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
	return s.server.Shutdown(context.Background())
}
