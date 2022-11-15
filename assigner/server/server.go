package server

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/filecoin-project/storetheindex/announce/gossiptopic"
	"github.com/filecoin-project/storetheindex/assigner/core"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("assigner/server")

type Server struct {
	assigner *core.Assigner
	server   *http.Server
	listener net.Listener
}

func New(listen string, assigner *core.Assigner, options ...ServerOption) (*Server, error) {
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
	s := &Server{
		assigner: assigner,
		server:   server,
		listener: l,
	}

	// Direct announce.
	r.HandleFunc("/ingest/announce", s.announce).Methods(http.MethodPut)

	return s, nil
}

func (s *Server) URL() string {
	return fmt.Sprint("http://", s.listener.Addr().String())
}

func (s *Server) Start() error {
	log.Infow("http server listening", "listen_addr", s.listener.Addr())
	return s.server.Serve(s.listener)
}

func (s *Server) Close() error {
	log.Info("http server shutdown")
	return s.server.Shutdown(context.Background())
}

// PUT /ingest/announce
func (s *Server) announce(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	defer r.Body.Close()

	an := gossiptopic.Message{}
	if err := an.UnmarshalCBOR(r.Body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(an.Addrs) == 0 {
		http.Error(w, "must specify location to fetch on direct announcments", http.StatusBadRequest)
		return
	}
	addrs, err := an.GetAddrs()
	if err != nil {
		err = fmt.Errorf("could not decode addrs from announce message: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(ais) > 1 {
		http.Error(w, "peer id must be the same for all addresses", http.StatusBadRequest)
		return
	}
	addrInfo := ais[0]

	if !s.assigner.Allowed(addrInfo.ID) {
		http.Error(w, "announce requests not allowed from peer", http.StatusForbidden)
		return
	}

	// Use background context because this will be an async process. We don't
	// want to attach the context to the request context that started this.
	err = s.assigner.Announce(context.Background(), an.Cid, addrInfo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
