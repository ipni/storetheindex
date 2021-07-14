package node

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

const (
	// TODO: these should come from external config
	apiWriteTimeout = 30 * time.Second
	apiReadTimeout  = 30 * time.Second
)

type api struct {
	server *http.Server
	l      net.Listener
}

func (n *Node) initAPI(listen string) error {
	var err error

	n.api = new(api)
	n.api.l, err = net.Listen("tcp", listen)
	if err != nil {
		return err
	}

	r := mux.NewRouter().StrictSlash(true)

	// Import routes
	r.HandleFunc("/import/manifest/{minerid}", n.ImportManifestHandler).Methods("POST")
	r.HandleFunc("/import/cidlist/{minerid}", n.ImportCidListHandler).Methods("POST")

	// Client routes
	r.HandleFunc("/cid/{cid}", n.GetSingleCidHandler).Methods("GET")

	// Admin routes
	r.HandleFunc("/healthcheck", n.HealthCheckHandler).Methods("GET")

	n.api.server = &http.Server{
		Handler:      r,
		WriteTimeout: apiWriteTimeout,
		ReadTimeout:  apiReadTimeout,
	}

	return nil
}

func (a *api) Serve() error {
	log.Infow("api listening", "addr", a.l.Addr())
	return a.server.Serve(a.l)
}

func (a *api) Shutdown(ctx context.Context) error {
	return a.server.Shutdown(ctx)
}
