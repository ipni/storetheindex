package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

type api struct {
	server *http.Server
	l      net.Listener
	doneCh chan struct{}
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

	// Admin routes
	r.HandleFunc("/healthcheck", n.HealthCheckHandler).Methods("GET")

	n.api.doneCh = make(chan struct{})
	n.api.server = &http.Server{
		Handler:      r,
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	}

	return nil
}

func (a *api) Serve() error {
	select {
	case <-a.doneCh:
		return fmt.Errorf("tried to reuse a stopped server")
	default:
	}

	log.Infow("api listening", "addr", a.l.Addr())
	return a.server.Serve(a.l)
}

func (a *api) Shutdown(ctx context.Context) error {
	defer close(a.doneCh)
	return a.server.Shutdown(ctx)
}
