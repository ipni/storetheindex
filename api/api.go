package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("api")

type API struct {
	server *http.Server
	l      net.Listener
	doneCh chan struct{}
}

func NewWithDependencies(listen string) (*API, error) {
	var err error

	api := new(API)
	api.l, err = net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}

	r := mux.NewRouter().StrictSlash(true)

	// Import routes
	r.HandleFunc("/import/manifest/{minerid}", handle.ImportManifest).Methods("POST")
	r.HandleFunc("/import/cidlist/{minerid}", handle.ImportCidList).Methods("POST")

	// Admin routes
	r.HandleFunc("/healthcheck", handle.HealthCheck).Methods("GET")

	api.doneCh = make(chan struct{})
	api.server = &http.Server{
		Handler:      r,
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	}

	return api, nil
}

func (a *API) Serve() error {
	select {
	case <-a.doneCh:
		return fmt.Errorf("tried to reuse a stopped server")
	default:
	}

	log.Infow("api listening", "addr", a.l.Addr())
	return a.server.Serve(a.l)
}

func (a *API) Shutdown(ctx context.Context) error {
	defer close(a.doneCh)
	return a.server.Shutdown(ctx)
}
