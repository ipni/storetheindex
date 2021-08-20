package httpfinderserver_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	httpclient "github.com/filecoin-project/storetheindex/api/v0/client/http"
	"github.com/filecoin-project/storetheindex/internal/providers"
	httpserver "github.com/filecoin-project/storetheindex/server/finder/http"
	"github.com/filecoin-project/storetheindex/server/finder/test"
)

func setupServer(ind indexer.Interface, reg *providers.Registry, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, reg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func setupClient(host string, t *testing.T) *httpclient.Finder {
	c, err := httpclient.NewFinder(host)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGetCidData(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s := setupServer(ind, reg, t)
	c := setupClient(s.URL(), t)

	// Start server
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	// Test must complete in 5 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	test.GetCidDataTest(ctx, t, c, ind, reg)

	err := s.Shutdown(ctx)
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}
}
