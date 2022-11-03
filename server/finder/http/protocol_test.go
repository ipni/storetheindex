package httpfinderserver_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	indexer "github.com/filecoin-project/go-indexer-core"
	httpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/filecoin-project/storetheindex/internal/counter"
	"github.com/filecoin-project/storetheindex/internal/registry"
	httpserver "github.com/filecoin-project/storetheindex/server/finder/http"
	"github.com/filecoin-project/storetheindex/server/finder/test"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/gen/proto"
)

func setupServer(ind indexer.Interface, reg *registry.Registry, idxCts *counter.IndexCounts, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, reg, httpserver.WithIndexCounts(idxCts))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func setupClient(host string, t *testing.T) *httpclient.Client {
	c, err := httpclient.New(host)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFindIndexData(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s := setupServer(ind, reg, nil, t)
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

	test.FindIndexTest(ctx, t, c, ind, reg)

	err := s.Close()
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestReframeFindIndexData(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s := setupServer(ind, reg, nil, t)
	c := setupClient(s.URL(), t)

	// create delegated routing client
	q, err := proto.New_DelegatedRouting_Client(s.URL() + "/reframe")
	if err != nil {
		t.Fatal(err)
	}
	reframeClient, err := client.NewClient(q, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

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

	test.ReframeFindIndexTest(ctx, t, c, reframeClient, ind, reg)

	err = s.Close()
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestProviderInfo(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	idxCts := counter.NewIndexCounts(datastore.NewMapDatastore())

	s := setupServer(ind, reg, idxCts, t)
	httpClient := setupClient(s.URL(), t)

	// Start server
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peerID := test.Register(ctx, t, reg)

	idxCts.AddCount(peerID, []byte("context-id"), 939)

	test.GetProviderTest(t, httpClient, peerID)

	test.ListProvidersTest(t, httpClient, peerID)

	err := s.Close()
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestGetStats(t *testing.T) {
	ind := test.InitIndex(t, true)
	defer ind.Close()
	reg := test.InitRegistry(t)
	defer reg.Close()

	s := setupServer(ind, reg, nil, t)
	httpClient := setupClient(s.URL(), t)

	// Start server
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	test.GetStatsTest(ctx, t, httpClient)

	err := s.Close()
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoveProvider(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s := setupServer(ind, reg, nil, t)
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

	test.RemoveProviderTest(ctx, t, c, ind, reg)

	err := s.Close()
	if err != nil {
		t.Error("shutdown error:", err)
	}
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}
