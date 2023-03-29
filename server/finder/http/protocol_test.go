package httpfinderserver_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/gen/proto"
	indexer "github.com/ipni/go-indexer-core"
	httpclient "github.com/ipni/go-libipni/find/client/http"
	"github.com/ipni/storetheindex/internal/counter"
	"github.com/ipni/storetheindex/internal/registry"
	httpserver "github.com/ipni/storetheindex/server/finder/http"
	"github.com/ipni/storetheindex/server/finder/test"
	"github.com/stretchr/testify/require"
)

func setupServer(ind indexer.Interface, reg *registry.Registry, idxCts *counter.IndexCounts, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, reg, httpserver.WithIndexCounts(idxCts))
	require.NoError(t, err)
	return s
}

func setupClient(host string, t *testing.T) *httpclient.Client {
	c, err := httpclient.New(host)
	require.NoError(t, err)
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
	require.NoError(t, err)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
}

func TestFindIndexWithExtendedProviders(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	// We don't want to have any restricitons around provider identities as they are generated in rkandom for extended providers
	reg := test.InitRegistryWithRestrictivePolicy(t, false)
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

	test.ProvidersShouldBeUnaffectedByExtendedProvidersOfEachOtherTest(ctx, t, c, ind, reg)
	test.ExtendedProviderShouldHaveOwnMetadataTest(ctx, t, c, ind, reg)
	test.ExtendedProviderShouldInheritMetadataOfMainProviderTest(ctx, t, c, ind, reg)
	test.ContextualExtendedProvidersShouldUnionUpWithChainLevelOnesTest(ctx, t, c, ind, reg)
	test.ContextualExtendedProvidersShouldOverrideChainLevelOnesTest(ctx, t, c, ind, reg)
	test.MainProviderChainRecordIsIncludedIfItsMetadataIsDifferentTest(ctx, t, c, ind, reg)
	test.MainProviderContextRecordIsIncludedIfItsMetadataIsDifferentTest(ctx, t, c, ind, reg)

	require.NoError(t, s.Close(), "shutdown error")
	err := <-errChan
	require.NoError(t, err)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
}

func TestReframeFindIndexData(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t)
	s := setupServer(ind, reg, nil, t)
	c := setupClient(s.URL(), t)

	// create delegated routing client
	q, err := proto.New_DelegatedRouting_Client(s.URL() + "/reframe")
	require.NoError(t, err)
	reframeClient, err := client.NewClient(q, nil, nil)
	require.NoError(t, err)

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

	require.NoError(t, s.Close(), "shutdown error")
	err = <-errChan
	require.NoError(t, err)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
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

	require.NoError(t, s.Close(), "shutdown error")
	err := <-errChan
	require.NoError(t, err)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
}

func TestGetStats(t *testing.T) {
	ind := test.InitPebbleIndex(t, false)
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

	test.GetStatsTest(ctx, t, ind, s.RefreshStats, httpClient)

	require.NoError(t, s.Close(), "shutdown error")
	err := <-errChan
	require.NoError(t, err)
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

	require.NoError(t, s.Close(), "shutdown error")
	err := <-errChan
	require.NoError(t, err)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
}
