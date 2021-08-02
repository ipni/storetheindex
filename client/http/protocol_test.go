package httprotocol_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/client"
	httpclient "github.com/filecoin-project/storetheindex/client/http/client"
	httpserver "github.com/filecoin-project/storetheindex/client/http/server"
	"github.com/filecoin-project/storetheindex/client/test"
)

func setupServer(ctx context.Context, ind *indexer.Engine, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func setupClient(ctx context.Context, t *testing.T) client.Interface {
	c, err := httpclient.New()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestGetCidData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	ind := test.InitIndex(t, true)
	c := setupClient(ctx, t)
	s := setupServer(ctx, ind, t)
	// Start server
	go s.Start()
	defer s.Shutdown(ctx)

	test.GetCidDataTest(ctx, t, c, s, ind)
}
