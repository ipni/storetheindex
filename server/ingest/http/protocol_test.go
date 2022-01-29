package httpingestserver_test

import (
	"net/http"
	"testing"

	indexer "github.com/filecoin-project/go-indexer-core"
	httpclient "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/registry"
	httpserver "github.com/filecoin-project/storetheindex/server/ingest/http"
	"github.com/filecoin-project/storetheindex/server/ingest/test"
)

var providerIdent = config.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

func setupServer(ind indexer.Interface, reg *registry.Registry, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, reg)
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

func TestRegisterProvider(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t, providerIdent.PeerID)
	s := setupServer(ind, reg, t)
	httpClient := setupClient(s.URL(), t)

	peerID, privKey, err := providerIdent.Decode()
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

	test.RegisterProviderTest(t, httpClient, peerID, privKey, "/ip4/127.0.0.1/tcp/9999", reg)

	test.IndexContent(t, httpClient, peerID, privKey, ind)

	test.IndexContentNewAddr(t, httpClient, peerID, privKey, ind, "/ip4/127.0.0.1/tcp/7777", reg)

	test.IndexContentFail(t, httpClient, peerID, privKey, ind)

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}
