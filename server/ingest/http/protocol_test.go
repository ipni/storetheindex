package httpingestserver_test

import (
	"net/http"
	"testing"

	indexer "github.com/filecoin-project/go-indexer-core"
	httpclient "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers"
	httpserver "github.com/filecoin-project/storetheindex/server/ingest/http"
	"github.com/filecoin-project/storetheindex/server/ingest/test"
	"github.com/libp2p/go-libp2p-core/peer"
)

var providerIdent = config.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

func setupServer(ind indexer.Interface, reg *providers.Registry, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, reg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func setupClient(host string, t *testing.T) *httpclient.IngestClient {
	c, err := httpclient.NewIngest(host)
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

	// Start server
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}
	test.RegisterProviderTest(t, httpClient, providerIdent, addrs, reg)

	peerID, err := peer.Decode(providerIdent.PeerID)
	if err != nil {
		t.Fatal(err)
	}

	test.GetProviderTest(t, httpClient, peerID)

	test.ListProvidersTest(t, httpClient, peerID)

	test.IndexContent(t, httpClient, providerIdent, ind)
}
