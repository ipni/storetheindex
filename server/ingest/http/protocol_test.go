package httpingestserver_test

import (
	"net/http"
	"net/url"
	"testing"

	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/announce/httpsender"
	httpclient "github.com/ipni/storetheindex/api/v0/ingest/client/http"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	httpserver "github.com/ipni/storetheindex/server/ingest/http"
	"github.com/ipni/storetheindex/server/ingest/test"
)

var providerIdent = config.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

func setupServer(ind indexer.Interface, ing *ingest.Ingester, reg *registry.Registry, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, ing, reg)
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

func setupSender(t *testing.T, baseURL string) *httpsender.Sender {
	const announcePath = "/ingest/announce"

	announceURL, err := url.Parse(baseURL + announcePath)
	if err != nil {
		t.Fatal(err)
	}

	httpSender, err := httpsender.New([]*url.URL{announceURL})
	if err != nil {
		t.Fatal(err)
	}

	return httpSender
}

func TestRegisterProvider(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t, providerIdent.PeerID)
	ing := test.InitIngest(t, ind, reg)
	s := setupServer(ind, ing, reg, t)
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

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}

func TestAnnounce(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t, providerIdent.PeerID)
	ing := test.InitIngest(t, ind, reg)
	s := setupServer(ind, ing, reg, t)
	httpSender := setupSender(t, s.URL())
	peerID, _, err := providerIdent.Decode()
	if err != nil {
		t.Fatal(err)
	}
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	test.AnnounceTest(t, peerID, httpSender)

	if err = reg.Close(); err != nil {
		t.Errorf("Error closing registry: %s", err)
	}
	if err = ind.Close(); err != nil {
		t.Errorf("Error closing indexer core: %s", err)
	}
}
