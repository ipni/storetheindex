package httpingestserver_test

import (
	"net/http"
	"net/url"
	"testing"

	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/announce/httpsender"
	httpclient "github.com/ipni/go-libipni/ingest/client"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	httpserver "github.com/ipni/storetheindex/server/ingest/http"
	"github.com/ipni/storetheindex/server/ingest/test"
	"github.com/stretchr/testify/require"
)

var providerIdent = config.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

func setupServer(ind indexer.Interface, ing *ingest.Ingester, reg *registry.Registry, t *testing.T) *httpserver.Server {
	s, err := httpserver.New("127.0.0.1:0", ind, ing, reg)
	require.NoError(t, err)
	return s
}

func setupClient(host string, t *testing.T) *httpclient.Client {
	c, err := httpclient.New(host)
	require.NoError(t, err)
	return c
}

func setupSender(t *testing.T, baseURL string) *httpsender.Sender {
	announceURL, err := url.Parse(baseURL + httpsender.DefaultAnnouncePath)
	require.NoError(t, err)

	peerID, _, err := providerIdent.Decode()
	require.NoError(t, err)

	httpSender, err := httpsender.New([]*url.URL{announceURL}, peerID)
	require.NoError(t, err)

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

	test.RegisterProviderTest(t, httpClient, peerID, privKey, "/ip4/127.0.0.1/tcp/9999", reg)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
}

func TestAnnounce(t *testing.T) {
	// Initialize everything
	ind := test.InitIndex(t, true)
	reg := test.InitRegistry(t, providerIdent.PeerID)
	ing := test.InitIngest(t, ind, reg)
	s := setupServer(ind, ing, reg, t)
	httpSender := setupSender(t, s.URL())
	peerID, _, err := providerIdent.Decode()
	require.NoError(t, err)
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	test.AnnounceTest(t, peerID, httpSender)

	reg.Close()
	require.NoError(t, ind.Close(), "Error closing indexer core")
}
