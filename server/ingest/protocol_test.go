package ingest_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/httpsender"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/ingest/client"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/ingest"
	"github.com/ipni/storetheindex/registry"
	httpserver "github.com/ipni/storetheindex/server/ingest"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
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

func setupClient(host string, t *testing.T) *client.Client {
	c, err := client.New(host)
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
	ind := initIndex(t, true)
	reg := initRegistry(t, providerIdent.PeerID)
	ing := initIngest(t, ind, reg)
	s := setupServer(ind, ing, reg, t)
	cl := setupClient(s.URL(), t)

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

	registerProviderTest(t, cl, peerID, privKey, "/ip4/127.0.0.1/tcp/9999", reg)
}

func TestAnnounce(t *testing.T) {
	// Initialize everything
	ind := initIndex(t, true)
	reg := initRegistry(t, providerIdent.PeerID)
	ing := initIngest(t, ind, reg)
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

	announceTest(t, peerID, httpSender)
}

// initIndex initialize a new indexer engine.
func initIndex(t *testing.T, withCache bool) indexer.Interface {
	ind := engine.New(memory.New())
	t.Cleanup(func() {
		require.NoError(t, ind.Close(), "Error closing indexer core")
	})
	return ind
}

// initRegistry initializes a new registry
func initRegistry(t *testing.T, trustedID string) *registry.Registry {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Allow:         false,
			Except:        []string{trustedID},
			Publish:       false,
			PublishExcept: []string{trustedID},
		},
	}
	reg, err := registry.New(context.Background(), discoveryCfg, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		reg.Close()
	})
	return reg
}

func initIngest(t *testing.T, indx indexer.Interface, reg *registry.Registry) *ingest.Ingester {
	cfg := config.NewIngest()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	dsTmp := dssync.MutexWrap(datastore.NewMapDatastore())
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)

	ing, err := ingest.NewIngester(cfg, host, indx, reg, ds, dsTmp)
	require.NoError(t, err)
	t.Cleanup(func() {
		ing.Close()
		host.Close()
	})
	return ing
}

func announceTest(t *testing.T, peerID peer.ID, sender announce.Sender) {
	ai, err := peer.AddrInfoFromString(fmt.Sprintf("/ip4/127.0.0.1/tcp/9999/p2p/%s", peerID))
	require.NoError(t, err)
	ai.ID = peerID

	p2pAddrs, err := peer.AddrInfoToP2pAddrs(ai)
	require.NoError(t, err)

	mhs := random.Multihashes(1)

	msg := message.Message{
		Cid: cid.NewCidV1(22, mhs[0]),
	}
	msg.SetAddrs(p2pAddrs)

	err = sender.Send(context.Background(), msg)
	require.NoError(t, err, "Failed to announce")
}

func registerProviderTest(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, addr string, reg *registry.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Log("registering provider")
	err := cl.Register(ctx, providerID, privateKey, []string{addr})
	require.NoError(t, err)

	require.True(t, reg.IsRegistered(providerID), "provider not registered")

	// Test signature fail
	t.Log("registering provider with bad signature")
	badPeerID, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	require.NoError(t, err)

	err = cl.Register(ctx, badPeerID, privateKey, []string{addr})
	require.Error(t, err, "expected bad signature error")
}

// TODO: Uncomment when supporting puts directly to indexer.
/*
func indexContent(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := random.Multihashes(1)

	contextID := []byte("test-context-id")
	metadata := []byte("test-metadata")

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	require.NoError(t, err)

	vals, ok, err := ind.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, ok, "did not find content")
	require.NotZero(t, len(vals), "no content values returned")

	expectValue := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadata,
	}
	ok = false
	for i := range vals {
		if vals[i].Equal(expectValue) {
			ok = true
			break
		}
	}
	require.True(t, ok, "did not get expected content")
    }

func indexContentNewAddr(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface, newAddr string, reg *registry.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := random.Multihashes(1)

	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	addrs := []string{newAddr}

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], ctxID, metadata, addrs)
	require.NoError(t, err)

	info, allowed := reg.ProviderInfo(providerID)
	require.NotNil(t, info, "did not get infor for provider")
	require.True(t, allowed, "provider not allowed")

	maddr, err := multiaddr.NewMultiaddr(newAddr)
	require.NoError(t, err)

	require.True(t, info.AddrInfo.Addrs[0].Equal(maddr), "Did not update address")
}

func indexContentFail(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := random.Multihashes(1)

	contextID := make([]byte, schema.MaxContextIDLen+1)
	metadata := []byte("test-metadata")

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "context id too long")

	contextID = []byte("test-context-id")
	metadata = make([]byte, schema.MaxMetadataLen+1)
	err = cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "metadata too long")

	var apierr *apierror.Error
	if errors.As(err, &apierr) {
		require.Equal(t, 400, apierr.Status())
	}
}
*/
