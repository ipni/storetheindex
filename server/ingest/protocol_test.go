package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

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
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
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
	ing, _ := initIngest(t, ind, reg)
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
	ing, _ := initIngest(t, ind, reg)
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

func initIngest(t *testing.T, indx indexer.Interface, reg *registry.Registry) (*ingest.Ingester, datastore.Batching) {
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
	return ing, ds
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

func TestAdStatus(t *testing.T) {
	ind := initIndex(t, true)
	reg := initRegistry(t, providerIdent.PeerID)
	ing, ds := initIngest(t, ind, reg)
	s := setupServer(ind, ing, reg, t)
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()
	t.Cleanup(func() {
		require.NoError(t, s.Close())
		require.NoError(t, <-errChan)
	})

	pubID, _, err := providerIdent.Decode()
	require.NoError(t, err)

	dagJSONAds := random.Cids(5)
	for i := range dagJSONAds {
		dagJSONAds[i] = cid.NewCidV1(cid.DagJSON, dagJSONAds[i].Hash())
	}

	getAdStatus := func(t *testing.T, ad cid.Cid) (int, http.Header, string) {
		t.Helper()
		res, err := http.Get(s.URL() + "/sync/status/ad/" + ad.String())
		require.NoError(t, err)
		body, err := io.ReadAll(res.Body)
		res.Body.Close()
		require.NoError(t, err)
		return res.StatusCode, res.Header, string(body)
	}

	// Unknown ad returns state "unknown".
	status, hdr, body := getAdStatus(t, dagJSONAds[0])
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "*", hdr.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "application/json; charset=utf-8", hdr.Get("Content-Type"))
	require.JSONEq(t, fmt.Sprintf(`{"Ad":%q,"Indexed":false,"State":"unknown","Frozen":false}`, dagJSONAds[0].String()), body)

	// Fully processed ad is indexed.
	require.NoError(t, ing.MarkAdProcessed(pubID, dagJSONAds[0]))
	status, hdr, body = getAdStatus(t, dagJSONAds[0])
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "*", hdr.Get("Access-Control-Allow-Origin"))
	require.JSONEq(t, fmt.Sprintf(`{"Ad":%q,"Indexed":true,"State":"indexed","Frozen":false}`, dagJSONAds[0].String()), body)

	// Skipped ad returns state "skipped" with reason, Indexed false.
	require.NoError(t, ing.MarkAdSkipped(pubID, dagJSONAds[1], "decodeErr", false))
	status, _, body = getAdStatus(t, dagJSONAds[1])
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, fmt.Sprintf(`{"Ad":%q,"Indexed":false,"State":"skipped","SkipReason":"decodeErr","Frozen":false}`, dagJSONAds[1].String()), body)

	// Pending ad (marker byte 0 written directly).
	require.NoError(t, ds.Put(context.Background(), datastore.NewKey("/adProcessed/"+dagJSONAds[2].String()), []byte{0}))
	status, _, body = getAdStatus(t, dagJSONAds[2])
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, fmt.Sprintf(`{"Ad":%q,"Indexed":false,"State":"pending","Frozen":false}`, dagJSONAds[2].String()), body)

	// Resyncing ad (marker byte 2 written directly).
	require.NoError(t, ds.Put(context.Background(), datastore.NewKey("/adProcessed/"+dagJSONAds[3].String()), []byte{2}))
	status, _, body = getAdStatus(t, dagJSONAds[3])
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, fmt.Sprintf(`{"Ad":%q,"Indexed":false,"State":"resyncing","Frozen":false}`, dagJSONAds[3].String()), body)

	// Frozen ad (processed + frozen key set).
	require.NoError(t, ing.MarkAdProcessed(pubID, dagJSONAds[4]))
	require.NoError(t, ds.Put(context.Background(), datastore.NewKey("/adF/"+dagJSONAds[4].String()), []byte{1}))
	status, _, body = getAdStatus(t, dagJSONAds[4])
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, fmt.Sprintf(`{"Ad":%q,"Indexed":false,"State":"indexed","Frozen":true}`, dagJSONAds[4].String()), body)

	// Codec guard: raw CID returns 400.
	rawCid := cid.NewCidV1(cid.Raw, random.Multihashes(1)[0])
	res, err := http.Get(s.URL() + "/sync/status/ad/" + rawCid.String())
	require.NoError(t, err)
	bodyBytes, err := io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Contains(t, string(bodyBytes), "dag-json")
	require.Contains(t, string(bodyBytes), "dag-cbor")

	// Codec guard: raw CID v0 returns 400.
	v0Cid := cid.NewCidV0(random.Multihashes(1)[0])
	res, err = http.Get(s.URL() + "/sync/status/ad/" + v0Cid.String())
	require.NoError(t, err)
	bodyBytes, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
	require.Contains(t, string(bodyBytes), "dag-json")
	require.Contains(t, string(bodyBytes), "dag-cbor")

	// Codec guard: dag-cbor CID is accepted.
	dagCBORCid := cid.NewCidV1(cid.DagCBOR, random.Multihashes(1)[0])
	res, err = http.Get(s.URL() + "/sync/status/ad/" + dagCBORCid.String())
	require.NoError(t, err)
	bodyBytes, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Bad CID format returns 400.
	res, err = http.Get(s.URL() + "/sync/status/ad/not-a-cid")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	// Empty marker value returns 500 without panicking.
	emptyCid := cid.NewCidV1(cid.DagJSON, random.Multihashes(1)[0])
	require.NoError(t, ds.Put(context.Background(), datastore.NewKey("/adProcessed/"+emptyCid.String()), []byte{}))
	status, _, _ = getAdStatus(t, emptyCid)
	require.Equal(t, http.StatusInternalServerError, status)

	// Nil ingester returns 503.
	nilServer, err := httpserver.New("127.0.0.1:0", ind, nil, reg)
	require.NoError(t, err)
	errChan2 := make(chan error, 1)
	go func() {
		err := nilServer.Start()
		if err != http.ErrServerClosed {
			errChan2 <- err
		}
		close(errChan2)
	}()
	t.Cleanup(func() {
		require.NoError(t, nilServer.Close())
		require.NoError(t, <-errChan2)
	})
	for i := 0; i < 50; i++ {
		res, err = http.Get(nilServer.URL() + "/health")
		if err == nil {
			res.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	res, err = http.Get(nilServer.URL() + "/sync/status/ad/" + dagJSONAds[0].String())
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)

	// OPTIONS is required for CORS preflight.
	req, err := http.NewRequest(http.MethodOptions, s.URL()+"/sync/status/ad/"+dagJSONAds[0].String(), nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"))
}

func TestSyncStatus(t *testing.T) {
	ind := initIndex(t, true)
	reg := initRegistry(t, providerIdent.PeerID)
	ing, _ := initIngest(t, ind, reg)
	s := setupServer(ind, ing, reg, t)
	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()
	t.Cleanup(func() {
		require.NoError(t, s.Close())
		require.NoError(t, <-errChan)
	})

	pubID, err := peer.Decode("12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ")
	require.NoError(t, err)
	provID, err := peer.Decode("12D3KooWQ9j3Ur5V9U63Vi6ved72TcA3sv34k74W3wpW5rwNvDc3")
	require.NoError(t, err)
	ad, err := cid.Decode("baguqeeraa5mjufqdwzgafkqxmllc4hwzd4qcjqzj4tnaswgvazawepoqwzqa")
	require.NoError(t, err)
	require.Equal(t, 1, ing.RecordAdScanned(pubID, provID, ad))

	st := ing.SyncStatus(pubID)
	require.NotNil(t, st)
	stData, err := json.Marshal(st)
	require.NoError(t, err)
	expected := mustJSONMap(t, map[string]json.RawMessage{
		pubID.String(): stData,
	})

	res, err := http.Get(s.URL() + "/sync/status")
	require.NoError(t, err)
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"))
	require.Equal(t, "application/json; charset=utf-8", res.Header.Get("Content-Type"))
	require.JSONEq(t, string(expected), string(body))

	res, err = http.Get(s.URL() + "/sync/status/" + pubID.String())
	require.NoError(t, err)
	body, err = io.ReadAll(res.Body)
	res.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"))
	require.JSONEq(t, string(stData), string(body))

	// Empty status for unknown publisher.
	unknownPub, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	require.NoError(t, err)
	res, err = http.Get(s.URL() + "/sync/status/" + unknownPub.String())
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusNoContent, res.StatusCode)
	require.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"))

	res, err = http.Get(s.URL() + "/sync/status/not-a-peer-id")
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	// OPTIONS is required for CORS preflight.
	req, err := http.NewRequest(http.MethodOptions, s.URL()+"/sync/status", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	res, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "*", res.Header.Get("Access-Control-Allow-Origin"))
}

func mustJSONMap(t *testing.T, m map[string]json.RawMessage) []byte {
	t.Helper()
	data, err := json.Marshal(m)
	require.NoError(t, err)
	return data
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
