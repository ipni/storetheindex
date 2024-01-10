package federation_test

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/federation"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var _ indexer.Interface = (*testIndexer)(nil)

type (
	testIndexer struct {
		store map[string][]indexer.Value
	}

	testFederationMember struct {
		*federation.Federation
		host     host.Host
		indexer  *testIndexer
		ingester *ingest.Ingester
		registry *registry.Registry
	}
)

func (t testIndexer) Get(multihash multihash.Multihash) ([]indexer.Value, bool, error) {
	values, found := t.store[string(multihash)]
	return values, found, nil
}

func (t testIndexer) Put(value indexer.Value, multihash ...multihash.Multihash) error {
	for _, m := range multihash {
		key := string(m)
		t.store[key] = append(t.store[key], value)
	}
	return nil
}

func (t testIndexer) Remove(indexer.Value, ...multihash.Multihash) error { panic("unimplemented") }
func (t testIndexer) RemoveProvider(context.Context, peer.ID) error      { panic("unimplemented") }
func (t testIndexer) RemoveProviderContext(peer.ID, []byte) error        { panic("unimplemented") }
func (t testIndexer) Size() (int64, error)                               { panic("unimplemented") }
func (t testIndexer) Flush() error                                       { panic("unimplemented") }
func (t testIndexer) Close() error                                       { return nil }
func (t testIndexer) Iter() (indexer.Iterator, error)                    { panic("unimplemented") }
func (t testIndexer) Stats() (*indexer.Stats, error)                     { return nil, nil }

func newTestFederationMember(t *testing.T, members ...peer.AddrInfo) *testFederationMember {
	h, err := libp2p.New()
	require.NoError(t, err)
	idxr := &testIndexer{store: make(map[string][]indexer.Value)}
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	reg, err := registry.New(context.Background(), config.NewDiscovery(), ds)
	require.NoError(t, err)
	igstr, err := ingest.NewIngester(config.NewIngest(), h, idxr, reg, ds, ds)
	require.NoError(t, err)
	f, err := federation.New(
		federation.WithHost(h),
		federation.WithRegistry(reg),
		federation.WithIngester(igstr),
		federation.WithReconciliationInterval(time.Second),
		federation.WithSnapshotInterval(time.Second),
		federation.WithHttpListenAddr("0.0.0.0:0"),
		federation.WithMembers(members...),
	)
	require.NoError(t, err)
	return &testFederationMember{
		Federation: f,
		host:       h,
		indexer:    idxr,
		ingester:   igstr,
		registry:   reg,
	}
}

func (tfm *testFederationMember) requireHeadEventually(t *testing.T, timeout time.Duration, retry time.Duration) *federation.Head {
	var head *federation.Head
	require.Eventually(t, func() bool {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/ipni/v1/fed/head", nil)
		tfm.ServeMux().ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK {
			return false
		}
		builder := federation.Prototypes.Head.NewBuilder()
		err := dagjson.Decode(builder, recorder.Body)
		if err != nil {
			t.Log(err)
			return false
		}
		head = bindnode.Unwrap(builder.Build()).(*federation.Head)
		if err := head.Verify(tfm.host.Peerstore().PubKey(tfm.host.ID())); err != nil {
			t.Log(err)
			return false
		}
		if head.Head == nil {
			return false
		}
		return true
	}, timeout, retry)
	return head
}

func (tfm *testFederationMember) requireSnapshot(t *testing.T, l ipld.Link) *federation.Snapshot {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/ipni/v1/fed/"+l.String(), nil)
	tfm.ServeMux().ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)

	builder := federation.Prototypes.Snapshot.NewBuilder()
	require.NoError(t, dagjson.Decode(builder, recorder.Body))
	return bindnode.Unwrap(builder.Build()).(*federation.Snapshot)
}

var buf [32]byte

func requireRandomCid(t *testing.T) cid.Cid {
	n, err := rand.Reader.Read(buf[:])
	require.NoError(t, err)
	mh, err := multihash.Sum(buf[:n], multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}
