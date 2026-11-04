package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-indexer-core/engine"
	headschema "github.com/ipni/go-libipni/dagsync/ipnisync/head"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/test/typehelpers"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// testAdAddr is the provider address of the ads that newAd builds. The
// indexer stores it in the registry and never connects to it, so it needs no
// listener and tests that run in parallel can share it.
const testAdAddr = "/ip4/127.0.0.1/tcp/9999"

// testPublisher is a publisher that serves an ad chain over HTTP, as an IPNI
// publisher does. It answers a request for a block with the handler of that
// block, which writes the block's bytes exactly as they were put, so it can
// serve blocks that are malformed in any way.
type testPublisher struct {
	key  crypto.PrivKey
	id   peer.ID
	addr multiaddr.Multiaddr
	// lsys stores IPLD nodes as blocks of the publisher, and reads them back
	lsys ipld.LinkSystem

	mu     sync.Mutex
	blocks map[cid.Cid]http.HandlerFunc
	head   cid.Cid
}

func newTestPublisher(t *testing.T, key crypto.PrivKey) *testPublisher {
	id, err := peer.IDFromPrivateKey(key)
	require.NoError(t, err)
	p := &testPublisher{key: key, id: id, blocks: make(map[cid.Cid]http.HandlerFunc)}
	p.lsys = cidlink.DefaultLinkSystem()
	p.lsys.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		h, ok := p.block(l.(cidlink.Link).Cid)
		if !ok {
			return nil, fmt.Errorf("block %s not found", l)
		}
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/"+l.String(), nil))
		if rec.Code != http.StatusOK {
			return nil, fmt.Errorf("block %s: status %d", l, rec.Code)
		}
		return rec.Body, nil
	}
	p.lsys.StorageWriteOpener = func(ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		var buf bytes.Buffer
		return &buf, func(l ipld.Link) error {
			p.put(l.(cidlink.Link).Cid, buf.Bytes())
			return nil
		}, nil
	}

	srv := httptest.NewServer(http.HandlerFunc(p.serve))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	p.addr, err = maurl.FromURL(u)
	require.NoError(t, err)
	return p
}

// serve answers a request for the signed head or for a block. It calls the
// block's handler without holding the lock, so that a handler that writes
// slowly does not hold up other requests.
func (p *testPublisher) serve(w http.ResponseWriter, r *http.Request) {
	name := path.Base(r.URL.Path)
	if name == "head" {
		p.mu.Lock()
		head := p.head
		p.mu.Unlock()
		signed, err := headschema.NewSignedHead(head, "", p.key)
		if err == nil {
			var msg []byte
			if msg, err = signed.Encode(); err == nil {
				_, _ = w.Write(msg)
				return
			}
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c, err := cid.Decode(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	h, ok := p.block(c)
	if !ok {
		http.NotFound(w, r)
		return
	}
	h(w, r)
}

// block returns the handler of block c
func (p *testPublisher) block(c cid.Cid) (http.HandlerFunc, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.blocks[c]
	return h, ok
}

// put adds block c, which the publisher serves as data
func (p *testPublisher) put(c cid.Cid, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocks[c] = func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(data)
	}
}

// setHead sets the CID that the publisher announces as its head
func (p *testPublisher) setHead(c cid.Cid) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = c
}

// testIngestEnv is an ingester that syncs from a testPublisher. The publisher
// is also the provider of the ads that newAd builds.
type testIngestEnv struct {
	cfg  config.Ingest
	pub  *testPublisher
	ing  *Ingester
	core *engine.Engine
	reg  *registry.Registry
}

func newTestIngestEnv(t *testing.T, cfg config.Ingest, pubKey crypto.PrivKey) *testIngestEnv {
	e := &testIngestEnv{
		cfg:  cfg,
		pub:  newTestPublisher(t, pubKey),
		core: mkIndexer(t, false),
		reg:  mkRegistry(t),
	}
	// The ingester host does not listen, so that many tests do not use up
	// local ports.
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	e.ing, err = NewIngester(cfg, h, e.core, e.reg, dssync.MutexWrap(datastore.NewMapDatastore()), dssync.MutexWrap(datastore.NewMapDatastore()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = e.ing.Close()
		e.reg.Close()
		_ = e.core.Close()
		_ = h.Close()
	})
	return e
}

// sync syncs the publisher's chain up to its head, and waits until the head
// is processed.
func (e *testIngestEnv) sync(ctx context.Context) error {
	_, err := e.ing.Sync(ctx, peer.AddrInfo{ID: e.pub.id, Addrs: []multiaddr.Multiaddr{e.pub.addr}}, 0, false, cid.Undef)
	return err
}

// store adds the node to the publisher, encoded in dag-json
func (e *testIngestEnv) store(t *testing.T, n datamodel.Node) ipld.Link {
	lnk, err := e.pub.lsys.Store(ipld.LinkContext{}, schema.Linkproto, n)
	require.NoError(t, err)
	return lnk
}

// chunk adds an entry chunk of mhs that links to next
func (e *testIngestEnv) chunk(t *testing.T, mhs []multihash.Multihash, next ipld.Link) ipld.Link {
	return e.store(t, entryChunkNode(t, mhs, next))
}

// newAd returns an ad of the publisher, as the provider, over entries. It
// links to the publisher's head. A test can change the ad before it
// publishes it.
func (e *testIngestEnv) newAd(entries ipld.Link) schema.Advertisement {
	ad := schema.Advertisement{
		Provider:  e.pub.id.String(),
		Addresses: []string{testAdAddr},
		Entries:   entries,
		ContextID: []byte("ctx"),
		Metadata:  []byte("md"),
	}
	if e.pub.head != cid.Undef {
		ad.PreviousID = cidlink.Link{Cid: e.pub.head}
	}
	return ad
}

// publish adds the ad to the publisher and makes it the head. An ad
// without a signature is signed with the publisher's key.
func (e *testIngestEnv) publish(t *testing.T, ad schema.Advertisement) cid.Cid {
	if len(ad.Signature) == 0 {
		require.NoError(t, ad.Sign(e.pub.key))
	}
	node, err := ad.ToNode()
	require.NoError(t, err)
	c := e.store(t, node).(cidlink.Link).Cid
	e.pub.setHead(c)
	return c
}

// adState returns the ingestion state of the ad
func (e *testIngestEnv) adState(t *testing.T, adCid cid.Cid) AdState {
	state, err := e.ing.GetAdState(t.Context(), adCid)
	require.NoError(t, err)
	return state
}

// requireIndexed requires that each of mhs is indexed for the publisher's
// provider.
func (e *testIngestEnv) requireIndexed(t *testing.T, mhs []multihash.Multihash) {
	t.Helper()
	require.NoError(t, checkAllIndexed(e.core, e.pub.id, mhs))
}

// requireNotIndexed requires that none of mhs is indexed for the
// publisher's provider.
func (e *testIngestEnv) requireNotIndexed(t *testing.T, mhs []multihash.Multihash) {
	t.Helper()
	requireNotIndexed(t, e.core, e.pub.id, mhs)
}

// IngestTestSuite tests what ingestion does with the content that a publisher
// serves, valid or not. Each test publishes blocks and ads to a testPublisher
// served over HTTP, syncs them, and checks the index, the state of each ad,
// and the registry.
type IngestTestSuite struct {
	suite.Suite
	*testIngestEnv
}

func TestIngestSuite(t *testing.T) {
	suite.Run(t, new(IngestTestSuite))
}

func (s *IngestTestSuite) SetupTest() {
	t := s.T()
	key, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	s.testIngestEnv = newTestIngestEnv(t, defaultTestIngestConfig, key)
}

func (s *IngestTestSuite) TestEntryChunks() {
	t := s.T()
	mhs := random.Multihashes(4)
	next := s.chunk(t, mhs[2:], nil)
	adCid := s.publish(t, s.newAd(s.chunk(t, mhs[:2], next)))

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
}

func (s *IngestTestSuite) TestHamt() {
	t := s.T()
	entries := typehelpers.RandomHamtEntryBuilder{BitWidth: 3, BucketSize: 1, MultihashCount: 200}.Build(t, s.pub.lsys)
	adCid := s.publish(t, s.newAd(entries))

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	mhs := typehelpers.AllMultihashesFromAdLink(t, cidlink.Link{Cid: adCid}, s.pub.lsys)
	require.Len(t, mhs, 200)
	s.requireIndexed(t, mhs)
}

func (s *IngestTestSuite) TestRemoval() {
	t := s.T()
	mhs := random.Multihashes(2)
	s.publish(t, s.newAd(s.chunk(t, mhs, nil)))
	rm := s.newAd(schema.NoEntries)
	rm.IsRm = true
	rmCid := s.publish(t, rm)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, rmCid).Processed)
	s.requireNotIndexed(t, mhs)
}
