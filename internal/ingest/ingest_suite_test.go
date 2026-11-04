package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
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
	"github.com/multiformats/go-multicodec"
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

	mu      sync.Mutex
	blocks  map[cid.Cid]http.HandlerFunc
	head    cid.Cid
	fetched []cid.Cid
	// conns holds the connections that block requests arrived on
	conns map[net.Conn]struct{}
}

// connKey is the request context key of the connection a request arrived on
type connKey struct{}

func newTestPublisher(t *testing.T, key crypto.PrivKey) *testPublisher {
	id, err := peer.IDFromPrivateKey(key)
	require.NoError(t, err)
	p := &testPublisher{key: key, id: id, blocks: make(map[cid.Cid]http.HandlerFunc), conns: make(map[net.Conn]struct{})}
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

	srv := httptest.NewUnstartedServer(http.HandlerFunc(p.serve))
	srv.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		return context.WithValue(ctx, connKey{}, c)
	}
	srv.Start()
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
	p.mu.Lock()
	p.fetched = append(p.fetched, c)
	p.conns[r.Context().Value(connKey{}).(net.Conn)] = struct{}{}
	p.mu.Unlock()
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

// fetchedBlocks returns the CIDs of the blocks that were fetched, in order
func (p *testPublisher) fetchedBlocks() []cid.Cid {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.fetched)
}

// blockConns returns the number of connections that block requests arrived on
func (p *testPublisher) blockConns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

// setHead sets the CID that the publisher announces as its head
func (p *testPublisher) setHead(c cid.Cid) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = c
}

// tmpDatastore is the temporary datastore of the ingester. It records the
// blocks put into it, which include each ad that the ingester accepts, and
// the largest number of blocks it held at once. A test can make it delete a
// block right after the block is first read, as another worker that synced
// the same block may do.
type tmpDatastore struct {
	datastore.Batching
	mu           sync.Mutex
	puts         map[datastore.Key]struct{}
	held         map[datastore.Key]struct{}
	peak         int
	deleteOnRead map[datastore.Key]struct{}
}

func newTmpDatastore() *tmpDatastore {
	return &tmpDatastore{
		Batching:     dssync.MutexWrap(datastore.NewMapDatastore()),
		puts:         make(map[datastore.Key]struct{}),
		held:         make(map[datastore.Key]struct{}),
		deleteOnRead: make(map[datastore.Key]struct{}),
	}
}

func (ds *tmpDatastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	ds.mu.Lock()
	ds.puts[key] = struct{}{}
	ds.held[key] = struct{}{}
	ds.peak = max(ds.peak, len(ds.held))
	ds.mu.Unlock()
	return ds.Batching.Put(ctx, key, value)
}

func (ds *tmpDatastore) Delete(ctx context.Context, key datastore.Key) error {
	ds.mu.Lock()
	delete(ds.held, key)
	ds.mu.Unlock()
	return ds.Batching.Delete(ctx, key)
}

func (ds *tmpDatastore) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	value, err := ds.Batching.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	ds.mu.Lock()
	_, del := ds.deleteOnRead[key]
	delete(ds.deleteOnRead, key)
	ds.mu.Unlock()
	if del {
		err = ds.Delete(ctx, key)
	}
	return value, err
}

// wasPut reports whether block c was put into the datastore
func (ds *tmpDatastore) wasPut(c cid.Cid) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	_, ok := ds.puts[datastore.NewKey(c.String())]
	return ok
}

// peakBlocks returns the largest number of blocks the datastore held at once
func (ds *tmpDatastore) peakBlocks() int {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.peak
}

// blocksHeld returns how many blocks the datastore holds now
func (ds *tmpDatastore) blocksHeld() int {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return len(ds.held)
}

// deleteAfterRead makes the datastore delete block c right after the next
// read that finds it.
func (ds *tmpDatastore) deleteAfterRead(c cid.Cid) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.deleteOnRead[datastore.NewKey(c.String())] = struct{}{}
}

// testIngestEnv is an ingester that syncs from a testPublisher. The publisher
// is also the provider of the ads that newAd builds.
type testIngestEnv struct {
	cfg  config.Ingest
	pub  *testPublisher
	ing  *Ingester
	core *engine.Engine
	reg  *registry.Registry
	// tmpStore is where ingestion stores the blocks it syncs
	tmpStore *tmpDatastore
	// stuck keeps the test cleanup from closing the ingester, which waits
	// for ingestion that does not finish.
	stuck bool
}

func newTestIngestEnv(t *testing.T, cfg config.Ingest, pubKey crypto.PrivKey) *testIngestEnv {
	e := &testIngestEnv{
		cfg:  cfg,
		pub:  newTestPublisher(t, pubKey),
		core: mkIndexer(t, false),
		reg:  mkRegistry(t),

		tmpStore: newTmpDatastore(),
	}
	// The ingester host does not listen, so that many tests do not use up
	// local ports.
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	e.ing, err = NewIngester(cfg, h, e.core, e.reg, dssync.MutexWrap(datastore.NewMapDatastore()), e.tmpStore)
	require.NoError(t, err)
	t.Cleanup(func() {
		if !e.stuck {
			_ = e.ing.Close()
			e.reg.Close()
			_ = e.core.Close()
		}
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

// putRaw adds a block of data, whatever it holds, linked with a CID of the
// codec.
func (e *testIngestEnv) putRaw(codec multicodec.Code, data []byte) ipld.Link {
	mh, _ := multihash.Sum(data, multihash.SHA2_256, -1)
	c := cid.NewCidV1(uint64(codec), mh)
	e.pub.put(c, data)
	return cidlink.Link{Cid: c}
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

// hamtNode adds a HAMT node below the root
func (e *testIngestEnv) hamtNode(t *testing.T, n *hamt.HashMapNode) ipld.Link {
	return e.store(t, bindnode.Wrap(n, hamt.HashMapNodePrototype.Type()).Representation())
}

// hamtRoot adds a HAMT root of sha2-256 hashes with the given bucket size,
// in its type-level form, as hamt.Builder writes it.
func (e *testIngestEnv) hamtRoot(t *testing.T, bucketSize int, n hamt.HashMapNode) ipld.Link {
	return e.hamtRootHash(t, multicodec.Sha2_256, bucketSize, n)
}

// hamtRootHash adds a HAMT root with the hash algorithm and bucket size, in
// its type-level form, as hamt.Builder writes it.
func (e *testIngestEnv) hamtRootHash(t *testing.T, hashAlg multicodec.Code, bucketSize int, n hamt.HashMapNode) ipld.Link {
	root := &hamt.HashMapRoot{HashAlg: hashAlg, BucketSize: bucketSize, Hamt: n}
	return e.store(t, bindnode.Wrap(root, hamt.HashMapRootPrototype.Type()))
}

// setHamtBit sets slot in a HAMT bitmap. Slot 0 is the most significant bit
// of the first byte.
func (s *IngestTestSuite) setHamtBit(bitmap []byte, slot int) {
	bitmap[slot>>3] |= 0x80 >> (slot & 7)
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

// signWithExtendedProviders signs the ad with the publisher's key. It signs
// the extended provider records other than the publisher's own with keys,
// in order.
func (e *testIngestEnv) signWithExtendedProviders(
	t *testing.T,
	ad *schema.Advertisement,
	keys ...crypto.PrivKey,
) {
	require.NoError(t, ad.SignWithExtendedProviders(
		e.pub.key,
		func(string) (crypto.PrivKey, error) {
			key := keys[0]
			keys = keys[1:]
			return key, nil
		},
	))
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

// requireExtendedProviders requires that the registry holds exactly want as
// the extended providers of the publisher's provider for the context, or
// for the whole chain when contextID is empty.
func (e *testIngestEnv) requireExtendedProviders(t *testing.T, contextID []byte, want ...peer.ID) {
	t.Helper()
	info, ok := e.reg.ProviderInfo(e.pub.id)
	require.True(t, ok)
	var got []peer.ID
	if eps := info.ExtendedProviders; eps != nil {
		infos := eps.Providers
		if len(contextID) != 0 {
			infos = eps.ContextualProviders[string(contextID)].Providers
		}
		for _, ep := range infos {
			got = append(got, ep.PeerID)
		}
	}
	require.ElementsMatch(t, want, got)
}

// requireSkipped requires that the ad was processed and skipped for the
// reason, an adIngestState.
func (e *testIngestEnv) requireSkipped(t *testing.T, adCid cid.Cid, reason adIngestState) {
	t.Helper()
	state := e.adState(t, adCid)
	require.True(t, state.Processed)
	require.True(t, state.Skipped)
	require.Contains(t, state.SkipReason, string(reason))
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
	s.withConfig(defaultTestIngestConfig)
}

// withConfig replaces the test's environment with one whose ingester has
// the configuration cfg.
func (s *IngestTestSuite) withConfig(cfg config.Ingest) {
	t := s.T()
	key, _ := s.newPeerKey(t)
	s.testIngestEnv = newTestIngestEnv(t, cfg, key)
}

// digestMultihashes returns a sha2-256 multihash for each of the digest
// lengths.
func (s *IngestTestSuite) digestMultihashes(t *testing.T, lengths ...int) []multihash.Multihash {
	mhs := make([]multihash.Multihash, len(lengths))
	for i, n := range lengths {
		var err error
		mhs[i], err = multihash.Encode(bytes.Repeat([]byte{byte(i)}, n), multihash.SHA2_256)
		require.NoError(t, err)
	}
	return mhs
}

// hamtBucket returns a HAMT element that is a bucket of the multihashes
func (s *IngestTestSuite) hamtBucket(mhs ...multihash.Multihash) hamt.Element {
	b := make(hamt.Bucket, len(mhs))
	for i, m := range mhs {
		b[i] = hamt.BucketEntry{Key: m, Value: basicnode.NewBool(true)}
	}
	return hamt.Element{Bucket: &b}
}

// hamtLink returns a HAMT element that links to a node
func (s *IngestTestSuite) hamtLink(lnk ipld.Link) hamt.Element {
	return hamt.Element{HashMapNode: &lnk}
}

// hamtKey returns a multihash whose sha2-256 hash selects slots, one slot of 3
// bits for each depth from the root, as in a HAMT with a 1-byte bitmap.
func (s *IngestTestSuite) hamtKey(slots ...int) multihash.Multihash {
	for {
		m := random.Multihashes(1)[0]
		if checkHamtKey(multicodec.Sha2_256, m, slots, 3) == nil {
			return m
		}
	}
}

// buildHamt adds a HAMT of mhs that hamt.Builder builds with the hash
// algorithm, the bit width and a bucket size of 1.
func (s *IngestTestSuite) buildHamt(t *testing.T, hashAlg multicodec.Code, bitWidth int, mhs []multihash.Multihash) ipld.Link {
	b := hamt.NewBuilder(hamt.Prototype{BitWidth: bitWidth, BucketSize: 1}.WithHashAlg(hashAlg)).
		WithLinking(s.pub.lsys, schema.Linkproto)
	ma, err := b.BeginMap(int64(len(mhs)))
	require.NoError(t, err)
	for _, m := range mhs {
		require.NoError(t, ma.AssembleKey().AssignBytes(m))
		require.NoError(t, ma.AssembleValue().AssignBool(true))
	}
	require.NoError(t, ma.Finish())
	return s.store(t, b.Build().(*hamt.Node).Substrate())
}

// newPeerKey returns a new key and its peer ID
func (s *IngestTestSuite) newPeerKey(t *testing.T) (crypto.PrivKey, peer.ID) {
	key, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(key)
	require.NoError(t, err)
	return key, id
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
	// A random HAMT of 200 multihashes can have more nodes than the default
	// test EntriesDepthLimit
	cfg := defaultTestIngestConfig
	cfg.EntriesDepthLimit = 1000
	s.withConfig(cfg)
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

// An extended provider record with no valid address is ignored, and the rest
// of the ad is processed. The record of the ad's own provider takes the ad's
// addresses.
func (s *IngestTestSuite) TestExtendedProviderWithoutAddress() {
	t := s.T()
	mhs := random.Multihashes(1)
	epKey, epID := s.newPeerKey(t)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String(), Metadata: []byte("md")},
		{ID: epID.String(), Addresses: []string{"not an address"}, Metadata: []byte("md")},
	}}
	s.signWithExtendedProviders(t, &ad, epKey)
	adCid := s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, ad.ContextID, s.pub.id)
	info, _ := s.reg.ProviderInfo(s.pub.id)
	ep := info.ExtendedProviders.ContextualProviders[string(ad.ContextID)].Providers[0]
	require.Equal(t, testAdAddr, ep.Addrs[0].String())
}

// An extended provider record with an undecodable peer ID is ignored, and
// the rest of the ad is processed.
func (s *IngestTestSuite) TestExtendedProviderWithInvalidID() {
	t := s.T()
	mhs := random.Multihashes(1)
	epKey, _ := s.newPeerKey(t)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String(), Metadata: []byte("md")},
		{ID: "not a peer ID", Addresses: []string{testAdAddr}, Metadata: []byte("md")},
	}}
	s.signWithExtendedProviders(t, &ad, epKey)
	adCid := s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, ad.ContextID, s.pub.id)
}

// An ad whose extended provider records are all ignored keeps the extended
// providers registered by an earlier ad: the ad is processed as if it had
// none.
func (s *IngestTestSuite) TestExtendedProvidersAllIgnored() {
	t := s.T()
	epKey, epID := s.newPeerKey(t)
	first := s.newAd(s.chunk(t, random.Multihashes(1), nil))
	first.ContextID = nil
	first.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String()},
		{ID: epID.String(), Addresses: []string{testAdAddr}},
	}}
	s.signWithExtendedProviders(t, &first, epKey)
	s.publish(t, first)
	// The ad has no addresses, so the record of its own provider, which
	// omits them, is ignored too.
	mhs := random.Multihashes(1)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ContextID = nil
	ad.Addresses = nil
	ad.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String()},
		{ID: epID.String(), Addresses: []string{"not an address"}},
	}}
	s.signWithExtendedProviders(t, &ad, epKey)
	adCid := s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, nil, s.pub.id, epID)
}

// An ExtendedProvider on a removal ad is ignored, and the removal is
// processed. The ExtendedProvider has no records, since a removal ad with
// records cannot be signed.
func (s *IngestTestSuite) TestExtendedProvidersOnRemoval() {
	t := s.T()
	mhs := random.Multihashes(1)
	s.publish(t, s.newAd(s.chunk(t, mhs, nil)))
	rm := s.newAd(schema.NoEntries)
	rm.IsRm = true
	rm.ExtendedProvider = &schema.ExtendedProvider{}
	s.signWithExtendedProviders(t, &rm)
	rmCid := s.publish(t, rm)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, rmCid).Processed)
	s.requireNotIndexed(t, mhs)
	s.requireExtendedProviders(t, rm.ContextID)
}

// Extended providers with Override set on an ad without a context ID are
// ignored, and the rest of the ad is processed.
func (s *IngestTestSuite) TestExtendedProvidersOverrideWithoutContextID() {
	t := s.T()
	mhs := random.Multihashes(1)
	epKey, epID := s.newPeerKey(t)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ContextID = nil
	ad.ExtendedProvider = &schema.ExtendedProvider{Override: true, Providers: []schema.Provider{
		{ID: s.pub.id.String()},
		{ID: epID.String(), Addresses: []string{testAdAddr}},
	}}
	s.signWithExtendedProviders(t, &ad, epKey)
	adCid := s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, nil)
}

// An entries block that does not decode skips the ad as malformed
func (s *IngestTestSuite) TestEntriesBlockNotDecoding() {
	t := s.T()
	adCid := s.publish(t, s.newAd(s.putRaw(multicodec.DagJson, []byte("{not json"))))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// A Next block that does not decode skips the ad as malformed
func (s *IngestTestSuite) TestNextEntriesBlockNotDecoding() {
	t := s.T()
	next := s.putRaw(multicodec.DagCbor, []byte{0xff})
	adCid := s.publish(t, s.newAd(s.chunk(t, random.Multihashes(1), next)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// A Next block that is not an entry chunk skips the ad, and the multihashes
// of the chunk before it stay indexed.
func (s *IngestTestSuite) TestNextBlockNotEntryChunk() {
	t := s.T()
	mhs := random.Multihashes(1)
	next := s.store(t, basicnode.NewString("not an entry chunk"))
	adCid := s.publish(t, s.newAd(s.chunk(t, mhs, next)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestEntryChunkErr)
	s.requireIndexed(t, mhs)
	// The block that is not an entry chunk is not left in the datastore.
	require.Zero(t, s.tmpStore.blocksHeld())
}

// An entries block that decodes but is not an entry chunk skips the ad, and
// the block is not left in the temporary datastore.
func (s *IngestTestSuite) TestEntriesBlockNotEntryChunk() {
	t := s.T()
	adCid := s.publish(t, s.newAd(s.store(t, basicnode.NewString("not an entry chunk"))))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestEntryChunkErr)
	require.Zero(t, s.tmpStore.blocksHeld())
}

// An entries block that is a valid advertisement signed for a provider the
// publisher may not publish for skips the ad as malformed. The block can
// never be an entry chunk, so a later change of policy cannot accept it.
func (s *IngestTestSuite) TestEntriesBlockPublishNotAllowed() {
	t := s.T()
	_, providerID := s.newPeerKey(t)
	signed := schema.Advertisement{
		Provider:  providerID.String(),
		Addresses: []string{testAdAddr},
		Entries:   schema.NoEntries,
		ContextID: []byte("inner"),
		Metadata:  []byte("m"),
	}
	require.NoError(t, signed.Sign(s.pub.key))
	node, err := signed.ToNode()
	require.NoError(t, err)
	adCid := s.publish(t, s.newAd(s.store(t, node)))
	require.NoError(t, s.reg.SetPolicy(config.Policy{Allow: true, Publish: false}))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// An entry chunk that is gone from the temporary datastore when ingest loads
// it, as when another worker that synced the same chunk has deleted it, leaves
// the ad to be retried, and a later sync indexes it. An ad is processed first,
// so that the retry starts from it.
func (s *IngestTestSuite) TestEntryChunkMissingFromDatastore() {
	t := s.T()
	s.publish(t, s.newAd(s.chunk(t, random.Multihashes(1), nil)))
	require.NoError(t, s.sync(t.Context()))

	mhs := random.Multihashes(2)
	next := s.chunk(t, mhs[1:], nil)
	adCid := s.publish(t, s.newAd(s.chunk(t, mhs[:1], next)))

	s.tmpStore.deleteAfterRead(next.(cidlink.Link).Cid)
	require.ErrorContains(t, s.sync(t.Context()), string(adIngestIndexerErr))
	require.False(t, s.adState(t, adCid).Processed)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
}

// notAnAd returns a node that the link system takes for an advertisement,
// since it has a Signature field, but that is not a valid advertisement.
func (s *IngestTestSuite) notAnAd(t *testing.T) datamodel.Node {
	n, err := qp.BuildMap(basicnode.Prototype.Any, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "Signature", qp.Bytes([]byte("not a signature")))
	})
	require.NoError(t, err)
	return n
}

// An entries block that the link system takes for an advertisement but that
// is not a valid one skips the ad as malformed.
func (s *IngestTestSuite) TestEntriesBlockIsBadAd() {
	t := s.T()
	adCid := s.publish(t, s.newAd(s.store(t, s.notAnAd(t))))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// A Next block that the link system takes for an advertisement but that is
// not a valid one skips the ad as malformed.
func (s *IngestTestSuite) TestNextEntriesBlockIsBadAd() {
	t := s.T()
	adCid := s.publish(t, s.newAd(s.chunk(t, random.Multihashes(1), s.store(t, s.notAnAd(t)))))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// A Next block larger than the dagsync block size limit skips the ad as
// malformed, and the multihashes of the chunk before it stay indexed. The
// block is refused before it is stored.
func (s *IngestTestSuite) TestNextEntriesBlockTooLarge() {
	t := s.T()
	mhs := random.Multihashes(1)
	next := s.putRaw(multicodec.Raw, make([]byte, ipnisync.DefaultMaxBlockSize+1))
	adCid := s.publish(t, s.newAd(s.chunk(t, mhs, next)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireIndexed(t, mhs)
	require.Zero(t, s.tmpStore.blocksHeld())
}

// A multihash with a digest longer than maxDigestLength is not indexed. The
// other multihashes of the ad are.
func (s *IngestTestSuite) TestMultihashDigestTooLong() {
	t := s.T()
	indexed := s.digestMultihashes(t, 32, maxDigestLength)
	tooLong := s.digestMultihashes(t, maxDigestLength+1, 1<<16)
	adCid := s.publish(t, s.newAd(s.chunk(t, append(slices.Clone(indexed), tooLong...), nil)))

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, indexed)
	s.requireNotIndexed(t, tooLong)
}

// A HAMT whose structure does not match the HAMT specification skips the ad
// as malformed. Each root below has a 1-byte bitmap, so a bit width of 3, and
// a bucket size of 3. Each HAMT holds fewer multihashes than one indexing
// batch, so none of them are indexed.
func (s *IngestTestSuite) TestHamtMalformed() {
	t := s.T()
	// The key name gives the slots that its hash selects, from the root down
	k0, k1, k00, k10 := s.hamtKey(0), s.hamtKey(1), s.hamtKey(0, 0), s.hamtKey(1, 0)
	keys := []multihash.Multihash{k0, k1, k00, k10}
	child := s.hamtNode(t, &hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k00)}})
	wideChild := s.hamtNode(t, &hamt.HashMapNode{Map: []byte{0x80, 0}, Data: []hamt.Element{s.hamtBucket(k00)}})
	misplacedChild := s.hamtNode(t, &hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k10)}})
	notNode := s.store(t, basicnode.NewString("not a HAMT node"))
	emptyChild := s.hamtNode(t, &hamt.HashMapNode{Map: []byte{0}})
	roots := map[string]hamt.HashMapNode{
		"bitmap has more bits than data":          {Map: []byte{0xc0}, Data: []hamt.Element{s.hamtBucket(k0)}},
		"bitmap has bits but no data":             {Map: []byte{0xff}},
		"data has more elements than bitmap bits": {Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k0), s.hamtBucket(k1)}},
		"empty bitmap":                            {Data: []hamt.Element{s.hamtBucket(k0)}},
		"bitmap length not a power of two":        {Map: []byte{0x80, 0, 0}, Data: []hamt.Element{s.hamtBucket(k0)}},
		"empty bucket":                            {Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket()}},
		"bucket larger than bucket size":          {Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k0, k0, k0, k0)}},
		"child bitmap length differs from root":   {Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(wideChild)}},
		"child linked twice":                      {Map: []byte{0xc0}, Data: []hamt.Element{s.hamtLink(child), s.hamtLink(child)}},
		"child is not a HAMT node":                {Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(notNode)}},
		"key in the wrong slot":                   {Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k1)}},
		"key in the wrong subtree":                {Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(misplacedChild)}},
		"child with no elements":                  {Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(emptyChild)}},
	}
	for name, root := range roots {
		s.Run(name, func() {
			t := s.T()
			adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))
			require.NoError(t, s.sync(t.Context()))
			s.requireSkipped(t, adCid, adIngestMalformedErr)
			s.requireNotIndexed(t, keys)
		})
	}
}

// A HAMT root with a hash algorithm that the HAMT specification does not
// define skips the ad as malformed.
func (s *IngestTestSuite) TestHamtUnsupportedHashAlgorithm() {
	t := s.T()
	k0 := s.hamtKey(0)
	root := &hamt.HashMapRoot{
		HashAlg:    multicodec.Sha2_512,
		BucketSize: 3,
		Hamt:       hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k0)}},
	}
	adCid := s.publish(t, s.newAd(s.store(t, bindnode.Wrap(root, hamt.HashMapRootPrototype.Type()))))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireNotIndexed(t, []multihash.Multihash{k0})
}

// A HAMT that hamt.Builder builds is indexed in full with each hash
// algorithm that the HAMT specification defines, and with bit widths from the
// smallest to one that needs a bitmap of more than a byte.
func (s *IngestTestSuite) TestHamtHashAlgorithms() {
	for _, hashAlg := range []multicodec.Code{multicodec.Identity, multicodec.Sha2_256, multicodec.Murmur3X64_64} {
		for _, bitWidth := range []int{3, 5, 8, 16} {
			s.Run(fmt.Sprintf("%s/bitwidth-%d", hashAlg, bitWidth), func() {
				t := s.T()
				mhs := random.Multihashes(100)
				adCid := s.publish(t, s.newAd(s.buildHamt(t, hashAlg, bitWidth, mhs)))
				require.NoError(t, s.sync(t.Context()))
				require.True(t, s.adState(t, adCid).Processed)
				require.False(t, s.adState(t, adCid).Skipped)
				s.requireIndexed(t, mhs)
			})
		}
	}
}

// A HAMT root with a bucket size of 0 skips the ad as malformed
func (s *IngestTestSuite) TestHamtBucketSizeZero() {
	t := s.T()
	k0 := s.hamtKey(0)
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k0)}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 0, root)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireNotIndexed(t, []multihash.Multihash{k0})
}

// With the identity hash algorithm, a key shorter than the bits that its
// depth needs skips the ad as malformed. The root's bitmap needs 3 bits of
// the key's hash, and the key is empty.
func (s *IngestTestSuite) TestHamtIdentityKeyTooShort() {
	t := s.T()
	root := &hamt.HashMapRoot{
		HashAlg:    multicodec.Identity,
		BucketSize: 3,
		Hamt:       hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(multihash.Multihash{})}},
	}
	adCid := s.publish(t, s.newAd(s.store(t, bindnode.Wrap(root, hamt.HashMapRootPrototype.Type()))))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// A HAMT that fails a check after the first batch of 4096 multihashes skips
// the ad as malformed, and the batch indexed before the failure stays
// indexed. The rest of the multihashes read before the failure are not
// indexed.
func (s *IngestTestSuite) TestHamtFailureAfterFirstBatch() {
	t := s.T()
	keys := make([]multihash.Multihash, 4100)
	for i := range keys {
		keys[i] = s.hamtKey(0)
	}
	notNode := s.store(t, basicnode.NewString("not a HAMT node"))
	root := hamt.HashMapNode{Map: []byte{0xc0}, Data: []hamt.Element{s.hamtBucket(keys...), s.hamtLink(notNode)}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, len(keys), root)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireIndexed(t, keys[:4096])
	s.requireNotIndexed(t, keys[4096:])
}

// A HAMT node that is gone from the temporary datastore when ingest loads it,
// as when another worker that synced the same node has deleted it, leaves
// the ad to be retried, and a later sync indexes it. An ad is processed first,
// so that the retry starts from it.
func (s *IngestTestSuite) TestHamtNodeMissingFromDatastore() {
	t := s.T()
	s.publish(t, s.newAd(s.chunk(t, random.Multihashes(1), nil)))
	require.NoError(t, s.sync(t.Context()))

	k00 := s.hamtKey(0, 0)
	child := s.hamtNode(t, &hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(k00)}})
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(child)}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

	s.tmpStore.deleteAfterRead(child.(cidlink.Link).Cid)
	require.ErrorContains(t, s.sync(t.Context()), string(adIngestIndexerErr))
	require.False(t, s.adState(t, adCid).Processed)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, []multihash.Multihash{k00})
}

// A HAMT whose root links to a chain of 40 nodes that each link twice to
// the node below is refused after a few fetches. Following every link would
// fetch 2^40 nodes. No key can match a path of 42 slots, so the walk refuses
// the HAMT at the first bucket it reaches.
func (s *IngestTestSuite) TestHamtLinkedTwiceAtEachLevel() {
	t := s.T()
	mh := random.Multihashes(1)
	node := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(mh...)}}
	for range 40 {
		below := s.hamtNode(t, &node)
		node = hamt.HashMapNode{Map: []byte{0xc0}, Data: []hamt.Element{s.hamtLink(below), s.hamtLink(below)}}
	}
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(s.hamtNode(t, &node))}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.sync(ctx))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireNotIndexed(t, mh)
}

// A HAMT node deeper below the root than EntriesDepthLimit skips the ad as
// malformed. The HAMT is otherwise valid: its key selects the slots on its
// path, so without the limit it would be indexed.
func (s *IngestTestSuite) TestHamtTooDeep() {
	t := s.T()
	cfg := defaultTestIngestConfig
	cfg.EntriesDepthLimit = 2
	s.withConfig(cfg)
	// Each node has its one element in slot 0, and the bucket is one level
	// below the deepest node allowed.
	key := s.hamtKey(make([]int, cfg.EntriesDepthLimit+2)...)
	node := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(key)}}
	for range cfg.EntriesDepthLimit + 1 {
		node = hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(s.hamtNode(t, &node))}}
	}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, node)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireNotIndexed(t, []multihash.Multihash{key})
}

// Ingest syncs at most EntriesDepthLimit nodes of a HAMT below the root, as
// it syncs at most that many entry chunks. The keys of the nodes before the
// limit are indexed and the ad is processed; a node past it is not fetched.
func (s *IngestTestSuite) TestHamtNodeCountLimit() {
	for _, tc := range []struct {
		name  string
		nodes int
	}{
		{name: "at limit", nodes: 2},
		{name: "over limit", nodes: 3},
	} {
		s.Run(tc.name, func() {
			t := s.T()
			cfg := defaultTestIngestConfig
			cfg.EntriesDepthLimit = 2
			s.withConfig(cfg)
			// The root links to one node in each of its first slots, and each
			// node holds a key in its slot 0.
			root := hamt.HashMapNode{Map: []byte{0}}
			var keys []multihash.Multihash
			var nodes []ipld.Link
			for slot := range tc.nodes {
				key := s.hamtKey(slot, 0)
				keys = append(keys, key)
				child := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(key)}}
				s.setHamtBit(root.Map, slot)
				nodes = append(nodes, s.hamtNode(t, &child))
				root.Data = append(root.Data, s.hamtLink(nodes[slot]))
			}
			adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

			require.NoError(t, s.sync(t.Context()))
			require.True(t, s.adState(t, adCid).Processed)
			require.False(t, s.adState(t, adCid).Skipped)
			limit := min(tc.nodes, cfg.EntriesDepthLimit)
			s.requireIndexed(t, keys[:limit])
			s.requireNotIndexed(t, keys[limit:])
			s.requireNotFetched(t, nodes[limit:]...)
		})
	}
}

// requireNotFetched requires that none of links was fetched from the
// publisher.
func (s *IngestTestSuite) requireNotFetched(t *testing.T, links ...ipld.Link) {
	t.Helper()
	fetched := s.pub.fetchedBlocks()
	for _, l := range links {
		require.NotContains(t, fetched, l.(cidlink.Link).Cid)
	}
}

// Ingest syncs only the block that a HAMT node links to, also when that block
// is an entry chunk whose Next links to more chunks and a sync would follow
// them up to SyncSegmentDepthLimit.
func (s *IngestTestSuite) TestHamtNodeSyncedAlone() {
	t := s.T()
	cfg := defaultTestIngestConfig
	cfg.SyncSegmentDepthLimit = 2000
	s.withConfig(cfg)
	chain := make([]ipld.Link, 10)
	var next ipld.Link
	for i := range chain {
		next = s.chunk(t, random.Multihashes(1), next)
		chain[len(chain)-1-i] = next
	}
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(chain[0])}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	s.requireNotFetched(t, chain[1:]...)
}

// A HAMT node too deep for any key below it to have enough bits of its hash
// to select its path skips the ad as malformed, and is not fetched. With a
// bit width of 3, sha2-256 allows 256 bits and murmur3 128, so a bucket is at
// most 85 or 42 levels deep.
func (s *IngestTestSuite) TestHamtDeeperThanHash() {
	for _, tc := range []struct {
		alg      multicodec.Code
		hashBits int
	}{
		{multicodec.Sha2_256, 256},
		{multicodec.Murmur3X64_64, 128},
	} {
		s.Run(tc.alg.String(), func() {
			t := s.T()
			const bitWidth = 3
			// The two deepest nodes are past the hash, so neither is fetched.
			n := tc.hashBits/bitWidth + 1
			node := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(random.Multihashes(1)...)}}
			nodes := make([]ipld.Link, n)
			for i := range nodes {
				nodes[i] = s.hamtNode(t, &node)
				node = hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(nodes[i])}}
			}
			adCid := s.publish(t, s.newAd(s.hamtRootHash(t, tc.alg, 3, node)))

			require.NoError(t, s.sync(t.Context()))
			s.requireSkipped(t, adCid, adIngestMalformedErr)
			s.requireNotFetched(t, nodes[0], nodes[1])
		})
	}
}

// A HAMT whose root links to a chain of 40 nodes that each link twice to
// the node below, with an empty node at the bottom, is refused after a few
// fetches. Following every link would fetch 2^40 nodes.
func (s *IngestTestSuite) TestHamtLinkedTwiceAboveEmptyNode() {
	t := s.T()
	node := hamt.HashMapNode{Map: []byte{0}}
	for range 40 {
		below := s.hamtNode(t, &node)
		node = hamt.HashMapNode{Map: []byte{0xc0}, Data: []hamt.Element{s.hamtLink(below), s.hamtLink(below)}}
	}
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(s.hamtNode(t, &node))}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.sync(ctx))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	require.Less(t, len(s.pub.fetchedBlocks()), 100)
}

// A HAMT node that the link system takes for an advertisement but that is not
// a valid one skips the ad as malformed.
func (s *IngestTestSuite) TestHamtChildIsBadAd() {
	t := s.T()
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(s.store(t, s.notAnAd(t)))}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
}

// A HAMT node that the publisher does not serve skips the ad as content not
// found.
func (s *IngestTestSuite) TestHamtChildNotServed() {
	t := s.T()
	missing := cidlink.Link{Cid: cid.NewCidV1(cid.DagJSON, random.Multihashes(1)[0])}
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(missing)}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, root)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestContentNotFound)
}

// A HAMT whose deepest node is EntriesDepthLimit levels below the root is
// indexed.
func (s *IngestTestSuite) TestHamtAtDepthLimit() {
	t := s.T()
	cfg := defaultTestIngestConfig
	cfg.EntriesDepthLimit = 2
	s.withConfig(cfg)
	// Each node has its one element in slot 0, and the bucket is in the
	// deepest node allowed.
	key := s.hamtKey(make([]int, cfg.EntriesDepthLimit+1)...)
	node := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(key)}}
	for range cfg.EntriesDepthLimit {
		node = hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtLink(s.hamtNode(t, &node))}}
	}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, 3, node)))

	require.NoError(t, s.sync(t.Context()))
	require.False(t, s.adState(t, adCid).Skipped)
	s.requireIndexed(t, []multihash.Multihash{key})
}

// A block with a "hamt" field that is not a HAMT root skips the ad as
// malformed. isHAMT accepts the field, and loading the root then fails.
func (s *IngestTestSuite) TestHamtFieldIsNotARoot() {
	t := s.T()
	n, err := qp.BuildMap(basicnode.Prototype.Any, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "hamt", qp.String("x"))
	})
	require.NoError(t, err)
	adCid := s.publish(t, s.newAd(s.store(t, n)))

	require.NoError(t, s.sync(t.Context()))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	require.Zero(t, s.tmpStore.blocksHeld())
}

// An indexer Put that fails after the first batch of a HAMT stops the walk.
// The batch already indexed stays indexed, and the ad is left to be retried.
func (s *IngestTestSuite) TestHamtIndexerPutFails() {
	t := s.T()
	const batch = 4096
	// Two batches, and keys still unread when the second Put fails, so the
	// walk stops before it has yielded every key.
	keys := make([]multihash.Multihash, 2*batch+4)
	for i := range keys {
		keys[i] = s.hamtKey(0)
	}
	root := hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(keys...)}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, len(keys), root)))
	s.ing.indexer = &limitedIndexer{Interface: s.ing.indexer, after: 1, fail: true}

	require.ErrorContains(t, s.sync(t.Context()), string(adIngestIndexerErr))
	require.False(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, keys[:batch])
	s.requireNotIndexed(t, keys[batch:])
}

// Cancelling the worker after the first batch of a HAMT stops the walk at
// the next link. The batch already indexed stays indexed, and the ad is left
// to be retried. The walk uses the worker's context, so the cancel is the
// ingester's, not the caller's.
func (s *IngestTestSuite) TestHamtCancelAfterFirstBatch() {
	t := s.T()
	const batch = 4096
	keys := make([]multihash.Multihash, batch)
	for i := range keys {
		keys[i] = s.hamtKey(0)
	}
	childKey := s.hamtKey(0)
	child := s.hamtNode(t, &hamt.HashMapNode{Map: []byte{0x80}, Data: []hamt.Element{s.hamtBucket(childKey)}})
	root := hamt.HashMapNode{Map: []byte{0xc0}, Data: []hamt.Element{s.hamtBucket(keys...), s.hamtLink(child)}}
	adCid := s.publish(t, s.newAd(s.hamtRoot(t, batch, root)))
	s.ing.indexer = &limitedIndexer{Interface: s.ing.indexer, after: 1, cancel: s.ing.cancelWorkers}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.ErrorIs(t, s.sync(ctx), context.Canceled)
	require.False(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, keys)
	s.requireNotIndexed(t, []multihash.Multihash{childKey})
	s.requireNotFetched(t, child)
}

// With the identity hash, a link too deep for any key that indexing would
// accept is not synced, and the rest of the ad is indexed. The longest
// indexable multihash is maxDigestLength bytes plus two varints.
func (s *IngestTestSuite) TestHamtIdentityLinkTooDeep() {
	t := s.T()
	const bitWidth = 16
	mapLen := 1 << (bitWidth - 3)
	identityBits := 8 * (maxDigestLength + 2*binary.MaxVarintLen64)
	// The two deepest nodes are past the hash, so neither is fetched.
	n := identityBits/bitWidth + 1
	node := hamt.HashMapNode{Map: make([]byte, mapLen)}
	node.Map[0] = 0x80
	nodes := make([]ipld.Link, n)
	for i := range nodes {
		nodes[i] = s.hamtNode(t, &node)
		node = hamt.HashMapNode{Map: make([]byte, mapLen)}
		node.Map[0] = 0x80
		node.Data = []hamt.Element{s.hamtLink(nodes[i])}
	}
	key := random.Multihashes(1)[0]
	keySlot := hashBits(key, 0, bitWidth)
	chainSlot := 0
	if keySlot == 0 {
		chainSlot = 1
	}
	rootMap := make([]byte, mapLen)
	s.setHamtBit(rootMap, keySlot)
	s.setHamtBit(rootMap, chainSlot)
	bucket := s.hamtBucket(key)
	link := s.hamtLink(nodes[n-1])
	data := []hamt.Element{link, bucket}
	if keySlot < chainSlot {
		data = []hamt.Element{bucket, link}
	}
	root := hamt.HashMapNode{Map: rootMap, Data: data}
	adCid := s.publish(t, s.newAd(s.hamtRootHash(t, multicodec.Identity, 3, root)))

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	require.False(t, s.adState(t, adCid).Skipped)
	s.requireIndexed(t, []multihash.Multihash{key})
	s.requireNotFetched(t, nodes[0], nodes[1])
}

// In an identity HAMT whose deepest links are skipped, the nodes below a node
// linked from two places hold no keys, so no key check finds the second path.
// Here every node links twice to the node below it, so the number of paths
// doubles with each level. The second link to a node that was synced skips the
// ad as malformed, with no limit on the number of nodes synced, after each
// node is fetched once.
func (s *IngestTestSuite) TestHamtIdentityLinkedTwice() {
	t := s.T()
	cfg := defaultTestIngestConfig
	cfg.EntriesDepthLimit = -1
	s.withConfig(cfg)
	const bitWidth = 16
	mapLen := 1 << (bitWidth - 3)
	identityBits := 8 * (maxDigestLength + 2*binary.MaxVarintLen64)
	// A chain whose deepest link is past the hash, as in
	// TestHamtIdentityLinkTooDeep, where each node links twice to the next.
	n := identityBits/bitWidth + 1
	node := hamt.HashMapNode{Map: make([]byte, mapLen)}
	node.Map[0] = 0x80
	for range n - 1 {
		below := s.hamtNode(t, &node)
		node = hamt.HashMapNode{Map: make([]byte, mapLen)}
		node.Map[0] = 0xc0
		node.Data = []hamt.Element{s.hamtLink(below), s.hamtLink(below)}
	}
	root := hamt.HashMapNode{Map: make([]byte, mapLen)}
	root.Map[0] = 0x80
	root.Data = []hamt.Element{s.hamtLink(s.hamtNode(t, &node))}
	adCid := s.publish(t, s.newAd(s.hamtRootHash(t, multicodec.Identity, 3, root)))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.sync(ctx))
	s.requireSkipped(t, adCid, adIngestMalformedErr)
	// The ad and the HAMT root, and the n nodes below the root
	require.LessOrEqual(t, len(s.pub.fetchedBlocks()), 2+n)
}

// The temporary datastore holds one HAMT node below the root at a time,
// however many nodes the HAMT has: ingest deletes each node as soon as it
// has loaded it. The node syncs share one connection with the publisher, so
// a HAMT with many nodes does not use up local ports.
func (s *IngestTestSuite) TestHamtNodesDeletedAsRead() {
	t := s.T()
	cfg := defaultTestIngestConfig
	cfg.EntriesDepthLimit = 1000
	s.withConfig(cfg)
	entries := typehelpers.RandomHamtEntryBuilder{BitWidth: 3, BucketSize: 1, MultihashCount: 300}.Build(t, s.pub.lsys)
	adCid := s.publish(t, s.newAd(entries))

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, typehelpers.AllMultihashesFromAdLink(t, cidlink.Link{Cid: adCid}, s.pub.lsys))
	// The ad, the HAMT root and one node
	require.LessOrEqual(t, s.tmpStore.peakBlocks(), 3)
	require.Equal(t, 1, s.pub.blockConns())
}

// An extended provider record signed by its own key is registered
func (s *IngestTestSuite) TestExtendedProviderSignedByOwnKey() {
	t := s.T()
	mhs := random.Multihashes(1)
	epKey, epID := s.newPeerKey(t)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String()},
		{ID: epID.String(), Addresses: []string{"/ip4/127.0.0.2/tcp/9999"}},
	}}
	s.signWithExtendedProviders(t, &ad, epKey)
	s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, ad.ContextID, s.pub.id, epID)
}

// An extended provider record signed by a key other than the extended
// provider's own is ignored, so that a publisher cannot name any peer as a
// provider of its content. The rest of the ad is processed.
func (s *IngestTestSuite) TestExtendedProviderSignedByOtherKey() {
	t := s.T()
	mhs := random.Multihashes(1)
	_, epID := s.newPeerKey(t)
	otherKey, _ := s.newPeerKey(t)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String()},
		{ID: epID.String(), Addresses: []string{"/ip4/127.0.0.2/tcp/9999"}},
	}}
	s.signWithExtendedProviders(t, &ad, otherKey)
	adCid := s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, ad.ContextID, s.pub.id)
}

// The record of the ad's own provider signed by a key that is neither the
// provider's nor the ad signer's is ignored. Its signature comes from a copy
// of the ad signed with another key, which is valid for the ad, since the
// signature of a record does not cover the signatures in the ad.
func (s *IngestTestSuite) TestOwnExtendedProviderSignedByOtherKey() {
	t := s.T()
	mhs := random.Multihashes(1)
	epKey, epID := s.newPeerKey(t)
	otherKey, _ := s.newPeerKey(t)
	ad := s.newAd(s.chunk(t, mhs, nil))
	ad.ExtendedProvider = &schema.ExtendedProvider{Providers: []schema.Provider{
		{ID: s.pub.id.String()},
		{ID: epID.String(), Addresses: []string{"/ip4/127.0.0.2/tcp/9999"}},
	}}
	other := ad
	other.ExtendedProvider = &schema.ExtendedProvider{Providers: slices.Clone(ad.ExtendedProvider.Providers)}
	require.NoError(t, other.SignWithExtendedProviders(otherKey, func(string) (crypto.PrivKey, error) { return epKey, nil }))
	s.signWithExtendedProviders(t, &ad, epKey)
	ad.ExtendedProvider.Providers[0].Signature = other.ExtendedProvider.Providers[0].Signature
	adCid := s.publish(t, ad)

	require.NoError(t, s.sync(t.Context()))
	require.True(t, s.adState(t, adCid).Processed)
	s.requireIndexed(t, mhs)
	s.requireExtendedProviders(t, ad.ContextID, epID)
}

// limitedIndexer wraps an indexer. after is how many puts succeed before
// fail makes the next Put return an error, or before cancel runs.
type limitedIndexer struct {
	indexer.Interface
	after  int
	puts   int
	fail   bool
	cancel context.CancelFunc
}

func (l *limitedIndexer) Put(v indexer.Value, mhs ...multihash.Multihash) error {
	if l.fail && l.puts >= l.after {
		return errors.New("indexer put failed")
	}
	err := l.Interface.Put(v, mhs...)
	if err != nil {
		return err
	}
	l.puts++
	if l.cancel != nil && l.puts >= l.after {
		l.cancel()
	}
	return nil
}
