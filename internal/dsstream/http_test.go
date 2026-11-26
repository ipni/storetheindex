package dsstream

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/ipfs/go-test/random"
	indexer "github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/stretchr/testify/require"
)

// testImportToken is the token of the test's senders and client
const testImportToken = "test-token"

var testValueKinds = []importer.Kind{importer.KindRecords, importer.KindContextMap, importer.KindMultihashes}

// openTestValueStore opens a Pebble value store in dir, closed when the test
// ends
func openTestValueStore(t *testing.T, dir string) indexer.Interface {
	t.Helper()
	s, err := pebble.New(dir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fillTestValueStore puts values of several providers and contexts into s,
// with multihashes shared between some of them, and removes one context
func fillTestValueStore(t *testing.T, s indexer.Interface) {
	t.Helper()
	providers := random.Peers(8)
	shared := random.Multihashes(200)
	for i, p := range providers {
		for c := range 5 {
			v := indexer.Value{
				ProviderID:    p,
				ContextID:     fmt.Appendf(nil, "context-%d-%d", i, c),
				MetadataBytes: fmt.Appendf(nil, "metadata-%d-%d", i, c),
			}
			require.NoError(t, s.Put(v, append(random.Multihashes(300), shared[:40*c]...)...))
		}
	}
	require.NoError(t, s.RemoveProviderContext(providers[0], []byte("context-0-1")))
	require.NoError(t, s.Flush())
}

// testSender starts a sender of sources, stopped when the test ends, and
// returns its URL
func testSender(t *testing.T, sources map[importer.Kind]importer.Source) *url.URL {
	t.Helper()
	h, err := importhttp.NewSender(sources, testImportToken, importhttp.WithPartBytes(16<<10))
	require.NoError(t, err)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return u
}

// testKindEntries returns every entry of kind that src serves
func testKindEntries(t *testing.T, src importer.Source, kind importer.Kind) []importer.Entry {
	t.Helper()
	return importertest.Entries(importertest.ReadParts(t, rand.New(rand.NewPCG(3, 4)), src, kind, 64<<10))
}

// TestImportOverHTTP copies an indexer's datastore and value store through
// one base URL, behind which a proxy routes each kind's path to the sender
// that holds the kind, as an ingress does: the datastore's kinds to one
// sender and the value store's kinds to another.
func TestImportOverHTTP(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))

	srcDS := openTestDatastore(t, t.TempDir())
	fillTestDatastore(t, srcDS)
	srcValues := openTestValueStore(t, t.TempDir())
	fillTestValueStore(t, srcValues)
	srcValueSource := srcValues.(importer.Source)

	dsSources := make(map[importer.Kind]importer.Source)
	for _, kind := range Kinds {
		dsSources[kind] = NewSource(srcDS)
	}
	valueSources := make(map[importer.Kind]importer.Source)
	for _, kind := range testValueKinds {
		valueSources[kind] = srcValueSource
	}
	dsURL := testSender(t, dsSources)
	valuesURL := testSender(t, valueSources)

	// The proxy routes by path, and streams each response as it arrives
	mux := http.NewServeMux()
	route := func(kinds []importer.Kind, to *url.URL) {
		proxy := httputil.NewSingleHostReverseProxy(to)
		proxy.FlushInterval = -1
		for _, kind := range kinds {
			mux.Handle(importhttp.PartPath+string(kind), proxy)
		}
	}
	route(Kinds, dsURL)
	route(testValueKinds, valuesURL)
	proxy := httptest.NewServer(mux)
	defer proxy.Close()

	remote, err := importhttp.NewClient(proxy.URL, testImportToken, importhttp.WithHTTPClient(proxy.Client()))
	require.NoError(t, err)

	dstDS := openTestDatastore(t, t.TempDir())
	dstDSTarget := NewTarget(dstDS)
	dstValues := openTestValueStore(t, t.TempDir())
	dstValueTarget := dstValues.(importer.Target)

	for _, kind := range Kinds {
		parts := importertest.Copy(t, rng, remote, dstDSTarget, kind, 0, 4)
		require.Positive(t, parts)
		if kind == importer.KindAdProcessed {
			require.Greater(t, parts, 5, "the sender's part size makes many parts")
		}
	}
	require.NoError(t, dstDSTarget.Sync())
	for _, kind := range testValueKinds {
		parts := importertest.Copy(t, rng, remote, dstValueTarget, kind, 0, 4)
		require.Positive(t, parts)
		if kind == importer.KindMultihashes {
			require.Greater(t, parts, 10, "the sender's part size makes many parts")
		}
	}
	require.NoError(t, dstValueTarget.Sync())

	// The destination serves the same entries as the source, read directly
	dstDSSource := NewSource(dstDS)
	for _, kind := range Kinds {
		want := testKindEntries(t, dsSources[kind], kind)
		require.NotEmpty(t, want, kind)
		require.Equal(t, want, testKindEntries(t, dstDSSource, kind), kind)
	}
	dstValueSource := dstValues.(importer.Source)
	for _, kind := range testValueKinds {
		want := testKindEntries(t, srcValueSource, kind)
		require.NotEmpty(t, want, kind)
		require.Equal(t, want, testKindEntries(t, dstValueSource, kind), kind)
	}

	// A kind that no route serves is refused for good
	_, _, err = remote.OpenStream(t.Context(), "unknown", nil, nil, 0)
	require.True(t, importer.IsPermanent(err), "%v", err)
}
