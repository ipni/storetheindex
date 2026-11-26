package command

import (
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"testing"

	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importertest"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	"github.com/ipni/go-indexer-core/store/pebble"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/dsstream"
	"github.com/ipni/storetheindex/server/importsender"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// testMigrateIn runs the migrate-in command with args
func testMigrateIn(t *testing.T, args ...string) error {
	app := &cli.App{Name: "storetheindex", Commands: []*cli.Command{MigrateInCmd}}
	return app.RunContext(t.Context(), append([]string{"storetheindex", "migrate-in"}, args...))
}

// testServe starts s, closed when the test ends
func testServe(t *testing.T, s *importhttp.Server) string {
	go func() { _ = s.Serve() }()
	t.Cleanup(func() { _ = s.Close() })
	return s.URL()
}

func TestMigrateIn(t *testing.T) {
	const token = "secret"
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(token+"\n"), 0o600))

	// The source's datastore and value store, each behind its own sender,
	// and one URL that routes each group of kinds to its sender by path
	// prefix, as an ingress does
	srcDS, err := leveldb.NewDatastore(filepath.Join(dir, "src-ds"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcDS.Close() })
	for i, c := range random.Cids(500) {
		require.NoError(t, srcDS.DB.Put([]byte(dskey.AdProcessedPrefix+c.String()), []byte{dskey.AdMarkerProcessed}, nil))
		require.NoError(t, srcDS.DB.Put([]byte(dskey.ProviderInfoPath+fmt.Sprintf("/provider-%d", i)), []byte("info"), nil))
	}
	dsSender, err := importsender.New("127.0.0.1:0", srcDS, token, 16<<10)
	require.NoError(t, err)

	srcVals, err := pebble.New(filepath.Join(dir, "src-pebble"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcVals.Close() })
	for _, p := range random.Peers(3) {
		require.NoError(t, srcVals.Put(indexer.Value{ProviderID: p, ContextID: []byte("ctx"), MetadataBytes: []byte("md")}, random.Multihashes(1000)...))
	}
	require.NoError(t, srcVals.Flush())
	sources := make(map[importer.Kind]importer.Source)
	for _, kind := range pebble.Kinds {
		sources[kind] = srcVals.(importer.Source)
	}
	h, err := importhttp.NewSender(sources, token, importhttp.WithPartBytes(16<<10))
	require.NoError(t, err)
	valSender, err := importhttp.Listen("127.0.0.1:0", h)
	require.NoError(t, err)

	mux := http.NewServeMux()
	route := func(prefix, to string) {
		u, err := url.Parse(to)
		require.NoError(t, err)
		proxy := httputil.NewSingleHostReverseProxy(u)
		proxy.FlushInterval = -1
		mux.Handle(importhttp.PartPath+prefix, proxy)
	}
	route("ds/", testServe(t, dsSender))
	route("vs/", testServe(t, valSender))
	source := httptest.NewServer(mux)
	t.Cleanup(source.Close)

	dstDSDir, dstPebbleDir := filepath.Join(dir, "dst-ds"), filepath.Join(dir, "dst-pebble")
	args := []string{
		"--source-url", source.URL,
		"--token-file", tokenFile,
		"--datastore-dir", dstDSDir,
		"--valuestore-dir", dstPebbleDir,
		"--workers", "4",
		"--worker-stagger", "0",
	}
	require.NoError(t, testMigrateIn(t, args...))
	// A done import does nothing again
	require.NoError(t, testMigrateIn(t, args...))

	// The destination stores hold the source's entries
	dstDS, err := leveldb.NewDatastore(dstDSDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstDS.Close() })
	dstVals, err := pebble.New(dstPebbleDir, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstVals.Close() })
	rng := rand.New(rand.NewPCG(1, 2))
	entries := func(src importer.Source, kind importer.Kind) []importer.Entry {
		return importertest.Entries(importertest.ReadParts(t, rng, src, kind, 0))
	}
	for _, kind := range dsstream.Kinds {
		require.Equal(t, entries(dsstream.NewSource(srcDS), kind), entries(dsstream.NewSource(dstDS), kind), kind)
	}
	for _, kind := range pebble.Kinds {
		want := entries(srcVals.(importer.Source), kind)
		require.NotEmpty(t, want, kind)
		require.Equal(t, want, entries(dstVals.(importer.Source), kind), kind)
	}
}

func TestMigrateInArgs(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	require.NoError(t, os.WriteFile(empty, []byte(" \n"), 0o600))
	base := []string{"--source-url", "http://127.0.0.1:1",
		"--datastore-dir", filepath.Join(dir, "ds"), "--valuestore-dir", filepath.Join(dir, "pebble")}

	require.ErrorContains(t, testMigrateIn(t, append(base, "--token-file", empty)...), "empty")
	require.ErrorContains(t, testMigrateIn(t, append(base, "--token-file", filepath.Join(dir, "missing"))...), "token")
	require.ErrorContains(t, testMigrateIn(t, append(base, "--token-file", empty, "--workers", "-1")...), "--workers")
	token := filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(token, []byte("secret"), 0o600))
	require.ErrorContains(t, testMigrateIn(t, append(base, "--token-file", token, "--valuestore-options", filepath.Join(dir, "missing"))...), "pebble options")
	// A store directory that is a file does not open
	for _, flag := range []string{"--datastore-dir", "--valuestore-dir"} {
		args := slices.Clone(base)
		args[slices.Index(args, flag)+1] = token
		require.ErrorContains(t, testMigrateIn(t, append(args, "--token-file", token)...), "cannot open")
	}
	// A sender that does not answer stops the import after the retry
	// window
	err := testMigrateIn(t, append(base, "--token-file", token, "--retry-window", "1ms", "--worker-stagger", "0")...)
	require.ErrorContains(t, err, "run it again to resume")
	// Store types that an import cannot write
	require.ErrorContains(t, testMigrateIn(t, append(base, "--token-file", token, "--datastore-type", "badger")...), "supported: levelds")
	require.ErrorContains(t, testMigrateIn(t, append(base, "--token-file", token, "--valuestore-type", "memory")...), "supported: pebble")
	args := slices.Clone(base)
	args[slices.Index(args, "--source-url")+1] = "http://%"
	require.ErrorContains(t, testMigrateIn(t, append(args, "--token-file", token)...), "URL")
}

func TestReadPebbleOptions(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	opts, err := readPebbleOptions("")
	require.NoError(t, err)
	require.NotNil(t, opts)

	opts, err = readPebbleOptions(write("options", `
[Options]
  l0_compaction_threshold=7
  not_an_option=1
[Level "0"]
  filter_policy=rocksdb.BuiltinBloomFilter
[Level "1"]
  filter_policy=none
`))
	require.NoError(t, err)
	require.Equal(t, 7, opts.L0CompactionThreshold)
	require.NotNil(t, opts.Levels[0].FilterPolicy)

	_, err = readPebbleOptions(write("bad-filter", "[Level \"0\"]\n  filter_policy=other\n"))
	require.ErrorContains(t, err, "unknown filter policy")
	_, err = readPebbleOptions(write("bad", "not options"))
	require.ErrorContains(t, err, "cannot parse")
}

// TestImportHTTPClient checks that the import's client speaks HTTP/1.1 to a
// server that offers HTTP/2
func TestImportHTTPClient(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, r.Proto)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	c := newImportHTTPClient(4)
	c.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	resp, err := c.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "HTTP/1.1", string(body))

	// The server's own client does use HTTP/2
	resp2, err := srv.Client().Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	require.Equal(t, 2, resp2.ProtoMajor)
}
