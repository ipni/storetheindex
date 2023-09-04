package main_test

//lint:file-ignore U1000 Currently skipping this test since it's slow and breaks
//often because it's non-reproducible. TODO fixme

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// This is a full end-to-end test with storetheindex as the indexer daemon,
// and index-provider/cmd/provider as a client.
// We build both programs, noting that we always build the latest provider.
// We initialize their setup, start the two daemons, and connect the peers.
// We then import a CAR file and query its CIDs.

type e2eTestRunner struct {
	t   *testing.T
	dir string
	ctx context.Context
	env []string

	indexerReady    chan struct{}
	providerReady   chan struct{}
	dhstoreReady    chan struct{}
	providerHasPeer chan struct{}
}

func (e *e2eTestRunner) run(name string, args ...string) []byte {
	e.t.Helper()

	e.t.Logf("run: %s %s", name, strings.Join(args, " "))

	cmd := exec.CommandContext(e.ctx, name, args...)
	cmd.Env = e.env
	out, err := cmd.CombinedOutput()
	require.NoError(e.t, err, string(out))
	return out
}

func (e *e2eTestRunner) start(prog string, args ...string) *exec.Cmd {
	e.t.Helper()

	name := filepath.Base(prog)
	e.t.Logf("run: %s %s", name, strings.Join(args, " "))

	cmd := exec.CommandContext(e.ctx, prog, args...)
	cmd.Env = e.env

	stdout, err := cmd.StdoutPipe()
	require.NoError(e.t, err)
	cmd.Stderr = cmd.Stdout

	scanner := bufio.NewScanner(stdout)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()

			// Logging every single line via the test output is verbose,
			// but helps see what's happening, especially when the test fails.
			e.t.Logf("%s: %s", name, line)

			switch name {
			case "storetheindex":
				if strings.Contains(line, "Indexer is ready") {
					e.indexerReady <- struct{}{}
				}
			case "provider":
				line = strings.ToLower(line)
				if strings.Contains(line, "connected to peer successfully") {
					e.providerHasPeer <- struct{}{}
				} else if strings.Contains(line, "admin http server listening") {
					e.providerReady <- struct{}{}
				}
			case "dhstore":
				if strings.Contains(line, "Store opened.") {
					e.dhstoreReady <- struct{}{}
				}
			}
		}
	}()

	err = cmd.Start()
	require.NoError(e.t, err)
	return cmd
}

func (e *e2eTestRunner) stop(cmd *exec.Cmd, timeout time.Duration) {
	sig := os.Interrupt
	if runtime.GOOS == "windows" {
		// Windows can't send SIGINT.
		sig = os.Kill
	}
	err := cmd.Process.Signal(sig)
	require.NoError(e.t, err)

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	select {
	case <-time.After(timeout):
		e.t.Logf("killing command after %s: %s", timeout, cmd)
		err = cmd.Process.Kill()
		require.NoError(e.t, err)
	case err = <-waitErr:
		require.NoError(e.t, err)
	}
}

func TestEndToEndWithAllProviderTypes(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping e2e test in CI environment")
	}
	switch runtime.GOOS {
	case "windows":
		t.Skip("skipping test on", runtime.GOOS)
	}

	// Test with publisher running HTTP ipnisync over libp2p.
	t.Run("Libp2pProvider", func(t *testing.T) {
		testEndToEndWithReferenceProvider(t, "libp2p")
	})

	// Test with publisher running plain HTTP only, not over libp2p.
	t.Run("PlainHTTPProvider", func(t *testing.T) {
		testEndToEndWithReferenceProvider(t, "http")
	})

	// Test with publisher running plain HTTP only, not over libp2p.
	t.Run("Libp2pWithHTTPProvider", func(t *testing.T) {
		testEndToEndWithReferenceProvider(t, "libp2phttp")
	})

	// Test with publisher running dtsync over libp2p.
	t.Run("DTSyncProvider", func(t *testing.T) {
		testEndToEndWithReferenceProvider(t, "dtsync")
	})
}

func testEndToEndWithReferenceProvider(t *testing.T, publisherProto string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	e := &e2eTestRunner{
		t:   t,
		dir: t.TempDir(),
		ctx: ctx,

		indexerReady:    make(chan struct{}, 1),
		providerReady:   make(chan struct{}, 1),
		dhstoreReady:    make(chan struct{}, 1),
		providerHasPeer: make(chan struct{}, 1),
	}

	carPath := filepath.Join(e.dir, "sample-wrapped-v2.car")
	err := downloadFile("https://github.com/ipni/index-provider/raw/main/testdata/sample-wrapped-v2.car", carPath)
	require.NoError(t, err)

	// Use a clean environment, with the host's PATH, and a temporary HOME.
	// We also tell "go install" to place binaries there.
	hostEnv := os.Environ()
	var filteredEnv []string
	for _, env := range hostEnv {
		if strings.Contains(env, "CC") || strings.Contains(env, "LDFLAGS") || strings.Contains(env, "CFLAGS") {
			// Bring in the C compiler flags from the host. For example on a Nix
			// machine, this compilation within the test will fail since the compiler
			// will not find correct libraries.
			filteredEnv = append(filteredEnv, env)
		} else if strings.HasPrefix(env, "PATH") {
			// Bring in the host's PATH.
			filteredEnv = append(filteredEnv, env)
		}
	}
	e.env = append(filteredEnv, []string{
		"HOME=" + e.dir,
		"GOBIN=" + e.dir,
	}...)
	if runtime.GOOS == "windows" {
		const gopath = "C:\\Projects\\Go"
		err = os.MkdirAll(gopath, 0666)
		require.NoError(t, err)
		e.env = append(e.env, fmt.Sprintf("GOPATH=%s", gopath))
	}
	t.Logf("Env: %s", strings.Join(e.env, " "))

	// Reuse the host's build and module download cache.
	// This should allow "go install" to reuse work.
	for _, name := range []string{"GOCACHE", "GOMODCACHE"} {
		out, err := exec.Command("go", "env", name).CombinedOutput()
		require.NoError(t, err)
		out = bytes.TrimSpace(out)
		e.env = append(e.env, fmt.Sprintf("%s=%s", name, out))
	}

	// install storetheindex
	indexer := filepath.Join(e.dir, "storetheindex")
	e.run("go", "install", ".")

	provider := filepath.Join(e.dir, "provider")
	dhstore := filepath.Join(e.dir, "dhstore")
	ipni := filepath.Join(e.dir, "ipni")

	cwd, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(e.dir)
	require.NoError(t, err)

	// install index-provider
	switch publisherProto {
	case "dtsync":
		// Install index-provider that supports dtsync.
		e.run("go", "install", "github.com/ipni/index-provider/cmd/provider@v0.13.6")
	case "libp2p", "libp2phttp", "http":
		e.run("go", "install", "github.com/ipni/index-provider/cmd/provider@latest")
	default:
		panic("providerProto must be one of: libp2phttp, http, dtsync")
	}
	// install dhstore
	e.run("go", "install", "-tags", "nofdb", "github.com/ipni/dhstore/cmd/dhstore@latest")

	// install ipni-cli
	e.run("go", "install", "github.com/ipni/ipni-cli/cmd/ipni@latest")

	err = os.Chdir(cwd)
	require.NoError(t, err)

	// initialize index-provider
	switch publisherProto {
	case "dtsync":
		e.run(provider, "init")
	case "http":
		e.run(provider, "init", "--pubkind=http")
	case "libp2p":
		e.run(provider, "init", "--pubkind=libp2phttp")
	case "libp2phttp":
		e.run(provider, "init", "--pubkind=libp2phttp")
	}
	providerCfgPath := filepath.Join(e.dir, ".index-provider", "config")
	cfg, err := config.Load(providerCfgPath)
	require.NoError(t, err)
	providerID := cfg.Identity.PeerID
	t.Logf("Initialized provider ID: %s", providerID)

	// initialize indexer
	e.run(indexer, "init", "--store", "pebble", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap")
	stiCfgPath := filepath.Join(e.dir, ".storetheindex", "config")
	cfg, err = config.Load(stiCfgPath)
	require.NoError(t, err)
	indexerID := cfg.Identity.PeerID
	cfg.Ingest.AdvertisementMirror = config.Mirror{
		Compress: "gzip",
		Write:    true,
		Storage: config.FileStore{
			Type: "local",
			Local: config.LocalFileStore{
				BasePath: e.dir,
			},
		},
	}
	cfg.Save(stiCfgPath)

	// start provider
	cmdProvider := e.start(provider, "daemon")
	select {
	case <-e.providerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to start")
	}

	// start dhstore
	cmdDhstore := e.start(dhstore, "--storePath", e.dir)
	select {
	case <-e.dhstoreReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for dhstore to start")
	}

	// start indexer
	cmdIndexer := e.start(indexer, "daemon")
	select {
	case <-e.indexerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer to start")
	}

	// connect provider to the indexer
	e.run(provider, "connect",
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3003/p2p/%s", indexerID),
		"--listen-admin", "http://localhost:3102",
	)
	select {
	case <-e.providerHasPeer:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to connect to indexer")
	}

	// Allow provider advertisements, regardless of default policy.
	e.run(indexer, "admin", "allow", "-i", "http://localhost:3002", "--peer", providerID)

	// Import a car file into the provider.  This will cause the provider to
	// publish an advertisement that the indexer will read.  The indexer will
	// then import the advertised content.
	outImport := e.run(provider, "import", "car",
		"-i", carPath,
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("import output:\n%s\n", outImport)

	// Wait for the CAR to be indexed
	require.Eventually(t, func() bool {
		for _, mh := range []string{
			"2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
			"2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
		} {
			findOutput := e.run(ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)

			if bytes.Contains(findOutput, []byte("not found")) {
				return false
			}
			if !bytes.Contains(findOutput, []byte("Provider:")) {
				t.Logf("mh %s: unexpected error: %s", mh, findOutput)
				return false
			}
		}
		return true
	}, 10*time.Second, time.Second)

	e.run("sync")

	// Check that ad was saved as CAR file.
	dir, err := os.Open(e.dir)
	require.NoError(t, err)
	names, err := dir.Readdirnames(-1)
	dir.Close()
	require.NoError(t, err)
	var carCount, headCount int
	carSuffix := carstore.CarFileSuffix + carstore.GzipFileSuffix
	for _, name := range names {
		if strings.HasSuffix(name, carSuffix) && strings.HasPrefix(name, "baguqeera") {
			carCount++
		} else if strings.HasSuffix(name, carstore.HeadFileSuffix) {
			headCount++
		}
	}
	require.Equal(t, 1, carCount)
	require.Equal(t, 1, headCount)

	root2 := filepath.Join(e.dir, ".storetheindex2")
	e.env = append(e.env, fmt.Sprintf("%s=%s", config.EnvDir, root2))
	e.run(indexer, "init", "--store", "dhstore", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap", "--dhstore", "http://127.0.0.1:40080",
		"--listen-admin", "/ip4/127.0.0.1/tcp/3202", "--listen-finder", "/ip4/127.0.0.1/tcp/3200", "--listen-ingest", "/ip4/127.0.0.1/tcp/3201",
		"--listen-p2p", "/ip4/127.0.0.1/tcp/3203")

	sti2CfgPath := filepath.Join(root2, "config")
	cfg, err = config.Load(sti2CfgPath)
	require.NoError(t, err)
	indexer2ID := cfg.Identity.PeerID

	cmdIndexer2 := e.start(indexer, "daemon")
	select {
	case <-e.indexerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer2 to start")
	}

	outProviders := e.run(ipni, "provider", "--all", "--indexer", "http://localhost:3200")
	require.Contains(t, string(outProviders), "No providers registered with indexer",
		"expected no providers message")

	// import providers from first indexer.
	e.run(indexer, "admin", "import-providers", "--indexer", "http://localhost:3202", "--from", "localhost:3000")

	// Check that provider ID now appears in providers output.
	outProviders = e.run(ipni, "provider", "--all", "--indexer", "http://localhost:3200", "--id-only")
	require.Contains(t, string(outProviders), providerID, "expected provider id in providers output after import-providers")

	// Connect provider to the 2nd indexer.
	e.run(provider, "connect",
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3203/p2p/%s", indexer2ID),
		"--listen-admin", "http://localhost:3102",
	)
	select {
	case <-e.providerHasPeer:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to connect to indexer")
	}

	// Tell provider to send direct announce to 2nd indexer.
	out := e.run(provider, "announce-http",
		"-i", "http://localhost:3201",
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("announce output:\n%s\n", out)

	// Create double hashed client and verify that 2nd indexer wrote
	// multihashes to dhstore.
	client, err := findclient.NewDHashClient(findclient.WithProvidersURL("http://127.0.0.1:3000"), findclient.WithDHStoreURL("http://127.0.0.1:40080"))
	require.NoError(t, err)

	mh, err := multihash.FromB58String("2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj")
	require.NoError(t, err)

	var dhResp *model.FindResponse
	require.Eventually(t, func() bool {
		dhResp, err = client.Find(e.ctx, mh)
		return err == nil && len(dhResp.MultihashResults) != 0
	}, 10*time.Second, time.Second)

	require.Equal(t, 1, len(dhResp.MultihashResults))
	require.Equal(t, dhResp.MultihashResults[0].Multihash, mh)
	require.Equal(t, 1, len(dhResp.MultihashResults[0].ProviderResults))
	require.Equal(t, providerID, dhResp.MultihashResults[0].ProviderResults[0].Provider.ID.String())

	// Remove a car file from the provider.  This will cause the provider to
	// publish an advertisement that tells the indexer to remove the car file
	// content by contextID.  The indexer will then import the advertisement
	// and remove content.
	outRemove := e.run(provider, "remove", "car",
		"-i", carPath,
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("remove output:\n%s\n", outRemove)

	// Wait for the CAR indexes to be removed
	require.Eventually(t, func() bool {
		for _, mh := range []string{
			"2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
			"2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
		} {
			findOutput := e.run(ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)
			if !bytes.Contains(findOutput, []byte("not found")) {
				return false
			}
		}
		return true
	}, 10*time.Second, time.Second)

	// Check that status is not frozen.
	outStatus := e.run(indexer, "admin", "status", "--indexer", "http://localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: false", "expected indexer to be frozen")

	e.run(indexer, "admin", "freeze", "--indexer", "http://localhost:3202")
	outProviders = e.run(ipni, "provider", "--all", "--indexer", "http://localhost:3200")

	// Check that provider ID now appears as frozen in providers output.
	require.Contains(t, string(outProviders), "FrozenAtTime", "expected provider to be frozen")

	// Check that status is frozen.
	outStatus = e.run(indexer, "admin", "status", "--indexer", "http://localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: true", "expected indexer to be frozen")

	e.stop(cmdIndexer2, time.Second)

	e.stop(cmdIndexer, time.Second)
	e.stop(cmdProvider, time.Second)
	e.stop(cmdDhstore, time.Second)
}

func downloadFile(fileURL, filePath string) error {
	rsp, err := http.Get(fileURL)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if rsp.StatusCode != 200 {
		return fmt.Errorf("error response getting file: %d", rsp.StatusCode)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, rsp.Body)
	return err
}
