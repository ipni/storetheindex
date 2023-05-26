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

	finderhttpclient "github.com/ipni/go-libipni/find/client/http"
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

func TestEndToEndWithReferenceProvider(t *testing.T) {
	switch runtime.GOOS {
	case "windows":
		t.Skip("skipping test on", runtime.GOOS)
	}
	t.Parallel()
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

	// install index-provider
	e.run("go", "install", "github.com/ipni/index-provider/cmd/provider@latest")

	// install dhstore
	e.run("go", "install", "github.com/ipni/dhstore/cmd/dhstore@latest")

	// install ipni-cli
	e.run("go", "install", "github.com/ipni/ipni-cli/cmd/ipni@latest")

	err = os.Chdir(cwd)
	require.NoError(t, err)

	// initialize index-provider
	e.run(provider, "init")
	cfg, err := config.Load(filepath.Join(e.dir, ".index-provider", "config"))
	require.NoError(t, err)
	providerID := cfg.Identity.PeerID
	t.Logf("Initialized provider ID: %s", providerID)

	// initialize indexer
	e.run(indexer, "init", "--store", "sth", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap", "--dhstore", "http://127.0.0.1:40080")
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
	e.run(indexer, "admin", "allow", "-i", "localhost:3002", "--peer", providerID)

	// Import a car file into the provider.  This will cause the provider to
	// publish an advertisement that the indexer will read.  The indexer will
	// then import the advertised content.
	outImport := e.run(provider, "import", "car",
		"-i", carPath,
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("import output:\n%s\n", outImport)

	// Wait for the CAR to be indexed
	retryWithTimeout(t, 10*time.Second, time.Second, func() error {
		for _, mh := range []string{
			"2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
			"2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
		} {
			findOutput := e.run(ipni, "find", "-i", "localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)

			if bytes.Contains(findOutput, []byte("not found")) {
				return fmt.Errorf("%s: index not found", mh)
			}
			if !bytes.Contains(findOutput, []byte("Provider:")) {
				return fmt.Errorf("%s: unexpected error: %s", mh, findOutput)
			}
		}

		return nil
	})

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

	outProvider := e.run(indexer, "providers", "get", "-p", providerID, "--indexer", "localhost:3000")
	// Check that IndexCount with correct value appears in providers output.
	require.Contains(t, string(outProvider), "IndexCount: 1043")

	// Create double hashed client and verify that the multihashes ended up in dhstore
	client, err := finderhttpclient.NewDHashClient("http://127.0.0.1:40080", "http://127.0.0.1:3000")
	require.NoError(t, err)

	mh, err := multihash.FromB58String("2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj")
	require.NoError(t, err)

	dhResp, err := client.Find(e.ctx, mh)
	require.NoError(t, err)

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
	retryWithTimeout(t, 10*time.Second, time.Second, func() error {
		for _, mh := range []string{
			"2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
			"2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
		} {
			findOutput := e.run(ipni, "find", "-i", "localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)

			if !bytes.Contains(findOutput, []byte("not found")) {
				return fmt.Errorf("%s: index not removed", mh)
			}
		}

		return nil
	})

	outProvider = e.run(indexer, "providers", "get", "-p", providerID, "--indexer", "localhost:3000")
	// Check that IndexCount is back to zero after removing car.
	require.Contains(t, string(outProvider), "IndexCount: 0")

	root2 := filepath.Join(e.dir, ".storetheindex2")
	e.env = append(e.env, fmt.Sprintf("%s=%s", config.EnvDir, root2))
	e.run(indexer, "init", "--store", "memory", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap",
		"--listen-admin", "/ip4/127.0.0.1/tcp/3202", "--listen-finder", "/ip4/127.0.0.1/tcp/3200", "--listen-ingest", "/ip4/127.0.0.1/tcp/3201")

	cmdIndexer2 := e.start(indexer, "daemon")
	select {
	case <-e.indexerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer2 to start")
	}

	outProviders := e.run(indexer, "providers", "list", "--indexer", "localhost:3200")
	if !strings.HasPrefix(string(outProviders), "No providers registered with indexer") {
		t.Errorf("expected no providers message, got %q", string(outProviders))
	}

	// import providers from first indexer.
	e.run(indexer, "admin", "import-providers", "--indexer", "localhost:3202", "--from", "localhost:3000")

	outProviders = e.run(indexer, "providers", "list", "--indexer", "localhost:3200")

	// Check that provider ID now appears in providers output.
	require.Contains(t, string(outProviders), providerID, "expected provider id in providers output after import-providers")

	// Check that status is not frozen.
	outStatus := e.run(indexer, "admin", "status", "--indexer", "localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: false", "expected indexer to be frozen")

	e.run(indexer, "admin", "freeze", "--indexer", "localhost:3202")
	outProviders = e.run(indexer, "providers", "list", "--indexer", "localhost:3200")

	// Check that provider ID now appears as frozen in providers output.
	require.Contains(t, string(outProviders), "FrozenAtTime", "expected provider to be frozen")

	// Check that status is frozen.
	outStatus = e.run(indexer, "admin", "status", "--indexer", "localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: true", "expected indexer to be frozen")

	e.stop(cmdIndexer2, time.Second)

	e.stop(cmdIndexer, time.Second)
	e.stop(cmdProvider, time.Second)
	e.stop(cmdDhstore, time.Second)
}

func retryWithTimeout(t *testing.T, timeout time.Duration, timeBetweenRetries time.Duration, retryableFn func() error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var errs []error
	// Loop until context is done
	timer := time.NewTimer(timeBetweenRetries)
	for {
		err := retryableFn()
		if err == nil {
			return
		}

		errs = append(errs, err)
		select {
		case <-ctx.Done():
			t.Fatalf("hit timeout while retrying. Errs seen: %s", errs)
			return
		case <-timer.C:
			timer.Reset(timeBetweenRetries)
		}
	}
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
