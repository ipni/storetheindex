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

	qt "github.com/frankban/quicktest"
	"github.com/ipni/storetheindex/config"
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
	providerHasPeer chan struct{}
}

func (e *e2eTestRunner) run(name string, args ...string) []byte {
	e.t.Helper()

	e.t.Logf("run: %s %s", name, strings.Join(args, " "))

	cmd := exec.CommandContext(e.ctx, name, args...)
	cmd.Env = e.env
	out, err := cmd.CombinedOutput()
	qt.Assert(e.t, err, qt.IsNil, qt.Commentf("err: %v, output: %s", err, out))
	return out
}

func (e *e2eTestRunner) start(prog string, args ...string) *exec.Cmd {
	e.t.Helper()

	name := filepath.Base(prog)
	e.t.Logf("run: %s %s", name, strings.Join(args, " "))

	cmd := exec.CommandContext(e.ctx, prog, args...)
	cmd.Env = e.env

	stdout, err := cmd.StdoutPipe()
	qt.Assert(e.t, err, qt.IsNil)
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
			}
		}
	}()

	err = cmd.Start()
	qt.Assert(e.t, err, qt.IsNil)
	return cmd
}

func (e *e2eTestRunner) stop(cmd *exec.Cmd, timeout time.Duration) {
	sig := os.Interrupt
	if runtime.GOOS == "windows" {
		// Windows can't send SIGINT.
		sig = os.Kill
	}
	err := cmd.Process.Signal(sig)
	qt.Assert(e.t, err, qt.IsNil)

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	select {
	case <-time.After(timeout):
		e.t.Logf("killing command after %s: %s", timeout, cmd)
		err := cmd.Process.Kill()
		qt.Assert(e.t, err, qt.IsNil)
	case err := <-waitErr:
		qt.Assert(e.t, err, qt.IsNil)
	}
}

func TestEndToEndWithReferenceProvider(t *testing.T) {
	switch runtime.GOOS {
	case "windows":
		t.Skip("skipping test on", runtime.GOOS)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	e := &e2eTestRunner{
		t:   t,
		dir: t.TempDir(),
		ctx: ctx,

		indexerReady:    make(chan struct{}, 1),
		providerReady:   make(chan struct{}, 1),
		providerHasPeer: make(chan struct{}, 1),
	}

	carPath := filepath.Join(e.dir, "sample-wrapped-v2.car")
	err := downloadFile("https://github.com/ipni/index-provider/raw/main/testdata/sample-wrapped-v2.car", carPath)
	qt.Assert(t, err, qt.IsNil)

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
		err := os.MkdirAll(gopath, 0666)
		qt.Assert(t, err, qt.IsNil)
		e.env = append(e.env, fmt.Sprintf("GOPATH=%s", gopath))
	}
	t.Logf("Env: %s", strings.Join(e.env, " "))

	// Reuse the host's build and module download cache.
	// This should allow "go install" to reuse work.
	for _, name := range []string{"GOCACHE", "GOMODCACHE"} {
		out, err := exec.Command("go", "env", name).CombinedOutput()
		qt.Assert(t, err, qt.IsNil)
		out = bytes.TrimSpace(out)
		e.env = append(e.env, fmt.Sprintf("%s=%s", name, out))
	}

	indexer := filepath.Join(e.dir, "storetheindex")
	e.run("go", "install", ".")

	provider := filepath.Join(e.dir, "provider")

	cwd, err := os.Getwd()
	qt.Assert(t, err, qt.IsNil)
	err = os.Chdir(e.dir)
	qt.Assert(t, err, qt.IsNil)
	e.run("git", "clone", "https://github.com/ipni/index-provider.git")
	err = os.Chdir("index-provider/cmd/provider")
	qt.Assert(t, err, qt.IsNil)
	e.run("go", "install")
	err = os.Chdir(cwd)
	qt.Assert(t, err, qt.IsNil)

	e.run(provider, "init")
	cfg, err := config.Load(filepath.Join(e.dir, ".index-provider", "config"))
	qt.Assert(t, err, qt.IsNil)
	providerID := cfg.Identity.PeerID
	t.Logf("Initialized provider ID: %s", providerID)

	e.run(indexer, "init", "--store", "sth", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap")
	cfg, err = config.Load(filepath.Join(e.dir, ".storetheindex", "config"))
	qt.Assert(t, err, qt.IsNil)
	indexerID := cfg.Identity.PeerID

	cmdProvider := e.start(provider, "daemon")
	select {
	case <-e.providerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to start")
	}

	cmdIndexer := e.start(indexer, "daemon")
	select {
	case <-e.indexerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer to start")
	}

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
			findOutput := e.run(provider, "find", "-i", "localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)

			if bytes.Contains(findOutput, []byte("not found")) {
				return fmt.Errorf("%s: index not found", mh)
			} else if !bytes.Contains(findOutput, []byte("Provider:")) {
				return fmt.Errorf("%s: unexpected error: %s", mh, findOutput)
			}
		}

		return nil
	})

	outProvider := e.run(indexer, "providers", "get", "-p", providerID, "--indexer", "localhost:3000")
	// Check that IndexCount with correct value appears in providers output.
	expect := "IndexCount: 1043"
	if !strings.Contains(string(outProvider), expect) {
		t.Errorf("expected provider to contains %q, got %q", expect, string(outProvider))
	}

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
			findOutput := e.run(provider, "find", "-i", "localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)

			if !bytes.Contains(findOutput, []byte("not found")) {
				return fmt.Errorf("%s: index not removed", mh)
			}
		}

		return nil
	})

	outProvider = e.run(indexer, "providers", "get", "-p", providerID, "--indexer", "localhost:3000")
	// Check that IndexCount is back to zero after removing car.
	expect = "IndexCount: 0"
	if !strings.Contains(string(outProvider), expect) {
		t.Errorf("expected provider to contains %q, got %q", expect, string(outProvider))
	}

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
	if !strings.Contains(string(outProviders), providerID) {
		t.Errorf("expected provider id in providers output after import-providers, got %q", string(outProviders))
	}

	// Check that status is not frozen.
	outStatus := e.run(indexer, "admin", "status", "--indexer", "localhost:3202")
	if !strings.Contains(string(outStatus), "Frozen: false") {
		t.Errorf("expected indexer to be frozen, got %q", string(outProviders))
	}

	e.run(indexer, "admin", "freeze", "--indexer", "localhost:3202")
	outProviders = e.run(indexer, "providers", "list", "--indexer", "localhost:3200")

	// Check that provider ID now appears as frozen in providers output.
	if !strings.Contains(string(outProviders), "FrozenAtTime") {
		t.Errorf("expected provider to be frozen, got %q", string(outProviders))
	}

	// Check that status is frozen.
	outStatus = e.run(indexer, "admin", "status", "--indexer", "localhost:3202")
	if !strings.Contains(string(outStatus), "Frozen: true") {
		t.Errorf("expected indexer to be frozen, got %q", string(outProviders))
	}

	e.stop(cmdIndexer2, time.Second)

	e.stop(cmdIndexer, time.Second)
	e.stop(cmdProvider, time.Second)
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
