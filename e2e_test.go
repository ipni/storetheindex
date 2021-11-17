package main_test

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

	"github.com/filecoin-project/storetheindex/config"
	qt "github.com/frankban/quicktest"
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
	qt.Assert(e.t, err, qt.IsNil, qt.Commentf("output: %s", out))
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
				if strings.Contains(line, "Indexer ready") {
					close(e.indexerReady)
				}
			case "provider":
				line = strings.ToLower(line)
				if strings.Contains(line, "connected to peer successfully") {
					close(e.providerHasPeer)
				} else if strings.Contains(line, "admin http server listening") {
					close(e.providerReady)
				}
			}
		}
	}()

	err = cmd.Start()
	qt.Assert(e.t, err, qt.IsNil)
	return cmd
}

func (e *e2eTestRunner) stop(cmd *exec.Cmd, timeout time.Duration) {
	if runtime.GOOS == "windows" {
		// Windows can't send SIGINT.
		timeout = 0
	}
	err := cmd.Process.Signal(os.Interrupt)
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
	if runtime.GOOS == "windows" {
		t.Skipf("TODO: fix test for Windows")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e := &e2eTestRunner{
		t:   t,
		dir: t.TempDir(),
		ctx: ctx,

		indexerReady:    make(chan struct{}),
		providerReady:   make(chan struct{}),
		providerHasPeer: make(chan struct{}),
	}

	carPath := filepath.Join(e.dir, "sample-wrapped-v2.car")
	err := downloadFile("https://github.com/filecoin-project/index-provider/raw/main/testdata/sample-wrapped-v2.car", carPath)
	qt.Assert(t, err, qt.IsNil)

	// Use a clean environment, with the host's PATH, and a temporary HOME.
	// We also tell "go install" to place binaries there.
	e.env = []string{
		"HOME=" + e.dir,
		"GOBIN=" + e.dir,
		"PATH=" + os.Getenv("PATH"),
	}
	// Reuse the host's build and module download cache.
	// This should allow "go install" to reuse work.
	for _, name := range []string{"GOCACHE", "GOMODCACHE"} {
		out, err := exec.Command("go", "env", name).CombinedOutput()
		qt.Assert(t, err, qt.IsNil)
		out = bytes.TrimSpace(out)
		e.env = append(e.env, fmt.Sprintf("%s=%s", name, out))
	}
	t.Logf("Env: %s", strings.Join(e.env, " "))

	indexer := filepath.Join(e.dir, "storetheindex")
	e.run("go", "install", ".")

	provider := filepath.Join(e.dir, "provider")

	cwd, err := os.Getwd()
	qt.Assert(t, err, qt.IsNil)
	err = os.Chdir(e.dir)
	qt.Assert(t, err, qt.IsNil)
	e.run("git", "clone", "https://github.com/filecoin-project/index-provider.git")
	err = os.Chdir("index-provider/cmd/provider")
	qt.Assert(t, err, qt.IsNil)
	e.run("go", "install")
	os.Chdir(cwd)

	e.run(provider, "init")
	cfg, err := config.Load(filepath.Join(e.dir, ".index-provider", "config"))
	qt.Assert(t, err, qt.IsNil)
	t.Logf("Initialized provider ID: %s", cfg.Identity.PeerID)

	e.run(indexer, "init", "--pubsub-peer", cfg.Identity.PeerID)
	cfg, err = config.Load(filepath.Join(e.dir, ".storetheindex", "config"))
	qt.Assert(t, err, qt.IsNil)
	indexerHost := cfg.Identity.PeerID

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
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3003/p2p/%s", indexerHost),
		"--listen-admin", "http://localhost:3102",
	)
	select {
	case <-e.providerHasPeer:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to connect to indexer")
	}

	// TODO: "find" always returns "index not found".
	// "import" seems to work on the provider side,
	// but the indexer doesn't seem to show anything at all.

	outImport := e.run(provider, "import", "car",
		"-i", carPath,
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("import output:\n%s\n", outImport)

	// TODO: use some other way to wait for the CAR to be indexed.
	time.Sleep(2 * time.Second)

	for _, mh := range []string{
		"2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
		"2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
	} {
		findOutput := e.run(provider, "find", "-i", "localhost", "-mh", mh)
		t.Logf("import output:\n%s\n", findOutput)

		if bytes.Contains(findOutput, []byte("not found")) {
			t.Errorf("%s: index not found", mh)
		} else if !bytes.Contains(findOutput, []byte("Provider:")) {
			t.Errorf("%s: unexpected error: %s", mh, findOutput)
		}

	}

	e.stop(cmdIndexer, time.Second)
	e.stop(cmdProvider, time.Second)
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
