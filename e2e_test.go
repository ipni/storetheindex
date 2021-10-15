package main_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// This is a full end-to-end test with storetheindex as the indexer daemon,
// and indexer-reference-provider as a client.
// We build both programs, noting that we always build the latest provider.
// We initialize their setup, start the two daemons, and connect the peers.
// We then import a CAR file and query its CIDs.

type e2eTestRunner struct {
	t   *testing.T
	dir string
	ctx context.Context
	env []string

	indexerHost     chan string
	providerHasPeer chan bool
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

var rxLibp2pHostID = regexp.MustCompile(`"host_id": "([^"]*)"`)

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
				if m := rxLibp2pHostID.FindStringSubmatch(line); m != nil {
					e.indexerHost <- m[1]
					close(e.indexerHost)
				}
			case "provider":
				if strings.Contains(line, "Connected successfully to peer") {
					close(e.providerHasPeer)
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
		_ = err
		// TODO: uncomment once the daemons stop with ^C without erroring.
		// See: https://github.com/filecoin-project/indexer-reference-provider/issues/61
		// qt.Assert(e.t, err, qt.IsNil)
	}
}

func TestEndToEndWithReferenceProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e := &e2eTestRunner{
		t:   t,
		dir: t.TempDir(),
		ctx: ctx,

		indexerHost:     make(chan string, 1),
		providerHasPeer: make(chan bool),
	}

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
	e.run("go", "install", "github.com/filecoin-project/indexer-reference-provider/cmd/provider@main")

	e.run(indexer, "init")
	e.run(provider, "init")

	cmdIndexer := e.start(indexer, "daemon")

	cmdProvider := e.start(provider, "daemon")

	var indexerHost string
	select {
	case s := <-e.indexerHost:
		indexerHost = s
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer host")
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

	time.Sleep(2 * time.Second)
	outImport := e.run(provider, "import", "car",
		"-i", "testdata/sample-wrapped-v2.car",
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("import output:\n%s\n", outImport)
	time.Sleep(5 * time.Second)
	findOutput := e.run(provider, "find",
		"-i", "localhost",
		"-mh", "2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
	)
	time.Sleep(2 * time.Second)
	t.Logf("import output:\n%s\n", findOutput)
	findOutput = e.run(provider, "find",
		"-i", "localhost",
		"-mh", "2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
	)
	t.Logf("import output:\n%s\n", findOutput)

	e.stop(cmdIndexer, time.Second)
	e.stop(cmdProvider, time.Second)
}
