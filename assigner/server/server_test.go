package server_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	client "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/filecoin-project/storetheindex/assigner/config"
	"github.com/filecoin-project/storetheindex/assigner/core"
	server "github.com/filecoin-project/storetheindex/assigner/server"
	sticfg "github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var pubIdent = sticfg.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

const pubsubTopic = "/indexer/ingest/mainnet"

func setupServer(t *testing.T, assigner *core.Assigner) *server.Server {
	s, err := server.New("127.0.0.1:0", assigner)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func setupClient(t *testing.T, host string) *client.Client {
	c, err := client.New(host)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAnnounce(t *testing.T) {
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

		indexerReady: make(chan struct{}, 1),
	}

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

	cwd, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(filepath.Dir(filepath.Dir(cwd)))
	require.NoError(t, err)
	e.run("go", "install", ".")
	/*
		err = os.Chdir(e.dir)
		require.NoError(t, err)
		e.run("git", "clone", "https://github.com/filecoin-project/storetheindex.git")
		err = os.Chdir("storetheindex")
		require.NoError(t, err)
		e.run("go", "install")
	*/

	err = os.Chdir(cwd)
	require.NoError(t, err)

	indexer := filepath.Join(e.dir, "storetheindex")
	e.run(indexer, "init", "--pubsub-topic", pubsubTopic, "--no-bootstrap")
	stiCfg, err := sticfg.Load(filepath.Join(e.dir, ".storetheindex", "config"))
	require.NoError(t, err)
	indexerID := stiCfg.Identity.PeerID
	t.Log("Initialized indexer", indexerID)

	cmdIndexer := e.start(indexer, "daemon")
	select {
	case <-e.indexerReady:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer to start")
	}

	rng := rand.New(rand.NewSource(1413))

	// Initialize everything
	peerID, _, err := pubIdent.Decode()
	if err != nil {
		t.Fatal(err)
	}
	assigner, cfg := initAssigner(t, pubIdent.PeerID)
	s := setupServer(t, assigner)
	cl := setupClient(t, s.URL())

	errChan := make(chan error, 1)
	go func() {
		err := s.Start()
		if err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()

	ai, err := peer.AddrInfoFromString(fmt.Sprintf("/ip4/127.0.0.1/tcp/9999/p2p/%s", peerID))
	if err != nil {
		t.Fatal(err)
	}
	ai.ID = peerID

	mhs := util.RandomMultihashes(1, rng)

	assignChan := assigner.OnAssignment(peerID)

	if err := cl.Announce(context.Background(), ai, cid.NewCidV1(22, mhs[0])); err != nil {
		t.Fatalf("Failed to announce to %s: %s", s.URL(), err)
	}

	select {
	case adminURL := <-assignChan:
		if adminURL != cfg.IndexerPool[0].AdminURL {
			t.Fatalf("assigned to wrong admin url, expected %s got %s", cfg.IndexerPool[0].AdminURL, adminURL)
		}
		t.Log("Assigned publisher to indexer at", adminURL)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for assignment")
	}

	// Check assignment
	//outProvider := e.run(indexer, "providers", "get", "-p", providerID, "--indexer", "localhost:3000")

	e.stop(cmdIndexer, time.Second)
}

// initAssigner initializes a new registry
func initAssigner(t *testing.T, trustedID string) (*core.Assigner, config.Assignment) {
	const indexerIP = "127.0.0.1"
	var cfg = config.Assignment{
		IndexerPool: []config.Indexer{
			config.Indexer{
				AdminURL:  fmt.Sprintf("http://%s:3002", indexerIP),
				FindURL:   fmt.Sprintf("http://%s:3000", indexerIP),
				IngestURL: fmt.Sprintf("http://%s:3001", indexerIP),
			},
		},
		Policy: config.Policy{
			Allow:  false,
			Except: []string{trustedID},
		},
		PubSubTopic: pubsubTopic,
	}
	assigner, err := core.NewAssigner(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	return assigner, cfg
}

type e2eTestRunner struct {
	t   *testing.T
	dir string
	ctx context.Context
	env []string

	indexerReady chan struct{}
}

func (e *e2eTestRunner) run(name string, args ...string) []byte {
	e.t.Helper()

	e.t.Logf("run: %s %s", name, strings.Join(args, " "))

	cmd := exec.CommandContext(e.ctx, name, args...)
	cmd.Env = e.env
	out, err := cmd.CombinedOutput()
	require.NoError(e.t, err, "err: %v, output: %s", err, out)
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
			if strings.Contains(line, "Indexer is ready") {
				e.indexerReady <- struct{}{}
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
		err := cmd.Process.Kill()
		require.NoError(e.t, err)
	case err := <-waitErr:
		require.NoError(e.t, err)
	}
}
