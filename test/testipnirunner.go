package test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// When this environ var is set to a value and running tests with -v flag,
	// then TestIpniRunner output is logged.
	EnvTestIpniRunnerOutput = "TEST_IPNI_RUNNER_OUTPUT"

	IndexerReadyMatch    = "Indexer is ready"
	ProviderHasPeerMatch = "connected to peer successfully"
	ProviderReadyMatch   = "admin http server listening"
	DhstoreReady         = "Store opened."
)

// StdoutWatcher is a helper for watching the stdout of a command for a
// specific string. It is used by TestIpniRunner to watch for specific
// output from the commands. The Signal channel will be sent on when the
// match string is found.
type StdoutWatcher struct {
	Match  string
	Signal chan struct{}
}

func NewStdoutWatcher(match string) StdoutWatcher {
	return StdoutWatcher{
		Match:  match,
		Signal: make(chan struct{}, 1),
	}
}

// TestIpniRunner is a helper for running the indexer and other commands.
// TestIpniRunner is not specifically tied to the indexer, but is designed
// to be used to manage multiple processes in a test; and is therefore useful
// for testing the indexer, the dhstore, and providers, all in a temporary
// directory and with a test environment.
type TestIpniRunner struct {
	t       *testing.T
	verbose bool

	Ctx context.Context
	Dir string
	Env []string
}

// NewTestIpniRunner creates a new TestIpniRunner for the given test,
// context, and temporary directory. It also takes a list of StdoutWatchers,
// which will be used to watch for specific output from the commands.
func NewTestIpniRunner(t *testing.T, ctx context.Context, dir string) *TestIpniRunner {
	tr := TestIpniRunner{
		t:       t,
		verbose: os.Getenv(EnvTestIpniRunnerOutput) != "",

		Ctx: ctx,
		Dir: dir,
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
	tr.Env = append(filteredEnv, []string{
		"HOME=" + tr.Dir,
		"GOBIN=" + tr.Dir,
	}...)
	if runtime.GOOS == "windows" {
		const gopath = "C:\\Projects\\Go"
		err := os.MkdirAll(gopath, 0666)
		require.NoError(t, err)
		tr.Env = append(tr.Env, fmt.Sprintf("GOPATH=%s", gopath))
	}
	if tr.verbose {
		t.Logf("Env: %s", strings.Join(tr.Env, " "))
	}

	// Reuse the host's build and module download cache.
	// This should allow "go install" to reuse work.
	for _, name := range []string{"GOCACHE", "GOMODCACHE"} {
		out, err := exec.Command("go", "env", name).CombinedOutput()
		require.NoError(t, err)
		out = bytes.TrimSpace(out)
		tr.Env = append(tr.Env, fmt.Sprintf("%s=%s", name, out))
	}

	return &tr
}

// Run runs a command and returns its output. This is useful for executing
// synchronous commands within the temporary environment.
func (tr *TestIpniRunner) Run(name string, args ...string) []byte {
	tr.t.Helper()

	if tr.verbose {
		tr.t.Logf("run: %s %s", name, strings.Join(args, " "))
	}

	cmd := exec.CommandContext(tr.Ctx, name, args...)
	cmd.Env = tr.Env
	out, err := cmd.CombinedOutput()
	require.NoError(tr.t, err, string(out))
	return out
}

type Execution struct {
	Name     string
	Args     []string
	Watchers []StdoutWatcher
}

func NewExecution(name string, args ...string) Execution {
	return Execution{
		Name:     name,
		Args:     args,
		Watchers: []StdoutWatcher{},
	}
}

func (p Execution) String() string {
	return p.Name + " " + strings.Join(p.Args, " ")
}

func (p Execution) WithWatcher(watcher StdoutWatcher) Execution {
	p.Watchers = append(append([]StdoutWatcher{}, p.Watchers...), watcher)
	return p
}

// Start starts a command and returns the command. This is useful for executing
// asynchronous commands within the temporary environment. It will watch the
// command's stdout for the given match string, and send on a watcher's
// channel when/if found.
func (tr *TestIpniRunner) Start(ex Execution) *exec.Cmd {
	tr.t.Helper()

	name := filepath.Base(ex.Name)
	if tr.verbose {
		tr.t.Logf("run: %s", ex.String())
	}

	cmd := exec.CommandContext(tr.Ctx, ex.Name, ex.Args...)
	cmd.Env = tr.Env

	stdout, err := cmd.StdoutPipe()
	require.NoError(tr.t, err)
	cmd.Stderr = cmd.Stdout

	scanner := bufio.NewScanner(stdout)

	if tr.verbose {
		for _, watcher := range ex.Watchers {
			tr.t.Logf("watching: %s for [%s]", name, watcher.Match)
		}
	}
	go func() {
		for scanner.Scan() {
			line := strings.ToLower(scanner.Text())

			if tr.verbose {
				// Logging every single line via the test output is verbose,
				// but helps see what's happening, especially when the test fails.
				tr.t.Logf("%s: %s", name, line)
			}

			for _, watcher := range ex.Watchers {
				if strings.Contains(line, strings.ToLower(watcher.Match)) {
					watcher.Signal <- struct{}{}
				}
			}
		}
	}()

	err = cmd.Start()
	require.NoError(tr.t, err)
	return cmd
}

// Stop stops a command. It sends SIGINT, and if that doesn't work, SIGKILL.
func (tr *TestIpniRunner) Stop(cmd *exec.Cmd, timeout time.Duration) {
	sig := os.Interrupt
	if runtime.GOOS == "windows" {
		// Windows can't send SIGINT.
		sig = os.Kill
	}
	err := cmd.Process.Signal(sig)
	require.NoError(tr.t, err)

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	select {
	case <-time.After(timeout):
		tr.t.Logf("killing command after %s: %s", timeout, cmd)
		err = cmd.Process.Kill()
		require.NoError(tr.t, err)
	case err = <-waitErr:
		require.NoError(tr.t, err)
	}
}
