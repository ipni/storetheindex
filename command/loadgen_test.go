//go:build !race

package command_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ipni/storetheindex/command"
	"github.com/ipni/storetheindex/command/loadgen"
	"github.com/ipni/storetheindex/config"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

const (
	finderAddr = "/ip4/127.0.0.1/tcp/13000"
	ingestAddr = "/ip4/127.0.0.1/tcp/13001"
	adminAddr  = "/ip4/127.0.0.1/tcp/13002"
)

func TestSmallLoadNoHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	ctx, cncl := context.WithTimeout(context.Background(), 60*time.Second)
	defer cncl()
	testLoadHelper(ctx, t, 1, 1000, false)
}

func TestSmallLoadOverHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	ctx, cncl := context.WithTimeout(context.Background(), 60*time.Second)
	defer cncl()
	testLoadHelper(ctx, t, 1, 1000, true)
}

func TestLargeLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}

	ctx, cncl := context.WithTimeout(context.Background(), 180*time.Second)
	defer cncl()
	testLoadHelper(ctx, t, 10, 1000, false)
}

func testLoadHelper(ctx context.Context, t *testing.T, concurrentProviders uint, numberOfEntriesPerProvider uint, useHTTP bool) {
	switch runtime.GOOS {
	case "windows":
		t.Skip("skipping test on", runtime.GOOS)
	}

	// Set up a context that is canceled when the command is interrupted
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tempDir := t.TempDir()
	os.Setenv(config.EnvDir, tempDir)
	os.Setenv("STORETHEINDEX_LISTEN_FINDER", finderAddr)
	os.Setenv("STORETHEINDEX_LISTEN_ADMIN", adminAddr)
	os.Setenv("STORETHEINDEX_LISTEN_INGEST", ingestAddr)
	os.Setenv("STORETHEINDE_PUBSUB_TOPIC", loadgen.DefaultConfig().GossipSubTopic)

	app := &cli.App{
		Name: "indexer",
		Commands: []*cli.Command{
			command.InitCmd,
			command.DaemonCmd,
			command.LoadGenCmd,
			command.LoadGenVerifyCmd,
		},
	}

	err := app.RunContext(ctx, []string{"storetheindex", "init"})
	require.NoError(t, err)

	daemonDone := make(chan struct{})
	go func() {
		app.RunContext(ctx, []string{"storetheindex", "daemon"})
		close(daemonDone)
	}()

	finderParts := strings.Split(finderAddr, "/")
	finderPort := finderParts[len(finderParts)-1]

	c := http.Client{}
	for {
		resp, err := c.Get("http://127.0.0.1:" + finderPort + "/health")
		if err == nil && resp.StatusCode == 200 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Sleep a bit for the ingester to finish spinning up (should be right after finder but CI runs slow)
	time.Sleep(1 * time.Second)
	ingestParts := strings.Split(ingestAddr, "/")
	ingestPort := ingestParts[len(ingestParts)-1]
	for {
		resp, err := c.Get("http://127.0.0.1:" + ingestPort + "/health")
		if err == nil && resp.StatusCode == 200 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	loadgenDone := make(chan struct{})
	go func() {
		startLoadgen(ctx, "http://127.0.0.1:"+ingestPort, concurrentProviders, numberOfEntriesPerProvider, useHTTP)
		close(loadgenDone)
	}()

	timer := time.NewTimer(time.Second)
	foundAll := false
LOOP:
	for {
		err = app.RunContext(ctx, []string{"storetheindex", "loadgen-verify", "--indexerFind=http://127.0.0.1:" + finderPort, "--concurrentProviders=" + fmt.Sprint(concurrentProviders), "--maxEntryNumber=" + fmt.Sprint(numberOfEntriesPerProvider)})
		if err == nil {
			foundAll = true
			break
		}
		select {
		case <-ctx.Done():
			break LOOP
		case <-timer.C:
			timer.Reset(time.Second)
		}
	}
	timer.Stop()

	require.True(t, foundAll, "Did not find all entries")

	// Wait until loadgen and daemon are stopped so that tempDir can be removed
	// on Windows.
	cancel()
	<-loadgenDone
	<-daemonDone
}

func startLoadgen(ctx context.Context, indexerAddr string, concurrentProviders uint, numberOfEntriesPerProvider uint, useHTTP bool) {
	loadConfig := loadgen.DefaultConfig()
	loadConfig.StopAfterNEntries = uint64(numberOfEntriesPerProvider)
	loadConfig.IsHttp = useHTTP

	loadgen.StartLoadGen(ctx, loadConfig, loadgen.LoadGenOpts{
		IndexerAddr:         indexerAddr,
		ConcurrentProviders: concurrentProviders,
	})
}
