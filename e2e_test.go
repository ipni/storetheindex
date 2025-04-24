package main_test

//lint:file-ignore U1000 Currently skipping this test since it's slow and breaks
//often because it's non-reproducible. TODO fixme

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	testcmd "github.com/ipfs/go-test/cmd"
	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// When the environ var "TEST_RUNNER_OUTPUT" is set to a value and running
// tests with -v flag, then testcmd.Runner output is logged.

const (
	indexerReadyMatch    = "Indexer is ready"
	providerHasPeerMatch = "connected to peer successfully"
	providerReadyMatch   = "admin http server listening"
	dhstoreReady         = "Store opened."
)

// This is a full end-to-end test with storetheindex as the indexer daemon,
// and index-provider/cmd/provider as a client.
// We build both programs, noting that we always build the latest provider.
// We initialize their setup, start the two daemons, and connect the peers.
// We then import a CAR file and query its CIDs.

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

	rnr := testcmd.NewRunner(t, t.TempDir())

	carPath := filepath.Join(rnr.Dir, "sample-wrapped-v2.car")
	err := downloadFile("https://github.com/ipni/index-provider/raw/main/testdata/sample-wrapped-v2.car", carPath)
	require.NoError(t, err)

	// install storetheindex
	indexer := filepath.Join(rnr.Dir, "storetheindex")
	rnr.Run(ctx, "go", "install", ".")

	provider := filepath.Join(rnr.Dir, "provider")
	dhstore := filepath.Join(rnr.Dir, "dhstore")
	ipni := filepath.Join(rnr.Dir, "ipni")

	cwd, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(rnr.Dir)
	require.NoError(t, err)

	// install index-provider
	switch publisherProto {
	case "dtsync":
		// Install index-provider that supports dtsync.
		rnr.Run(ctx, "go", "install", "github.com/ipni/index-provider/cmd/provider@v0.13.6")
	case "libp2p", "libp2phttp", "http":
		rnr.Run(ctx, "go", "install", "github.com/ipni/index-provider/cmd/provider@latest")
	default:
		panic("providerProto must be one of: libp2phttp, http, dtsync")
	}
	// install dhstore
	rnr.Run(ctx, "go", "install", "-tags", "nofdb", "github.com/ipni/dhstore/cmd/dhstore@latest")

	// install ipni-cli
	rnr.Run(ctx, "go", "install", "github.com/ipni/ipni-cli/cmd/ipni@latest")

	err = os.Chdir(cwd)
	require.NoError(t, err)

	// initialize index-provider
	switch publisherProto {
	case "dtsync":
		rnr.Run(ctx, provider, "init")
	case "http":
		rnr.Run(ctx, provider, "init", "--pubkind=http")
	case "libp2p":
		rnr.Run(ctx, provider, "init", "--pubkind=libp2p")
	case "libp2phttp":
		rnr.Run(ctx, provider, "init", "--pubkind=libp2phttp")
	}
	providerCfgPath := filepath.Join(rnr.Dir, ".index-provider", "config")
	cfg, err := config.Load(providerCfgPath)
	require.NoError(t, err)
	providerID := cfg.Identity.PeerID
	t.Logf("Initialized provider ID: %s", providerID)

	// initialize indexer
	rnr.Run(ctx, indexer, "init", "--store", "pebble", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap")
	stiCfgPath := filepath.Join(rnr.Dir, ".storetheindex", "config")
	cfg, err = config.Load(stiCfgPath)
	require.NoError(t, err)
	indexerID := cfg.Identity.PeerID
	cfg.Ingest.AdvertisementMirror = config.Mirror{
		Compress: "gzip",
		Write:    true,
		Storage: filestore.Config{
			Type: "local",
			Local: filestore.LocalConfig{
				BasePath: rnr.Dir,
			},
		},
	}
	rdMirrorDir := rnr.Dir
	err = cfg.Save(stiCfgPath)
	require.NoError(t, err)

	// start provider
	providerReady := testcmd.NewStderrWatcher(providerReadyMatch)
	providerHasPeer := testcmd.NewStderrWatcher(providerHasPeerMatch)
	cmdProvider := rnr.Start(ctx, testcmd.Args(provider, "daemon"), providerReady, providerHasPeer)
	err = providerReady.Wait(ctx)
	require.NoError(t, err, "timed out waiting for provider to start")

	// start dhstore
	dhstoreReady := testcmd.NewStderrWatcher(dhstoreReady)
	cmdDhstore := rnr.Start(ctx, testcmd.Args(dhstore, "--storePath", rnr.Dir), dhstoreReady)
	err = dhstoreReady.Wait(ctx)
	require.NoError(t, err, "timed out waiting for dhstore to start")

	// start indexer
	indexerReady := testcmd.NewStdoutWatcher(indexerReadyMatch)
	cmdIndexer := rnr.Start(ctx, testcmd.Args(indexer, "daemon"), indexerReady)
	err = indexerReady.Wait(ctx)
	require.NoError(t, err, "timed out waiting for indexer to start")

	// connect provider to the indexer
	rnr.Run(ctx, provider, "connect",
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3003/p2p/%s", indexerID),
		"--listen-admin", "http://localhost:3102",
	)
	err = providerHasPeer.Wait(ctx)
	require.NoError(t, err, "timed out waiting for provider to connect to indexer")

	// Allow provider advertisements, regardless of default policy.
	rnr.Run(ctx, indexer, "admin", "allow", "-i", "http://localhost:3002", "--peer", providerID)

	// Import a car file into the provider.  This will cause the provider to
	// publish an advertisement that the indexer will read.  The indexer will
	// then import the advertised content.
	outImport := rnr.Run(ctx, provider, "import", "car",
		"-i", carPath,
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("import output:\n%s\n", string(outImport))

	// Wait for the CAR to be indexed
	require.Eventually(t, func() bool {
		for _, mh := range []string{
			"2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj",
			"2DrjgbFY1BnkgZwA3oL7ijiDn7sJMf4bhhQNTtDqgZP826vGzv",
		} {
			findOutput := rnr.Run(ctx, ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
			t.Logf("find output:\n%s\n", findOutput)

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

	rnr.Run(ctx, "sync")

	// Check that ad was saved as CAR file.
	dir, err := os.Open(rnr.Dir)
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

	outRates := rnr.Run(ctx, indexer, "admin", "telemetry", "-i", "http://localhost:3002")
	require.Contains(t, string(outRates), "1043 multihashes from 1 ads")
	t.Logf("Telemetry:\n%s", outRates)

	root2 := filepath.Join(rnr.Dir, ".storetheindex2")
	rnr.Env = append(rnr.Env, fmt.Sprintf("%s=%s", config.EnvDir, root2))
	rnr.Run(ctx, indexer, "init", "--store", "dhstore", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap", "--dhstore", "http://127.0.0.1:40080",
		"--listen-admin", "/ip4/127.0.0.1/tcp/3202", "--listen-finder", "/ip4/127.0.0.1/tcp/3200", "--listen-ingest", "/ip4/127.0.0.1/tcp/3201",
		"--listen-p2p", "/ip4/127.0.0.1/tcp/3203")

	sti2CfgPath := filepath.Join(root2, "config")
	cfg, err = config.Load(sti2CfgPath)
	require.NoError(t, err)

	indexer2ID := cfg.Identity.PeerID
	cfg.Ingest.AdvertisementMirror = config.Mirror{
		Compress: "gzip",
		Read:     true,
		Write:    true,
		Retrieval: filestore.Config{
			Type: "local",
			Local: filestore.LocalConfig{
				BasePath: rdMirrorDir,
			},
		},
		Storage: filestore.Config{
			Type: "local",
			Local: filestore.LocalConfig{
				BasePath: rnr.Dir,
			},
		},
	}
	err = cfg.Save(sti2CfgPath)
	require.NoError(t, err)
	wrMirrorDir := rnr.Dir

	indexerReady2 := testcmd.NewStdoutWatcher(indexerReadyMatch)
	cmdIndexer2 := rnr.Start(ctx, testcmd.Args(indexer, "daemon"), indexerReady2)
	err = indexerReady2.Wait(ctx)
	require.NoError(t, err, "timed out waiting for indexer2 to start")

	outProviders := rnr.Run(ctx, ipni, "provider", "--all", "--indexer", "http://localhost:3200")
	require.Contains(t, string(outProviders), "No providers registered with indexer",
		"expected no providers message")

	// import providers from first indexer.
	rnr.Run(ctx, indexer, "admin", "import-providers", "--indexer", "http://localhost:3202", "--from", "localhost:3000")

	// Check that provider ID now appears in providers output.
	outProviders = rnr.Run(ctx, ipni, "provider", "--all", "--indexer", "http://localhost:3200", "--id-only")
	require.Contains(t, string(outProviders), providerID, "expected provider id in providers output after import-providers")

	// Connect provider to the 2nd indexer.
	rnr.Run(ctx, provider, "connect",
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3203/p2p/%s", indexer2ID),
		"--listen-admin", "http://localhost:3102",
	)
	err = providerHasPeer.Wait(ctx)
	require.NoError(t, err, "timed out waiting for provider to connect to indexer")

	// Tell provider to send direct announce to 2nd indexer.
	out := rnr.Run(ctx, provider, "announce-http",
		"-i", "http://localhost:3201",
		"--listen-admin", "http://localhost:3102",
	)
	t.Logf("announce output:\n%s\n", string(out))

	// Create double hashed client and verify that 2nd indexer wrote
	// multihashes to dhstore.
	client, err := findclient.NewDHashClient(findclient.WithProvidersURL("http://127.0.0.1:3000"), findclient.WithDHStoreURL("http://127.0.0.1:40080"))
	require.NoError(t, err)

	mh, err := multihash.FromB58String("2DrjgbFdhNiSJghFWcQbzw6E8y4jU1Z7ZsWo3dJbYxwGTNFmAj")
	require.NoError(t, err)

	var dhResp *model.FindResponse
	require.Eventually(t, func() bool {
		dhResp, err = client.Find(ctx, mh)
		return err == nil && len(dhResp.MultihashResults) != 0
	}, 10*time.Second, time.Second)

	require.Equal(t, 1, len(dhResp.MultihashResults))
	require.Equal(t, dhResp.MultihashResults[0].Multihash, mh)
	require.Equal(t, 1, len(dhResp.MultihashResults[0].ProviderResults))
	require.Equal(t, providerID, dhResp.MultihashResults[0].ProviderResults[0].Provider.ID.String())

	// Get the CAR file from the read mirror.
	rdCarFile, err := carFromMirror(ctx, rdMirrorDir)
	require.NoError(t, err)
	require.NotZero(t, rdCarFile.Size)

	// Get the CAR file from the write mirror and compare size.
	wrCarFS, err := filestore.NewLocal(wrMirrorDir)
	require.NoError(t, err)
	wrCarFile, err := wrCarFS.Head(ctx, rdCarFile.Path)
	require.NoError(t, err)
	require.Equal(t, rdCarFile.Size, wrCarFile.Size)
	t.Logf("CAR file %q is same size in read and write mirror: %d bytes", wrCarFile.Path, wrCarFile.Size)

	// Remove a car file from the provider. This will cause the provider to
	// publish an advertisement that tells the indexer to remove the car file
	// content by contextID. The indexer will then import the advertisement and
	// remove content.
	outRemove := rnr.Run(ctx, provider, "remove", "car",
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
			findOutput := rnr.Run(ctx, ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
			t.Logf("find output:\n%s\n", findOutput)
			if !bytes.Contains(findOutput, []byte("not found")) {
				return false
			}
		}
		return true
	}, 10*time.Second, time.Second)

	// Check that status is not frozen.
	outStatus := rnr.Run(ctx, indexer, "admin", "status", "--indexer", "http://localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: false", "expected indexer to be frozen")

	rnr.Run(ctx, indexer, "admin", "freeze", "--indexer", "http://localhost:3202")
	outProviders = rnr.Run(ctx, ipni, "provider", "--all", "--indexer", "http://localhost:3200")

	// Check that provider ID now appears as frozen in providers output.
	require.Contains(t, string(outProviders), "FrozenAtTime", "expected provider to be frozen")

	// Check that status is frozen.
	outStatus = rnr.Run(ctx, indexer, "admin", "status", "--indexer", "http://localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: true", "expected indexer to be frozen")

	logLevel := "info"
	if testing.Verbose() {
		logLevel = "debug"
	}
	outgc := string(rnr.Run(ctx, indexer, "gc", "provider", "-pid", providerID, "-ll", logLevel,
		"-i", "http://localhost:3200",
		"-i", "http://localhost:3000",
		"-sync-segment-size", "2",
	))
	t.Logf("GC Results:\n%s\n", outgc)
	require.Contains(t, outgc, `"count": 1043, "total": 1043, "source": "CAR"`)

	rnr.Stop(cmdIndexer2, time.Second)
	rnr.Stop(cmdIndexer, time.Second)
	rnr.Stop(cmdProvider, time.Second)
	rnr.Stop(cmdDhstore, time.Second)
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

func carFromMirror(ctx context.Context, mirrorDir string) (*filestore.File, error) {
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	mirrorFS, err := filestore.NewLocal(mirrorDir)
	if err != nil {
		return nil, err
	}
	files, errs := mirrorFS.List(listCtx, "/", false)
	for f := range files {
		if strings.HasSuffix(f.Path, ".car.gz") {
			return f, nil
		}
	}
	return nil, <-errs
}
