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

	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/carstore"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
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

	e := test.NewTestIpniRunner(t, ctx, t.TempDir())

	carPath := filepath.Join(e.Dir, "sample-wrapped-v2.car")
	err := downloadFile("https://github.com/ipni/index-provider/raw/main/testdata/sample-wrapped-v2.car", carPath)
	require.NoError(t, err)

	// install storetheindex
	indexer := filepath.Join(e.Dir, "storetheindex")
	e.Run("go", "install", ".")

	provider := filepath.Join(e.Dir, "provider")
	dhstore := filepath.Join(e.Dir, "dhstore")
	ipni := filepath.Join(e.Dir, "ipni")

	cwd, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(e.Dir)
	require.NoError(t, err)

	// install index-provider
	switch publisherProto {
	case "dtsync":
		// Install index-provider that supports dtsync.
		e.Run("go", "install", "github.com/ipni/index-provider/cmd/provider@v0.13.6")
	case "libp2p", "libp2phttp", "http":
		e.Run("go", "install", "github.com/ipni/index-provider/cmd/provider@latest")
	default:
		panic("providerProto must be one of: libp2phttp, http, dtsync")
	}
	// install dhstore
	e.Run("go", "install", "-tags", "nofdb", "github.com/ipni/dhstore/cmd/dhstore@latest")

	// install ipni-cli
	e.Run("go", "install", "github.com/ipni/ipni-cli/cmd/ipni@latest")

	err = os.Chdir(cwd)
	require.NoError(t, err)

	// initialize index-provider
	switch publisherProto {
	case "dtsync":
		e.Run(provider, "init")
	case "http":
		e.Run(provider, "init", "--pubkind=http")
	case "libp2p":
		e.Run(provider, "init", "--pubkind=libp2p")
	case "libp2phttp":
		e.Run(provider, "init", "--pubkind=libp2phttp")
	}
	providerCfgPath := filepath.Join(e.Dir, ".index-provider", "config")
	cfg, err := config.Load(providerCfgPath)
	require.NoError(t, err)
	providerID := cfg.Identity.PeerID
	t.Logf("Initialized provider ID: %s", providerID)

	// initialize indexer
	e.Run(indexer, "init", "--store", "pebble", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap")
	stiCfgPath := filepath.Join(e.Dir, ".storetheindex", "config")
	cfg, err = config.Load(stiCfgPath)
	require.NoError(t, err)
	indexerID := cfg.Identity.PeerID
	cfg.Ingest.AdvertisementMirror = config.Mirror{
		Compress: "gzip",
		Write:    true,
		Storage: config.FileStore{
			Type: "local",
			Local: config.LocalFileStore{
				BasePath: e.Dir,
			},
		},
	}
	cfg.Save(stiCfgPath)

	// start provider
	providerReady := test.NewStdoutWatcher(test.ProviderReadyMatch)
	providerHasPeer := test.NewStdoutWatcher(test.ProviderHasPeerMatch)
	cmdProvider := e.Start(test.NewExecution(provider, "daemon").WithWatcher(providerReady).WithWatcher(providerHasPeer))
	select {
	case <-providerReady.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to start")
	}

	// start dhstore
	dhstoreReady := test.NewStdoutWatcher(test.DhstoreReady)
	cmdDhstore := e.Start(test.NewExecution(dhstore, "--storePath", e.Dir).WithWatcher(dhstoreReady))
	select {
	case <-dhstoreReady.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for dhstore to start")
	}

	// start indexer
	indexerReady := test.NewStdoutWatcher(test.IndexerReadyMatch)
	cmdIndexer := e.Start(test.NewExecution(indexer, "daemon").WithWatcher(indexerReady))
	select {
	case <-indexerReady.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer to start")
	}

	// connect provider to the indexer
	e.Run(provider, "connect",
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3003/p2p/%s", indexerID),
		"--listen-admin", "http://localhost:3102",
	)
	select {
	case <-providerHasPeer.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to connect to indexer")
	}

	// Allow provider advertisements, regardless of default policy.
	e.Run(indexer, "admin", "allow", "-i", "http://localhost:3002", "--peer", providerID)

	// Import a car file into the provider.  This will cause the provider to
	// publish an advertisement that the indexer will read.  The indexer will
	// then import the advertised content.
	outImport := e.Run(provider, "import", "car",
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
			findOutput := e.Run(ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
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

	e.Run("sync")

	// Check that ad was saved as CAR file.
	dir, err := os.Open(e.Dir)
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

	outRates := e.Run(indexer, "admin", "telemetry", "-i", "http://localhost:3002")
	t.Logf("Telemetry:\n%s", outRates)

	root2 := filepath.Join(e.Dir, ".storetheindex2")
	e.Env = append(e.Env, fmt.Sprintf("%s=%s", config.EnvDir, root2))
	e.Run(indexer, "init", "--store", "dhstore", "--pubsub-topic", "/indexer/ingest/mainnet", "--no-bootstrap", "--dhstore", "http://127.0.0.1:40080",
		"--listen-admin", "/ip4/127.0.0.1/tcp/3202", "--listen-finder", "/ip4/127.0.0.1/tcp/3200", "--listen-ingest", "/ip4/127.0.0.1/tcp/3201",
		"--listen-p2p", "/ip4/127.0.0.1/tcp/3203")

	sti2CfgPath := filepath.Join(root2, "config")
	cfg, err = config.Load(sti2CfgPath)
	require.NoError(t, err)
	indexer2ID := cfg.Identity.PeerID

	indexerReady2 := test.NewStdoutWatcher(test.IndexerReadyMatch)
	cmdIndexer2 := e.Start(test.NewExecution(indexer, "daemon").WithWatcher(indexerReady2))
	select {
	case <-indexerReady2.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for indexer2 to start")
	}

	outProviders := e.Run(ipni, "provider", "--all", "--indexer", "http://localhost:3200")
	require.Contains(t, string(outProviders), "No providers registered with indexer",
		"expected no providers message")

	// import providers from first indexer.
	e.Run(indexer, "admin", "import-providers", "--indexer", "http://localhost:3202", "--from", "localhost:3000")

	// Check that provider ID now appears in providers output.
	outProviders = e.Run(ipni, "provider", "--all", "--indexer", "http://localhost:3200", "--id-only")
	require.Contains(t, string(outProviders), providerID, "expected provider id in providers output after import-providers")

	// Connect provider to the 2nd indexer.
	e.Run(provider, "connect",
		"--imaddr", fmt.Sprintf("/dns/localhost/tcp/3203/p2p/%s", indexer2ID),
		"--listen-admin", "http://localhost:3102",
	)
	select {
	case <-providerHasPeer.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for provider to connect to indexer")
	}

	// Tell provider to send direct announce to 2nd indexer.
	out := e.Run(provider, "announce-http",
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
		dhResp, err = client.Find(e.Ctx, mh)
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
	outRemove := e.Run(provider, "remove", "car",
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
			findOutput := e.Run(ipni, "find", "--no-priv", "-i", "http://localhost:3000", "-mh", mh)
			t.Logf("import output:\n%s\n", findOutput)
			if !bytes.Contains(findOutput, []byte("not found")) {
				return false
			}
		}
		return true
	}, 10*time.Second, time.Second)

	// Check that status is not frozen.
	outStatus := e.Run(indexer, "admin", "status", "--indexer", "http://localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: false", "expected indexer to be frozen")

	e.Run(indexer, "admin", "freeze", "--indexer", "http://localhost:3202")
	outProviders = e.Run(ipni, "provider", "--all", "--indexer", "http://localhost:3200")

	// Check that provider ID now appears as frozen in providers output.
	require.Contains(t, string(outProviders), "FrozenAtTime", "expected provider to be frozen")

	// Check that status is frozen.
	outStatus = e.Run(indexer, "admin", "status", "--indexer", "http://localhost:3202")
	require.Contains(t, string(outStatus), "Frozen: true", "expected indexer to be frozen")

	e.Stop(cmdIndexer2, time.Second)

	e.Stop(cmdIndexer, time.Second)
	e.Stop(cmdProvider, time.Second)
	e.Stop(cmdDhstore, time.Second)
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
