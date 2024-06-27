package server_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	testcmd "github.com/ipfs/go-test/cmd"
	"github.com/ipfs/go-test/random"
	client "github.com/ipni/go-libipni/ingest/client"
	adminclient "github.com/ipni/storetheindex/admin/client"
	"github.com/ipni/storetheindex/assigner/config"
	"github.com/ipni/storetheindex/assigner/core"
	server "github.com/ipni/storetheindex/assigner/server"
	sticfg "github.com/ipni/storetheindex/config"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

var pubIdent = sticfg.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

var pubIdent2 = sticfg.Identity{
	PeerID:  "12D3KooWQ9j3Ur5V9U63Vi6ved72TcA3sv34k74W3wpW5rwNvDc3",
	PrivKey: "CAESQLtIPpQ0cFqLyK9Wnkd0J8lkslrd/g3KJSZOog7MLLt31PlBaXUpIIU5WaTuEJPioGK3+jEbDzFxDNrkQX5xKTg=",
}

const (
	indexerReadyMatch = "Indexer is ready"
	pubsubTopic       = "/indexer/ingest/mainnet"
)

func setupServer(t *testing.T, assigner *core.Assigner) *server.Server {
	s, err := server.New("127.0.0.1:0", assigner)
	require.NoError(t, err)
	return s
}

func setupClient(t *testing.T, host string) *client.Client {
	c, err := client.New(host)
	require.NoError(t, err)
	return c
}

func TestAssignOnAnnounce(t *testing.T) {
	switch runtime.GOOS {
	case "windows":
		t.Skip("skipping test on", runtime.GOOS)
	}
	t.Parallel()

	rnr := testcmd.NewRunner(t, t.TempDir())

	cwd, err := os.Getwd()
	require.NoError(t, err)

	err = os.Chdir(filepath.Dir(filepath.Dir(cwd)))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rnr.Run(ctx, "go", "install", ".")

	err = os.Chdir(cwd)
	require.NoError(t, err)

	indexer := filepath.Join(rnr.Dir, "storetheindex")
	rnr.Run(ctx, indexer, "init", "--pubsub-topic", pubsubTopic, "--no-bootstrap", "--use-assigner",
		"--listen-admin=/ip4/127.0.0.1/tcp/3602",
		"--listen-finder=/ip4/127.0.0.1/tcp/3600",
		"--listen-ingest=/ip4/127.0.0.1/tcp/3601",
		"--listen-p2p=/ip4/127.0.0.1/tcp/3603",
	)
	stiCfg, err := sticfg.Load(filepath.Join(rnr.Dir, ".storetheindex", "config"))
	require.NoError(t, err)
	indexerID := stiCfg.Identity.PeerID
	t.Log("Initialized indexer", indexerID)

	indexerReady := testcmd.NewStdoutWatcher(indexerReadyMatch)
	cmdIndexer := rnr.Start(ctx, testcmd.Args(indexer, "daemon"), indexerReady)
	err = indexerReady.Wait(ctx)
	require.NoError(t, err, "timed out waiting for indexer to start")

	// Assign a peer, to test that assigner reads this at startup.
	err = assign(ctx, "localhost:3602", pubIdent2.PeerID)
	require.NoError(t, err)

	// Initialize everything
	peerID, _, err := pubIdent.Decode()
	require.NoError(t, err)
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
	require.NoError(t, err)
	ai.ID = peerID

	mhs := random.Multihashes(1)

	assignChan, cancel := assigner.OnAssignment(peerID)
	defer cancel()

	err = cl.Announce(context.Background(), ai, cid.NewCidV1(22, mhs[0]))
	require.NoErrorf(t, err, "Failed to announce to %s", s.URL())

	select {
	case indexerNum := <-assignChan:
		require.Equal(t, 0, indexerNum, "assigned to wrong indexer, expected 0 got %d", indexerNum)
		t.Log("Assigned publisher to indexer", indexerNum)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for assignment")
	}

	// Check assignment
	outProvider := rnr.Run(ctx, indexer, "admin", "list-assigned", "--indexer", cfg.IndexerPool[0].AdminURL)
	expect := peerID.String()
	require.Contains(t, string(outProvider), expect)

	rnr.Stop(cmdIndexer, 5*time.Second)

	// Start index again to check that assignment was persisted.
	cmdIndexer = rnr.Start(ctx, testcmd.Args(indexer, "daemon"), indexerReady)
	err = indexerReady.Wait(ctx)
	require.NoError(t, err, "timed out waiting for indexer to start")

	outProvider = rnr.Run(ctx, indexer, "admin", "list-assigned", "--indexer", cfg.IndexerPool[0].AdminURL)
	require.Contains(t, string(outProvider), expect)

	outProvider = rnr.Run(ctx, indexer, "admin", "unassign", "--indexer", cfg.IndexerPool[0].AdminURL, "-p", peerID.String())
	require.Contains(t, string(outProvider), expect)

	outProvider = rnr.Run(ctx, indexer, "admin", "list-assigned", "--indexer", cfg.IndexerPool[0].AdminURL)
	require.NotContains(t, string(outProvider), expect)

	rnr.Stop(cmdIndexer, 5*time.Second)
}

// initAssigner initializes a new registry
func initAssigner(t *testing.T, trustedID string) (*core.Assigner, config.Assignment) {
	const indexerIP = "127.0.0.1"
	var cfg = config.Assignment{
		IndexerPool: []config.Indexer{
			{
				AdminURL:  fmt.Sprintf("http://%s:3602", indexerIP),
				IngestURL: fmt.Sprintf("http://%s:3601", indexerIP),
			},
		},
		Policy: config.Policy{
			Allow:  false,
			Except: []string{trustedID},
		},
		PubSubTopic: pubsubTopic,
	}
	assigner, err := core.NewAssigner(context.Background(), cfg, nil)
	require.NoError(t, err)

	return assigner, cfg
}

func assign(ctx context.Context, indexer, peerIDStr string) error {
	cl, err := adminclient.New(indexer)
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return err
	}
	err = cl.Assign(ctx, peerID)
	if err != nil {
		return err
	}
	return nil
}
