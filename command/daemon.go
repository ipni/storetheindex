package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/urfave/cli/v2"

	core "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	adminserver "github.com/filecoin-project/storetheindex/server/admin"
	httpfinderserver "github.com/filecoin-project/storetheindex/server/finder/http"
	p2pfinderserver "github.com/filecoin-project/storetheindex/server/finder/libp2p"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/mitchellh/go-homedir"
)

const defaultStorageDir = ".storetheindex"

// shutdownTimeout is the duration that a graceful shutdown has to complete
const shutdownTimeout = 5 * time.Second

var log = logging.Logger("command/storetheindex")

var (
	ErrDaemonStart = errors.New("daemon did not start correctly")
	ErrDaemonStop  = errors.New("daemon did not stop correctly")
)

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Start an indexer daemon, accepting http requests",
	Flags:  DaemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	// TODO: get from config file
	storeDir := cctx.String("dir")
	storeType := cctx.String("storage")
	valueStore, err := createValueStore(storeDir, storeType)
	if err != nil {
		return err
	}
	log.Infow("Value storage initialized", "type", storeType, "dir", storeDir)

	var resultCache cache.Interface
	// TODO: get from config file
	cacheSize := int(cctx.Int64("cachesize"))
	if cacheSize != 0 {
		resultCache = radixcache.New(cacheSize)
		log.Infow("result cache enabled", "size", cacheSize)
	} else {
		log.Info("result cache disabled")
	}

	indexerCore := core.NewEngine(resultCache, valueStore)
	log.Infow("Indexer engine initialized")

	// TODO: get from config file
	finderAddr := cctx.String("finder_ep")
	finderAPI, err := httpfinderserver.New(finderAddr, indexerCore)
	if err != nil {
		return err
	}
	log.Infow("Admin API initialized")

	adminAddr := cctx.String("admin_ep")
	adminAPI, err := adminserver.New(adminAddr, indexerCore)
	if err != nil {
		return err
	}
	log.Infow("Client finder API initialized")

	var (
		p2pAPI          *p2pfinderserver.Server
		cancelP2pFinder context.CancelFunc
	)

	// TODO: get from config file
	p2pEnabled := cctx.Bool("enablep2p")
	if p2pEnabled {
		ctx, cancel := context.WithCancel(context.Background())

		// NOTE: We are creating a new flat libp2p host here because no other
		// process in the indexer node needs a libp2p host. In the future, when
		// the indexer node starts using other libp2p protocols to interact with
		// miners and other indexers, we may need to initialize it before and
		// use it here so we have a single libp2p host giving service to the whole indexer.
		p2pHost, err := libp2p.New(ctx)
		if err != nil {
			cancel()
			return err
		}

		p2pAPI, err = p2pfinderserver.New(ctx, p2pHost, indexerCore)
		if err != nil {
			cancel()
			return err
		}
		cancelP2pFinder = cancel
		log.Infow("Client libp2p API initialized")
	}

	log.Info("Starting daemon servers")
	errChan := make(chan error, 2)
	go func() {
		errChan <- finderAPI.Start()
	}()
	go func() {
		errChan <- adminAPI.Start()
	}()

	var finalErr error
	// Wait for SIGINT (CTRL-c), then close server and exit.
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	select {
	case <-sigint:
	case err = <-errChan:
		log.Errorw("Failed to start server", "err", err)
		finalErr = ErrDaemonStart
	}

	log.Infow("Shutting down daemon")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
			fmt.Println("Timed out on shutdown, terminating...")
		case <-sigint:
			fmt.Println("Received another interrupt before graceful shutdown, terminating...")
		}
		os.Exit(-1)
	}()

	if p2pAPI != nil {
		cancelP2pFinder()
	}

	if err = adminAPI.Shutdown(ctx); err != nil {
		log.Errorw("Error shutting down admin api", "err", err)
		finalErr = ErrDaemonStop
	}
	if err = finderAPI.Shutdown(ctx); err != nil {
		log.Errorw("Error shutting down finder api", "err", err)
		finalErr = ErrDaemonStop
	}

	if err = valueStore.Close(); err != nil {
		log.Errorw("Error closing value store", "err", err)
		finalErr = ErrDaemonStop
	}

	log.Infow("node stopped")
	return finalErr
}

func createValueStore(dir, storeType string) (store.Interface, error) {
	dir, err := checkStorageDir(dir)
	if err != nil {
		return nil, err
	}

	if storeType == "sth" {
		return storethehash.New(dir)
	}
	if storeType == "prgreb" {
		return pogreb.New(dir)
	}

	return nil, fmt.Errorf("unrecognized store type: %s", storeType)
}

func checkStorageDir(dir string) (string, error) {
	var err error
	if dir != "" {
		dir, err = homedir.Expand(dir)
		if err != nil {
			return "", err
		}
	} else {
		home, err := homedir.Dir()
		if err != nil {
			return "", err
		}
		if home == "" {
			return "", errors.New("could not determine storage directory, home dir not set")
		}

		dir = filepath.Join(home, defaultStorageDir)
	}

	if err = checkMkDir(dir); err != nil {
		return "", err
	}

	return dir, nil
}

// checkMkDir checks that the directory exists, and if not, creates it
func checkMkDir(dir string) error {
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.Mkdir(dir, 0644); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	return nil
}
