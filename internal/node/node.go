package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

var log = logging.Logger("node")

const defaultStorageDir = ".storetheindex"

// Node wraps an indexer node engine and process
type Node struct {
	indexer   *core.Engine
	finderAPI *httpfinderserver.Server
	adminAPI  *adminserver.Server
	p2pAPI    *p2pfinderserver.Server

	cancelP2pFinder context.CancelFunc
}

// New creates a new Node process from CLI
func New(cacheSize int, dir, storeType, finderAddr, adminAddr string, p2pEnabled bool) (*Node, error) {
	n := new(Node)
	var resultCache cache.Interface
	var valueStore store.Interface

	if cacheSize != 0 {
		resultCache = radixcache.New(cacheSize)
		log.Infow("result cache enabled", "size", cacheSize)
	} else {
		log.Info("result cache disabled")
	}

	storageDir, err := checkStorageDir(dir)
	if err != nil {
		return nil, err
	}

	switch storeType {
	case "sth":
		valueStore, err = storethehash.New(storageDir)
	case "prgreb":
		valueStore, err = pogreb.New(storageDir)
	default:
		err = fmt.Errorf("unrecognized store type: %s", storeType)
	}
	if err != nil {
		return nil, err
	}
	log.Infow("Value storage initialized", "type", storeType, "dir", storageDir)

	n.indexer = core.NewEngine(resultCache, valueStore)
	log.Infow("Indexer engine initialized")
	n.finderAPI, err = httpfinderserver.New(finderAddr, n.indexer)
	if err != nil {
		return nil, err
	}
	log.Infow("Admin API initialized")
	n.adminAPI, err = adminserver.New(adminAddr, n.indexer)
	if err != nil {
		return nil, err
	}
	log.Infow("Client finder API initialized")

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
			return nil, err
		}

		n.p2pAPI, err = p2pfinderserver.New(ctx, p2pHost, n.indexer)
		if err != nil {
			cancel()
			return nil, err
		}
		n.cancelP2pFinder = cancel
		log.Infow("Client libp2p API initialized")
	}

	return n, nil
}

// Start node process
func (n *Node) Start() {

	// TODO: Handle server startups smarter so we catch
	// potential errors in finderAPI. Sticking to this
	// until we wrap up the refactor
	log.Info("Starting daemon servers")
	go n.finderAPI.Start()
	go n.adminAPI.Start()
}

// Shutdown node process
func (n *Node) Shutdown(ctx context.Context) error {
	if n.p2pAPI != nil {
		n.cancelP2pFinder()
	}
	if err := n.adminAPI.Shutdown(ctx); err != nil {
		return err
	}
	return n.finderAPI.Shutdown(ctx)
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
