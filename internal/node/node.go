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
	adminserver "github.com/filecoin-project/storetheindex/internal/admin"
	httpfinderserver "github.com/filecoin-project/storetheindex/internal/finder/http"
	p2pfinderserver "github.com/filecoin-project/storetheindex/internal/finder/libp2p"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("node")

const defaultStorageDir = ".storetheindex"

// Node wraps an indexer node engine and process
type Node struct {
	indexer   *core.Engine
	finderAPI *httpfinderserver.Server
	adminAPI  *adminserver.Server
	p2pAPI    *p2pfinderserver.Server
}

// New creates a new Node process from CLI
func New(ctx context.Context, cctx *cli.Context) (*Node, error) {
	n := new(Node)
	var resultCache cache.Interface
	var valueStore store.Interface

	cacheSize := int(cctx.Int64("cachesize"))
	if cacheSize != 0 {
		resultCache = radixcache.New(cacheSize)
		log.Infow("result cache enabled", "size", cacheSize)
	} else {
		log.Info("result cache disabled")
	}

	storageDir, err := checkStorageDir(cctx.String("dir"))
	if err != nil {
		return nil, err
	}

	storageType := cctx.String("storage")
	switch storageType {
	case "sth":
		valueStore, err = storethehash.New(storageDir)
	case "prgreb":
		valueStore, err = pogreb.New(storageDir)
	default:
		err = fmt.Errorf("unrecognized storage type: %s", storageType)
	}
	if err != nil {
		return nil, err
	}
	log.Infow("Value storage initialized", "type", storageType, "dir", storageDir)

	n.indexer = core.NewEngine(resultCache, valueStore)
	log.Infow("Indexer engine initialized")
	n.finderAPI, err = httpfinderserver.New(cctx.String("finder_ep"), n.indexer)
	if err != nil {
		return nil, err
	}
	log.Infow("Admin API initialized")
	n.adminAPI, err = adminserver.New(cctx.String("admin_ep"), n.indexer)
	if err != nil {
		return nil, err
	}
	log.Infow("Client finder API initialized")
	p2pEnabled := cctx.Bool("enablep2p")
	if p2pEnabled {
		// NOTE: We are creating a new flat libp2p host here because no other
		// process in the indexer node needs a libp2p host. In the future, when
		// the indexer node starts using other libp2p protocols to interact with
		// miners and other indexers, we may need to initialize it before and
		// use it here so we have a single libp2p host giving service to the whole indexer.
		h, err := libp2p.New(ctx)
		if err != nil {
			return nil, err
		}
		n.p2pAPI, err = p2pfinderserver.New(ctx, h, n.indexer)
		if err != nil {
			return nil, err
		}
		log.Infow("Client libp2p API initialized")
	}

	return n, nil
}

// Start node process
func (n *Node) Start() error {

	// TODO: Handle server startups smarter so we catch
	// potential errors in finderAPI. Sticking to this
	// until we wrap up the refactor
	log.Info("Starting daemon servers")
	if n.finderAPI != nil {
		go n.finderAPI.Start()
	}

	return n.adminAPI.Start()

}

// Shutdown node process
func (n *Node) Shutdown(ctx context.Context) error {
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
