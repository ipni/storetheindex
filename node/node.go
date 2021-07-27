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
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("node")

const defaultStorageDir = ".storetheindex"

type Node struct {
	indexer *core.Engine
	api     *api
}

func New(cctx *cli.Context) (*Node, error) {
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
	err = n.initAPI(cctx.String("endpoint"))
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) Start() error {
	log.Info("Started server")
	// TODO: Start required processes for stores
	return n.api.Serve()

}

func (n *Node) Shutdown(ctx context.Context) error {
	return n.api.Shutdown(ctx)
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
