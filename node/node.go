package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/persistent/pogreb"
	"github.com/filecoin-project/storetheindex/store/persistent/storethehash"
	"github.com/filecoin-project/storetheindex/store/primary"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("node")

const defaultStorageDir = ".storetheindex"

type Node struct {
	primary    store.Storage
	persistent store.Storage
	api        *api
}

func New(cctx *cli.Context) (*Node, error) {
	n := new(Node)

	cacheSize := int(cctx.Int64("cachesize"))
	if cacheSize != 0 {
		n.primary = primary.New(cacheSize)
		log.Infow("cachee enabled", "size", cacheSize)
	} else {
		log.Info("cachee disabled")
	}

	switch storageType := cctx.String("storge"); storageType {
	case "none":
		if n.primary == nil {
			return nil, errors.New("cache and storage cannot both be disabled")
		}
		log.Info("persistent storage disabled")

	case "sth", "prgreb":
		storageDir, err := checkStorageDir(cctx.String("dir"))
		if err != nil {
			return nil, err
		}

		if storageType == "sth" {
			n.persistent, err = storethehash.New(storageDir)
		} else {
			n.persistent, err = pogreb.New(storageDir)
		}
		if err != nil {
			return nil, err
		}

		log.Infow("Persistent storage enabled", "type", storageType, "dir", storageDir)

	default:
		return nil, fmt.Errorf("unrecognized storage type: %s", storageType)
	}

	err := n.initAPI(cctx.String("endpoint"))
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
