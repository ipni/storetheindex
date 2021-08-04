package command

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/urfave/cli/v2"
)

var InitCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize indexer node config file and identity",
	Flags:  InitFlags,
	Action: initCommand,
}

func initCommand(cctx *cli.Context) error {
	log.Info("Initializing indexer config file")

	configRoot, err := config.PathRoot()
	if err != nil {
		return err
	}

	if err = checkWritable(configRoot); err != nil {
		return err
	}

	configFile, err := config.Filename(configRoot)
	if err != nil {
		return err
	}

	if fileExists(configFile) {
		return config.ErrInitialized
	}

	cfg, err := config.Init(os.Stderr)
	if err != nil {
		return err
	}

	store := cctx.String("store")
	switch store {
	case "":
		// Use config default
	case "sth", "pogreb":
		cfg.Indexer.StoreType = store
	default:
		return fmt.Errorf("unrecognized store type: %s", store)
	}

	return cfg.Save(configFile)
}

func checkWritable(dir string) error {
	_, err := os.Stat(dir)
	if err == nil {
		// dir exists, make sure we can write to it
		testfile := filepath.Join(dir, "test")
		fi, err := os.Create(testfile)
		if err != nil {
			if os.IsPermission(err) {
				return fmt.Errorf("%s is not writeable by the current user", dir)
			}
			return fmt.Errorf("unexpected error while checking writeablility of repo root: %s", err)
		}
		fi.Close()
		return os.Remove(testfile)
	}

	if os.IsNotExist(err) {
		// dir doesn't exist, check that we can create it
		return os.Mkdir(dir, 0775)
	}

	if os.IsPermission(err) {
		return fmt.Errorf("cannot write to %s, incorrect permissions", err)
	}

	return err
}

func fileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	if fi != nil || (err != nil && !os.IsNotExist(err)) {
		return true
	}
	return false
}
