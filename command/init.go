package command

import (
	"fmt"
	"os"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/multiformats/go-multiaddr"
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

	// First read and validate flag values
	adminAddr := cctx.String("adminaddr")
	if adminAddr != "" {
		_, err := multiaddr.NewMultiaddr(adminAddr)
		if err != nil {
			return fmt.Errorf("bad admiaddr: %s", err)
		}
	}

	finderAddr := cctx.String("finderaddr")
	if finderAddr != "" {
		_, err := multiaddr.NewMultiaddr(finderAddr)
		if err != nil {
			return fmt.Errorf("bad finderaddr: %s", err)
		}
	}

	cacheSize := int(cctx.Int64("cachesize"))

	storeType := cctx.String("store")
	switch storeType {
	case "":
		// Use config default
	case "sth", "pogreb":
		// These are good
	default:
		return fmt.Errorf("unrecognized store type: %s", storeType)
	}

	// Check that the config root exists and it writable.
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

	// Use values from flags to override defaults
	if cacheSize >= 0 {
		cfg.Indexer.CacheSize = cacheSize
	}
	if storeType != "" {
		cfg.Indexer.ValueStoreType = storeType
	}
	if adminAddr != "" {
		cfg.Addresses.Admin = adminAddr
	}
	if finderAddr != "" {
		cfg.Addresses.Finder = finderAddr
	}

	return cfg.Save(configFile)
}
