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
	Flags:  initFlags,
	Action: initCommand,
}

func initCommand(cctx *cli.Context) error {
	// Check that the config root exists and it writable.
	configRoot, err := config.PathRoot()
	if err != nil {
		return err
	}

	fmt.Println("Initializing indexer node at", configRoot)

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
	cacheSize := int(cctx.Int64("cachesize"))
	if cacheSize >= 0 {
		cfg.Indexer.CacheSize = cacheSize
	}

	storeType := cctx.String("store")
	switch storeType {
	case "":
		// Use config default
	case "sth", "pogreb", "memory":
		// These are good
		cfg.Indexer.ValueStoreType = storeType
	default:
		return fmt.Errorf("unrecognized store type: %s", storeType)
	}

	adminAddr := cctx.String("listen-admin")
	if adminAddr != "" {
		_, err := multiaddr.NewMultiaddr(adminAddr)
		if err != nil {
			return fmt.Errorf("bad listen-admin: %s", err)
		}
		cfg.Addresses.Admin = adminAddr
	}

	finderAddr := cctx.String("listen-finder")
	if finderAddr != "" {
		_, err := multiaddr.NewMultiaddr(finderAddr)
		if err != nil {
			return fmt.Errorf("bad listen-finder: %s", err)
		}
		cfg.Addresses.Finder = finderAddr
	}

	ingestAddr := cctx.String("listen-ingest")
	if ingestAddr != "" {
		_, err := multiaddr.NewMultiaddr(ingestAddr)
		if err != nil {
			return fmt.Errorf("bad listen-ingest: %s", err)
		}
		cfg.Addresses.Ingest = ingestAddr
	}

	lotusGateway := cctx.String("lotus-gateway")
	if lotusGateway != "" {
		cfg.Discovery.LotusGateway = lotusGateway
	}

	pubsubPeer := cctx.String("pubsub-peer")
	if pubsubPeer != "" {
		cfg.Ingest.PubSubPeer = pubsubPeer
	}

	return cfg.Save(configFile)
}
