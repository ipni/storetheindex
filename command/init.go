package command

import (
	"fmt"
	"os"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/fsutil"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var InitCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize or upgrade indexer node config file",
	Flags:  initFlags,
	Action: initCommand,
}

func initCommand(cctx *cli.Context) error {
	// Check that the config root exists and it writable.
	configRoot, err := config.PathRoot()
	if err != nil {
		return err
	}

	if err = fsutil.DirWritable(configRoot); err != nil {
		return err
	}

	configFile, err := config.Filename(configRoot)
	if err != nil {
		return err
	}

	if cctx.Bool("upgrade") {
		cfg, err := config.Load(configFile)
		if err != nil {
			return err
		}
		prevVer := cfg.Version
		err = cfg.UpgradeConfig(configFile)
		if err != nil {
			return fmt.Errorf("cannot upgrade: %s", err)
		}
		fmt.Println("Upgraded", configFile, "from version", prevVer, "to", cfg.Version)
		return nil
	}

	fmt.Println("Initializing indexer node at", configRoot)

	if fsutil.FileExists(configFile) {
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
	case vstoreMemory, vstorePebble, vstorePogreb, vstoreStorethehash:
		// These are good
		cfg.Indexer.ValueStoreType = storeType
	default:
		return fmt.Errorf("unrecognized store type: %s", storeType)
	}

	adminAddr := cctx.String("listen-admin")
	if adminAddr != "" {
		if adminAddr != "none" {
			_, err := multiaddr.NewMultiaddr(adminAddr)
			if err != nil {
				return fmt.Errorf("bad listen-admin: %s", err)
			}
		}
		cfg.Addresses.Admin = adminAddr
	}

	finderAddr := cctx.String("listen-finder")
	if finderAddr != "" {
		if finderAddr != "none" {
			_, err := multiaddr.NewMultiaddr(finderAddr)
			if err != nil {
				return fmt.Errorf("bad listen-finder: %s", err)
			}
		}
		cfg.Addresses.Finder = finderAddr
	}

	ingestAddr := cctx.String("listen-ingest")
	if ingestAddr != "" {
		if ingestAddr != "none" {
			_, err := multiaddr.NewMultiaddr(ingestAddr)
			if err != nil {
				return fmt.Errorf("bad listen-ingest: %s", err)
			}
		}
		cfg.Addresses.Ingest = ingestAddr
	}

	p2pAddr := cctx.String("listen-p2p")
	if p2pAddr != "" {
		if p2pAddr != "none" {
			_, err := multiaddr.NewMultiaddr(p2pAddr)
			if err != nil {
				return fmt.Errorf("bad listen-p2p: %s", err)
			}
		}
		cfg.Addresses.P2PAddr = p2pAddr
	}

	lotusGateway := cctx.String("lotus-gateway")
	if lotusGateway != "" {
		cfg.Discovery.LotusGateway = lotusGateway
	}

	noBootstrap := cctx.Bool("no-bootstrap")
	if noBootstrap {
		cfg.Bootstrap.Peers = []string{}
		cfg.Bootstrap.MinimumPeers = 1
	}

	topic := cctx.String("pubsub-topic")
	if topic != "" {
		cfg.Ingest.PubSubTopic = topic
	}

	if cctx.Bool("use-assigner") {
		cfg.Discovery.UseAssigner = true
	}

	return cfg.Save(configFile)
}
