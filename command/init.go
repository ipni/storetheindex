package command

import (
	"fmt"
	"os"

	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var InitCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize or upgrade indexer node config file",
	Flags:  initFlags,
	Action: initAction,
}

var initFlags = []cli.Flag{
	cacheSizeFlag,
	listenAdminFlag,
	listenFindFlag,
	listenIngestFlag,
	listenP2PFlag,
	&cli.StringFlag{
		Name:     "store",
		Usage:    "Type of value store (pebble, memory). Default is \"pebble\"",
		Aliases:  []string{"s"},
		EnvVars:  []string{"STORETHEINDEX_VALUE_STORE"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "no-bootstrap",
		Usage:    "Do not configure bootstrap peers",
		EnvVars:  []string{"NO_BOOTSTRAP"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "pubsub-topic",
		Usage:    "Subscribe to this pubsub topic to receive advertisement notification",
		EnvVars:  []string{"STORETHEINDE_PUBSUB_TOPIC"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "upgrade",
		Usage:    "Upgrade the config file to the current version, saving the old config as config.prev, and ignoring other flags ",
		Aliases:  []string{"u"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "use-assigner",
		Usage:    "Configure the indexer to work with an assigner service",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "dhstore",
		Usage:    "Url of DHStore for double hashed index",
		Required: false,
	},
}

func initAction(cctx *cli.Context) error {
	// Check that the config root exists and it writable.
	configRoot, err := config.PathRoot()
	if err != nil {
		return err
	}

	if err = fsutil.DirWritable(configRoot); err != nil {
		return err
	}

	configFile, err := config.Path(configRoot, "")
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
	case vstoreDHStore, vstoreMemory, vstorePebble, vstoreRelayx:
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

	findAddr := cctx.String("listen-finder")
	if findAddr != "" {
		if findAddr != "none" {
			_, err := multiaddr.NewMultiaddr(findAddr)
			if err != nil {
				return fmt.Errorf("bad listen-finder: %s", err)
			}
		}
		cfg.Addresses.Finder = findAddr
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

	dhstoreUrl := cctx.String("dhstore")
	if dhstoreUrl != "" {
		log.Infow("dhstoreUrl", dhstoreUrl)
		cfg.Indexer.DHStoreURL = dhstoreUrl
		cfg.Indexer.DHBatchSize = -1
	}

	dhstoreClusterUrls := cctx.StringSlice("dhstore-cluster")
	if len(dhstoreClusterUrls) > 0 {
		log.Infow("dhstoreClusterUrls", dhstoreClusterUrls)
		cfg.Indexer.DHStoreClusterURLs = dhstoreClusterUrls
	}

	return cfg.Save(configFile)
}
