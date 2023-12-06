package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core/store/dhstore"
	"github.com/ipni/go-libipni/pcache"
	sticfg "github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/ipni-gc/config"
	"github.com/ipni/storetheindex/ipni-gc/reaper"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("ipni-gc")

const progName = "assigner"

var providerCmd = &cli.Command{
	Name:   "provider",
	Usage:  "Run ipni garbage collection for specified providers",
	Flags:  providerFlags,
	Action: providerAction,
}

var providerFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    "commit",
		Usage:   "Commit changes to storage if set. Otherwise, only report what GC would have deleted.",
		Aliases: []string{"w"},
	},
	&cli.StringSliceFlag{
		Name:     "indexer",
		Usage:    "Indexer URL. Specifies one or more URL to get provider info from",
		Aliases:  []string{"i"},
		Required: true,
	},
	&cli.StringSliceFlag{
		Name:  "pid",
		Usage: "Provider's peer ID, multiple allowed. Reads IDs from stdin if none are specified.",
	},
	&cli.DurationFlag{
		Name:        "timeout",
		Aliases:     []string{"to"},
		Usage:       "Timeout for http and libp2phttp connections, example: 2m30s",
		Value:       10 * time.Second,
		DefaultText: "10s",
	},
	&cli.StringFlag{
		Name:  "topic",
		Usage: "Topic on which index advertisements are published. Only needed if connecting via Graphsync with non-standard topic.",
		Value: "/indexer/ingest/mainnet",
	},
}

func providerAction(cctx *cli.Context) error {
	cfg, err := loadConfig("")
	if err != nil {
		if errors.Is(err, sticfg.ErrNotInitialized) {
			fmt.Fprintln(os.Stderr, "gc is not initialized")
			fmt.Fprintln(os.Stderr, "To initialize, run the command: ./gc init")
			os.Exit(1)
		}
		return err
	}

	err = setLoggingConfig(cfg.Logging)
	if err != nil {
		return err
	}

	if cfg.Version != config.Version {
		log.Warnf("Configuration file out-of-date. Upgrade by running: ./%s init --upgrade", progName)
	}

	if cfg.DHStore.URL == "" {
		return errors.New("DHStoreURL is not configured")
	}

	// Create a dhstore valuestore.
	dhs, err := dhstore.New(cfg.DHStore.URL,
		dhstore.WithDHBatchSize(cfg.DHStore.BatchSize),
		dhstore.WithDHStoreCluster(cfg.DHStore.ClusterURLs),
		dhstore.WithHttpClientTimeout(time.Duration(cfg.DHStore.HttpClientTimeout)))
	if err != nil {
		return fmt.Errorf("failed to create dhstore valuestore: %w", err)
	}
	defer dhs.Close()

	pids := cctx.StringSlice("pid")
	if len(pids) == 0 {
		return errors.New("no provider id specified")
	}
	peerIDs := make([]peer.ID, len(pids))
	for i, pid := range pids {
		peerIDs[i], err = peer.Decode(pid)
		if err != nil {
			return fmt.Errorf("invalid peer ID %s: %s", pid, err)
		}
	}

	var pc *pcache.ProviderCache
	if len(peerIDs) > 1 {
		pc, err = pcache.New(pcache.WithRefreshInterval(0),
			pcache.WithSourceURL(cctx.StringSlice("indexer")...))
	} else {
		pc, err = pcache.New(pcache.WithPreload(false), pcache.WithRefreshInterval(0),
			pcache.WithSourceURL(cctx.StringSlice("indexer")...))
	}
	if err != nil {
		return err
	}

	dsDir, err := config.Path("", cfg.Datastore.Dir)
	if err != nil {
		return err
	}
	dsTmpDir, err := config.Path("", cfg.Datastore.TmpDir)
	if err != nil {
		return err
	}

	var fileStore filestore.Interface
	if cfg.Mirror.Read || cfg.Mirror.Write {
		fileStore, err = filestore.MakeFilestore(cfg.Mirror.Storage)
		if err != nil {
			return err
		}
	}

	grim, err := reaper.New(dhs, fileStore,
		reaper.WithCarCompress(cfg.Mirror.Compress),
		reaper.WithCarDelete(cfg.Mirror.Write),
		reaper.WithCarRead(cfg.Mirror.Read),
		reaper.WithCommit(cctx.Bool("commit")),
		reaper.WithDatastoreDir(dsDir),
		reaper.WithDatastoreTempDir(dsTmpDir),
		reaper.WithPCache(pc),
		reaper.WithTopicName(cctx.String("topic")),
		reaper.WithHttpTimeout(cctx.Duration("timeout")),
	)
	if err != nil {
		return err
	}
	defer grim.Close()

	if cctx.Bool("commit") {
		fmt.Println("Starting IPNI GC, committing changes")
	} else {
		fmt.Println("Starting IPNI GC, dry-run")
	}

	for _, pid := range peerIDs {
		err = grim.Reap(cctx.Context, pid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ipni-gc failed for provider %s: %s\n", pid, err)
			continue
		}

		stats := grim.Stats()
		grim.ClearStats()
		fmt.Println("GC stats for provider", pid)
		fmt.Println("  AdsProcessed:", stats.AdsProcessed)
		fmt.Println("  CarsDataSize:", stats.CarsDataSize)
		fmt.Println("  CarsRemoved:", stats.CarsRemoved)
		fmt.Println("  CtxIDsKept:", stats.CtxIDsKept)
		fmt.Println("  CtxIDsRemoved:", stats.CtxIDsRemoved)
		fmt.Println("  IndexAdsKept:", stats.IndexAdsKept)
		fmt.Println("  IndexAdsRemoved:", stats.IndexAdsRemoved)
		fmt.Println("  IndexesRemoved:", stats.IndexesRemoved)
		fmt.Println("  RemovalAds:", stats.RemovalAds)
		fmt.Println("  ReusedCtxIDs:", stats.ReusedCtxIDs)
	}

	return nil
}

func setLoggingConfig(cfgLogging config.Logging) error {
	err := logging.SetLogLevel("*", "warn")
	if err != nil {
		return err
	}

	// Set overall log level.
	err = logging.SetLogLevel("ipni-gc", cfgLogging.Level)
	if err != nil {
		return err
	}

	// Set level for individual loggers.
	for loggerName, level := range cfgLogging.Loggers {
		err = logging.SetLogLevel(loggerName, level)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadConfig(filePath string) (*config.Config, error) {
	cfg, err := config.Load(filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot load config file: %w", err)
	}
	if cfg.Version != config.Version {
		log.Warnf("Configuration file out-of-date. Upgrade by running: ./%s init --upgrade", progName)
	}

	return cfg, nil
}
