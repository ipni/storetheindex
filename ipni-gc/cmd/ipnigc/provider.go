package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core/store/dhstore"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/ipni-gc/reaper"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("ipni-gc")

const defaultIndexerURL = "http://localhost:3000"

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
		Name:        "indexer",
		Usage:       "Indexer URL. Specifies one or more URL to get provider info from",
		Aliases:     []string{"i"},
		DefaultText: "http://localhost:3000",
	},
	&cli.StringSliceFlag{
		Name:  "pid",
		Usage: "Provider's peer ID, multiple allowed. Reads IDs from stdin if none are specified.",
	},
	&cli.StringFlag{
		Name:    "log-level",
		Aliases: []string{"ll"},
		Usage:   "Set log level for ipni-gc",
		Value:   "info",
	},
	&cli.StringFlag{
		Name:    "log-level-other",
		Aliases: []string{"llo"},
		Usage:   "Set log level for other loggers that are not ipni-gc",
		Value:   "error",
	},
}

func providerAction(cctx *cli.Context) error {
	cfg, err := loadConfig("")
	if err != nil {
		return err
	}

	err = setLoggingConfig(cctx.String("log-level"), cctx.String("log-level-other"))
	if err != nil {
		return err
	}

	if cfg.Indexer.DHStoreURL == "" {
		return errors.New("DHStoreURL is not configured")
	}

	// Create a dhstore valuestore.
	dhs, err := dhstore.New(cfg.Indexer.DHStoreURL,
		dhstore.WithDHBatchSize(cfg.Indexer.DHBatchSize),
		dhstore.WithDHStoreCluster(cfg.Indexer.DHStoreClusterURLs),
		dhstore.WithHttpClientTimeout(time.Duration(cfg.Indexer.DHStoreHttpClientTimeout)))
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

	indexerURLs := cctx.StringSlice("indexer")
	if len(indexerURLs) == 0 {
		indexerURLs = []string{defaultIndexerURL}
	}

	dsDir, err := config.Path("", cfg.Datastore.Dir+"-gc")
	if err != nil {
		return err
	}
	dsTmpDir, err := config.Path("", cfg.Datastore.TmpDir+"-gc")
	if err != nil {
		return err
	}

	var pc *pcache.ProviderCache
	if len(peerIDs) > 1 {
		pc, err = pcache.New(pcache.WithRefreshInterval(0),
			pcache.WithSourceURL(indexerURLs...))
	} else {
		pc, err = pcache.New(pcache.WithPreload(false), pcache.WithRefreshInterval(0),
			pcache.WithSourceURL(indexerURLs...))
	}
	if err != nil {
		return err
	}

	var fileStore filestore.Interface
	cfgMirror := cfg.Ingest.AdvertisementMirror
	if cfgMirror.Read || cfgMirror.Write {
		fileStore, err = filestore.MakeFilestore(cfgMirror.Storage)
		if err != nil {
			return err
		}
	}

	grim, err := reaper.New(dhs, fileStore,
		reaper.WithCarCompress(cfgMirror.Compress),
		reaper.WithCarDelete(cfgMirror.Write),
		reaper.WithCarRead(true),
		reaper.WithCommit(cctx.Bool("commit")),
		reaper.WithDatastoreDir(dsDir),
		reaper.WithDatastoreTempDir(dsTmpDir),
		reaper.WithPCache(pc),
		reaper.WithTopicName(cfg.Ingest.PubSubTopic),
		reaper.WithHttpTimeout(time.Duration(cfg.Ingest.HttpSyncTimeout)),
	)
	if err != nil {
		return err
	}
	defer grim.Close()

	if cctx.Bool("commit") {
		fmt.Println("Starting IPNI GC, committing changes")
	} else {
		fmt.Println("Starting IPNI GC, dry-run - GC progress and changes will not be saved")
	}

	var gcCount int
	for _, pid := range peerIDs {
		err = grim.Reap(cctx.Context, pid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ipni-gc failed for provider %s: %s\n", pid, err)
			continue
		}
		gcCount++
	}

	if gcCount > 1 {
		stats := grim.Stats()
		log.Infow("Total gc stats for all providers",
			"AdsProcessed:", stats.AdsProcessed,
			"CarsDataSize:", stats.CarsDataSize,
			"CarsRemoved:", stats.CarsRemoved,
			"CtxIDsKept:", stats.CtxIDsKept,
			"CtxIDsRemoved:", stats.CtxIDsRemoved,
			"IndexAdsKept:", stats.IndexAdsKept,
			"IndexAdsRemoved:", stats.IndexAdsRemoved,
			"IndexesRemoved:", stats.IndexesRemoved,
			"RemovalAds:", stats.RemovalAds,
			"ReusedCtxIDs:", stats.ReusedCtxIDs,
			"TimeElapsed:", stats.TimeElapsed,
		)
	}

	return nil
}

func loadConfig(filePath string) (*config.Config, error) {
	cfg, err := config.Load(filePath)
	if err != nil {
		if errors.Is(err, config.ErrNotInitialized) {
			return nil, fmt.Errorf("cannot find storetheindex config")
		}
		return nil, fmt.Errorf("cannot load storetheindex config file: %w", err)
	}
	if cfg.Version != config.Version {
		log.Warnf("Configuration file out-of-date. Upgrade by running: storetheindex init --upgrade")
	}
	return cfg, nil
}

func setLoggingConfig(level, otherLevel string) error {
	err := logging.SetLogLevel("*", otherLevel)
	if err != nil {
		return err
	}

	err = logging.SetLogLevel("ipni-gc", level)
	if err != nil {
		return err
	}
	return nil
}
