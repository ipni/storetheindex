package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipni/go-indexer-core/store/dhstore"
	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/ipni-gc/reaper"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var daemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Run ipni-gc daemon to do periocid GC for all providers",
	Flags:  daemonFlags,
	Action: daemonAction,
}

var daemonFlags = []cli.Flag{
	&cli.DurationFlag{
		Name:     "run-interval",
		Usage:    "Time to wait between each gc run",
		Aliases:  []string{"r"},
		Required: false,
		Value:    time.Hour,
	},
	&cli.IntFlag{
		Name:    "batch-size",
		Usage:   "Set batch size for dhstore requests",
		Aliases: []string{"b"},
		Value:   1024,
	},
	&cli.StringSliceFlag{
		Name:        "indexer",
		Usage:       "Indexer URL. Specifies one or more URL to get provider info from",
		Aliases:     []string{"i"},
		DefaultText: "http://localhost:3000",
	},
	&cli.StringSliceFlag{
		Name:    "exclude",
		Usage:   "Specify provider to exclude from GC",
		Aliases: []string{"x"},
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

func daemonAction(cctx *cli.Context) error {
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
		dhstore.WithDHBatchSize(cctx.Int("batch-size")),
		dhstore.WithDHStoreCluster(cfg.Indexer.DHStoreClusterURLs),
		dhstore.WithHttpClientTimeout(time.Duration(cfg.Indexer.DHStoreHttpClientTimeout)))
	if err != nil {
		return fmt.Errorf("failed to create dhstore valuestore: %w", err)
	}
	defer dhs.Close()

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

	pc, err := pcache.New(pcache.WithRefreshInterval(0),
		pcache.WithSourceURL(indexerURLs...))
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
		reaper.WithCommit(true),
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

	var excludes map[peer.ID]struct{}
	xpids := cctx.StringSlice("exclude")
	if len(xpids) != 0 {
		excludes = make(map[peer.ID]struct{}, len(xpids))
		for _, pid := range xpids {
			peerID, err := peer.Decode(pid)
			if err != nil {
				return fmt.Errorf("invalid peer ID %s: %s", pid, err)
			}
			excludes[peerID] = struct{}{}
		}
	}

	timer := time.NewTimer(time.Second)
	interval := cctx.Duration("run-interval")

	for {
		select {
		case <-timer.C:
			runGC(cctx.Context, grim, pc, excludes)
			timer.Reset(interval)
		case <-cctx.Context.Done():
			return nil
		}
	}

	return nil
}

func runGC(ctx context.Context, grim *reaper.Reaper, pc *pcache.ProviderCache, excludes map[peer.ID]struct{}) {
	// Get all providers from pcache, but only use the provider ID here. GC
	// needs to fetch the latest provider info from pcache, for each provider,
	// since that info may have changed by the time it is needed.
	provs := pc.List()
	if len(provs) == 0 {
		log.Info("No providers registered with indexer")
		return
	}
	pids := make([]peer.ID, 0, len(provs))
	for _, pinfo := range provs {
		if _, ok := excludes[pinfo.AddrInfo.ID]; ok {
			continue
		}
		pids = append(pids, pinfo.AddrInfo.ID)
	}

	log.Infow("Starting GC for all providers", "count", len(pids))
	start := time.Now()
	var success int
	for _, pid := range pids {
		err := grim.Reap(ctx, pid)
		if err != nil {
			log.Errorw("Failed GC for provider", "err", err, "provider", pid)
			continue
		}
		success++
	}
	log.Infow("Finished GC for all providers", "success", success, "fail", len(pids)-success, "elapsed", time.Since(start).String(), "stats", grim.Stats().String())
}
