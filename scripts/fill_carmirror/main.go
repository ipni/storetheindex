package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ipfs/go-cid"
	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := &cli.App{
		Name:  "fill_carmirror",
		Usage: "Fill missing or invalid advertisement CAR files for one provider",
		Description: `Uses the daemon AdvertisementMirror config. MainMode must be readwrite.
External mirrors are tried before downloading from the publisher.
A CAR already on main that fails validation is overwritten from external
or the publisher instead of stopping the walk.
IsRm and no-entries advertisements are not stored. An IsRm advertisement
is reported (including whether a CAR already exists on main) and not written.
Does not open the indexer value store.

--estimate counts advertisements (ads only, no entries) before filling
so progress can show a total. --estimate-timeout bounds that count;
fill still runs if the count times out.

By default each event is a timestamped line on stdout. Pass --log for
structured logs (GOLOG_LOG_FMT, GOLOG_LOG_LEVEL, GOLOG_FILE, ...).`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Usage: "Path to storetheindex config file (default: $STORETHEINDEX_PATH/config)",
			},
			&cli.StringFlag{
				Name:     "provider",
				Usage:    "Provider peer ID whose advertisement chain to fill",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "cid",
				Usage: "Advertisement CID to start from (default: provider LastAdvertisement from --indexer)",
			},
			&cli.StringFlag{
				Name:  "indexer",
				Usage: "Finder URL used to look up the provider's last processed advertisement and publisher AddrInfo",
			},
			&cli.StringFlag{
				Name:  "addr-info",
				Usage: "Publisher multiaddr (overrides indexer publisher), e.g. /ip4/1.2.3.4/tcp/24001/p2p/<id>",
			},
			&cli.IntFlag{
				Name:  "depth",
				Usage: "Maximum advertisements to process; 0 means unlimited",
			},
			&cli.DurationFlag{
				Name:  "progress",
				Usage: "How often to print running stats",
				Value: 10 * time.Second,
			},
			&cli.BoolFlag{
				Name:  "estimate",
				Usage: "Count advertisements in the chain before filling (ads only)",
			},
			&cli.DurationFlag{
				Name:  "estimate-timeout",
				Usage: "Time limit for --estimate; 0 means no limit",
				Value: 5 * time.Minute,
			},
			&cli.BoolFlag{
				Name:  "log",
				Usage: "Write structured logs instead of timestamped lines",
			},
		},
		Action: run,
	}

	if err := app.RunContext(ctx, os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(cctx *cli.Context) error {
	providerID, err := peer.Decode(cctx.String("provider"))
	if err != nil {
		return fmt.Errorf("bad --provider: %w", err)
	}

	cfgFile, err := resolveConfigPath(cctx.String("config"))
	if err != nil {
		return fmt.Errorf("cannot load config: %w", err)
	}
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("cannot load config: %w", err)
	}
	if err := resolveRelativeMirrorPaths(cfg, cfgFile); err != nil {
		return err
	}

	opts := Options{
		Mirror:            cfg.Ingest.AdvertisementMirror,
		HttpTimeout:       time.Duration(cfg.Ingest.HttpSyncTimeout),
		HttpRetryMax:      cfg.Ingest.HttpSyncRetryMax,
		HttpRetryWaitMin:  time.Duration(cfg.Ingest.HttpSyncRetryWaitMin),
		HttpRetryWaitMax:  time.Duration(cfg.Ingest.HttpSyncRetryWaitMax),
		EntriesDepthLimit: int64(cfg.Ingest.EntriesDepthLimit),
		Provider:          providerID,
		Depth:             cctx.Int("depth"),
		Estimate:          cctx.Bool("estimate"),
		EstimateTimeout:   cctx.Duration("estimate-timeout"),
	}

	ctx := cctx.Context
	if cidStr := cctx.String("cid"); cidStr != "" {
		opts.StartAd, err = cid.Decode(cidStr)
		if err != nil {
			return fmt.Errorf("bad --cid: %w", err)
		}
	} else if !cctx.IsSet("indexer") {
		return fmt.Errorf("required: --indexer (reads LastAdvertisement from provider info) or --cid")
	}
	if addrInfoStr := cctx.String("addr-info"); addrInfoStr != "" {
		ai, err := peer.AddrInfoFromString(addrInfoStr)
		if err != nil {
			return fmt.Errorf("bad --addr-info: %w", err)
		}
		opts.Publisher = *ai
	} else if !cctx.IsSet("indexer") {
		return fmt.Errorf("required: --indexer (reads publisher AddrInfo from provider info) or --addr-info")
	}

	hadStart := opts.StartAd != cid.Undef
	if indexerURL := cctx.String("indexer"); indexerURL != "" {
		if err := applyIndexer(ctx, indexerURL, providerID, &opts); err != nil {
			return fmt.Errorf("indexer lookup: %w", err)
		}
	}

	out, err := newCLIProgress(cctx)
	if err != nil {
		return err
	}
	opts.Out = out

	if indexerURL := cctx.String("indexer"); indexerURL != "" && !hadStart {
		out.UsingIndexer(opts.StartAd, indexerURL)
	}

	if progressEvery := cctx.Duration("progress"); progressEvery > 0 {
		ticker := time.NewTicker(progressEvery)
		defer ticker.Stop()
		opts.Out = &throttlePeriodic{Progress: opts.Out, ticker: ticker}
	} else {
		opts.Out = noPeriodic{opts.Out}
	}

	opts.Out.Start(opts)
	st, err := Fill(ctx, opts)
	if st != nil {
		opts.Out.Done(*st, err)
	} else if err != nil {
		opts.Out.Done(Stats{}, err)
	}
	if err != nil {
		return fmt.Errorf("fill failed: %w", err)
	}
	return nil
}

func newCLIProgress(cctx *cli.Context) (Progress, error) {
	if !cctx.Bool("log") {
		return NewPrintProgress(os.Stdout), nil
	}
	log, err := setupLogger()
	if err != nil {
		return nil, err
	}
	return NewLogProgress(log), nil
}

func applyIndexer(ctx context.Context, indexerURL string, providerID peer.ID, opts *Options) error {
	cl, err := findclient.New(indexerURL)
	if err != nil {
		return err
	}
	info, err := cl.GetProvider(ctx, providerID)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("provider %s not known by indexer", providerID)
	}
	if opts.StartAd == cid.Undef {
		if info.LastAdvertisement == cid.Undef {
			return fmt.Errorf("indexer has no LastAdvertisement for %s", providerID)
		}
		opts.StartAd = info.LastAdvertisement
	}
	if opts.Publisher.ID == "" && info.Publisher != nil && info.Publisher.ID != "" {
		opts.Publisher = *info.Publisher
	}
	return nil
}

func resolveConfigPath(flagVal string) (string, error) {
	if flagVal == "" {
		return config.Path("", "")
	}
	expanded, err := fsutil.ExpandHome(flagVal)
	if err != nil {
		return "", err
	}
	return filepath.Abs(expanded)
}

// resolveRelativeMirrorPaths makes local BasePath values absolute. Relative
// paths are resolved against the directory containing the config file so a
// local test config can use "mirror" next to itself.
func resolveRelativeMirrorPaths(cfg *config.Config, configFile string) error {
	baseDir := filepath.Dir(configFile)
	resolve := func(path *string) error {
		if path == nil || *path == "" || filepath.IsAbs(*path) {
			return nil
		}
		abs, err := filepath.Abs(filepath.Join(baseDir, *path))
		if err != nil {
			return err
		}
		if err := os.MkdirAll(abs, 0o755); err != nil {
			return fmt.Errorf("cannot create local car mirror dir %s: %w", abs, err)
		}
		*path = abs
		return nil
	}
	if err := resolve(&cfg.Ingest.AdvertisementMirror.Main.Local.BasePath); err != nil {
		return err
	}
	for i := range cfg.Ingest.AdvertisementMirror.External {
		if err := resolve(&cfg.Ingest.AdvertisementMirror.External[i].Local.BasePath); err != nil {
			return err
		}
	}
	return nil
}
