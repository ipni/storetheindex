package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/memory"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/ingest"
	"github.com/filecoin-project/storetheindex/internal/lotus"
	"github.com/filecoin-project/storetheindex/internal/registry"
	httpadminserver "github.com/filecoin-project/storetheindex/server/admin/http"
	httpfinderserver "github.com/filecoin-project/storetheindex/server/finder/http"
	p2pfinderserver "github.com/filecoin-project/storetheindex/server/finder/libp2p"
	httpingestserver "github.com/filecoin-project/storetheindex/server/ingest/http"
	p2pingestserver "github.com/filecoin-project/storetheindex/server/ingest/libp2p"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-ipfs/core/bootstrap"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
)

const (
	// configCheckInterval is the time between config update checks.
	configCheckInterval = 30 * time.Second
	// shutdownTimeout is the duration that a graceful shutdown has to complete.
	shutdownTimeout = 10 * time.Second
)

// Recognized valuestore type names.
const (
	vstoreMemory       = "memory"
	vstorePogreb       = "pogreb"
	vstoreStorethehash = "sth"
)

var log = logging.Logger("indexer")

var (
	ErrDaemonStart = errors.New("daemon did not start correctly")
	ErrDaemonStop  = errors.New("daemon did not stop correctly")
)

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Start an indexer daemon, accepting http requests",
	Flags:  daemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	logLevel := strings.ToLower(cctx.String("log-level"))
	err := logging.SetLogLevel("*", logLevel)
	if err != nil {
		return err
	}
	// Do not log some facilities at info or debug level, unless "log-all" flag
	// is true.
	if !cctx.Bool("log-all") && logLevel == "info" || logLevel == "debug" {
		logging.SetLogLevel("basichost", "warn")
		logging.SetLogLevel("bootstrap", "warn")
		logging.SetLogLevel("dt_graphsync", "warn")
		logging.SetLogLevel("dt-impl", "warn")
		logging.SetLogLevel("graphsync", "warn")
	}

	cfg, err := loadConfig("")
	if err != nil {
		if errors.Is(err, config.ErrNotInitialized) {
			fmt.Fprintln(os.Stderr, "storetheindex is not initialized")
			fmt.Fprintln(os.Stderr, "To initialize, run the command: ./storetheindex init")
			os.Exit(1)
		}
		return err
	}
	if cfg.Version != config.Version {
		log.Warn("Configuration file out-of-date. Upgrade by running: ./storetheindex init --upgrade")
	}

	if cfg.Datastore.Type != "levelds" {
		return fmt.Errorf("only levelds datastore type supported, %q not supported", cfg.Datastore.Type)
	}

	// Create a valuestore of the configured type.
	valueStore, err := createValueStore(cfg.Indexer)
	if err != nil {
		return err
	}
	log.Info("Valuestore initialized")

	// Create result cache
	var resultCache cache.Interface
	cacheSize := int(cctx.Int64("cachesize"))
	if cacheSize < 0 {
		cacheSize = cfg.Indexer.CacheSize
	}
	if cacheSize > 0 {
		resultCache = radixcache.New(cacheSize)
		log.Infow("Result cache enabled", "size", cacheSize)
	} else {
		log.Info("Result cache disabled")
	}

	// Create indexer core
	indexerCore := engine.New(resultCache, valueStore)

	// Create datastore
	dataStorePath, err := config.Path("", cfg.Datastore.Dir)
	if err != nil {
		return err
	}
	err = checkWritable(dataStorePath)
	if err != nil {
		return err
	}
	dstore, err := leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		return err
	}

	var lotusDiscoverer *lotus.Discoverer
	if cfg.Discovery.LotusGateway != "none" {
		log.Infow("discovery using lotus", "gateway", cfg.Discovery.LotusGateway)
		// Create lotus client
		lotusDiscoverer, err = lotus.NewDiscoverer(cfg.Discovery.LotusGateway)
		if err != nil {
			return fmt.Errorf("cannot create lotus client: %s", err)
		}
	}

	// Create registry
	reg, err := registry.NewRegistry(cctx.Context, cfg.Discovery, dstore, lotusDiscoverer)
	if err != nil {
		return fmt.Errorf("cannot create provider registry: %s", err)
	}

	// Create finder HTTP server
	var finderSvr *httpfinderserver.Server
	if cfg.Addresses.Finder != "none" && !cctx.Bool("nofinder") {
		maddr, err := multiaddr.NewMultiaddr(cfg.Addresses.Finder)
		if err != nil {
			return fmt.Errorf("bad finder address in config %s: %s", cfg.Addresses.Finder, err)
		}
		finderAddr, err := manet.ToNetAddr(maddr)
		if err != nil {
			return err
		}
		finderSvr, err = httpfinderserver.New(finderAddr.String(), indexerCore, reg)
		if err != nil {
			return err
		}
	}

	var (
		cancelP2pServers context.CancelFunc
		ingester         *ingest.Ingester
		p2pHost          host.Host
	)

	// Create libp2p host and servers
	ctx, cancel := context.WithCancel(cctx.Context)
	defer cancel()
	if cfg.Addresses.P2PAddr != "none" && !cctx.Bool("nop2p") {
		cancelP2pServers = cancel

		peerID, privKey, err := cfg.Identity.Decode()
		if err != nil {
			return err
		}
		p2pmaddr, err := multiaddr.NewMultiaddr(cfg.Addresses.P2PAddr)
		if err != nil {
			return fmt.Errorf("bad p2p address in config %s: %s", cfg.Addresses.P2PAddr, err)
		}
		p2pOpts := []libp2p.Option{
			// Use the keypair generated during init
			libp2p.Identity(privKey),
			// Listen at specific address
			libp2p.ListenAddrs(p2pmaddr),
		}
		if cfg.Addresses.NoResourceManager {
			log.Info("libp2p resource manager disabled")
			p2pOpts = append(p2pOpts, libp2p.ResourceManager(network.NullResourceManager))
		}

		p2pHost, err = libp2p.New(p2pOpts...)
		if err != nil {
			return err
		}

		if finderSvr != nil {
			p2pfinderserver.New(ctx, p2pHost, indexerCore, reg)
		}

		// Initialize ingester.
		ingester, err = ingest.NewIngester(cfg.Ingest, p2pHost, indexerCore, reg, dstore)
		if err != nil {
			return err
		}

		// If there are bootstrap peers and bootstrapping is enabled, then try to
		// connect to the minimum set of peers.  This connects the indexer to other
		// nodes in the gossip mesh, allowing it to receive advertisements from
		// providers.
		if len(cfg.Bootstrap.Peers) != 0 && cfg.Bootstrap.MinimumPeers != 0 {
			addrs, err := cfg.Bootstrap.PeerAddrs()
			if err != nil {
				return fmt.Errorf("bad bootstrap peer: %s", err)
			}

			bootCfg := bootstrap.BootstrapConfigWithPeers(addrs)
			bootCfg.MinPeerThreshold = cfg.Bootstrap.MinimumPeers

			bootstrapper, err := bootstrap.Bootstrap(peerID, p2pHost, nil, bootCfg)
			if err != nil {
				return fmt.Errorf("bootstrap failed: %s", err)
			}
			defer bootstrapper.Close()
		}

		log.Infow("libp2p servers initialized", "host_id", p2pHost.ID(), "multiaddr", p2pmaddr)
	}

	// Create ingest HTTP server
	var ingestSvr *httpingestserver.Server
	if cfg.Addresses.Ingest != "none" && !cctx.Bool("noingest") {
		maddr, err := multiaddr.NewMultiaddr(cfg.Addresses.Ingest)
		if err != nil {
			return fmt.Errorf("bad ingest address in config %s: %s", cfg.Addresses.Ingest, err)
		}
		ingestAddr, err := manet.ToNetAddr(maddr)
		if err != nil {
			return err
		}
		ingestSvr, err = httpingestserver.New(ingestAddr.String(), indexerCore, ingester, reg)
		if err != nil {
			return err
		}
		if cfg.Addresses.P2PAddr != "none" && !cctx.Bool("nop2p") {
			p2pingestserver.New(ctx, p2pHost, indexerCore, ingester, reg)
		}
	}

	reloadErrChan := make(chan chan error, 1)

	// Create admin HTTP server
	var adminSvr *httpadminserver.Server
	if cfg.Addresses.Admin != "" && !cctx.Bool("noadmin") {
		maddr, err := multiaddr.NewMultiaddr(cfg.Addresses.Admin)
		if err != nil {
			return fmt.Errorf("bad admin address in config %s: %s", cfg.Addresses.Admin, err)
		}
		adminAddr, err := manet.ToNetAddr(maddr)
		if err != nil {
			return err
		}
		adminSvr, err = httpadminserver.New(adminAddr.String(), indexerCore, ingester, reg, reloadErrChan)
		if err != nil {
			return err
		}
	}

	log.Info("Starting http servers")
	errChan := make(chan error, 3)
	if adminSvr != nil {
		go func() {
			errChan <- adminSvr.Start()
		}()
		fmt.Println("Admin server:\t", cfg.Addresses.Admin)
	} else {
		fmt.Println("Admin server:\t disabled")
	}
	if finderSvr != nil {
		go func() {
			errChan <- finderSvr.Start()
		}()
		fmt.Println("Finder server:\t", cfg.Addresses.Finder)
	} else {
		fmt.Println("Finder server:\t disabled")
	}
	if ingestSvr != nil {
		go func() {
			errChan <- ingestSvr.Start()
		}()
		fmt.Println("Ingest server:\t", cfg.Addresses.Ingest)
	} else {
		fmt.Println("Ingest server:\t disabled")
	}

	reloadSig := make(chan os.Signal, 1)
	signal.Notify(reloadSig, syscall.SIGHUP)

	// Output message to user (not to log).
	fmt.Println("Indexer is ready")

	var cfgPath string
	if cctx.Bool("watch-config") {
		cfgPath, err = config.Filename("")
		if err != nil {
			log.Errorw("Cannot get config file name", "err", err)
		}
	}

	var finalErr, statErr error
	var modTime time.Time
	var ticker *time.Ticker
	var timeChan <-chan time.Time

	if cfgPath != "" {
		modTime, _, statErr = fileChanged(cfgPath, modTime)
		if statErr != nil {
			log.Error(err)
		}
		ticker = time.NewTicker(configCheckInterval)
		timeChan = ticker.C
	}

	for endDaemon := false; !endDaemon; {
		select {
		case <-cctx.Done():
			// Command was canceled (ctrl-c)
			endDaemon = true
		case err = <-errChan:
			log.Errorw("Failed to start server", "err", err)
			finalErr = ErrDaemonStart
			endDaemon = true
		case <-reloadSig:
			reloadErrChan <- nil
		case errChan := <-reloadErrChan:
			err = reloadConfig("", ingester, reg)
			if err != nil {
				log.Errorw("Error reloading conifg", "err", err)
				if errChan != nil {
					err = errors.New("could not reload configuration")
				}
			}
			if errChan != nil {
				errChan <- err
			}
		case <-timeChan:
			var changed bool
			modTime, changed, err = fileChanged(cfgPath, modTime)
			if err != nil {
				if statErr == nil {
					log.Errorw("Cannot stat config file", "err", err, "path", cfgPath)
					statErr = err
				}
				continue
			}
			statErr = nil
			if changed {
				reloadErrChan <- nil
			}
		}
	}
	if ticker != nil {
		ticker.Stop()
	}

	log.Infow("Shutting down daemon")

	ctx, cancel = context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	go func() {
		// Wait for context to be canceled.  If timeout, then exit with error.
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("Timed out on shutdown, terminating...")
			os.Exit(-1)
		}
	}()

	if cancelP2pServers != nil {
		cancelP2pServers()
	}

	if ingestSvr != nil {
		if err = ingestSvr.Shutdown(ctx); err != nil {
			log.Errorw("Error shutting down ingest server", "err", err)
			finalErr = ErrDaemonStop
		}
	}
	if finderSvr != nil {
		if err = finderSvr.Shutdown(ctx); err != nil {
			log.Errorw("Error shutting down finder server", "err", err)
			finalErr = ErrDaemonStop
		}
	}
	if adminSvr != nil {
		if err = adminSvr.Shutdown(ctx); err != nil {
			log.Errorw("Error shutting down admin server", "err", err)
			finalErr = ErrDaemonStop
		}
	}

	// If ingester set, close ingester
	if ingester != nil {
		if err = ingester.Close(); err != nil {
			log.Errorw("Error closing ingester", "err", err)
			finalErr = ErrDaemonStop
		}
	}

	if err = valueStore.Close(); err != nil {
		log.Errorw("Error closing value store", "err", err)
		finalErr = ErrDaemonStop
	}

	cancel()

	log.Info("Indexer stopped")
	return finalErr
}

func fileChanged(filePath string, modTime time.Time) (time.Time, bool, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return modTime, false, fmt.Errorf("cannot stat config file: %w", err)
	}
	if fi.ModTime() != modTime {
		return fi.ModTime(), true, nil
	}
	return modTime, false, nil
}

func createValueStore(cfgIndexer config.Indexer) (indexer.Interface, error) {
	dir, err := config.Path("", cfgIndexer.ValueStoreDir)
	if err != nil {
		return nil, err
	}
	log.Infow("Valuestore initializing/opening", "type", cfgIndexer.ValueStoreType, "path", dir)

	if err = checkWritable(dir); err != nil {
		return nil, err
	}

	switch cfgIndexer.ValueStoreType {
	case vstoreStorethehash:
		return storethehash.New(dir, storethehash.GCInterval(time.Duration(cfgIndexer.GCInterval)))
	case vstorePogreb:
		return pogreb.New(dir)
	case vstoreMemory:
		return memory.New(), nil
	}

	return nil, fmt.Errorf("unrecognized store type: %s", cfgIndexer.ValueStoreType)
}

func loadConfig(filePath string) (*config.Config, error) {
	cfg, err := config.Load(filePath)
	if err != nil {
		return nil, fmt.Errorf("cannot load config file: %w", err)
	}
	if cfg.Version != config.Version {
		log.Warn("Configuration file out-of-date. Upgrade by running: ./storetheindex init --upgrade")
	}

	if cfg.Datastore.Type != "levelds" {
		return nil, fmt.Errorf("only levelds datastore type supported, %q not supported", cfg.Datastore.Type)
	}

	return cfg, nil
}

func reloadConfig(filePath string, ingester *ingest.Ingester, reg *registry.Registry) error {
	cfg, err := loadConfig(filePath)
	if err != nil {
		return err
	}

	err = reg.SetPolicy(cfg.Discovery.Policy)
	if err != nil {
		return fmt.Errorf("failed to set policy config: %w", err)
	}

	if ingester != nil {
		err = ingester.SetRateLimit(cfg.Ingest.RateLimit)
		if err != nil {
			return fmt.Errorf("failed to set rate limit config: %w", err)
		}
		ingester.SetBatchSize(cfg.Ingest.StoreBatchSize)
		ingester.RunWorkers(cfg.Ingest.IngestWorkerCount)
	}

	fmt.Println("Reloaded policy and rate limit configuration")
	return nil
}
