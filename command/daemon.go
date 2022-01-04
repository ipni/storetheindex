package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store/memory"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/storetheindex/config"
	legingest "github.com/filecoin-project/storetheindex/internal/ingest"
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
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
)

// shutdownTimeout is the duration that a graceful shutdown has to complete.
const shutdownTimeout = 5 * time.Second

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
	err := logging.SetLogLevel("*", cctx.String("log-level"))
	if err != nil {
		return err
	}

	cfg, err := config.Load("")
	if err != nil {
		if err == config.ErrNotInitialized {
			fmt.Fprintln(os.Stderr, "storetheindex is not initialized")
			fmt.Fprintln(os.Stderr, "To initialize, run the command: ./storetheindex init")
			os.Exit(1)
		}
		return fmt.Errorf("cannot load config file: %w", err)
	}

	if cfg.Datastore.Type != "levelds" {
		return fmt.Errorf("only levelds datastore type supported, %q not supported", cfg.Datastore.Type)
	}

	// Create a valuestore of the configured type.
	valueStorePath, err := config.Path("", cfg.Indexer.ValueStoreDir)
	if err != nil {
		return err
	}
	log.Infow("Valuestore initializing/opening", "type", cfg.Indexer.ValueStoreType, "path", valueStorePath)
	valueStore, err := createValueStore(valueStorePath, cfg.Indexer.ValueStoreType)
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
	if cfg.Discovery.LotusGateway != "" {
		log.Infow("discovery using lotus", "gateway", cfg.Discovery.LotusGateway)
		// Create lotus client
		lotusDiscoverer, err = lotus.NewDiscoverer(cfg.Discovery.LotusGateway)
		if err != nil {
			return fmt.Errorf("cannot create lotus client: %s", err)
		}
	}

	// Create registry
	reg, err := registry.NewRegistry(cfg.Discovery, dstore, lotusDiscoverer)
	if err != nil {
		return fmt.Errorf("cannot create provider registry: %s", err)
	}

	// Create finder HTTP server
	maddr, err := multiaddr.NewMultiaddr(cfg.Addresses.Finder)
	if err != nil {
		return fmt.Errorf("bad finder address in config %s: %s", cfg.Addresses.Finder, err)
	}
	finderAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return err
	}
	finderSvr, err := httpfinderserver.New(finderAddr.String(), indexerCore, reg)
	if err != nil {
		return err
	}

	// Create ingest HTTP server
	maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Ingest)
	if err != nil {
		return fmt.Errorf("bad ingest address in config %s: %s", cfg.Addresses.Ingest, err)
	}
	ingestAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return err
	}
	ingestSvr, err := httpingestserver.New(ingestAddr.String(), indexerCore, reg)
	if err != nil {
		return err
	}

	var (
		cancelP2pServers context.CancelFunc
		ingester         *legingest.Ingester
	)
	// Create libp2p host and servers
	if !cfg.Addresses.DisableP2P && !cctx.Bool("nop2p") {
		ctx, cancel := context.WithCancel(cctx.Context)
		defer cancel()
		cancelP2pServers = cancel

		peerID, privKey, err := cfg.Identity.Decode()
		if err != nil {
			return err
		}
		p2pmaddr, err := multiaddr.NewMultiaddr(cfg.Addresses.P2PAddr)
		if err != nil {
			return fmt.Errorf("bad p2p address in config %s: %s", cfg.Addresses.P2PAddr, err)
		}
		p2pHost, err := libp2p.New(ctx,
			// Use the keypair generated during init
			libp2p.Identity(privKey),
			// Listen at specific address
			libp2p.ListenAddrs(p2pmaddr),
		)
		if err != nil {
			return err
		}

		p2pfinderserver.New(ctx, p2pHost, indexerCore, reg)
		p2pingestserver.New(ctx, p2pHost, indexerCore, reg)

		// Initialize ingester.
		ingester, err = legingest.NewIngester(cfg.Ingest, p2pHost, indexerCore, reg, dstore)
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

	// Create admin HTTP server
	maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Admin)
	if err != nil {
		return fmt.Errorf("bad admin address in config %s: %s", cfg.Addresses.Admin, err)
	}
	adminAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return err
	}
	adminSvr, err := httpadminserver.New(adminAddr.String(), indexerCore, ingester, reg)
	if err != nil {
		return err
	}

	log.Info("Starting http servers")
	errChan := make(chan error, 3)
	go func() {
		errChan <- adminSvr.Start()
	}()
	go func() {
		errChan <- finderSvr.Start()
	}()
	go func() {
		errChan <- ingestSvr.Start()
	}()

	// Out message to user (not to log).
	fmt.Println("Finder server:\t", cfg.Addresses.Finder)
	fmt.Println("Ingest server:\t", cfg.Addresses.Ingest)
	fmt.Println("Admin server:\t", cfg.Addresses.Admin)
	fmt.Println("Indexer is ready")

	var finalErr error
	select {
	case <-cctx.Done():
		// Command was canceled (ctrl-c)
	case err = <-errChan:
		log.Errorw("Failed to start server", "err", err)
		finalErr = ErrDaemonStart
	}

	log.Infow("Shutting down daemon")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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

	if err = ingestSvr.Shutdown(ctx); err != nil {
		log.Errorw("Error shutting down ingest server", "err", err)
		finalErr = ErrDaemonStop
	}
	if err = finderSvr.Shutdown(ctx); err != nil {
		log.Errorw("Error shutting down finder server", "err", err)
		finalErr = ErrDaemonStop
	}
	if err = adminSvr.Shutdown(ctx); err != nil {
		log.Errorw("Error shutting down admin server", "err", err)
		finalErr = ErrDaemonStop
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

func createValueStore(dir, storeType string) (indexer.Interface, error) {
	err := checkWritable(dir)
	if err != nil {
		return nil, err
	}

	switch storeType {
	case vstoreStorethehash:
		return storethehash.New(dir)
	case vstorePogreb:
		return pogreb.New(dir)
	case vstoreMemory:
		return memory.New(), nil
	}

	return nil, fmt.Errorf("unrecognized store type: %s", storeType)
}
