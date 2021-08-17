package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/go-indexer-core/cache"
	"github.com/filecoin-project/go-indexer-core/cache/radixcache"
	"github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-indexer-core/store"
	"github.com/filecoin-project/go-indexer-core/store/pogreb"
	"github.com/filecoin-project/go-indexer-core/store/storethehash"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/lotus"
	"github.com/filecoin-project/storetheindex/internal/providers"
	adminserver "github.com/filecoin-project/storetheindex/server/admin"
	httpfinderserver "github.com/filecoin-project/storetheindex/server/finder/http"
	p2pfinderserver "github.com/filecoin-project/storetheindex/server/finder/libp2p"
	ingestserver "github.com/filecoin-project/storetheindex/server/ingest"
	"github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
)

// shutdownTimeout is the duration that a graceful shutdown has to complete
const shutdownTimeout = 5 * time.Second

var log = logging.Logger("command/storetheindex")

var (
	ErrDaemonStart = errors.New("daemon did not start correctly")
	ErrDaemonStop  = errors.New("daemon did not stop correctly")
)

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Start an indexer daemon, accepting http requests",
	Flags:  DaemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
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

	// Create value store
	valueStorePath, err := config.Path("", cfg.Indexer.ValueStoreDir)
	if err != nil {
		return err
	}
	valueStoreType := cfg.Indexer.ValueStoreType
	valueStore, err := createValueStore(valueStorePath, valueStoreType)
	if err != nil {
		return err
	}
	log.Infow("Value storage initialized", "type", valueStoreType, "path", valueStorePath)

	// Create result cache
	var resultCache cache.Interface
	cacheSize := int(cctx.Int64("cachesize"))
	if cacheSize < 0 {
		cacheSize = cfg.Indexer.CacheSize
	}
	if cacheSize > 0 {
		resultCache = radixcache.New(cacheSize)
		log.Infow("result cache enabled", "size", cacheSize)
	} else {
		log.Info("result cache disabled")
	}

	// Create indexer core
	indexerCore := engine.New(resultCache, valueStore)
	log.Infow("Indexer engine initialized")

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

	var lotusDiscovery *lotus.Discovery
	if cfg.Discovery.LotusGateway != "" {
		log.Infow("discovery using lotus", "gateway", cfg.Discovery.LotusGateway)
		// Create lotus client
		lotusDiscovery, err = lotus.SetupGateway(context.Background(), cfg.Discovery.LotusGateway)
		if err != nil {
			return fmt.Errorf("cannot create lotus client: %s", err)
		}
		defer lotusDiscovery.Close()
	}

	// Create registry
	registry, err := providers.NewRegistry(cfg.Discovery, dstore, lotusDiscovery)
	if err != nil {
		return fmt.Errorf("cannot create provider registry: %s", err)
	}

	// Create admin HTTP server
	maddr, err := multiaddr.NewMultiaddr(cfg.Addresses.Admin)
	if err != nil {
		return fmt.Errorf("bad admin address in config %s: %s", cfg.Addresses.Admin, err)
	}
	adminAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return err
	}
	adminSvr, err := adminserver.New(adminAddr.String(), indexerCore)
	if err != nil {
		return err
	}
	log.Infow("admin server initialized", "address", adminAddr)

	// Create finder HTTP server
	maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Finder)
	if err != nil {
		return fmt.Errorf("bad finder address in config %s: %s", cfg.Addresses.Finder, err)
	}
	finderAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return err
	}
	finderSvr, err := httpfinderserver.New(finderAddr.String(), indexerCore, registry)
	if err != nil {
		return err
	}
	log.Infow("finder server initialized", "address", finderAddr)

	// Create ingest HTTP server
	maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Ingest)
	if err != nil {
		return fmt.Errorf("bad ingest address in config %s: %s", cfg.Addresses.Ingest, err)
	}
	ingestAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return err
	}
	ingestSvr, err := ingestserver.New(ingestAddr.String(), indexerCore, registry)
	if err != nil {
		return err
	}
	log.Infow("ingest server initialized", "address", finderAddr)

	var (
		p2pSvr          *p2pfinderserver.Server
		cancelP2pFinder context.CancelFunc
	)
	// Create libp2p host and finder libp2p server
	if !cfg.Addresses.DisableP2P && !cctx.Bool("disablep2p") {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		privKey, err := cfg.Identity.DecodePrivateKey("")
		if err != nil {
			return err
		}
		// TODO: Do we want to the libp2p host to listen on any particular
		// addresss and port?
		p2pHost, err := libp2p.New(ctx,
			// Use the keypair generated during init
			libp2p.Identity(privKey),
		)
		if err != nil {
			return err
		}

		p2pSvr, err = p2pfinderserver.New(ctx, p2pHost, indexerCore, registry)
		if err != nil {
			return err
		}
		cancelP2pFinder = cancel
		log.Infow("Finder libp2p server  initialized", "host_id", p2pHost.ID())
	}

	log.Info("Starting daemon servers")
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

	if p2pSvr != nil {
		cancelP2pFinder()
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

	if err = valueStore.Close(); err != nil {
		log.Errorw("Error closing value store", "err", err)
		finalErr = ErrDaemonStop
	}
	cancel()

	log.Infow("node stopped")
	return finalErr
}

func createValueStore(dir, storeType string) (store.Interface, error) {
	err := checkWritable(dir)
	if err != nil {
		return nil, err
	}

	if storeType == "sth" {
		return storethehash.New(dir)
	}
	if storeType == "prgreb" {
		return pogreb.New(dir)
	}

	return nil, fmt.Errorf("unrecognized store type: %s", storeType)
}
