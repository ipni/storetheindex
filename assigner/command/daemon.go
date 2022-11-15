package command

import (
	"errors"
	"fmt"
	"os"

	"github.com/filecoin-project/storetheindex/assigner/config"
	"github.com/filecoin-project/storetheindex/assigner/core"
	server "github.com/filecoin-project/storetheindex/assigner/server"
	sticfg "github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/mautil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/kubo/core/bootstrap"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("assigner")

const progName = "assigner"

var DaemonCmd = &cli.Command{
	Name:   "daemon",
	Usage:  "Start daemon",
	Flags:  daemonFlags,
	Action: daemonCommand,
}

func daemonCommand(cctx *cli.Context) error {
	cfg, err := loadConfig("")
	if err != nil {
		if errors.Is(err, sticfg.ErrNotInitialized) {
			fmt.Fprintf(os.Stderr, "%s is not initialized\n", progName)
			fmt.Fprintf(os.Stderr, "To initialize, run the command: ./%s init\n", progName)
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

	var p2pHost host.Host
	p2pAddr := cfg.Daemon.P2PAddr
	if cctx.String("listen-p2p") != "" {
		p2pAddr = cctx.String("listen-p2p")
	}
	if p2pAddr != "none" {
		peerID, privKey, err := cfg.Identity.Decode()
		if err != nil {
			return err
		}
		p2pmaddr, err := multiaddr.NewMultiaddr(p2pAddr)
		if err != nil {
			return fmt.Errorf("bad p2p address %s: %w", p2pAddr, err)
		}
		p2pOpts := []libp2p.Option{
			// Use the keypair generated during init
			libp2p.Identity(privKey),
			// Listen at specific address
			libp2p.ListenAddrs(p2pmaddr),
		}
		if cfg.Daemon.NoResourceManager {
			log.Info("libp2p resource manager disabled")
			p2pOpts = append(p2pOpts, libp2p.ResourceManager(network.NullResourceManager))
		}

		p2pHost, err = libp2p.New(p2pOpts...)
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

	assigner, err := core.NewAssigner(cctx.Context, cfg.Assignment, p2pHost)
	if err != nil {
		return err
	}

	// Create HTTP server
	var httpServer *server.Server
	httpAddr := cfg.Daemon.HTTPAddr
	if cctx.String("listen-http") != "" {
		httpAddr = cctx.String("listen-http")
	}
	if httpAddr != "none" {
		httpNetAddr, err := mautil.MultiaddrStringToNetAddr(httpAddr)
		if err != nil {
			return fmt.Errorf("bad http address %s: %w", httpAddr, err)
		}

		httpServer, err = server.New(httpNetAddr.String(), assigner)
		if err != nil {
			return err
		}
	}

	svrErrChan := make(chan error, 3)

	log.Info("Starting http servers")
	if httpServer != nil {
		go func() {
			svrErrChan <- httpServer.Start()
		}()
		fmt.Println("http server:\t", httpAddr)
	} else {
		fmt.Println("http server:\t disabled")
	}

	// Output message to user (not to log).
	fmt.Println("Daemon is ready")
	var finalErr error

	for endDaemon := false; !endDaemon; {
		select {
		case <-cctx.Done():
			// Command was canceled (ctrl-c)
			endDaemon = true
		case err = <-svrErrChan:
			finalErr = fmt.Errorf("failed to start server: %w", err)
			endDaemon = true
		}
	}

	log.Infow("Shutting down daemon")

	if httpServer != nil {
		if err = httpServer.Close(); err != nil {
			finalErr = fmt.Errorf("error shutting down http server: %w", err)
		}
	}

	if err = assigner.Close(); err != nil {
		finalErr = fmt.Errorf("error closing assigner: %w", err)
	}

	log.Info("Daemon stopped")
	return finalErr
}

func setLoggingConfig(cfgLogging config.Logging) error {
	// Set overall log level.
	err := logging.SetLogLevel("*", cfgLogging.Level)
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
