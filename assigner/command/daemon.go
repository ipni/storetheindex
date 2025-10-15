package command

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ipfs/boxo/bootstrap"
	"github.com/ipfs/boxo/peering"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/mautil"
	"github.com/ipni/storetheindex/assigner/config"
	"github.com/ipni/storetheindex/assigner/core"
	server "github.com/ipni/storetheindex/assigner/server"
	sticfg "github.com/ipni/storetheindex/config"
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
	Usage:  "Start assigner service daemon",
	Flags:  daemonFlags,
	Action: daemonAction,
}

var daemonFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "listen-admin",
		Usage:    "Admin HTTP API listen address",
		EnvVars:  []string{"ASSIGNER_LISTEN_ADMIN"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-http",
		Usage:    "HTTP listen address",
		EnvVars:  []string{"ASSIGNER_LISTEN_HTTP"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-p2p",
		Usage:    "P2P listen address",
		EnvVars:  []string{"ASSIGNER_LISTEN_P2P"},
		Required: false,
	},
}

func daemonAction(cctx *cli.Context) error {
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
		_, privKey, err := cfg.Identity.Decode()
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
			p2pOpts = append(p2pOpts, libp2p.ResourceManager(&network.NullResourceManager{}))
		}

		p2pHost, err = libp2p.New(p2pOpts...)
		if err != nil {
			return err
		}
		defer p2pHost.Close()

		bootstrapper, err := startBootstrapper(cfg.Bootstrap, p2pHost)
		if err != nil {
			return fmt.Errorf("cannot start bootstrapper: %s", err)
		}
		if bootstrapper != nil {
			defer bootstrapper.Close()
		}

		peeringService, err := startPeering(cfg.Peering, p2pHost)
		if err != nil {
			return fmt.Errorf("cannot start peering service: %s", err)
		}
		if peeringService != nil {
			defer peeringService.Stop()
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

		httpServer, err = server.New(httpNetAddr.String(), assigner,
			server.WithVersion(cctx.App.Version))
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
			if errors.Is(err, logging.ErrNoSuchLogger) {
				log.Warnf("Ignoring configuration for nonexistent logger: %s", loggerName)
				continue
			}
			return fmt.Errorf("failed to set log level %q for logger %q: %s", level, loggerName, err)
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

func startBootstrapper(cfg sticfg.Bootstrap, p2pHost host.Host) (io.Closer, error) {
	// If there are bootstrap peers and bootstrapping is enabled, then try to
	// connect to the minimum set of peers.  This connects the indexer to other
	// nodes in the gossip mesh, allowing it to receive advertisements from
	// providers.
	if len(cfg.Peers) == 0 || cfg.MinimumPeers == 0 {
		return nil, nil
	}
	addrs, err := cfg.PeerAddrs()
	if err != nil {
		return nil, fmt.Errorf("bad bootstrap peer: %s", err)
	}

	bootCfg := bootstrap.BootstrapConfigWithPeers(addrs)
	bootCfg.MinPeerThreshold = cfg.MinimumPeers

	return bootstrap.Bootstrap(p2pHost.ID(), p2pHost, nil, bootCfg)
}

func startPeering(cfg sticfg.Peering, p2pHost host.Host) (*peering.PeeringService, error) {
	if len(cfg.Peers) == 0 {
		return nil, nil
	}

	curPeers, err := cfg.PeerAddrs()
	if err != nil {
		return nil, fmt.Errorf("bad peering peer: %s", err)
	}

	peeringService := peering.NewPeeringService(p2pHost)
	for i := range curPeers {
		peeringService.AddPeer(curPeers[i])
	}
	if err = peeringService.Start(); err != nil {
		return nil, err
	}
	return peeringService, nil
}
