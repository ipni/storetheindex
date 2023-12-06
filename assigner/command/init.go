package command

import (
	"fmt"
	"os"

	"github.com/ipni/storetheindex/assigner/config"
	sticfg "github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/urfave/cli/v2"
)

var InitCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize or upgrade config file",
	Flags:  initFlags,
	Action: initAction,
}

var initFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:     "no-bootstrap",
		Usage:    "Do not configure bootstrap peers",
		EnvVars:  []string{"NO_BOOTSTRAP"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "pubsub-topic",
		Usage:    "Subscribe to this pubsub topic to receive advertisement notification",
		EnvVars:  []string{"ASSIGNER_PUBSUB_TOPIC"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "upgrade",
		Usage:    "Upgrade the config file to the current version, saving the old config as config.prev, and ignoring other flags ",
		Aliases:  []string{"u"},
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

	fmt.Println("Initializing", progName, "at", configRoot)

	if fsutil.FileExists(configFile) {
		return sticfg.ErrInitialized
	}

	cfg, err := config.Init(os.Stderr)
	if err != nil {
		return err
	}

	noBootstrap := cctx.Bool("no-bootstrap")
	if noBootstrap {
		cfg.Bootstrap.Peers = []string{}
		cfg.Bootstrap.MinimumPeers = 0
	}

	topic := cctx.String("pubsub-topic")
	if topic != "" {
		cfg.Assignment.PubSubTopic = topic
	}

	return cfg.Save(configFile)
}
