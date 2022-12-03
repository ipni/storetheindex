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
	Action: initCommand,
}

func initCommand(cctx *cli.Context) error {
	// Check that the config root exists and it writable.
	configRoot, err := config.PathRoot()
	if err != nil {
		return err
	}

	if err = fsutil.DirWritable(configRoot); err != nil {
		return err
	}

	configFile, err := config.Filename(configRoot)
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
