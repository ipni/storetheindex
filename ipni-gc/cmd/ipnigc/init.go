package main

import (
	"fmt"

	"github.com/ipni/storetheindex/ipni-gc/config"
	"github.com/urfave/cli/v2"
)

var initCmd = &cli.Command{
	Name:   "init",
	Usage:  "Initialize or upgrade config file",
	Flags:  initFlags,
	Action: initAction,
}

var initFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:     "upgrade",
		Usage:    "Upgrade the config file to the current version, saving the old config as config.prev, and ignoring other flags ",
		Aliases:  []string{"u"},
		Required: false,
	},
}

func initAction(cctx *cli.Context) error {
	_, err := config.Init("", cctx.Bool("upgrade"))
	if err != nil {
		return fmt.Errorf("failed to initialize gc config: %w", err)
	}
	return nil
}
