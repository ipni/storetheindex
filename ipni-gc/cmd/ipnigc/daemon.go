package main

import (
	"errors"
	"time"

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
		Usage:    "Time to wait betrwwn gc for each provider",
		Aliases:  []string{"r"},
		Required: false,
		Value:    time.Hour,
	},
}

func daemonAction(cctx *cli.Context) error {
	return errors.New("daemon command not available yet")
}
