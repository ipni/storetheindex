package command

import (
	"github.com/ipni/storetheindex/command/gc"
	"github.com/urfave/cli/v2"
)

var GCCmd = &cli.Command{
	Name:        "gc",
	Usage:       "IPNI indexer garbage collector",
	Description: "Remove deleted indexes from value store.",
	Subcommands: []*cli.Command{
		gc.DaemonCmd,
		gc.ProviderCmd,
	},
}
