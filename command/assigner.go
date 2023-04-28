package command

import (
	"github.com/ipni/storetheindex/assigner/command"
	"github.com/urfave/cli/v2"
)

var AssignerCmd = &cli.Command{
	Name:        "assigner",
	Usage:       "Start a network indexer assigner service daemon",
	Description: "The assigner service is responsible for assigning content advertisement publishers to indexers.",
	Subcommands: []*cli.Command{
		command.DaemonCmd,
		command.InitCmd,
	},
}
