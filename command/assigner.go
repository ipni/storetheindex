package command

import (
	"github.com/filecoin-project/storetheindex/assigner/command"
	"github.com/urfave/cli/v2"
)

var AssignerCmd = &cli.Command{
	Name:  "assigner",
	Usage: "Assigner service",
	Subcommands: []*cli.Command{
		command.DaemonCmd,
		command.InitCmd,
	},
}
