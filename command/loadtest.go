package command

import (
	"github.com/urfave/cli/v2"
)

var LoadtestCmd = &cli.Command{
	Name:  "loadtest",
	Usage: "Perform load testing with an indexer",
	Subcommands: []*cli.Command{
		loadGenCmd,
		loadGenVerifyCmd,
	},
}
