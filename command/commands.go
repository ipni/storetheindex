package command

import (
	_ "embed"
	"fmt"

	"github.com/urfave/cli/v2"
)

//go:embed tree.txt
var commandTree string

var CommandsCmd = &cli.Command{
	Name:  "commands",
	Usage: "Print tree of commands and subcommands",
	Action: func(_ *cli.Context) error {
		fmt.Println(commandTree)
		return nil
	},
}
