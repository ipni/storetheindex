package commands

import (
	"context"

	"github.com/urfave/cli/v2"
)

var importCidList = &cli.Command{
	Name:   "cidList",
	Usage:  "Import indexer data from cidList",
	Flags:  ImportFlags,
	Action: importListCmd,
}

var importCar = &cli.Command{
	Name:   "car",
	Usage:  "Import indexer data from car",
	Flags:  ImportFlags,
	Action: importCarCmd, //
}
var ImportCmd = &cli.Command{
	Name:  "import",
	Usage: "Imports data to indexer from different sources",
	Subcommands: []*cli.Command{
		importCidList,
		importCar,
	},
}

func importListCmd(c *cli.Context) error {
	_, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	log.Errorw("Importing from cidList not implemented yet")

	return nil

}

func importCarCmd(c *cli.Context) error {
	_, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	log.Errorw("Importing from cidList not implemented yet")

	return nil

}
