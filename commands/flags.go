package commands

import (
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("dealbot")

var MockFlags []cli.Flag = []cli.Flag{
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:    "mock",
		Usage:   "mocking flag to test flag operation",
		Aliases: []string{"mck"},
	}),
}

var DaemonFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    "persistence",
		Usage:   "Enable persistence storage",
		Aliases: []string{"p"},
		EnvVars: []string{"ENABLE_PERSISTENCE"},
		Value:   true,
	},
}

var ImportFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "dir",
		Usage:    "Source directory for import",
		Aliases:  []string{"d"},
		Required: true,
	},
	&cli.StringFlag{
		Name:     "provider",
		Usage:    "Provider of the data imported",
		Aliases:  []string{"prov"},
		Required: true,
	},
	&cli.StringFlag{
		Name:     "piece",
		Usage:    "Piece ID where the CIDs are sealed at provider",
		Aliases:  []string{"pc"},
		Required: false,
	},
}
