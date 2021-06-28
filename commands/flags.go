package commands

import (
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("indexer-node")

var MockFlags []cli.Flag = []cli.Flag{
	altsrc.NewStringFlag(&cli.StringFlag{
		Name:    "mock",
		Usage:   "mocking flag to test flag operation",
		Aliases: []string{"mck"},
	}),
}

var EndpointFlag = altsrc.NewStringFlag(&cli.StringFlag{
	Name:     "endpoint",
	Usage:    "Node API endpoint",
	Aliases:  []string{"e"},
	EnvVars:  []string{"NODE_ENDPOINT"},
	Required: true,
})

var DirFlag = &cli.StringFlag{
	Name:     "dir",
	Usage:    "Source directory for import",
	Aliases:  []string{"d"},
	Required: true,
}
var DaemonFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    "persistence",
		Usage:   "Enable persistence storage",
		Aliases: []string{"p"},
		EnvVars: []string{"ENABLE_PERSISTENCE"},
		Value:   true,
	},
	EndpointFlag,
}

var ImportFlags = []cli.Flag{
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
	DirFlag,
	EndpointFlag,
}

var SyntheticFlags = []cli.Flag{
	DirFlag,
	&cli.StringFlag{
		Name:     "type",
		Usage:    "Type of synthetic load to generate (manifest, cidlist, car)",
		Aliases:  []string{"t"},
		Required: true,
	},
	&cli.IntFlag{
		Name:     "num",
		Usage:    "Number of entries to generate",
		Aliases:  []string{"n"},
		Value:    1000,
		Required: false,
	},
}
