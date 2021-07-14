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
	&cli.Int64Flag{
		Name:     "cachesize",
		Usage:    "Maximum number of CIDs that cache can hold",
		Aliases:  []string{"c"},
		EnvVars:  []string{"CACHE_SIZE"},
		Value:    100000,
		Required: false,
	},
	&cli.StringFlag{
		Name:     "storage",
		Usage:    "Type of persistent storage (none, sth, pogreb)",
		Aliases:  []string{"s"},
		EnvVars:  []string{"STORAGE_TYPE"},
		Required: true,
	},
	&cli.StringFlag{
		Name:     "dir",
		Usage:    "Directory for persistent storage, default: ~/.storetheindex",
		Aliases:  []string{"d"},
		Required: false,
	},
	EndpointFlag,
}

var ClientCmdFlags = []cli.Flag{
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
		Name:     "metadata",
		Usage:    "Bytes of opaque metadata corresponding to protocol 0",
		Aliases:  []string{"m"},
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
	&cli.Int64Flag{
		Name:     "num",
		Usage:    "Number of entries to generate",
		Aliases:  []string{"n"},
		Required: false,
	},
	&cli.Int64Flag{
		Name:     "size",
		Usage:    "Total size of the CIDs to generate",
		Aliases:  []string{"s"},
		Required: false,
	}}
