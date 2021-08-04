package command

import (
	_ "github.com/lib/pq"
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

var FinderEndpointFlag = altsrc.NewStringFlag(&cli.StringFlag{
	Name:     "finder_ep",
	Usage:    "Finder HTTP API endpoint",
	Aliases:  []string{"fep"},
	EnvVars:  []string{"FINDER_ENDPOINT"},
	Required: false,
	Value:    "127.0.0.0:3000",
})

var AdminEndpointFlag = altsrc.NewStringFlag(&cli.StringFlag{
	Name:     "admin_ep",
	Usage:    "Admin HTTP API endpoint",
	Aliases:  []string{"aep"},
	EnvVars:  []string{"ADMIN_ENDPOINT"},
	Required: false,
	Value:    "127.0.0.0:3001",
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
		Value:    "sth",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "dir",
		Usage:    "Directory for persistent storage, default: ~/.storetheindex",
		Aliases:  []string{"d"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "enablep2p",
		Usage:    "Enable libp2p client api for indexer",
		Aliases:  []string{"p2p"},
		Value:    false,
		Required: false,
	},
	FinderEndpointFlag,
	AdminEndpointFlag,
}

var ClientCmdFlags = []cli.Flag{
	FinderEndpointFlag,
	&cli.StringFlag{
		Name:     "protocol",
		Usage:    "Protocol to query the indexer (http, libp2p currently supported)",
		Aliases:  []string{"proto"},
		Value:    "http",
		Required: false,
	},
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
	AdminEndpointFlag,
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
