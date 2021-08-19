package command

import (
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

var FinderAddrFlag = altsrc.NewStringFlag(&cli.StringFlag{
	Name:     "finderaddr",
	Usage:    "Finder HTTP API address",
	Aliases:  []string{"fep"},
	EnvVars:  []string{"FINDER_ADDRESS"},
	Required: false,
	Value:    "127.0.0.0:3000",
})

var AdminAddrFlag = altsrc.NewStringFlag(&cli.StringFlag{
	Name:     "adminaddr",
	Usage:    "Admin HTTP API address",
	Aliases:  []string{"aep"},
	EnvVars:  []string{"ADMIN_ARRDESS"},
	Required: false,
	Value:    "127.0.0.0:3001",
})

var CacheSizeFlag = &cli.Int64Flag{
	Name:     "cachesize",
	Usage:    "Maximum number of CIDs that cache can hold, 0 to disable cache",
	Aliases:  []string{"c"},
	Required: false,
	Value:    -1,
}

var DirFlag = &cli.StringFlag{
	Name:     "dir",
	Usage:    "Source directory for import",
	Aliases:  []string{"d"},
	Required: true,
}

var DaemonFlags = []cli.Flag{
	CacheSizeFlag,
	&cli.BoolFlag{
		Name:     "disablep2p",
		Usage:    "Disable libp2p client api for indexer",
		Aliases:  []string{"nop2p"},
		Value:    false,
		Required: false,
	},
}

var ClientCmdFlags = []cli.Flag{
	FinderAddrFlag,
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
	AdminAddrFlag,
}

var InitFlags = []cli.Flag{
	CacheSizeFlag,
	&cli.StringFlag{
		Name:     "store",
		Usage:    "Type of value store (sth, pogreb). Default is \"sth\"",
		Aliases:  []string{"s"},
		EnvVars:  []string{"STORETHEINDEX_VALUE_STORE"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-admin",
		Usage:    "Admin HTTP API listen address",
		EnvVars:  []string{"STORETHEINDEX_LISTEN_ADMIN"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-finder",
		Usage:    "Finder HTTP API listen address",
		EnvVars:  []string{"STORETHEINDEX_LISTEN_FINDER"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "listen-ingest",
		Usage:    "Ingestion and discovery HTTP API listen address",
		EnvVars:  []string{"STORETHEINDEX_LISTEN_INGEST"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "lotus-gateway",
		Usage:    "Address for a lotus gateway to collect chain information",
		EnvVars:  []string{"STORETHEINDEX_LOTUS_GATEWAY"},
		Required: false,
	},
}

var RegisterFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "config",
		Usage:    "Config file containing provider's peer ID and private key",
		Required: true,
	},
	&cli.StringFlag{
		Name:     "indexer-addr",
		Usage:    "Address:port of indexer to register with",
		Required: false,
		Value:    "localhos:3001",
	},
	&cli.StringFlag{
		Name:     "provider-addr",
		Usage:    "Provider address as multiaddr string, example: \"/ip4/127.0.0.1/tcp/3333\"",
		Required: true,
	},
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
	},
}
