package command

import (
	"github.com/urfave/cli/v2"
	"github.com/urfave/cli/v2/altsrc"
)

var indexerHostFlag = altsrc.NewStringFlag(&cli.StringFlag{
	Name:     "indexer",
	Usage:    "Host or host:port of indexer to use",
	EnvVars:  []string{"INDEXER"},
	Aliases:  []string{"i"},
	Required: false,
	Value:    "localhost",
})

var cacheSizeFlag = &cli.Int64Flag{
	Name:     "cachesize",
	Usage:    "Maximum number of multihashes that result cache can hold, 0 to disable cache",
	Required: false,
	Value:    -1,
}

var fileFlag = &cli.StringFlag{
	Name:     "file",
	Usage:    "Source file for import",
	Aliases:  []string{"f"},
	Required: true,
}

var logLevelFlag = &cli.StringFlag{
	Name:     "log-level",
	Usage:    "Set the log level",
	EnvVars:  []string{"GOLOG_LOG_LEVEL"},
	Value:    "info",
	Required: false,
}

var providerFlag = &cli.StringFlag{
	Name:     "provider",
	Usage:    "Provider's peer ID",
	Aliases:  []string{"p"},
	Required: true,
}

var daemonFlags = []cli.Flag{
	cacheSizeFlag,
	logLevelFlag,
	&cli.BoolFlag{
		Name:     "log-all",
		Usage:    "Log for subsystems that are normally only logged at warn level",
		EnvVars:  []string{"LOG_ALL"},
		Value:    false,
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "noadmin",
		Usage:    "Disable admin server",
		Value:    false,
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "noingest",
		Usage:    "Disable ingest server (register, discover, single item ingest)",
		Value:    false,
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "nofinder",
		Usage:    "Disable finder server",
		Value:    false,
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "nop2p",
		Usage:    "Disable libp2p hosting indexer",
		Value:    false,
		Required: false,
	},
}

var findFlags = []cli.Flag{
	&cli.StringSliceFlag{
		Name:     "mh",
		Usage:    "Specify multihash to use as indexer key, multiple OK",
		Required: false,
	},
	&cli.StringSliceFlag{
		Name:     "cid",
		Usage:    "Specify CID to use as indexer key, multiple OK",
		Required: false,
	},
	indexerHostFlag,
	&cli.StringFlag{
		Name:     "indexerid",
		Usage:    "Indexer peer ID to use when protocol=libp2p",
		Aliases:  []string{"iid"},
		EnvVars:  []string{"INDEXER_ID"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "protocol",
		Usage:    "Protocol to query the indexer (http, libp2p currently supported)",
		Value:    "http",
		Required: false,
	},
}

var importFlags = []cli.Flag{
	providerFlag,
	&cli.StringFlag{
		Name:     "ctxid",
		Usage:    "Context ID of data imported",
		Aliases:  []string{"c"},
		Required: true,
	},
	&cli.StringFlag{
		Name:     "metadata",
		Usage:    "Bytes of opaque metadata corresponding to protocol 0",
		Aliases:  []string{"m"},
		Required: false,
	},
	fileFlag,
	indexerHostFlag,
}

var adminPolicyFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "peer",
		Usage:    "Peer ID of publisher or provider to allow or block",
		Aliases:  []string{"p"},
		Required: true,
	},
	indexerHostFlag,
}

var adminReloadPolicyFlags = []cli.Flag{
	indexerHostFlag,
}

var adminSyncFlags = []cli.Flag{
	indexerHostFlag,
	&cli.StringFlag{
		Name:     "pubid",
		Usage:    "Publisher peer ID",
		Aliases:  []string{"p"},
		Required: true,
	},
	&cli.StringFlag{
		Name:  "addr",
		Usage: "Multiaddr address of peer to sync with",
	},
	&cli.Int64Flag{
		Name:  "depth",
		Usage: "Depth limit of advertisements (distance from current) to sync. No limit if unspecified.",
	},
	&cli.BoolFlag{
		Name:  "resync",
		Usage: "Ignore the latest synced advertisement and sync advertisements as far back as the depth limit allows.",
		Value: false,
	},
}

var initFlags = []cli.Flag{
	cacheSizeFlag,
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
	&cli.BoolFlag{
		Name:     "no-bootstrap",
		Usage:    "Do not configure bootstrap peers",
		EnvVars:  []string{"NO_BOOTSTRAP"},
		Required: false,
	},
	&cli.StringFlag{
		Name:     "pubsub-topic",
		Usage:    "Subscribe to this pubsub topic to receive advertisement notification",
		EnvVars:  []string{"STORETHEINDE_PUBSUB_TOPIC"},
		Required: false,
	},
	&cli.BoolFlag{
		Name:     "upgrade",
		Usage:    "Upgrade the config file to the current version, saving the old config as config.prev, and ignoring other flags ",
		Aliases:  []string{"u"},
		Required: false,
	},
}

var providersGetFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "provid",
		Usage:    "Provider peer ID",
		Aliases:  []string{"p"},
		Required: true,
	},
	indexerHostFlag,
}

var providersListFlags = []cli.Flag{
	indexerHostFlag,
}

var registerFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "config",
		Usage:    "Config file containing provider's peer ID and private key",
		Required: true,
	},
	indexerHostFlag,
	&cli.StringSliceFlag{
		Name:     "provider-addr",
		Usage:    "Provider address as multiaddr string, example: \"/ip4/127.0.0.1/tcp/3102\"",
		Aliases:  []string{"pa"},
		Required: true,
	},
}

var syntheticFlags = []cli.Flag{
	fileFlag,
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
