package command

import (
	"fmt"

	"github.com/ipni/storetheindex/config"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var indexerHostFlag = &cli.StringFlag{
	Name:     "indexer",
	Usage:    "Host or host:port of indexer to use",
	EnvVars:  []string{"INDEXER"},
	Aliases:  []string{"i"},
	Required: false,
}

var cacheSizeFlag = &cli.Int64Flag{
	Name:     "cachesize",
	Usage:    "Maximum number of multihashes that result cache can hold, -1 to disable cache",
	Required: false,
}

var fileFlag = &cli.StringFlag{
	Name:     "file",
	Usage:    "Source file for import",
	Aliases:  []string{"f"},
	Required: true,
}

var providerFlag = &cli.StringFlag{
	Name:     "provider",
	Usage:    "Provider's peer ID",
	Aliases:  []string{"p"},
	Required: true,
}

var listenAdminFlag = &cli.StringFlag{
	Name:     "listen-admin",
	Usage:    "Admin HTTP API listen address or 'none' to disable, overrides config",
	EnvVars:  []string{"STORETHEINDEX_LISTEN_ADMIN"},
	Required: false,
}
var listenFinderFlag = &cli.StringFlag{
	Name:     "listen-finder",
	Usage:    "Finder HTTP API listen address or 'none' to disable, overrides config",
	EnvVars:  []string{"STORETHEINDEX_LISTEN_FINDER"},
	Required: false,
}
var listenIngestFlag = &cli.StringFlag{
	Name:     "listen-ingest",
	Usage:    "Ingestion HTTP API listen address or 'none' to disable, overrides config",
	EnvVars:  []string{"STORETHEINDEX_LISTEN_INGEST"},
	Required: false,
}
var listenP2PFlag = &cli.StringFlag{
	Name:     "listen-p2p",
	Usage:    "P2P listen address or 'none' to disable, overrides config",
	EnvVars:  []string{"STORETHEINDEX_LISTEN_P2P"},
	Required: false,
}

var daemonFlags = []cli.Flag{
	cacheSizeFlag,
	listenAdminFlag,
	listenFinderFlag,
	listenIngestFlag,
	listenP2PFlag,
	&cli.BoolFlag{
		Name:     "watch-config",
		Usage:    "Watch for changes to config file and automatically reload",
		EnvVars:  []string{"STORETHEINDEX_WATCH_CONFIG"},
		Value:    true,
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

var adminFreezeFlags = []cli.Flag{
	indexerHostFlag,
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

var adminReloadConfigFlags = []cli.Flag{
	indexerHostFlag,
}

var adminListAssignedFlags = []cli.Flag{
	indexerHostFlag,
}

var adminListPreferredFlags = []cli.Flag{
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
		Usage: "Depth limit of advertisements (distance from current) to sync. No limit if -1. Unspecified or 0 defaults to indexer config.",
	},
	&cli.BoolFlag{
		Name:  "resync",
		Usage: "Ignore the latest synced advertisement and sync advertisements as far back as the depth limit allows.",
		Value: false,
	},
}

var initFlags = []cli.Flag{
	cacheSizeFlag,
	listenAdminFlag,
	listenFinderFlag,
	listenIngestFlag,
	listenP2PFlag,
	&cli.StringFlag{
		Name:     "store",
		Usage:    "Type of value store (pebble, sth). Default is \"pebble\"",
		Aliases:  []string{"s"},
		EnvVars:  []string{"STORETHEINDEX_VALUE_STORE"},
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
	&cli.BoolFlag{
		Name:     "use-assigner",
		Usage:    "Configure the indexer to work with an assigner service",
		Required: false,
	},
	&cli.StringFlag{
		Name:     "dhstore",
		Usage:    "Url of DHStore for double hashed index",
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

var importProvidersFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "from",
		Usage:    "Host or host:port of indexer to get providers from",
		Aliases:  []string{"f"},
		Required: true,
	},
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

var statusFlags = []cli.Flag{
	indexerHostFlag,
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

// cliIndexer reads the indexer host from CLI flag or from config.
func cliIndexer(cctx *cli.Context, addrType string) string {
	idxr := cctx.String("indexer")
	if idxr != "" {
		return idxr
	}

	idxr = indexerHost(addrType)
	if idxr != "" {
		return idxr
	}

	return "localhost"
}

func indexerHost(addrType string) string {
	// No indexer given on command line, get from config.
	cfg, err := config.Load("")
	if err != nil {
		return ""
	}
	if cfg.Addresses.Finder == "" {
		return ""
	}
	var maddr multiaddr.Multiaddr
	switch addrType {
	case "finder":
		maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Finder)
	case "admin":
		maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Admin)
	case "ingest":
		maddr, err = multiaddr.NewMultiaddr(cfg.Addresses.Ingest)
	default:
		return ""
	}
	if err != nil {
		return ""
	}
	return multiaddrHost(maddr)
}

func multiaddrHost(maddr multiaddr.Multiaddr) string {
	for _, proto := range []int{multiaddr.P_IP4, multiaddr.P_IP6} {
		addr, err := maddr.ValueForProtocol(proto)
		if err == nil {
			port, err := maddr.ValueForProtocol(multiaddr.P_TCP)
			if err == nil {
				addr = fmt.Sprint(addr, ":", port)
			}
			return addr
		}
	}
	return ""
}
