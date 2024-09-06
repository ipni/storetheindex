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

var listenAdminFlag = &cli.StringFlag{
	Name:     "listen-admin",
	Usage:    "Admin HTTP API listen address or 'none' to disable, overrides config",
	EnvVars:  []string{"STORETHEINDEX_LISTEN_ADMIN"},
	Required: false,
}
var listenFindFlag = &cli.StringFlag{
	Name:     "listen-finder",
	Usage:    "HTTP listen address or 'none' to disable, overrides config",
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
	case "find":
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
