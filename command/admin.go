package command

import (
	"fmt"
	"net/url"

	httpclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var sync = &cli.Command{
	Name:   "sync",
	Usage:  "Sync indexer with provider",
	Flags:  adminSyncFlags,
	Action: syncCmd,
}

var allow = &cli.Command{
	Name:   "allow",
	Usage:  "Allow advertisements and content from peer",
	Flags:  adminPolicyFlags,
	Action: allowCmd,
}

var block = &cli.Command{
	Name:   "block",
	Usage:  "Block advertisements and content from peer",
	Flags:  adminPolicyFlags,
	Action: blockCmd,
}

var importProviders = &cli.Command{
	Name:   "import-providers",
	Usage:  "Import provider information from another indexer",
	Flags:  importProvidersFlags,
	Action: importProvidersCmd,
}

var listAssigned = &cli.Command{
	Name:   "list-assigned",
	Usage:  "List assigned peers when configured to work with assigner service",
	Flags:  adminListAssignedFlags,
	Action: listAssignedCmd,
}

var listPreferred = &cli.Command{
	Name:   "list-preferred",
	Usage:  "List unassigned peers that indexer has retrieved content from",
	Flags:  adminListPreferredFlags,
	Action: listPreferredCmd,
}

var reload = &cli.Command{
	Name:  "reload-config",
	Usage: "Reload various settings from the configuration file",
	Description: "Reloades the following portions of the config file:" +
		" Discovery.Policy," +
		" Indexer.ConfigCheckInterval," +
		" Indexer.ShutdownTimeout," +
		" Ingest.IngestWorkerCount," +
		" Ingest.RateLimit," +
		" Ingest.StoreBatchSize," +
		" Logging," +
		" Peering",
	Flags:  adminReloadConfigFlags,
	Action: reloadConfigCmd,
}

var AdminCmd = &cli.Command{
	Name:  "admin",
	Usage: "Perform admin activities with an indexer",
	Subcommands: []*cli.Command{
		allow,
		block,
		importProviders,
		listAssigned,
		listPreferred,
		reload,
		sync,
	},
}

func syncCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("pubid"))
	if err != nil {
		return err
	}
	var addr multiaddr.Multiaddr
	addrStr := cctx.String("addr")
	if addrStr != "" {
		addr, err = multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return err
		}
	}
	err = cl.Sync(cctx.Context, peerID, addr, cctx.Int64("depth"), cctx.Bool("resync"))
	if err != nil {
		return err
	}
	fmt.Println("Syncing request accepted. Come back later to check if syncing was successful")
	return nil
}

func allowCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("peer"))
	if err != nil {
		return err
	}
	err = cl.Allow(cctx.Context, peerID)
	if err != nil {
		return err
	}
	fmt.Println("Allowing advertisements and content from peer", peerID)
	return nil
}

func listAssignedCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	assigned, err := cl.ListAssignedPeers(cctx.Context)
	if err != nil {
		return err
	}
	for _, peerID := range assigned {
		fmt.Println(peerID)
	}
	return nil
}

func listPreferredCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	assigned, err := cl.ListPreferredPeers(cctx.Context)
	if err != nil {
		return err
	}
	for _, peerID := range assigned {
		fmt.Println(peerID)
	}
	return nil
}

func blockCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("peer"))
	if err != nil {
		return err
	}
	err = cl.Block(cctx.Context, peerID)
	if err != nil {
		return err
	}
	fmt.Println("Blocking advertisements and content from peer", peerID)
	return nil
}

func importProvidersCmd(cctx *cli.Context) error {
	fromURL := &url.URL{
		Scheme: "http",
		Host:   cctx.String("from"),
		Path:   "/providers",
	}
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	err = cl.ImportProviders(cctx.Context, fromURL)
	if err != nil {
		return err
	}
	fmt.Println("Imported providers from indexer", fromURL.String())
	return nil
}

func reloadConfigCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	err = cl.ReloadConfig(cctx.Context)
	if err != nil {
		return err
	}
	fmt.Println("Reloaded indexer configuration")
	return nil
}
