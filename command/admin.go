package command

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/ipni/storetheindex/admin/client"
	"github.com/ipni/storetheindex/rate"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var AdminCmd = &cli.Command{
	Name:  "admin",
	Usage: "Perform admin activities with an indexer",
	Subcommands: []*cli.Command{
		allowCmd,
		blockCmd,
		freezeIndexerCmd,
		importProvidersCmd,
		listAssignedCmd,
		listPreferredCmd,
		reloadCmd,
		statusCmd,
		syncCmd,
		unassignCmd,
		telemetryCmd,
	},
}

var syncCmd = &cli.Command{
	Name:   "sync",
	Usage:  "Sync indexer with provider.",
	Flags:  adminSyncFlags,
	Action: syncAction,
	Subcommands: []*cli.Command{
		listPendinSyncsCmd,
	},
}

var listPendinSyncsCmd = &cli.Command{
	Name:   "list-pending",
	Usage:  "Returns a list of currently pending syncs.",
	Action: listPendingSyncsAction,
}

var adminSyncFlags = []cli.Flag{
	indexerHostFlag,
	&cli.StringFlag{
		Name:    "pubid",
		Usage:   "Publisher peer ID",
		Aliases: []string{"p"},
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

var allowCmd = &cli.Command{
	Name:   "allow",
	Usage:  "Allow advertisements and content from peer",
	Flags:  adminPolicyFlags,
	Action: allowAction,
}

var blockCmd = &cli.Command{
	Name:   "block",
	Usage:  "Block advertisements and content from peer",
	Flags:  adminPolicyFlags,
	Action: blockAction,
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

var freezeIndexerCmd = &cli.Command{
	Name:  "freeze",
	Usage: "Put indexer into frozen mode",
	Flags: []cli.Flag{
		indexerHostFlag,
	},

	Action: freezeAction,
}

var importProvidersCmd = &cli.Command{
	Name:   "import-providers",
	Usage:  "Import provider information from another indexer",
	Flags:  importProvidersFlags,
	Action: importProvidersAction,
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

var listAssignedCmd = &cli.Command{
	Name:  "list-assigned",
	Usage: "List assigned peers when configured to work with assigner service",
	Flags: []cli.Flag{
		indexerHostFlag,
	},
	Action: listAssignedAction,
}

var listPreferredCmd = &cli.Command{
	Name:  "list-preferred",
	Usage: "List unassigned peers that indexer has retrieved content from",
	Flags: []cli.Flag{
		indexerHostFlag,
	},
	Action: listPreferredAction,
}

var reloadCmd = &cli.Command{
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
	Flags: []cli.Flag{
		indexerHostFlag,
	},
	Action: reloadConfigAction,
}

var statusCmd = &cli.Command{
	Name:  "status",
	Usage: "Show indexer status",
	Flags: []cli.Flag{
		indexerHostFlag,
	},
	Action: statusAction,
}

var unassignCmd = &cli.Command{
	Name:   "unassign",
	Usage:  "Un-assign a publisher from the indexer",
	Flags:  unassignFlags,
	Action: unassignAction,
}

var unassignFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "peer",
		Usage:    "Peer ID of publisher un-assign",
		Aliases:  []string{"p"},
		Required: true,
	},
	indexerHostFlag,
}

var telemetryCmd = &cli.Command{
	Name:   "telemetry",
	Usage:  "Show provider telemetry info",
	Flags:  telemetryFlags,
	Action: telemetryAction,
}

var telemetryFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    "provider",
		Usage:   "Provider ID to show latest telemetry info for",
		Aliases: []string{"p"},
	},
	indexerHostFlag,
}

func listPendingSyncsAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	peers, err := cl.GetPendingSyncs(cctx.Context)
	if err != nil {
		return err
	}

	fmt.Printf("\nTotal pending syncs: %d", len(peers))
	for _, p := range peers {
		fmt.Printf("\n\t%s", p)
	}
	fmt.Println()
	return nil
}

func syncAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	if cctx.String("pubid") == "" {
		return errors.New("publisher peer id must be provided")
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

func allowAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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

func listAssignedAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	assigned, err := cl.ListAssignedPeers(cctx.Context)
	if err != nil {
		return err
	}
	for publisher, continued := range assigned {
		if continued.Validate() == nil {
			fmt.Println(publisher, "continued-from:", continued)
		} else {
			fmt.Println(publisher)
		}
	}
	return nil
}

func listPreferredAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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

func blockAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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

func freezeAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	if err = cl.Freeze(cctx.Context); err != nil {
		return err
	}
	fmt.Println("Indexer frozen")
	return nil
}

func importProvidersAction(cctx *cli.Context) error {
	fromHost := cctx.String("from")
	if !strings.HasPrefix(fromHost, "http://") && !strings.HasPrefix(fromHost, "https://") {
		fromHost = "http://" + fromHost
	}
	fromURL, err := url.Parse(fromHost)
	if err != nil {
		return err
	}
	fromURL.Path = ""

	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	if err = cl.ImportProviders(cctx.Context, fromURL); err != nil {
		return err
	}
	fmt.Println("Imported providers from indexer", fromURL.String())
	return nil
}

func reloadConfigAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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

func statusAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	st, err := cl.Status(cctx.Context)
	if err != nil {
		return err
	}
	fmt.Println("ID:", st.ID)
	fmt.Println("Frozen:", st.Frozen)
	var percent string
	if st.Usage < 0 {
		percent = "not available"
	} else {
		percent = fmt.Sprintf("%0.2f%%", st.Usage)
	}
	fmt.Println("Usage:", percent)
	return nil
}

func unassignAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("peer"))
	if err != nil {
		return err
	}
	err = cl.Unassign(cctx.Context, peerID)
	if err != nil {
		return err
	}
	fmt.Println("Un-assigned", peerID, "from indexer")
	fmt.Println()
	fmt.Println("Restart assigner service see this change. To prevent re-assignment to this indexer, first block the peer on this indexer or configure a pre-set assignment.")
	return nil
}

func telemetryAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	if cctx.String("provider") == "" {
		rateMap, err := cl.GetAllTelemetry(cctx.Context)
		if err != nil {
			return err
		}
		if len(rateMap) == 0 {
			fmt.Println("no new rate information")
			return nil
		}
		for pid, irate := range rateMap {
			printIngestRate(peer.ID(pid), irate)
		}
		return nil
	}

	pid, err := peer.Decode(cctx.String("provider"))
	if err != nil {
		return err
	}
	irate, ok, err := cl.GetTelemetry(cctx.Context, pid)
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println("no new rate information")
		return nil
	}
	printIngestRate(pid, irate)
	return nil
}

func printIngestRate(pid peer.ID, irate rate.Rate) {
	fmt.Println("Provider", pid, "ingest rate:")
	sec := irate.Elapsed.Seconds()
	fmt.Printf("    %d mh, %d ads, %f seconds: %f mh/s\n", irate.Count, irate.Samples, sec, float64(irate.Count)/sec)
}
