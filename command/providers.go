package command

import (
	"fmt"

	httpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var ProvidersCmd = &cli.Command{
	Name:  "providers",
	Usage: "Commands to get provider information",
	Subcommands: []*cli.Command{
		get,
		list,
	},
}

var get = &cli.Command{
	Name:   "get",
	Usage:  "Show information about a specific provider",
	Flags:  providersGetFlags,
	Action: getProvidersCmd,
}

var list = &cli.Command{
	Name:   "list",
	Usage:  "Show information about all known providers",
	Flags:  providersListFlags,
	Action: listProvidersCmd,
}

func getProvidersCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "finder"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("provid"))
	if err != nil {
		return err
	}
	prov, err := cl.GetProvider(cctx.Context, peerID)
	if err != nil {
		return err
	}
	if prov == nil {
		fmt.Println("Provider not found on indexer")
		return nil
	}

	showProviderInfo(prov)
	return nil
}

func listProvidersCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "finder"))
	if err != nil {
		return err
	}
	provs, err := cl.ListProviders(cctx.Context)
	if err != nil {
		return err
	}
	if len(provs) == 0 {
		fmt.Println("No providers registered with indexer")
		return nil
	}

	for _, pinfo := range provs {
		showProviderInfo(pinfo)
	}

	return nil
}

func showProviderInfo(pinfo *model.ProviderInfo) {
	fmt.Println("Provider", pinfo.AddrInfo.ID)
	fmt.Println("    Addresses:", pinfo.AddrInfo.Addrs)
	fmt.Println("    LastAdvertisement:", pinfo.LastAdvertisement)
	fmt.Println("    LastAdvertisementTime:", pinfo.LastAdvertisementTime)
}
