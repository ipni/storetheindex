package command

import (
	"context"

	httpclient "github.com/filecoin-project/storetheindex/client/http"
	"github.com/filecoin-project/storetheindex/server/net"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
)

var importCidList = &cli.Command{
	Name:   "cidlist",
	Usage:  "Import indexer data from cidList",
	Flags:  ImportFlags,
	Action: importListCmd,
}

var importCar = &cli.Command{
	Name:   "car",
	Usage:  "Import indexer data from car",
	Flags:  ImportFlags,
	Action: importCarCmd,
}

var importManifest = &cli.Command{
	Name:   "manifest",
	Usage:  "Import manifest of CID aggregator",
	Flags:  ImportFlags,
	Action: importManifestCmd,
}
var ImportCmd = &cli.Command{
	Name:  "import",
	Usage: "Imports data to indexer from different sources",
	Subcommands: []*cli.Command{
		importCidList,
		importCar,
		importManifest,
	},
}

func importListCmd(cctx *cli.Context) error {
	// NOTE: Importing manually from CLI only supported for http protocol
	// for now. This feature is mainly for testing purposes
	endpoint := cctx.String("admin_ep")
	cl, err := httpclient.New()
	if err != nil {
		return err
	}
	end := net.NewHTTPEndpoint(endpoint)
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	dir := cctx.String("dir")

	log.Infow("Starting to import from cidlist file")
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	if err != nil {
		return err
	}
	return cl.ImportFromCidList(ctx, dir, p, end)
}

func importCarCmd(c *cli.Context) error {
	log.Infow("Starting to import from CAR file")
	_, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	log.Errorw("Importing from Car not implemented yet")

	return nil

}

func importManifestCmd(cctx *cli.Context) error {
	endpoint := cctx.String("admin_ep")
	cl, err := httpclient.New()
	if err != nil {
		return err
	}
	end := net.NewHTTPEndpoint(endpoint)
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	dir := cctx.String("dir")

	log.Infow("Starting to import from manifest file")
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	if err != nil {
		return err
	}
	return cl.ImportFromManifest(ctx, dir, p, end)
}
