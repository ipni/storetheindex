package command

import (
	"errors"
	"fmt"

	httpclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
)

var importCidList = &cli.Command{
	Name:   "cidlist",
	Usage:  "Import indexer data from cidList",
	Flags:  importFlags,
	Action: importListCmd,
}

var importCar = &cli.Command{
	Name:   "car",
	Usage:  "Import indexer data from car",
	Flags:  importFlags,
	Action: importCarCmd,
}

var importManifest = &cli.Command{
	Name:   "manifest",
	Usage:  "Import manifest of CID aggregator",
	Flags:  importFlags,
	Action: importManifestCmd,
}

var ImportCmd = &cli.Command{
	Name:  "import",
	Usage: "Imports data directly into indexer, bypassing ingestion process",
	Subcommands: []*cli.Command{
		importCidList,
		importCar,
		importManifest,
	},
}

func importListCmd(cctx *cli.Context) error {
	// NOTE: Importing manually from CLI only supported for http protocol
	// for now. This feature is mainly for testing purposes
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	if err != nil {
		return err
	}
	fileName := cctx.String("file")

	fmt.Println("Telling indexer to import cidlist file:", fileName)
	err = cl.ImportFromCidList(cctx.Context, fileName, p, []byte(cctx.String("ctxid")), []byte(cctx.String("metadata")))
	if err != nil {
		return err
	}
	fmt.Println("Indexer imported manifest file")
	return nil
}

func importCarCmd(c *cli.Context) error {
	//fmt.Println("Telling indexer to import manifest file:", fileName)
	return errors.New("importing from car not implemented yet")
}

func importManifestCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cliIndexer(cctx, "admin"))
	if err != nil {
		return err
	}
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	if err != nil {
		return err
	}
	fileName := cctx.String("file")

	fmt.Println("Telling indexer to import manifest file:", fileName)
	// TODO: Should there be a timeout?  Since this may take a long time, it
	// would make sense that the request should complete immediately with a
	// redirect to a URL where the status can be polled for.
	err = cl.ImportFromManifest(cctx.Context, fileName, p, []byte(cctx.String("ctxid")), []byte(cctx.String("metadata")))
	if err != nil {
		return err
	}
	fmt.Println("Indexer imported manifest file")
	return nil
}
