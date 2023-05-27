package command

import (
	"errors"
	"fmt"

	client "github.com/ipni/storetheindex/admin/client"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/urfave/cli/v2"
)

var importCidListCmd = &cli.Command{
	Name:   "cidlist",
	Usage:  "Import indexer data from cidList",
	Flags:  importFlags,
	Action: importListAction,
}

var importCarCmd = &cli.Command{
	Name:   "car",
	Usage:  "Import indexer data from car",
	Flags:  importFlags,
	Action: importCarAction,
}

var importManifestCmd = &cli.Command{
	Name:   "manifest",
	Usage:  "Import manifest of CID aggregator",
	Flags:  importFlags,
	Action: importManifestAction,
}

var importFlags = []cli.Flag{
	&cli.StringFlag{
		Name:     "provider",
		Usage:    "Provider's peer ID",
		Aliases:  []string{"p"},
		Required: true,
	},
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

var ImportCmd = &cli.Command{
	Name:  "import",
	Usage: "Imports data directly into indexer, bypassing ingestion process",
	Subcommands: []*cli.Command{
		importCidListCmd,
		importCarCmd,
		importManifestCmd,
	},
}

func importListAction(cctx *cli.Context) error {
	// NOTE: Importing manually from CLI only supported for http protocol
	// for now. This feature is mainly for testing purposes
	cl, err := client.New(cliIndexer(cctx, "admin"))
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

func importCarAction(c *cli.Context) error {
	return errors.New("importing from car not implemented yet")
}

func importManifestAction(cctx *cli.Context) error {
	cl, err := client.New(cliIndexer(cctx, "admin"))
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
