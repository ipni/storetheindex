package commands

import (
	"context"
	"fmt"

	"github.com/adlrocha/indexer-node/api/client"
	"github.com/adlrocha/indexer-node/importer"
	"github.com/ipfs/go-cid"
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

func importListCmd(c *cli.Context) error {
	log.Infow("Starting to import from Manifest file")
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	// TODO: Get from context and flag
	cl := client.NewFromEndpoint("http://localhost:3000")
	return cl.ImportFromCidList(ctx, "/home/adlrocha/Desktop/main/work/ProtocolLabs/repos/datasystems/indexer-node/cids.out")
}

func importCarCmd(c *cli.Context) error {
	log.Infow("Starting to import from CAR file")
	_, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	log.Errorw("Importing from Car not implemented yet")

	return nil

}

func importManifestCmd(c *cli.Context) error {
	log.Infow("Starting to import from Manifest file")
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	// TODO: Get from context and flag
	cl := client.NewFromEndpoint("http://localhost:3000")
	return cl.ImportFromManifest(ctx, "/home/adlrocha/Desktop/main/work/ProtocolLabs/repos/datasystems/indexer-node/@BatchManifest.ndjson")
}

func importReader(ctx context.Context, c *cli.Context, i importer.Importer) error {
	prov := c.String("provider")
	piece := c.String("piece")

	// TODO: Check if prov is Peer.ID and piece is CID

	log.Infof("Reading from provider: %s, and piece %s", prov, piece)
	cids := make(chan cid.Cid)
	done := make(chan error)
	// TODO: Check if a daemon is running before importing.

	go i.Read(ctx, cids, done)
	for {
		select {
		case <-ctx.Done():
			return nil
		case c := <-cids:
			// TODO: Process cids and store conveniently
			fmt.Println(c)
		case e := <-done:
			return e
		}
	}
}
