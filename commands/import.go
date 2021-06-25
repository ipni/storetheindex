package commands

import (
	"context"

	"github.com/adlrocha/indexer-node/client"
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

func importListCmd(c *cli.Context) error {
	cl := client.New(c)
	prov := c.String("provider")
	p, err := peer.IDB58Decode(prov)
	dir := c.String("dir")

	log.Infow("Starting to import from cidlist file")
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	if err != nil {
		return err
	}
	return cl.ImportFromCidList(ctx, dir, p)
}

func importCarCmd(c *cli.Context) error {
	log.Infow("Starting to import from CAR file")
	_, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	log.Errorw("Importing from Car not implemented yet")

	return nil

}

func importManifestCmd(c *cli.Context) error {
	cl := client.New(c)
	prov := c.String("provider")
	p, err := peer.IDB58Decode(prov)
	dir := c.String("dir")

	log.Infow("Starting to import from Manifest file")
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	if err != nil {
		return err
	}
	return cl.ImportFromCidList(ctx, dir, p)
}

/*
func importReader(ctx context.Context, c *cli.Context, i importer.Importer) error {
	prov := c.String("provider")
	piece := c.String("piece")

	// TODO:

	log.Infof("Reading from provider: %s, and piece %s", prov, piece)
	cids := make(chan cid.Cid)
	done := make(chan error)

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
*/
