package command

import (
	"fmt"

	httpclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
)

var sync = &cli.Command{
	Name:   "sync",
	Usage:  "Sync indexer with provider",
	Flags:  ingestFlags,
	Action: syncCmd,
}

var subscribe = &cli.Command{
	Name:   "subscribe",
	Usage:  "Subscribe indexer with provider",
	Flags:  ingestFlags,
	Action: subscribeCmd,
}

var unsubscribe = &cli.Command{
	Name:   "unsubscribe",
	Usage:  "Unsubscribe indexer from provider",
	Flags:  ingestFlags,
	Action: unsubscribeCmd,
}

var IngestCmd = &cli.Command{
	Name:  "ingest",
	Usage: "Admin commands to sync indexer with a provider",
	Subcommands: []*cli.Command{
		sync,
		subscribe,
		unsubscribe,
	},
}

func syncCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	if err != nil {
		return err
	}
	err = cl.Sync(cctx.Context, p)
	if err != nil {
		return err
	}
	fmt.Println("Syncing request accepted. Come back later to check if syncing was successful")
	return nil
}

func subscribeCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	if err != nil {
		return err
	}
	err = cl.Subscribe(cctx.Context, p)
	if err != nil {
		return err
	}
	fmt.Println("Successfully subscribed to provider")
	return nil
}

func unsubscribeCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	prov := cctx.String("provider")
	p, err := peer.Decode(prov)
	if err != nil {
		return err
	}
	err = cl.Unsubscribe(cctx.Context, p)
	if err != nil {
		return err
	}
	fmt.Println("Successfully unsubscribed from provider")
	return nil
}
