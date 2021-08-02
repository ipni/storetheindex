package commands

import (
	"context"
	"fmt"

	"github.com/filecoin-project/storetheindex/client"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/urfave/cli/v2"

	httpclient "github.com/filecoin-project/storetheindex/client/http/client"
	p2pclient "github.com/filecoin-project/storetheindex/client/libp2p/client"
)

var GetCmd = &cli.Command{
	Name:   "get",
	Usage:  "Get single Cid from idexer",
	Flags:  ClientCmdFlags,
	Action: getCidCmd,
}

func getCidCmd(cctx *cli.Context) error {
	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()

	protocol := cctx.String("protocol")
	endpoint := cctx.String("endpoint")
	var err error
	var cl client.Interface
	var end client.Endpoint

	switch protocol {
	case "http":
		cl, err = httpclient.New()
		end = httpclient.NewEndpoint(endpoint)
	case "libp2p":
		end, err = p2pclient.NewEndpoint(endpoint)
		if err != nil {
			return err
		}
		// NOTE: Creaeting a new host just for querying purposes.
		// Libp2p protocol requests from CLI should only be used
		// for testing purposes. This interface is in place
		/// for long-running peers.
		var host host.Host
		host, err = libp2p.New(ctx)
		if err != nil {
			return err
		}
		cl, err = p2pclient.New(ctx, host)
	default:
		err = fmt.Errorf("unrecognized protocol type for client interaction: %s", protocol)
	}
	if err != nil {
		return err
	}

	cget := cctx.Args().Get(0)
	if cget == "" {
		return fmt.Errorf("no cid provided as input")
	}
	ccid, err := cid.Decode(cget)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	resp, err := cl.Get(ctx, ccid, end)
	log.Info("Response:")
	resp.PrettyPrint()
	return err

}
