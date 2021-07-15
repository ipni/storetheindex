package commands

import (
	"context"
	"fmt"

	"github.com/filecoin-project/storetheindex/client"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

var GetCmd = &cli.Command{
	Name:   "get",
	Usage:  "Get single Cid from idexer",
	Flags:  ClientCmdFlags,
	Action: getCidCmd,
}

func getCidCmd(c *cli.Context) error {
	cl := client.New(c)

	ctx, cancel := context.WithCancel(ProcessContext())
	defer cancel()
	cget := c.Args().Get(0)
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

	return cl.Get(ctx, ccid)

}
