package command

import (
	"encoding/base64"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/client"
	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var FindCmd = &cli.Command{
	Name:   "find",
	Usage:  "Find value by CID or multihash in indexer",
	Flags:  findFlags,
	Action: findAction,
}

var findFlags = []cli.Flag{
	&cli.StringSliceFlag{
		Name:     "mh",
		Usage:    "Specify multihash to use as indexer key, multiple OK",
		Required: false,
	},
	&cli.StringSliceFlag{
		Name:     "cid",
		Usage:    "Specify CID to use as indexer key, multiple OK",
		Required: false,
	},
	indexerHostFlag,
}

func findAction(cctx *cli.Context) error {
	mhArgs := cctx.StringSlice("mh")
	cidArgs := cctx.StringSlice("cid")
	mhs := make([]multihash.Multihash, 0, len(mhArgs)+len(cidArgs))
	for i := range mhArgs {
		m, err := multihash.FromB58String(mhArgs[i])
		if err != nil {
			return err
		}
		mhs = append(mhs, m)
	}
	for i := range cidArgs {
		c, err := cid.Decode(cidArgs[i])
		if err != nil {
			return err
		}
		mhs = append(mhs, c.Hash())
	}

	var cl client.Interface
	var err error

	cl, err = findclient.New(cliIndexer(cctx, "find"))
	if err != nil {
		return err
	}

	resp, err := cl.FindBatch(cctx.Context, mhs)
	if err != nil {
		return err
	}

	if len(resp.MultihashResults) == 0 {
		fmt.Println("index not found")
		return nil
	}

	fmt.Println("Content providers:")
	for i := range resp.MultihashResults {
		fmt.Println("   Multihash:", resp.MultihashResults[i].Multihash.B58String(), "==>")
		for _, pr := range resp.MultihashResults[i].ProviderResults {
			fmt.Println("       Provider:", pr.Provider)
			fmt.Println("       ContextID:", base64.StdEncoding.EncodeToString(pr.ContextID))
			fmt.Println("       Metadata:", base64.StdEncoding.EncodeToString(pr.Metadata))
		}
	}
	return nil
}
