package command

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/storetheindex/internal/libp2pclient"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"

	"github.com/filecoin-project/storetheindex/api/v0/finder/client"
	httpclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	p2pclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/libp2p"
)

const getTimeout = 15 * time.Second

var FindCmd = &cli.Command{
	Name:   "find",
	Usage:  "Find value by multihash in idexer",
	Flags:  findFlags,
	Action: findCmd,
}

func findCmd(cctx *cli.Context) error {
	protocol := cctx.String("protocol")

	mhArg := cctx.String("mh")
	cidArg := cctx.String("cid")
	if mhArg == "" && cidArg == "" {
		return errors.New("must specify --cid or --mh")
	}
	if mhArg != "" && cidArg != "" {
		return errors.New("only one --cid or --mh allowed")
	}
	var mh multihash.Multihash
	var err error

	if mhArg != "" {
		mh, err = multihash.FromB58String(mhArg)
		if err != nil {
			return err
		}
	} else if cidArg != "" {
		var ccid cid.Cid
		ccid, err = cid.Decode(cidArg)
		if err != nil {
			return err
		}
		mh = ccid.Hash()
	}

	var cl client.Finder

	ctx, cancel := context.WithTimeout(context.Background(), getTimeout)
	defer cancel()

	switch protocol {
	case "http":
		cl, err = httpclient.NewFinder(cctx.String("indexerer"))
		if err != nil {
			return err
		}
	case "libp2p":
		var options []libp2pclient.Option
		hostname := cctx.String("indexerer")
		if hostname != "" {
			hostport := strings.SplitN(hostname, ":", 2)
			var port int
			if len(hostport) > 1 {
				hostname = hostport[0]
				port, err = strconv.Atoi(hostport[1])
				if err != nil {
					return err
				}
				options = append(options, libp2pclient.Port(port))
			}
			options = append(options, libp2pclient.Hostname(hostname))
		}
		peerID, err := peer.Decode(cctx.String("peerid"))
		if err != nil {
			return err
		}

		cl, err = p2pclient.NewFinder(ctx, peerID, options...)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized protocol type for client interaction: %s", protocol)
	}

	resp, err := cl.Find(ctx, mh)
	if err != nil {
		return err
	}
	log.Info("Response: %v", resp)
	return nil
}
