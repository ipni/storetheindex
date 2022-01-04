package command

import (
	"fmt"

	httpclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
)

var sync = &cli.Command{
	Name:   "sync",
	Usage:  "Sync indexer with provider",
	Flags:  ingestSyncFlags,
	Action: syncCmd,
}

var allow = &cli.Command{
	Name:   "allow",
	Usage:  "Allow advertisements and content from peer",
	Flags:  ingestPolicyFlags,
	Action: allowCmd,
}

var block = &cli.Command{
	Name:   "block",
	Usage:  "Block advertisements and content from peer",
	Flags:  ingestPolicyFlags,
	Action: blockCmd,
}

var reload = &cli.Command{
	Name:   "reload-policy",
	Usage:  "Reload the policy from the configuration file",
	Flags:  ingestReloadPolicyFlags,
	Action: reloadPolicyCmd,
}

var IngestCmd = &cli.Command{
	Name:  "ingest",
	Usage: "Admin commands to sync indexer with a provider",
	Subcommands: []*cli.Command{
		sync,
		allow,
		block,
		reload,
	},
}

func syncCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("peer"))
	if err != nil {
		return err
	}
	var addr multiaddr.Multiaddr
	addrStr := cctx.String("addr")
	if addrStr != "" {
		addr, err = multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return err
		}
	}
	err = cl.Sync(cctx.Context, peerID, addr)
	if err != nil {
		return err
	}
	fmt.Println("Syncing request accepted. Come back later to check if syncing was successful")
	return nil
}

func allowCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("peer"))
	if err != nil {
		return err
	}
	err = cl.Allow(cctx.Context, peerID)
	if err != nil {
		return err
	}
	fmt.Println("Allowing advertisements and content from peer", peerID)
	return nil
}

func blockCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	peerID, err := peer.Decode(cctx.String("peer"))
	if err != nil {
		return err
	}
	err = cl.Block(cctx.Context, peerID)
	if err != nil {
		return err
	}
	fmt.Println("Blocking advertisements and content from peer", peerID)
	return nil
}

func reloadPolicyCmd(cctx *cli.Context) error {
	cl, err := httpclient.New(cctx.String("indexer"))
	if err != nil {
		return err
	}
	err = cl.ReloadPolicy(cctx.Context)
	if err != nil {
		return err
	}
	fmt.Println("Reloaded policy from configuration file")
	return nil
}
