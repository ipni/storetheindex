package node

import (
	"context"

	"github.com/filecoin-project/storetheindex/store"
	"github.com/filecoin-project/storetheindex/store/primary"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("node")

type Node struct {
	primary    store.Storage
	persistent store.Storage
	api        *api
	doneCh     chan struct{}
}

func New(ctx context.Context, cctx *cli.Context) (*Node, error) {
	// TODO: Create flag for the size of primary storage
	e := cctx.String("endpoint")

	n := &Node{
		doneCh:  make(chan struct{}),
		primary: primary.New(1000000),
		// TODO: Initialize persistence
		// persistent: storethehash.New()...
	}

	err := n.initAPI(e)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) Start() error {
	log.Info("Started server")
	// TODO: Start required processes for stores
	return n.api.Serve()

}

func (n *Node) Shutdown(ctx context.Context) error {
	defer close(n.doneCh)
	return n.api.Shutdown(ctx)
}
