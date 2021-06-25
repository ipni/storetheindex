package node

import (
	"context"

	"github.com/adlrocha/indexer-node/api"
	"github.com/adlrocha/indexer-node/persistent"
	"github.com/adlrocha/indexer-node/primary"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("node")

type Node struct {
	primary    primary.Storage
	persistent persistent.Store
	a          *api.API
	doneCh     chan struct{}
}

func New(ctx context.Context, cctx *cli.Context) (*Node, error) {
	var err error
	// TODO: Configure node
	// TODO: Initialize stores.
	// TODO: Initialize APIs.
	n := new(Node)
	n.doneCh = make(chan struct{})
	n.primary = primary.New(1000000)
	n.persistent = persistent.New()
	n.a, err = api.NewWithDependencies(":3000")
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) Start() error {
	log.Infow("Started server")
	// TODO: Start required processes of stores
	return n.a.Serve()

}

func (n *Node) Shutdown(ctx context.Context) error {
	defer func() {
		close(n.doneCh)
	}()
	return n.a.Shutdown(ctx)
}
