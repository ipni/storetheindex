package node

import (
	"context"

	"github.com/adlrocha/indexer-node/store"
	"github.com/adlrocha/indexer-node/store/persistent"
	"github.com/adlrocha/indexer-node/store/primary"
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

	var err error
	n := new(Node)
	n.doneCh = make(chan struct{})
	n.primary = primary.New(1000000)
	n.persistent = persistent.New()
	err = n.initAPI(e)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) Start() error {
	log.Infow("Started server")
	// TODO: Start required processes for stores
	return n.api.Serve()

}

func (n *Node) Shutdown(ctx context.Context) error {
	defer func() {
		close(n.doneCh)
	}()
	return n.api.Shutdown(ctx)
}
