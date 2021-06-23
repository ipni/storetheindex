package node

import (
	"context"

	"github.com/adlrocha/indexer-node/persistent"
	"github.com/adlrocha/indexer-node/primary"
	"github.com/urfave/cli/v2"
)

type Node struct {
	primary    primary.Storage
	persistent persistent.Store
}

func New(ctx context.Context, cctx *cli.Context) (Node, error) {
	// TODO: Configure node
	// TODO: Initialize stores.
	// TODO: Initialize APIs.
	panic("Node new not implemented")
}
