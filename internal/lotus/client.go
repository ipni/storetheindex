package lotus

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api/client"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

type nodeCloser struct {
	jsonCloser jsonrpc.ClientCloser
}

func (c *nodeCloser) Close() error {
	c.jsonCloser()
	return nil
}

func SetupGateway(ctx context.Context, gatewayApi string) (*Discovery, error) {
	apiInfo := cliutil.ParseApiInfo(gatewayApi)

	addr, err := apiInfo.DialArgs("v1")
	if err != nil {
		return nil, fmt.Errorf("parse listen address: %w", err)
	}

	node, jsoncloser, err := client.NewGatewayRPCV1(ctx, addr, apiInfo.AuthHeader())
	if err != nil {
		return nil, err
	}

	closer := &nodeCloser{
		jsonCloser: jsoncloser,
	}

	return &Discovery{
		node:   node,
		closer: closer,
	}, nil
}
