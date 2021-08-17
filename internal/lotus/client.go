package lotus

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api/client"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/mitchellh/go-homedir"
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
	return openGateway(ctx, apiInfo)
}

func SetupLocal(ctx context.Context, repoPath string) (*Discovery, error) {
	if repoPath == "" {
		return nil, errors.New("lotus repo path is empty")
	}

	p, err := homedir.Expand(repoPath)
	if err != nil {
		return nil, fmt.Errorf("expand home dir (%s): %w", repoPath, err)
	}

	r, err := repo.NewFS(p)
	if err != nil {
		return nil, fmt.Errorf("open repo at path: %s; %w", p, err)
	}

	ma, err := r.APIEndpoint()
	if err != nil {
		return nil, fmt.Errorf("api endpoint: %w", err)
	}

	token, err := r.APIToken()
	if err != nil {
		return nil, fmt.Errorf("api token: %w", err)
	}

	apiInfo := cliutil.APIInfo{
		Addr:  ma.String(),
		Token: token,
	}

	//return openFillNode(ctx, apiInfo)
	return openGateway(ctx, apiInfo)
}

func openGateway(ctx context.Context, apiInfo cliutil.APIInfo) (*Discovery, error) {
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

// Enable this if FullNode access is ever needed
/*
func openFullNode(ctx context.Context, apiInfo cliutil.APIInfo) (*Discovery, error) {
	addr, err := apiInfo.DialArgs("v1")
	if err != nil {
		return nil, nil, fmt.Errorf("parse listen address: %w", err)
	}

	node, jsoncloser, err := client.NewFullNodeRPCV1(ctx, addr, apiInfo.AuthHeader())
	if err != nil {
		return nil, nil, err
	}

	closer := &nodeCloser{
		apiCloser:  apiCloser,
		jsonCloser: jsoncloser,
	}

	return &Discovery{
		node:   node,
		closer: closer,
	}, nil
}
*/
