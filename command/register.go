package command

import (
	"errors"
	"fmt"

	v0client "github.com/ipni/storetheindex/api/v0/ingest/client/http"
	"github.com/ipni/storetheindex/config"
	"github.com/urfave/cli/v2"
)

var RegisterCmd = &cli.Command{
	Name:   "register",
	Usage:  "Register provider information with an indexer",
	Flags:  registerFlags,
	Action: registerCommand,
}

func registerCommand(cctx *cli.Context) error {
	cfg, err := config.Load(cctx.String("config"))
	if err != nil {
		if err == config.ErrNotInitialized {
			err = errors.New("config file not found")
		}
		return fmt.Errorf("cannot load config file: %w", err)
	}

	peerID, privKey, err := cfg.Identity.Decode()
	if err != nil {
		return err
	}

	indexerHost := cliIndexer(cctx, "admin")
	client, err := v0client.New(indexerHost)
	if err != nil {
		return err
	}

	err = client.Register(cctx.Context, peerID, privKey, cctx.StringSlice("addr"))
	if err != nil {
		return fmt.Errorf("failed to register providers: %s", err)
	}

	fmt.Println("Registered provider", cfg.Identity.PeerID, "at indexer", indexerHost)
	return nil
}
