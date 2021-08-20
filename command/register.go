package command

import (
	"errors"
	"fmt"

	v0client "github.com/filecoin-project/storetheindex/api/v0/client/http"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/urfave/cli/v2"
)

var RegisterCmd = &cli.Command{
	Name:   "register",
	Usage:  "Register provider information with an indexer that trusts the provider",
	Flags:  RegisterFlags,
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

	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		return fmt.Errorf("could not decode private key: %s", err)
	}

	client, err := v0client.NewIngest(cctx.String("indexer-host"))
	if err != nil {
		return err
	}
	err = client.Register(cctx.Context, cfg.Identity.PeerID, privKey, cctx.StringSlice("provider-addr"))
	if err != nil {
		return fmt.Errorf("failed to register providers: %s", err)
	}

	fmt.Println("Registered provider", cfg.Identity.PeerID, "at indexer", cctx.String("indexer-host"))
	return nil
}
