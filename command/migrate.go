package command

import (
	"fmt"
	"os"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/migrate"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/urfave/cli/v2"
)

var MigrateCmd = &cli.Command{
	Name:   "migrate",
	Usage:  "Migrate indexer datastore",
	Flags:  migrateFlags,
	Action: migrateCommand,
}

func migrateCommand(cctx *cli.Context) error {
	cfg, err := config.Load("")
	if err != nil {
		if err == config.ErrNotInitialized {
			fmt.Fprintln(os.Stderr, "storetheindex is not initialized")
			fmt.Fprintln(os.Stderr, "To initialize, run the command: ./storetheindex init")
			os.Exit(1)
		}
		return fmt.Errorf("cannot load config file: %w", err)
	}

	if cfg.Datastore.Type != "levelds" {
		return fmt.Errorf("only levelds datastore type supported, %q not supported", cfg.Datastore.Type)
	}

	// Create datastore
	dataStorePath, err := config.Path("", cfg.Datastore.Dir)
	if err != nil {
		return err
	}
	err = checkWritable(dataStorePath)
	if err != nil {
		return err
	}
	dstore, err := leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		return err
	}

	if cctx.Bool("check") {
		need, err := migrate.NeedMigration(cctx.Context, dstore)
		if err != nil {
			return err
		}
		if cctx.Bool("revert") {
			if need {
				fmt.Println("Cannot revert, datastore not current version")
			} else {
				fmt.Println("Can revert datastore")
			}
			return nil
		}
		if need {
			fmt.Println("Datastore needs migration")
		} else {
			fmt.Println("Datastore does not need migration")
		}
		return nil
	}

	if cctx.Bool("revert") {
		reverted, err := migrate.Revert(cctx.Context, dstore)
		if err != nil {
			return err
		}
		if !reverted {
			fmt.Println("Datastore already at previous version")
		} else {
			fmt.Println("Datastore reverted to previous version")
		}
		return nil
	}

	migrated, err := migrate.Migrate(cctx.Context, dstore)
	if err != nil {
		return err
	}
	if !migrated {
		fmt.Println("Datastore already at current version")
	} else {
		fmt.Println("Datastore migrated to current version")
	}
	return nil
}
