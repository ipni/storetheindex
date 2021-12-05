package command

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/urfave/cli/v2"
)

func TestInit(t *testing.T) {
	// Set up a context that is canceled when the command is interrupted
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir := t.TempDir()
	os.Setenv(config.EnvDir, tempDir)

	app := &cli.App{
		Name: "indexer",
		Commands: []*cli.Command{
			InitCmd,
		},
	}

	badAddr := "ip3/127.0.0.1/tcp/9999"
	err := app.RunContext(ctx, []string{"storetheindex", "init", "-listen-admin", badAddr})
	if err == nil {
		t.Fatal("expected error")
	}

	err = app.RunContext(ctx, []string{"storetheindex", "init", "-listen-finder", badAddr})
	if err == nil {
		t.Fatal("expected error")
	}

	err = app.RunContext(ctx, []string{"storetheindex", "init", "-listen-ingest", badAddr})
	if err == nil {
		t.Fatal("expected error")
	}

	goodAddr := "/ip4/127.0.0.1/tcp/7777"
	goodAddr2 := "/ip4/127.0.0.1/tcp/17171"
	storeType := "pogreb"
	cacheSize := 2701
	args := []string{
		"storetheindex", "init",
		"-listen-finder", goodAddr,
		"-listen-ingest", goodAddr2,
		"-cachesize", fmt.Sprint(cacheSize),
		"-store", storeType,
	}
	err = app.RunContext(ctx, args)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Addresses.Finder != goodAddr {
		t.Error("finder listen address was not configured")
	}
	if cfg.Addresses.Ingest != goodAddr2 {
		t.Error("ingest listen address was not configured")
	}
	if cfg.Indexer.CacheSize != cacheSize {
		t.Error("cache size was tno configured")
	}
	if cfg.Indexer.ValueStoreType != storeType {
		t.Error("value store type was not configured")
	}

	t.Log(cfg.String())
}
