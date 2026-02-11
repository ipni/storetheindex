package command

import (
	"fmt"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestInit(t *testing.T) {
	// Set up a context that is canceled when the command is interrupted
	ctx := t.Context()

	tempDir := t.TempDir()
	t.Setenv(config.EnvDir, tempDir)

	app := &cli.App{
		Name: "indexer",
		Commands: []*cli.Command{
			InitCmd,
		},
	}

	const (
		badAddr   = "ip3/127.0.0.1/tcp/9999"
		cacheSize = 2701
		goodAddr  = "/ip4/127.0.0.1/tcp/7777"
		goodAddr2 = "/ip4/127.0.0.1/tcp/17171"
		storeType = "pebble"
		topicName = "index/mytopic"
	)

	err := app.RunContext(ctx, []string{"storetheindex", "init", "-listen-admin", badAddr})
	require.Error(t, err)

	err = app.RunContext(ctx, []string{"storetheindex", "init", "-listen-finder", badAddr})
	require.Error(t, err)

	err = app.RunContext(ctx, []string{"storetheindex", "init", "-listen-ingest", badAddr})
	require.Error(t, err)

	args := []string{
		"storetheindex", "init",
		"-listen-finder", goodAddr,
		"-listen-ingest", goodAddr2,
		"-cachesize", fmt.Sprint(cacheSize),
		"--pubsub-topic", topicName,
		"-store", storeType,
	}
	err = app.RunContext(ctx, args)
	require.NoError(t, err)

	cfg, err := config.Load("")
	require.NoError(t, err)

	require.Equal(t, goodAddr, cfg.Addresses.Finder, "finder listen address was not configured")
	require.Equal(t, goodAddr2, cfg.Addresses.Ingest, "ingest listen address was not configured")
	require.Equal(t, cacheSize, cfg.Indexer.CacheSize, "cache size was tno configured")
	require.Equal(t, storeType, cfg.Indexer.ValueStoreType, "value store type was not configured")
	require.Equal(t, topicName, cfg.Ingest.PubSubTopic)
	require.Equal(t, config.Version, cfg.Version)

	t.Log(cfg.String())
}
