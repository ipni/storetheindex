package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/ipni/storetheindex/internal/migrate"
	"github.com/stretchr/testify/require"
)

func TestNewImportSender(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("secret"), 0o600))
	ds, err := leveldb.NewDatastore(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	for _, listen := range []string{"/ip4/127.0.0.1/tcp/0", "127.0.0.1:0"} {
		s, err := newImportSender(config.Import{ListenAddr: listen, TokenFile: tokenFile}, ds)
		require.NoError(t, err, listen)
		require.NoError(t, s.Close())
	}

	_, err = newImportSender(config.Import{ListenAddr: "/ip4/not-an-ip/tcp/0", TokenFile: tokenFile}, ds)
	require.ErrorContains(t, err, "bad import listen address")
	_, err = newImportSender(config.Import{ListenAddr: "127.0.0.1:0"}, ds)
	require.ErrorContains(t, err, "TokenFile")
	_, err = newImportSender(config.Import{ListenAddr: "127.0.0.1:0", TokenFile: tokenFile}, dssync.MutexWrap(datastore.NewMapDatastore()))
	require.ErrorContains(t, err, "LevelDB")
}

func TestCheckMigration(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	cfg := config.Datastore{Dir: dir, Type: "levelds"}
	require.NoError(t, checkMigration(ctx, cfg, false), "no import")

	ds, err := leveldb.NewDatastore(dir, nil)
	require.NoError(t, err)
	require.NoError(t, ds.Put(ctx, datastore.NewKey(dskey.MigrateState), []byte(`{"started":"2026-11-26T10:00:00Z","steps":[
		{"name":"adProcessed","done":true},{"name":"adSkipReason"},{"name":"sync"},{"name":"providers"},{"name":"syncRecheck"},
		{"name":"records"},{"name":"contextMap"},{"name":"validate"},{"name":"multihashes"}]}`)))
	require.NoError(t, ds.Close())

	require.ErrorIs(t, checkMigration(ctx, cfg, false), migrate.ErrUnfinished)
	require.NoError(t, checkMigration(ctx, cfg, true), "forced")
	require.Error(t, checkMigration(ctx, config.Datastore{Dir: dir, Type: "badger"}, false))
}
