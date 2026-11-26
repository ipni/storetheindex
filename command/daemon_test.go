package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/storetheindex/config"
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
