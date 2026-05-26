package carmirror_test

import (
	"io"
	"strings"
	"testing"

	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/server/carmirror"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	ctx := t.Context()

	backend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	const fileName = "test.car"
	const data = "car data"
	_, err = backend.Put(ctx, fileName, strings.NewReader(data))
	require.NoError(t, err)

	srv, err := carmirror.New("127.0.0.1:0", backend)
	require.NoError(t, err)

	go func() {
		_ = srv.Start()
	}()
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
	})

	client, err := filestore.NewHTTP(srv.URL() + "/carmirror/")
	require.NoError(t, err)

	_, rc, err := client.Get(ctx, fileName)
	require.NoError(t, err)
	defer rc.Close()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, string(got))
}
