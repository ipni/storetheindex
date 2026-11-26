package importsender

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"testing"

	leveldbds "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	"github.com/ipni/storetheindex/internal/dskey"
	"github.com/stretchr/testify/require"
)

const testToken = "test-token"

// testProviders writes n provider records of valueBytes random bytes each
// to a new datastore, and returns it with the entries that the sender sends
// for them
func testProviders(t *testing.T, n, valueBytes int) (*leveldbds.Datastore, []importer.Entry) {
	t.Helper()
	ds, err := leveldbds.NewDatastore(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	want := make([]importer.Entry, n)
	for i := range want {
		key := fmt.Sprintf("p%06d", i)
		value := make([]byte, valueBytes)
		_, _ = rand.Read(value)
		require.NoError(t, ds.DB.Put([]byte(dskey.ProviderInfoPath+"/"+key), value, nil))
		want[i] = importer.Entry{Key: []byte(key), Value: value}
	}
	return ds, want
}

// testServer starts a server of ds, closed when the test ends
func testServer(t *testing.T, ds *leveldbds.Datastore) *importhttp.Server {
	t.Helper()
	s, err := New("127.0.0.1:0", ds, testToken, 0)
	require.NoError(t, err)
	errc := make(chan error, 1)
	go func() { errc <- s.Serve() }()
	t.Cleanup(func() {
		_ = s.Close()
		require.ErrorIs(t, <-errc, http.ErrServerClosed)
	})
	return s
}

func TestServerServesDatastore(t *testing.T) {
	ds, want := testProviders(t, 100, 32)
	s := testServer(t, ds)
	c, err := importhttp.NewClient(s.URL(), testToken)
	require.NoError(t, err)

	_, entries, err := c.OpenStream(t.Context(), importer.KindProviders, nil, nil, 0)
	require.NoError(t, err)
	var got []importer.Entry
	for e, err := range entries {
		require.NoError(t, err)
		got = append(got, importer.Entry{Key: append([]byte(nil), e.Key...), Value: append([]byte(nil), e.Value...)})
	}
	require.Equal(t, want, got)
}
