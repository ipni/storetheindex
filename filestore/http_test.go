package filestore_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"
)

func TestHTTPReadOnlyServer(t *testing.T) {
	ctx := t.Context()

	client, err := filestore.NewHTTP(setupHTTPReadOnlyServer(t) + "/")
	require.NoError(t, err)

	_, err = client.Put(ctx, "new.txt", strings.NewReader("x"))
	require.Error(t, err)

	err = client.Delete(ctx, fileName)
	require.Error(t, err)
}

func setupHTTPReadOnlyServer(t *testing.T) string {
	t.Helper()

	backend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(backend)
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestHTTPWithPathPrefix(t *testing.T) {
	ctx := t.Context()

	backend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	_, err = backend.Put(ctx, fileName, strings.NewReader(data))
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(backend)
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.Handle("/cars/", http.StripPrefix("/cars", handler))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client, err := filestore.NewHTTP(srv.URL + "/cars/")
	require.NoError(t, err)

	fileInfo, r, err := client.Get(ctx, fileName)
	require.NoError(t, err)
	require.Equal(t, fileName, fileInfo.Path)
	require.NoError(t, r.Close())
}

func TestHTTPClientCustomClient(t *testing.T) {
	backend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(backend)
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	custom := srv.Client()
	_, err = filestore.NewHTTP(srv.URL+"/", filestore.WithHTTPClient(custom))
	require.NoError(t, err)

	_, err = filestore.NewHTTP("")
	require.Error(t, err)

	_, err = filestore.NewHTTPHandler(nil)
	require.Error(t, err)

	_, err = filestore.NewHTTP("not-a-url")
	require.Error(t, err)

	_, err = filestore.NewHTTP("/relative")
	require.Error(t, err)
}

func TestHTTPListNDJSON(t *testing.T) {
	ctx := t.Context()

	backend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)
	_, err = backend.Put(ctx, fileName, strings.NewReader(data))
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(backend)
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/?list=true&path=", nil)
	require.NoError(t, err)

	rsp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer rsp.Body.Close()

	require.Equal(t, http.StatusOK, rsp.StatusCode)
	require.Equal(t, "application/x-ndjson", rsp.Header.Get("Content-Type"))

	var files int
	scanner := bufio.NewScanner(rsp.Body)
	for scanner.Scan() {
		var rec struct {
			File  *filestore.File `json:"file"`
			Error string          `json:"error"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &rec))
		require.Empty(t, rec.Error)
		require.NotNil(t, rec.File)
		files++
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, 1, files)
}

type listErrorStore struct {
	filestore.Interface
}

func (listErrorStore) List(ctx context.Context, path string, recursive bool) (<-chan *filestore.File, <-chan error) {
	fc := make(chan *filestore.File)
	ec := make(chan error, 1)
	close(fc)
	ec <- context.DeadlineExceeded
	close(ec)
	return fc, ec
}

func TestHTTPListPropagatesError(t *testing.T) {
	handler, err := filestore.NewHTTPHandler(listErrorStore{})
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client, err := filestore.NewHTTP(srv.URL + "/")
	require.NoError(t, err)

	fileCh, errCh := client.List(t.Context(), "", false)
	for range fileCh {
	}
	err = <-errCh
	require.Error(t, err)
	require.Contains(t, err.Error(), "deadline exceeded")
}

func TestHTTPGetNotExist(t *testing.T) {
	backend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(backend)
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client, err := filestore.NewHTTP(srv.URL + "/")
	require.NoError(t, err)

	_, _, err = client.Get(t.Context(), "missing.car")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

type readErrStore struct {
	filestore.Interface
	errAfterBytes int
	err           error
}

func (s readErrStore) Get(ctx context.Context, path string) (*filestore.File, io.ReadCloser, error) {
	fl, rdr, err := s.Interface.Get(ctx, path)
	if err != nil {
		return nil, nil, err
	}

	return fl, struct {
		io.Reader
		io.Closer
	}{
		Reader: io.MultiReader(
			// Read up to errAfterBytes bytes...
			io.LimitReader(rdr, int64(s.errAfterBytes)),
			// ...then return the error.
			iotest.ErrReader(s.err),
		),
		Closer: rdr,
	}, nil
}

func TestHTTPGetPropagatesReadError(t *testing.T) {
	streamErr := errors.New("backend read failed")

	localBackend, err := filestore.NewLocal(t.TempDir())
	require.NoError(t, err)

	_, err = localBackend.Put(t.Context(), fileName, strings.NewReader(strings.Repeat("!", 100)))
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(readErrStore{
		Interface:     localBackend,
		errAfterBytes: 10,
		err:           streamErr,
	})
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client, err := filestore.NewHTTP(srv.URL + "/")
	require.NoError(t, err)

	_, rc, err := client.Get(t.Context(), fileName)
	require.NoError(t, err)

	data, err := io.ReadAll(rc)
	require.Error(t, err)
	require.Len(t, data, 10)
}
