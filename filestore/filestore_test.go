package filestore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/ipni/storetheindex/filestore"
	"github.com/stretchr/testify/require"

	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
)

const (
	fileName           = "testfile.txt"
	fileName1          = "testfile1.txt"
	fileName2          = "testfile2.txt"
	fileName3          = "abc/testfile3.txt"
	concurrentFileName = "concurrent.txt"
	subdir             = "abc"

	data  = "hello world"
	data1 = "foo"
	data2 = "bar"
	data3 = "baz"
)

func TestLocation(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		base := t.TempDir()
		cars := filepath.Join(base, "cars")
		require.NoError(t, os.MkdirAll(cars, 0755))
		unclean := filepath.Join(base, "cars", "..", "cars")
		store, err := filestore.NewLocal(unclean)
		require.NoError(t, err)
		require.Equal(t, filepath.Clean(unclean), store.Location())
		require.Equal(t, cars, store.Location())
	})

	t.Run("http", func(t *testing.T) {
		store, err := filestore.NewHTTP("https://mirror.example.com:8443/cars/")
		require.NoError(t, err)
		require.Equal(t, "mirror.example.com:8443", store.Location())
	})

	t.Run("s3-default", func(t *testing.T) {
		store, err := filestore.NewS3("mycars", filestore.WithRegion("us-east-1"))
		require.NoError(t, err)
		require.Equal(t, "s3://mycars", store.Location())
	})

	t.Run("s3-endpoint", func(t *testing.T) {
		store, err := filestore.NewS3("mycars",
			filestore.WithEndpoint("http://127.0.0.1:4566/"),
			filestore.WithRegion("us-east-1"),
		)
		require.NoError(t, err)
		require.Equal(t, "s3://mycars@127.0.0.1:4566", store.Location())
	})
}

func TestS3(t *testing.T) {
	const bucketName = "testbucket"

	tempDir := t.TempDir()
	err := os.MkdirAll(filepath.Join(tempDir, bucketName), 0755)
	require.NoError(t, err)

	p := localstack.Preset(
		localstack.WithServices(localstack.S3),
		localstack.WithS3Files(tempDir),
	)
	localS3, err := gnomock.Start(p)
	if err != nil {
		if strings.HasPrefix(err.Error(), "can't start container") {
			t.Skip("Docker required for s3 tests")
		}
	}
	require.NoError(t, err)
	defer func() { _ = gnomock.Stop(localS3) }()

	fileStore, err := filestore.NewS3(bucketName,
		filestore.WithEndpoint(fmt.Sprintf("http://%s/", localS3.Address(localstack.APIPort))),
		filestore.WithKeys("abcd1234", "1qaz2wsx"),
		filestore.WithPageSize(5),
	)
	require.NoError(t, err)
	require.Equal(t, "s3", fileStore.Type())

	t.Run("test-S3-PutAtomic", func(t *testing.T) {
		testPutDoesNotTruncateOnError(t, fileStore)
	})

	t.Run("test-S3-Put", func(t *testing.T) {
		testPut(t, fileStore)
	})

	t.Run("test-S3-Head", func(t *testing.T) {
		testHead(t, fileStore)
	})

	t.Run("test-S3-Get", func(t *testing.T) {
		testGet(t, fileStore)
	})

	t.Run("test-S3-List", func(t *testing.T) {
		testList(t, fileStore)
	})

	t.Run("test-S3-Delete", func(t *testing.T) {
		testDelete(t, fileStore)
	})

	t.Run("test-S3-Concurrent", func(t *testing.T) {
		testConcurrent(t, fileStore)
	})
}

// splitFilestore sends write operations to write and read operations to read.
type splitFilestore struct {
	read  filestore.Interface
	write filestore.Interface
}

func newSplitFilestore(read, write filestore.Interface) splitFilestore {
	return splitFilestore{read: read, write: write}
}

func (s splitFilestore) Delete(ctx context.Context, path string) error {
	return s.write.Delete(ctx, path)
}

func (s splitFilestore) Get(ctx context.Context, path string) (*filestore.File, io.ReadCloser, error) {
	return s.read.Get(ctx, path)
}

func (s splitFilestore) Head(ctx context.Context, path string) (*filestore.File, error) {
	return s.read.Head(ctx, path)
}

func (s splitFilestore) List(ctx context.Context, path string, recursive bool) (<-chan *filestore.File, <-chan error) {
	return s.read.List(ctx, path, recursive)
}

func (s splitFilestore) Put(ctx context.Context, path string, reader io.Reader) (*filestore.File, error) {
	return s.write.Put(ctx, path, reader)
}

func (s splitFilestore) Type() string {
	return s.read.Type()
}

func (s splitFilestore) Location() string {
	return s.read.Location()
}

func setupHTTPFilestore(t *testing.T) (splitFilestore, string) {
	t.Helper()

	carDir := t.TempDir()

	backend, err := filestore.NewLocal(carDir)
	require.NoError(t, err)

	handler, err := filestore.NewHTTPHandler(backend)
	require.NoError(t, err)

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client, err := filestore.NewHTTP(srv.URL + "/")
	require.NoError(t, err)

	return newSplitFilestore(client, backend), carDir
}

func TestHTTP(t *testing.T) {
	fileStore, carDir := setupHTTPFilestore(t)
	require.Equal(t, "http", fileStore.Type())

	t.Run("test-HTTP-PutAtomic", func(t *testing.T) {
		testPutDoesNotTruncateOnError(t, fileStore)
	})

	t.Run("test-HTTP-Put", func(t *testing.T) {
		testPut(t, fileStore)
	})

	require.FileExists(t, filepath.Join(carDir, fileName))

	t.Run("test-HTTP-Head", func(t *testing.T) {
		testHead(t, fileStore)
	})

	t.Run("test-HTTP-Get", func(t *testing.T) {
		testGet(t, fileStore)
	})

	t.Run("test-HTTP-List", func(t *testing.T) {
		testList(t, fileStore)
	})

	t.Run("test-HTTP-Delete", func(t *testing.T) {
		testDelete(t, fileStore)
	})

	t.Run("test-HTTP-Concurrent", func(t *testing.T) {
		testConcurrent(t, fileStore)
	})
}

func TestLocal(t *testing.T) {
	carDir := t.TempDir()

	fileStore, err := filestore.NewLocal(carDir)
	require.NoError(t, err)
	require.Equal(t, "local", fileStore.Type())

	t.Run("test-Local-PutAtomic", func(t *testing.T) {
		testPutDoesNotTruncateOnError(t, fileStore)

		entries, err := os.ReadDir(carDir)
		require.NoError(t, err)

		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}

		require.ElementsMatch(t, []string{
			fileName,
			filestore.ConfigMetadataFileName,
		}, names)
	})

	t.Run("test-Local-Put", func(t *testing.T) {
		testPut(t, fileStore)
	})

	require.FileExists(t, filepath.Join(carDir, fileName))

	t.Run("test-Local-Head", func(t *testing.T) {
		testHead(t, fileStore)
	})

	t.Run("test-Local-Get", func(t *testing.T) {
		testGet(t, fileStore)
	})

	t.Run("test-Local-List", func(t *testing.T) {
		testList(t, fileStore)
	})

	t.Run("test-Local-Delete", func(t *testing.T) {
		testDelete(t, fileStore)
	})

	t.Run("test-Local-Concurrent", func(t *testing.T) {
		testConcurrent(t, fileStore)
	})
}

func TestLocalWithPathSplit(t *testing.T) {
	carDir := t.TempDir()

	fileStore, err := filestore.NewLocal(carDir, filestore.WithDefaultPathSplit(2, 1))
	require.NoError(t, err)
	require.Equal(t, "local", fileStore.Type())

	t.Run("test-Local-PutAtomic", func(t *testing.T) {
		testPutDoesNotTruncateOnError(t, fileStore)
	})

	t.Run("test-Local-Put", func(t *testing.T) {
		testPut(t, fileStore)
	})

	require.FileExists(t,
		filepath.Join(carDir, filestore.ConfigMetadataFileName),
		"path split requires metadata file",
	)
	require.FileExists(t, filepath.Join(carDir, fileName[:2], fileName[2:3], fileName))

	t.Run("test-Local-Head", func(t *testing.T) {
		testHead(t, fileStore)
	})

	t.Run("test-Local-Get", func(t *testing.T) {
		testGet(t, fileStore)
	})

	// Some extra bogus filesystem entries that should be skipped
	for data, fName := range map[string]string{
		"no-path-prefix":              filepath.Join(carDir, fileName),
		"wrong-path-prefix":           filepath.Join(carDir, fileName[:2], fileName),
		"prefix-not-on-path-boundary": filepath.Join(carDir, "corner-"+fileName[:2], fileName[2:3], fileName),
	} {
		require.NoError(t, os.MkdirAll(filepath.Dir(fName), 0700))
		require.NoError(t, os.WriteFile(fName, []byte(data), 0600))
	}

	t.Run("test-Local-List", func(t *testing.T) {
		testList(t, fileStore)
	})

	t.Run("test-Local-Delete", func(t *testing.T) {
		testDelete(t, fileStore)
	})

	t.Run("test-Local-Concurrent", func(t *testing.T) {
		testConcurrent(t, fileStore)
	})
}

func TestLocalMetadata(t *testing.T) {
	t.Run("legacy format detection", func(t *testing.T) {
		carDir := t.TempDir()

		// Prepare legacy, flat structure
		require.NoError(t, os.WriteFile(filepath.Join(carDir, fileName), []byte(data), 0666))
		require.NoError(t, os.WriteFile(filepath.Join(carDir, fileName1), []byte(data1), 0666))

		// Create filestore with default path split, it should be overwritten though
		fileStore, err := filestore.NewLocal(carDir, filestore.WithDefaultPathSplit(2, 1))
		require.NoError(t, err)

		fi, err := fileStore.Head(t.Context(), fileName)
		require.NoError(t, err)
		require.NotNil(t, fi)

		fi, err = fileStore.Put(t.Context(), fileName2, strings.NewReader(data2))
		require.NoError(t, err)
		require.NotNil(t, fi)

		require.FileExists(t, filepath.Join(carDir, fileName2))
		require.NoFileExists(t, filepath.Join(carDir, filestore.ConfigMetadataFileName))
	})

	t.Run("reopen local filestore with metadata", func(t *testing.T) {
		carDir := t.TempDir()

		_, err := filestore.NewLocal(carDir, filestore.WithDefaultPathSplit(2, 1))
		require.NoError(t, err)

		// Reopen must read the metadata file and read config from it
		fileStore, err := filestore.NewLocal(carDir)
		require.NoError(t, err)

		require.FileExists(t, filepath.Join(carDir, filestore.ConfigMetadataFileName))

		_, err = fileStore.Put(t.Context(), fileName, strings.NewReader(data))
		require.NoError(t, err)

		require.FileExists(t, filepath.Join(carDir, fileName[:2], fileName[2:3], fileName))
	})

	t.Run("invalid metadata file", func(t *testing.T) {
		carDir := t.TempDir()

		require.NoError(t, os.WriteFile(
			filepath.Join(carDir, filestore.ConfigMetadataFileName),
			[]byte("not a valid json file"),
			0666,
		))

		_, err := filestore.NewLocal(carDir)
		require.ErrorContains(t, err, "failed to decode filestore configuration file")

		require.NoError(t, os.WriteFile(
			filepath.Join(carDir, filestore.ConfigMetadataFileName),
			[]byte(`{"Version": "bogus"}`),
			0666,
		))

		_, err = filestore.NewLocal(carDir)
		require.ErrorContains(t, err, "invalid filestore configuration file")
		require.ErrorContains(t, err, "unknown version")

		require.NoError(t, os.WriteFile(
			filepath.Join(carDir, filestore.ConfigMetadataFileName),
			[]byte(`{"Version": "v1", "PathSplit": [-1]}`),
			0666,
		))

		_, err = filestore.NewLocal(carDir)
		require.ErrorContains(t, err, "invalid filestore configuration file")
		require.ErrorContains(t, err, "invalid path split config")
	})
}

func TestMakeFilestore(t *testing.T) {
	cfg := filestore.Config{
		Type: "none",
	}
	fs, err := filestore.MakeFilestore(cfg)
	require.NoError(t, err)
	require.Nil(t, fs)

	cfg.Type = "unknown"
	_, err = filestore.MakeFilestore(cfg)
	require.ErrorContains(t, err, "unsupported")

	cfg.Type = ""
	_, err = filestore.MakeFilestore(cfg)
	require.ErrorContains(t, err, "not defined")

	cfg.Type = "local"
	_, err = filestore.MakeFilestore(cfg)
	require.ErrorContains(t, err, "base path")

	cfg.Local.BasePath = t.TempDir()
	fs, err = filestore.MakeFilestore(cfg)
	require.NoError(t, err)
	require.NotNil(t, fs)

	cfg.Local.DefaultPathSplit = []int{0, -1}
	_, err = filestore.MakeFilestore(cfg)
	require.ErrorContains(t, err, "invalid path split")

	cfg.Local.DefaultPathSplit = []int{7, 5}
	fs, err = filestore.MakeFilestore(cfg)
	require.NoError(t, err)
	require.NotNil(t, fs)
}

func testPut(t *testing.T, fileStore filestore.Interface) {
	fileInfo, err := fileStore.Put(t.Context(), fileName, strings.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, fileName, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
}

func testPutDoesNotTruncateOnError(t *testing.T, fileStore filestore.Interface) {
	ctx := t.Context()
	failing := func() io.Reader {
		return io.MultiReader(
			io.LimitReader(strings.NewReader(data), 3),
			iotest.ErrReader(errors.New("injected read failure")),
		)
	}

	_, err := fileStore.Put(ctx, fileName, failing())
	require.Error(t, err)
	_, _, err = fileStore.Get(ctx, fileName)
	require.ErrorIs(t, err, fs.ErrNotExist)

	_, err = fileStore.Put(ctx, fileName, strings.NewReader(data))
	require.NoError(t, err)

	_, err = fileStore.Put(ctx, fileName, failing())
	require.Error(t, err)

	fileInfo, rc, err := fileStore.Get(ctx, fileName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, data, string(got))
	require.Equal(t, int64(len(data)), fileInfo.Size)
}

func testConcurrent(t *testing.T, store filestore.Interface) {
	t.Run("Puts", func(t *testing.T) {
		testConcurrentPuts(t, store)
	})
	t.Run("GetWhilePutInProgress", func(t *testing.T) {
		testGetWhilePutInProgress(t, store)
	})
	t.Run("PutsWithReaders", func(t *testing.T) {
		testConcurrentPutsWithReaders(t, store)
	})
}

func testConcurrentPuts(t *testing.T, store filestore.Interface) {
	ctx := t.Context()
	const size = 32 << 10
	const writers = 8

	_, err := store.Put(ctx, concurrentFileName, bytes.NewReader(bytes.Repeat([]byte{0xff}, size)))
	require.NoError(t, err)

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	addErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	for i := range writers {
		b := byte(i + 1)
		wg.Go(func() {
			<-start
			payload := bytes.Repeat([]byte{b}, size)
			_, err := store.Put(ctx, concurrentFileName, bytes.NewReader(payload))
			addErr(err)
		})
	}

	close(start)
	wg.Wait()
	require.Empty(t, errs)

	got := readAll(t, store, concurrentFileName)
	require.Len(t, got, size)
	require.True(t, bytes.Equal(got, bytes.Repeat([]byte{got[0]}, size)), "result is not a complete single payload")
	require.GreaterOrEqual(t, got[0], byte(1))
	require.LessOrEqual(t, got[0], byte(writers))
}

func testGetWhilePutInProgress(t *testing.T, store filestore.Interface) {
	ctx := t.Context()
	old := bytes.Repeat([]byte{'A'}, 4096)
	next := bytes.Repeat([]byte{'B'}, 8192)

	_, err := store.Put(ctx, concurrentFileName, bytes.NewReader(old))
	require.NoError(t, err)

	started := make(chan struct{})
	goAhead := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		_, err := store.Put(ctx, concurrentFileName, &stallOnFirstRead{
			payload: next,
			started: started,
			goAhead: goAhead,
		})
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Put did not start copying")
	}

	for range 20 {
		require.Equal(t, old, readAll(t, store, concurrentFileName))
		info, err := store.Head(ctx, concurrentFileName)
		require.NoError(t, err)
		require.Equal(t, int64(len(old)), info.Size)
	}

	close(goAhead)
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Put did not finish")
	}
	require.Equal(t, next, readAll(t, store, concurrentFileName))
}

func testConcurrentPutsWithReaders(t *testing.T, store filestore.Interface) {
	ctx := t.Context()
	const size = 32 << 10
	const writers = 8
	const readers = 4

	_, err := store.Put(ctx, concurrentFileName, bytes.NewReader(bytes.Repeat([]byte{0xff}, size)))
	require.NoError(t, err)

	start := make(chan struct{})
	stopReads := make(chan struct{})
	var writersWG, readersWG sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	addErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	for i := range writers {
		b := byte(i + 1)
		writersWG.Go(func() {
			<-start
			payload := bytes.Repeat([]byte{b}, size)
			_, err := store.Put(ctx, concurrentFileName, bytes.NewReader(payload))
			addErr(err)
		})
	}

	validByte := func(b byte) bool {
		return b == 0xff || (b >= 1 && b <= byte(writers))
	}
	for range readers {
		readersWG.Go(func() {
			<-start
			for {
				select {
				case <-stopReads:
					return
				default:
				}
				got, err := tryReadAll(ctx, store, concurrentFileName)
				if err != nil {
					addErr(err)
					return
				}
				if len(got) != size {
					addErr(fmt.Errorf("read length %d, want %d", len(got), size))
					return
				}
				if !bytes.Equal(got, bytes.Repeat([]byte{got[0]}, size)) {
					addErr(fmt.Errorf("mixed or truncated payload, first=%d", got[0]))
					return
				}
				if !validByte(got[0]) {
					addErr(fmt.Errorf("unexpected payload byte %d", got[0]))
					return
				}
			}
		})
	}

	close(start)
	writersWG.Wait()
	close(stopReads)
	readersWG.Wait()
	require.Empty(t, errs)

	got := readAll(t, store, concurrentFileName)
	require.Len(t, got, size)
	require.True(t, bytes.Equal(got, bytes.Repeat([]byte{got[0]}, size)))
	require.True(t, validByte(got[0]))
}

func readAll(t *testing.T, store filestore.Interface, name string) []byte {
	t.Helper()
	got, err := tryReadAll(t.Context(), store, name)
	require.NoError(t, err)
	return got
}

func tryReadAll(ctx context.Context, store filestore.Interface, name string) ([]byte, error) {
	_, rc, err := store.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

type stallOnFirstRead struct {
	payload []byte
	off     int
	once    sync.Once
	started chan struct{}
	goAhead <-chan struct{}
}

func (s *stallOnFirstRead) Read(p []byte) (int, error) {
	if s.off == 0 {
		s.once.Do(func() { close(s.started) })
		<-s.goAhead
	}
	if s.off >= len(s.payload) {
		return 0, io.EOF
	}
	n := copy(p, s.payload[s.off:])
	s.off += n
	return n, nil
}

func testHead(t *testing.T, fileStore filestore.Interface) {
	// Get file that does not exist.
	fileInfo, err := fileStore.Head(t.Context(), "not-here")
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Nil(t, fileInfo)

	_, err = fileStore.Put(t.Context(), fileName3, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, err = fileStore.Head(t.Context(), fileName3)
	require.NoError(t, err)
	require.Equal(t, fileName3, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
	require.False(t, fileInfo.Modified.IsZero())

	// Should get fs.ErrNotExist when looking for subdirectory.
	_, err = fileStore.Head(t.Context(), subdir)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func testGet(t *testing.T, fileStore filestore.Interface) {
	// Get file that does not exist.
	fileInfo, _, err := fileStore.Get(t.Context(), "not-here")
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Nil(t, fileInfo)

	_, err = fileStore.Put(t.Context(), fileName, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, r, err := fileStore.Get(t.Context(), fileName)
	require.NoError(t, err)
	require.Equal(t, fileName, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
	require.False(t, fileInfo.Modified.IsZero())

	data2 := make([]byte, len(data))
	n, err := r.Read(data2)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, []byte(data), data2)
	_, err = r.Read(data2)
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, r.Close())

	_, err = fileStore.Put(t.Context(), fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	_, _, err = fileStore.Get(t.Context(), subdir)
	require.ErrorIs(t, err, fs.ErrNotExist)

	fileInfo, r, err = fileStore.Get(t.Context(), fileName3)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, int64(len(data3)), fileInfo.Size)
}

func testList(t *testing.T, fileStore filestore.Interface) {
	// List file that does not exist.
	fileCh, errCh := fileStore.List(t.Context(), "not-here/", false)
	fileInfo, ok := <-fileCh
	require.Nil(t, fileInfo)
	require.False(t, ok)
	err := <-errCh
	require.NoError(t, err)

	_, err = fileStore.Put(t.Context(), fileName1, strings.NewReader(data1))
	require.NoError(t, err)

	_, err = fileStore.Put(t.Context(), fileName2, strings.NewReader(data2))
	require.NoError(t, err)

	_, err = fileStore.Put(t.Context(), fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	t.Run("list non-recursively", func(t *testing.T) {
		fileCh, errCh = fileStore.List(t.Context(), "", false)
		infos := make([]*filestore.File, 0, 3)
		for fileInfo := range fileCh {
			infos = append(infos, fileInfo)
		}
		err = <-errCh
		require.NoError(t, err)
		require.Equal(t, 3, len(infos))
		expectNames := []string{fileName, fileName1, fileName2}
		expectSizes := []int64{int64(len(data)), int64(len(data1)), int64(len(data2))}
		for i := range infos {
			require.Equal(t, expectNames[i], infos[i].Path)
			require.Equal(t, expectSizes[i], infos[i].Size)
			require.False(t, infos[0].Modified.IsZero())
		}
	})

	t.Run("list recursively", func(t *testing.T) {
		fileCh, errCh = fileStore.List(t.Context(), "", true)
		infos := make([]*filestore.File, 0, 3)
		for fileInfo := range fileCh {
			infos = append(infos, fileInfo)
		}
		err = <-errCh
		require.NoError(t, err)
		require.Equal(t, 4, len(infos))
		require.Equal(t, fileName3, infos[0].Path)
		require.Equal(t, int64(len(data3)), infos[0].Size)
	})

	t.Run("specific file", func(t *testing.T) {
		fileCh, errCh = fileStore.List(t.Context(), fileName1, false)
		infos := make([]*filestore.File, 0, 3)
		for fileInfo := range fileCh {
			infos = append(infos, fileInfo)
		}
		err = <-errCh
		require.NoError(t, err)
		require.Equal(t, 1, len(infos))
		require.Equal(t, fileName1, infos[0].Path)
	})

	t.Run("specific file at a sub-dir", func(t *testing.T) {
		fileCh, errCh = fileStore.List(t.Context(), fileName3, false)
		infos := make([]*filestore.File, 0, 3)
		for fileInfo := range fileCh {
			infos = append(infos, fileInfo)
		}
		err = <-errCh
		require.NoError(t, err)
		require.Equal(t, 1, len(infos))
		require.Equal(t, fileName3, infos[0].Path)
	})

	t.Run("list files in a sub-folder", func(t *testing.T) {
		fileCh, errCh = fileStore.List(t.Context(), subdir+"/", false)
		infos := make([]*filestore.File, 0, 3)
		for fileInfo := range fileCh {
			infos = append(infos, fileInfo)
		}
		err = <-errCh
		require.NoError(t, err)
		require.Equal(t, 1, len(infos))
		require.Equal(t, fileName3, infos[0].Path)
	})

	t.Run("list larger dataset", func(t *testing.T) {
		const entriesCount = 20
		const path = "largeset/"

		fileNames := make([]string, 0, entriesCount)
		fileContents := make([]string, 0, entriesCount)
		for i := range entriesCount {
			fileNames = append(fileNames, fmt.Sprintf("%stestfile-%05d.txt", path, i))
			fileContents = append(fileContents, fmt.Sprintf("data-%05d", i))
		}

		for i := range entriesCount {
			_, err = fileStore.Put(t.Context(), fileNames[i], strings.NewReader(fileContents[i]))
			require.NoError(t, err)
		}

		fileCh, errCh = fileStore.List(t.Context(), path, false)
		infos := make([]*filestore.File, 0, entriesCount)
		for fileInfo := range fileCh {
			infos = append(infos, fileInfo)
		}
		err = <-errCh
		require.NoError(t, err)
		require.Equal(t, entriesCount, len(infos))

		fetchedFileNames := make([]string, 0, entriesCount)

		for i := range infos {
			fetchedFileNames = append(fetchedFileNames, infos[i].Path)
		}

		require.ElementsMatch(t, fileNames, fetchedFileNames)
	})
}

func testDelete(t *testing.T, fileStore filestore.Interface) {
	ctx := t.Context()

	_, err := fileStore.Put(ctx, fileName1, strings.NewReader(data1))
	require.NoError(t, err)

	_, err = fileStore.Put(ctx, fileName2, strings.NewReader(data2))
	require.NoError(t, err)

	_, err = fileStore.Put(ctx, fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	// File exists before delete.
	_, err = fileStore.Head(ctx, fileName1)
	require.NoError(t, err)

	err = fileStore.Delete(ctx, fileName1)
	require.NoError(t, err)

	// File gone after delete.
	_, err = fileStore.Head(ctx, fileName1)
	require.ErrorIs(t, err, fs.ErrNotExist)

	// Delete non-existant file should be OK.
	err = fileStore.Delete(ctx, fileName1)
	require.NoError(t, err)

	err = fileStore.Delete(ctx, fileName2)
	require.NoError(t, err)

	err = fileStore.Delete(ctx, fileName3)
	require.NoError(t, err)
}
