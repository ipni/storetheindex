package filestore_test

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipni/storetheindex/filestore"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/stretchr/testify/require"

	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
)

const (
	fileName  = "testfile.txt"
	fileName1 = "testfile1.txt"
	fileName2 = "testfile2.txt"
	fileName3 = "abc/testfile3.txt"
	subdir    = "abc"

	data  = "hello world"
	data1 = "foo"
	data2 = "bar"
	data3 = "baz"
)

func TestS3(t *testing.T) {
	const bucketName = "testbucket"

	tempDir := t.TempDir()
	err := os.MkdirAll(fmt.Sprintf("%s/%s", tempDir, bucketName), 0755)
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
		filestore.WithKeys("abcd1234", "1qaz2wsx"))
	require.NoError(t, err)
	require.Equal(t, "s3", fileStore.Type())

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
}

func TestLocal(t *testing.T) {
	carDir := t.TempDir()

	fileStore, err := filestore.NewLocal(carDir)
	require.NoError(t, err)
	require.Equal(t, "local", fileStore.Type())

	t.Run("test-Local-Put", func(t *testing.T) {
		testPut(t, fileStore)
	})

	require.True(t, fsutil.FileExists(filepath.Join(carDir, fileName)))

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
}

func testPut(t *testing.T, fileStore filestore.Interface) {
	fileInfo, err := fileStore.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, fileName, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
}

func testHead(t *testing.T, fileStore filestore.Interface) {
	// Get file that does not exist.
	fileInfo, err := fileStore.Head(context.Background(), "not-here")
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Nil(t, fileInfo)

	_, err = fileStore.Put(context.Background(), fileName3, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, err = fileStore.Head(context.Background(), fileName3)
	require.NoError(t, err)
	require.Equal(t, fileName3, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
	require.False(t, fileInfo.Modified.IsZero())

	// Should get fs.ErrNotExist when looking for subdirectory.
	_, err = fileStore.Head(context.Background(), subdir)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func testGet(t *testing.T, fileStore filestore.Interface) {
	// Get file that does not exist.
	fileInfo, _, err := fileStore.Get(context.Background(), "not-here")
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Nil(t, fileInfo)

	_, err = fileStore.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, r, err := fileStore.Get(context.Background(), fileName)
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

	_, err = fileStore.Put(context.Background(), fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	_, _, err = fileStore.Get(context.Background(), subdir)
	require.ErrorIs(t, err, fs.ErrNotExist)

	fileInfo, r, err = fileStore.Get(context.Background(), fileName3)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, int64(len(data3)), fileInfo.Size)
}

func testList(t *testing.T, fileStore filestore.Interface) {
	// List file that does not exist.
	fileCh, errCh := fileStore.List(context.Background(), "not-here/", false)
	fileInfo, ok := <-fileCh
	require.Nil(t, fileInfo)
	require.False(t, ok)
	err := <-errCh
	require.NoError(t, err)

	_, err = fileStore.Put(context.Background(), fileName1, strings.NewReader(data1))
	require.NoError(t, err)

	_, err = fileStore.Put(context.Background(), fileName2, strings.NewReader(data2))
	require.NoError(t, err)

	_, err = fileStore.Put(context.Background(), fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	fileCh, errCh = fileStore.List(context.Background(), "", false)
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

	fileCh, errCh = fileStore.List(context.Background(), "", true)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 4, len(infos))
	require.Equal(t, fileName3, infos[0].Path)
	require.Equal(t, int64(len(data3)), infos[0].Size)

	// File specific file.
	fileCh, errCh = fileStore.List(context.Background(), fileName1, false)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	require.Equal(t, fileName1, infos[0].Path)

	// File specific file.
	fileCh, errCh = fileStore.List(context.Background(), fileName3, false)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	require.Equal(t, fileName3, infos[0].Path)
}

func testDelete(t *testing.T, fileStore filestore.Interface) {
	ctx := context.Background()

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
