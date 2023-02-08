package filestore_test

import (
	"context"
	//"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/ipni/storetheindex/internal/filestore"
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
	err := os.MkdirAll(fmt.Sprintf("./%s/%s", tempDir, bucketName), 0755)
	require.NoError(t, err)

	p := localstack.Preset(
		localstack.WithServices(localstack.S3),
		localstack.WithS3Files(fmt.Sprintf("./%s", tempDir)),
	)
	localS3, err := gnomock.Start(p)
	if err != nil {
		if strings.HasPrefix(err.Error(), "can't start container") {
			t.Skip("Docker required for s3 tests")
		}
	}
	require.NoError(t, err)
	defer func() { _ = gnomock.Stop(localS3) }()

	cfg := config.FileStore{
		Type: "s3",
		S3: config.S3FileStore{
			BucketName: bucketName,
			Endpoint:   fmt.Sprintf("http://%s/", localS3.Address(localstack.APIPort)),
			AccessKey:  "abcd1234",
			SecretKey:  "1qaz2wsx",
		},
	}

	t.Run("test-S3-Put", func(t *testing.T) {
		testPut(t, cfg)
	})

	t.Run("test-S3-Head", func(t *testing.T) {
		testHead(t, cfg)
	})

	t.Run("test-S3-Get", func(t *testing.T) {
		testGet(t, cfg)
	})

	t.Run("test-S3-List", func(t *testing.T) {
		testList(t, cfg)
	})

	t.Run("test-S3-Delete", func(t *testing.T) {
		testDelete(t, cfg)
	})
}

func TestLocal(t *testing.T) {
	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	t.Run("test-Local-Put", func(t *testing.T) {
		testPut(t, cfg)
	})

	require.True(t, fsutil.FileExists(filepath.Join(carDir, fileName)))

	t.Run("test-Local-Head", func(t *testing.T) {
		testHead(t, cfg)
	})

	t.Run("test-Local-Get", func(t *testing.T) {
		testGet(t, cfg)
	})

	t.Run("test-Local-List", func(t *testing.T) {
		testList(t, cfg)
	})

	t.Run("test-Local-Delete", func(t *testing.T) {
		testDelete(t, cfg)
	})
}

func testPut(t *testing.T, cfg config.FileStore) {
	fs, err := filestore.New(cfg)
	require.NoError(t, err)
	require.Equal(t, cfg.Type, fs.Type())

	fileInfo, err := fs.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, fileName, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
}

func testHead(t *testing.T, cfg config.FileStore) {
	fs, err := filestore.New(cfg)
	require.NoError(t, err)

	// Get file that does not exist.
	fileInfo, err := fs.Head(context.Background(), "not-here")
	require.ErrorIs(t, err, filestore.ErrNotFound)
	require.Nil(t, fileInfo)

	_, err = fs.Put(context.Background(), fileName3, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, err = fs.Head(context.Background(), fileName3)
	require.NoError(t, err)
	require.Equal(t, fileName3, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
	require.False(t, fileInfo.Modified.IsZero())

	// Should get ErrNotFound when looking for subdirectory.
	_, err = fs.Head(context.Background(), subdir)
	require.ErrorIs(t, err, filestore.ErrNotFound)
}

func testGet(t *testing.T, cfg config.FileStore) {
	fs, err := filestore.New(cfg)
	require.NoError(t, err)

	// Get file that does not exist.
	fileInfo, _, err := fs.Get(context.Background(), "not-here")
	require.ErrorIs(t, err, filestore.ErrNotFound)
	require.Nil(t, fileInfo)

	_, err = fs.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, r, err := fs.Get(context.Background(), fileName)
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

	_, err = fs.Put(context.Background(), fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	_, _, err = fs.Get(context.Background(), subdir)
	require.ErrorIs(t, err, filestore.ErrNotFound)

	fileInfo, r, err = fs.Get(context.Background(), fileName3)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, int64(len(data3)), fileInfo.Size)
}

func testList(t *testing.T, cfg config.FileStore) {
	fs, err := filestore.New(cfg)
	require.NoError(t, err)

	// List file that does not exist.
	fileCh, errCh := fs.List(context.Background(), "not-here/", false)
	fileInfo, ok := <-fileCh
	require.Nil(t, fileInfo)
	require.False(t, ok)
	err = <-errCh
	require.ErrorIs(t, err, filestore.ErrNotFound)

	_, err = fs.Put(context.Background(), fileName1, strings.NewReader(data1))
	require.NoError(t, err)

	_, err = fs.Put(context.Background(), fileName2, strings.NewReader(data2))
	require.NoError(t, err)

	_, err = fs.Put(context.Background(), fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	fileCh, errCh = fs.List(context.Background(), "", false)
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

	fileCh, errCh = fs.List(context.Background(), "", true)
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
	fileCh, errCh = fs.List(context.Background(), fileName1, false)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	require.Equal(t, fileName1, infos[0].Path)

	// File specific file.
	fileCh, errCh = fs.List(context.Background(), fileName3, false)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	require.Equal(t, fileName3, infos[0].Path)
}

func testDelete(t *testing.T, cfg config.FileStore) {
	fs, err := filestore.New(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	_, err = fs.Put(ctx, fileName1, strings.NewReader(data1))
	require.NoError(t, err)

	_, err = fs.Put(ctx, fileName2, strings.NewReader(data2))
	require.NoError(t, err)

	_, err = fs.Put(ctx, fileName3, strings.NewReader(data3))
	require.NoError(t, err)

	// File exists before delet.
	_, err = fs.Head(ctx, fileName1)
	require.NoError(t, err)

	err = fs.Delete(ctx, fileName1)
	require.NoError(t, err)

	// File gone after delete.
	_, err = fs.Head(ctx, fileName1)
	require.ErrorIs(t, err, filestore.ErrNotFound)

	// Delete non-existant file should be OK.
	err = fs.Delete(ctx, fileName1)
	require.NoError(t, err)

	err = fs.Delete(ctx, fileName2)
	require.NoError(t, err)

	err = fs.Delete(ctx, fileName3)
	require.NoError(t, err)

	// Delete empty directory should be ok.
	err = fs.Delete(ctx, subdir)
	require.NoError(t, err)
}
