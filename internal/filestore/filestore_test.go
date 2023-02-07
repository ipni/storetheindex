package filestore_test

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil"
	"github.com/ipni/storetheindex/internal/filestore"
	"github.com/stretchr/testify/require"
)

func TestLocalPut(t *testing.T) {
	const fileName = "testfile.txt"

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	ls, err := filestore.New(cfg)
	require.NoError(t, err)
	_, ok := ls.(*filestore.Local)
	require.True(t, ok)
	require.Equal(t, "local", ls.Type())

	data := "hello world"
	fileInfo, err := ls.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)

	require.True(t, fsutil.FileExists(filepath.Join(carDir, fileName)))
	require.Equal(t, fileName, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)

	roDir := filepath.Join(carDir, "readonly")
	require.NoError(t, os.Mkdir(roDir, 0500))

	// Check for error with non-writable directory.
	cfg.Local.BasePath = roDir
	ls, err = filestore.New(cfg)
	require.ErrorIs(t, err, fs.ErrPermission)
}

func TestLocalGet(t *testing.T) {
	const fileName = "testfile.txt"

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	ls, err := filestore.New(cfg)
	require.NoError(t, err)

	// Get file that does not exist.
	fileInfo, _, err := ls.Get(context.Background(), fileName)
	require.ErrorIs(t, err, filestore.ErrNotFound)
	require.Nil(t, fileInfo)

	data := "hello world"
	fileInfo, err = ls.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, r, err := ls.Get(context.Background(), fileName)
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

	subdir := "abc"
	subName := filepath.Join(subdir, fileName)
	fileInfo, err = ls.Put(context.Background(), subName, strings.NewReader(data))
	require.NoError(t, err)

	_, _, err = ls.Get(context.Background(), subdir)
	require.ErrorIs(t, err, filestore.ErrNotFound)
}

func TestLocalHead(t *testing.T) {
	const fileName = "abc/testfile.txt"

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	ls, err := filestore.New(cfg)
	require.NoError(t, err)

	// Get file that does not exist.
	fileInfo, err := ls.Head(context.Background(), fileName)
	require.ErrorIs(t, err, filestore.ErrNotFound)
	require.Nil(t, fileInfo)

	data := "hello world"
	fileInfo, err = ls.Put(context.Background(), fileName, strings.NewReader(data))
	require.NoError(t, err)

	fileInfo, err = ls.Head(context.Background(), fileName)
	require.NoError(t, err)
	require.Equal(t, fileName, fileInfo.Path)
	require.Equal(t, int64(len(data)), fileInfo.Size)
	require.False(t, fileInfo.Modified.IsZero())

	// Should get ErrNotFound when looking for subdirectory.
	fileInfo, err = ls.Head(context.Background(), "abc")
	require.ErrorIs(t, err, filestore.ErrNotFound)
}

func TestLocalList(t *testing.T) {
	const (
		fileName1 = "testfile1.txt"
		fileName2 = "testfile2.txt"
		fileName3 = "abc/testfile3.txt"
	)

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	ls, err := filestore.New(cfg)
	require.NoError(t, err)

	// Get file that does not exist.
	fileCh, errCh := ls.List(context.Background(), "/not-here", false)
	fileInfo, ok := <-fileCh
	require.Nil(t, fileInfo)
	require.False(t, ok)
	err = <-errCh
	require.ErrorIs(t, err, filestore.ErrNotFound)

	data0 := "hello world"
	_, err = ls.Put(context.Background(), fileName1, strings.NewReader(data0))
	require.NoError(t, err)

	data1 := "foo"
	_, err = ls.Put(context.Background(), fileName2, strings.NewReader(data1))
	require.NoError(t, err)

	data2 := "bar"
	_, err = ls.Put(context.Background(), fileName3, strings.NewReader(data2))
	require.NoError(t, err)

	fileCh, errCh = ls.List(context.Background(), "", false)
	infos := make([]*filestore.File, 0, 3)
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 2, len(infos))
	require.Equal(t, fileName1, infos[0].Path)
	require.Equal(t, int64(len(data0)), infos[0].Size)
	require.False(t, infos[0].Modified.IsZero())
	require.Equal(t, fileName2, infos[1].Path)
	require.Equal(t, int64(len(data1)), infos[1].Size)
	require.False(t, infos[1].Modified.IsZero())

	fileCh, errCh = ls.List(context.Background(), "", true)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 3, len(infos))
	require.Equal(t, fileName3, infos[0].Path)
	require.Equal(t, int64(len(data2)), infos[0].Size)

	// File specific file.
	fileCh, errCh = ls.List(context.Background(), fileName1, false)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	require.Equal(t, fileName1, infos[0].Path)

	// File specific file.
	fileCh, errCh = ls.List(context.Background(), fileName3, false)
	infos = infos[:0]
	for fileInfo := range fileCh {
		infos = append(infos, fileInfo)
	}
	err = <-errCh
	require.NoError(t, err)
	require.Equal(t, 1, len(infos))
	require.Equal(t, fileName3, infos[0].Path)
}

func TestLocalDelete(t *testing.T) {
	const (
		fileName1 = "testfile1.txt"
		fileName2 = "testfile2.txt"
		fileName3 = "abc/testfile3.txt"
	)

	carDir := t.TempDir()
	cfg := config.FileStore{
		Type: "local",
		Local: config.LocalFileStore{
			BasePath: carDir,
		},
	}

	ls, err := filestore.New(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	data0 := "hello world"
	_, err = ls.Put(ctx, fileName1, strings.NewReader(data0))
	require.NoError(t, err)

	data1 := "foo"
	_, err = ls.Put(ctx, fileName2, strings.NewReader(data1))
	require.NoError(t, err)

	data2 := "bar"
	_, err = ls.Put(ctx, fileName3, strings.NewReader(data2))
	require.NoError(t, err)

	// File exists before delet.
	_, err = ls.Head(ctx, fileName1)
	require.NoError(t, err)

	err = ls.Delete(ctx, fileName1)
	require.NoError(t, err)

	// File gone after delete.
	_, err = ls.Head(ctx, fileName1)
	require.ErrorIs(t, err, filestore.ErrNotFound)

	// Delete non-existant file should be OK.
	err = ls.Delete(ctx, fileName1)
	require.NoError(t, err)

	// Delete non-empty directory should fail.
	err = ls.Delete(ctx, "abc")
	require.Error(t, err)
}
