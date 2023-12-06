package config

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPath(t *testing.T) {
	const dir = "vstore"

	var absdir string
	if runtime.GOOS == "windows" {
		absdir = "c:\\tmp\vstore"
	} else {
		absdir = "/tmp/vstore"
	}

	path, err := Path("", dir)
	require.NoError(t, err)

	configRoot, err := PathRoot()
	require.NoError(t, err)

	require.Equal(t, filepath.Join(configRoot, dir), path)

	path, err = Path("altroot", dir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join("altroot", dir), path)

	path, err = Path("altroot", absdir)
	require.NoError(t, err)
	require.Equal(t, filepath.Clean(absdir), path)
}
