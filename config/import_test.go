package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImportEnabled(t *testing.T) {
	require.False(t, Import{}.Enabled())
	require.False(t, Import{ListenAddr: "none"}.Enabled())
	require.True(t, Import{ListenAddr: "/ip4/0.0.0.0/tcp/3006"}.Enabled())
}

func TestImportReadToken(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	token, err := Import{TokenFile: write("token", " secret\n")}.ReadToken()
	require.NoError(t, err)
	require.Equal(t, "secret", token, "whitespace around the token is ignored")

	_, err = Import{TokenFile: write("empty", " \n")}.ReadToken()
	require.ErrorContains(t, err, "empty")
	_, err = Import{TokenFile: filepath.Join(dir, "missing")}.ReadToken()
	require.Error(t, err)
	_, err = Import{}.ReadToken()
	require.ErrorContains(t, err, "TokenFile")
}
