package config_test

import (
	"path/filepath"
	"testing"

	"github.com/ipni/storetheindex/gc/config"
	"github.com/stretchr/testify/require"
)

func TestSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cfgFile, err := config.Path(tmpDir, "")
	require.NoError(t, err)

	require.Equal(t, tmpDir, filepath.Dir(cfgFile), "wrong root dir")

	cfgFile, err = config.Init(cfgFile, false)
	require.NoError(t, err)

	cfg, err := config.Load(cfgFile)
	require.NoError(t, err)

	err = cfg.Save(cfgFile)
	require.NoError(t, err)

	cfg2, err := config.Load(cfgFile)
	require.NoError(t, err)

	cfgBytes, err := config.Marshal(cfg)
	require.NoError(t, err)

	cfg2Bytes, err := config.Marshal(cfg2)
	require.NoError(t, err)

	require.Equal(t, cfgBytes, cfg2Bytes, "config data different after being loaded")
}
