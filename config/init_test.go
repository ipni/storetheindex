package config

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"testing"

	crypto_pb "github.com/libp2p/go-libp2p/core/crypto/pb"
	"github.com/stretchr/testify/require"
)

func TestCreateIdentity(t *testing.T) {
	id, err := CreateIdentity(io.Discard)
	require.NoError(t, err)
	pk, err := id.DecodePrivateKey("")
	require.NoError(t, err)
	require.Equal(t, crypto_pb.KeyType_Ed25519, pk.Type())
}

func TestMarshalUnmarshal(t *testing.T) {
	cfg, err := Init(io.Discard)
	require.NoError(t, err)

	b, err := json.MarshalIndent(&cfg, "  ", "  ")
	require.NoError(t, err)

	t.Log("default config:\n", string(b))

	cfg2 := Config{}
	err = json.Unmarshal(b, &cfg2)
	require.NoError(t, err)

	require.Equal(t, cfg.Identity.PeerID, cfg2.Identity.PeerID)
	require.Equal(t, cfg.Identity.PrivKey, cfg2.Identity.PrivKey)
}

func TestSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cfgFile, err := Path(tmpDir, "")
	require.NoError(t, err)

	require.Equal(t, tmpDir, filepath.Dir(cfgFile), "wrong root dir")

	cfg, err := Init(io.Discard)
	require.NoError(t, err)
	cfgBytes, err := Marshal(cfg)
	require.NoError(t, err)

	err = cfg.Save(cfgFile)
	require.NoError(t, err)

	cfg2, err := Load(cfgFile)
	require.NoError(t, err)
	cfg2Bytes, err := Marshal(cfg2)
	require.NoError(t, err)

	require.True(t, bytes.Equal(cfgBytes, cfg2Bytes), "config data different after being loaded")
}
