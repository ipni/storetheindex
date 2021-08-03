package config

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"testing"

	crypto_pb "github.com/libp2p/go-libp2p-core/crypto/pb"
)

func TestCreateIdentity(t *testing.T) {
	id, err := CreateIdentity(ioutil.Discard)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := id.DecodePrivateKey("")
	if err != nil {
		t.Fatal(err)
	}
	if pk.Type() != crypto_pb.KeyType_Ed25519 {
		t.Fatal("unexpected type:", pk.Type())
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	cfg, err := Init(ioutil.Discard)
	if err != nil {
		t.Fatal(err)
	}

	b, err := json.MarshalIndent(&cfg, "  ", "  ")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("default config:\n", string(b))

	cfg2 := Config{}
	if err = json.Unmarshal(b, &cfg2); err != nil {
		t.Fatal(err)
	}

	if cfg.Identity.PeerID != cfg2.Identity.PeerID {
		t.Fatal("identity no same")
	}
	if cfg.Identity.PrivKey != cfg2.Identity.PrivKey {
		t.Fatal("private key not same")
	}
}

func TestSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	cfgFile, err := Filename(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if filepath.Dir(cfgFile) != tmpDir {
		t.Fatal("wrong root dir", cfgFile)
	}

	cfg, err := Init(ioutil.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfgBytes, err := Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = cfg.Save(cfgFile)
	if err != nil {
		t.Fatal(err)
	}

	cfg2, err := Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	cfg2Bytes, err := Marshal(cfg2)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(cfgBytes, cfg2Bytes) {
		t.Fatal("config data different after being loaded")
	}
}
