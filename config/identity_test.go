package config_test

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ipni/storetheindex/config"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestIdentity_Decode(t *testing.T) {
	testB64PrivKey := "CAESQFcdYoJlGW2TPMxX3h1yH/29ML3873kW8sKeBJ/lSy2sscZbeFVI+TEz8T7F9vuw6KPYbokzZnPR4HgXkxw/xg0="
	pkb, err := base64.StdEncoding.DecodeString(testB64PrivKey)
	require.NoError(t, err)
	testPrivKeyPath := filepath.Join(t.TempDir(), "identity.key")
	err = os.WriteFile(testPrivKeyPath, pkb, 0666)
	require.NoError(t, err)
	testPrivKey, err := crypto.UnmarshalPrivateKey(pkb)
	require.NoError(t, err)
	testPeerID, err := peer.IDFromPrivateKey(testPrivKey)
	require.NoError(t, err)

	tests := []struct {
		name        string
		givenConfig config.Identity
		givenEnvVar string
		wantPeerID  peer.ID
		wantPrivKey crypto.PrivKey
		wantErr     string
	}{
		{
			name:    "Empty config and env var is error",
			wantErr: "could not decode private key: private key not specified; it must be specified either in config or via STORETHEINDEX_PRIV_KEY_PATH env var",
		},
		{
			name: "Invalid private key in config is error",
			givenConfig: config.Identity{
				PrivKey: "invalid private key",
			},
			wantErr: "could not decode private key: illegal base64 data at input byte 7",
		},
		{
			name: "Invalid peer ID in config is error",
			givenConfig: config.Identity{
				PeerID:  "invalid peer ID",
				PrivKey: testB64PrivKey,
			},
			wantErr: "could not decode peer id: failed to parse peer ID:",
		},
		{
			name:        "Invalid path via env var is error",
			givenEnvVar: "non-existing-file",
			wantErr:     "could not decode private key: open non-existing-file",
		},
		{
			name: "Mismatching peer ID in config is error",
			givenConfig: config.Identity{
				PeerID:  "12D3KooWP5UZNGnCPsCoCgxbc9BRDVwwgFguZ7EaW6FEMHTzN2q7",
				PrivKey: testB64PrivKey,
			},
			wantErr: "provided peer ID must either match the peer ID generated from private key or be omitted: expected 12D3KooWMnKm1H932NjBzETsTEFSeXpaPUiwCoCqQMrBc6sVnUeQ but got 12D3KooWP5UZNGnCPsCoCgxbc9BRDVwwgFguZ7EaW6FEMHTzN2q7",
		},
		{
			name: "Matching peer ID and private key in config is valid",
			givenConfig: config.Identity{
				PeerID:  testPeerID.String(),
				PrivKey: testB64PrivKey,
			},
			wantPeerID:  testPeerID,
			wantPrivKey: testPrivKey,
		},
		{
			name: "Matching peer ID and private key from env var is valid",
			givenConfig: config.Identity{
				PeerID: testPeerID.String(),
			},
			givenEnvVar: testPrivKeyPath,
			wantPeerID:  testPeerID,
			wantPrivKey: testPrivKey,
		},
		{
			name: "Mismatching peer ID and private key from env var is error",
			givenConfig: config.Identity{
				PeerID: "12D3KooWP5UZNGnCPsCoCgxbc9BRDVwwgFguZ7EaW6FEMHTzN2q7",
			},
			givenEnvVar: testPrivKeyPath,
			wantErr:     "provided peer ID must either match the peer ID generated from private key or be omitted: expected 12D3KooWMnKm1H932NjBzETsTEFSeXpaPUiwCoCqQMrBc6sVnUeQ but got 12D3KooWP5UZNGnCPsCoCgxbc9BRDVwwgFguZ7EaW6FEMHTzN2q7",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.givenEnvVar != "" {
				t.Setenv(config.PrivateKeyPathEnvVar, tt.givenEnvVar)
			}
			gotPeerID, gotPrivKey, err := tt.givenConfig.Decode()
			if tt.wantErr != "" {
				// Only check prefix of error message to keep CI happy. Message returned in 1.18 is different.
				require.True(t, strings.HasPrefix(err.Error(), tt.wantErr))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPeerID.String(), gotPeerID.String())
			require.Equal(t, tt.wantPrivKey, gotPrivKey)
		})
	}
}
