package spinfo_test

import (
	"context"
	"os"
	"testing"

	"github.com/ipni/storetheindex/spinfo"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const (
	gateway  = "api.chain.love"
	testSPID = "t01000"
)

func TestSPAddrInfo(t *testing.T) {
	if os.Getenv("STI_LOTUS_TEST") == "" {
		t.Skip("Skipping lotus test. Set STI_LOTUS_TEST=yes to enable.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addrInfo, err := spinfo.SPAddrInfo(ctx, gateway, testSPID)
	require.NoError(t, err)

	peerID, err := peer.Decode("12D3KooWGuQafP1HDkE2ixXZnX6q6LLygsUG1uoxaQEtfPAt5ygp")
	require.NoError(t, err)

	require.Equal(t, peerID, addrInfo.ID)
	t.Logf("AddrInfo for storae provider %q: %s", testSPID, addrInfo.String())
}
