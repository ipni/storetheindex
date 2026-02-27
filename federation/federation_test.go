package federation_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestFederationStandalone(t *testing.T) {
	provider, err := libp2p.New()
	require.NoError(t, err)

	subject := newTestFederationMember(t)
	require.NoError(t, subject.Start(context.Background()))

	madeUpAdCid := requireRandomCid(t)
	require.NoError(t, subject.ingester.MarkAdProcessed(provider.ID(), madeUpAdCid))
	providerAddrInfo := peer.AddrInfo{
		ID:    provider.ID(),
		Addrs: provider.Addrs(),
	}
	require.NoError(t, subject.registry.Update(context.TODO(), providerAddrInfo, providerAddrInfo, madeUpAdCid, nil, 0))

	head := subject.requireHeadEventually(t, 20*time.Second, time.Second)
	snapshot := subject.requireSnapshot(t, head.Head)

	require.True(t, snapshot.Epoch > 0)
	require.True(t, snapshot.VectorClock > 0)
	require.Len(t, snapshot.Providers.Keys, 1)
	require.Len(t, snapshot.Providers.Values, 1)
	status, found := snapshot.Providers.Get(provider.ID().String())
	require.True(t, found)
	require.Equal(t, madeUpAdCid.String(), status.LastAdvertisement.String())
	require.Nil(t, status.HeightHint)

	require.NoError(t, subject.Shutdown(context.TODO()))
}
