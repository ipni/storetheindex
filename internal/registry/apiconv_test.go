package registry

import (
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/test"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestRegToApiProviderInfo(t *testing.T) {
	peerID, err := peer.Decode(limitedID)
	require.NoError(t, err)
	maddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/3002")
	require.NoError(t, err)
	provAddrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: []multiaddr.Multiaddr{maddr},
	}

	pubID, err := peer.Decode(publisherID)
	require.NoError(t, err)
	pubAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")
	require.NoError(t, err)

	mh, err := multihash.Sum([]byte("somedata"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	lastAdCid := cid.NewCidV1(cid.Raw, mh)
	lastAdTime := time.Now()

	mh, err = multihash.Sum([]byte("somedata"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	frozenAtCid := cid.NewCidV1(cid.Raw, mh)
	frozenAtTime := lastAdTime.Add(-time.Hour)

	maddrs := test.RandomMultiaddrs(2)
	ep1Addrs := maddrs[:1]
	ep2Addrs := maddrs[1:]

	epContextId := []byte("ep-context-id")
	ep1, _, _ := test.RandomIdentity()
	ep1Metadata := []byte("ep1-metadata")
	ep2, _, _ := test.RandomIdentity()
	ep2Metadata := []byte("ep2-metadata")

	regPI := ProviderInfo{
		AddrInfo:              provAddrInfo,
		LastAdvertisement:     lastAdCid,
		LastAdvertisementTime: lastAdTime,
		Publisher:             pubID,
		PublisherAddr:         pubAddr,
		ExtendedProviders: &ExtendedProviders{
			Providers: []ExtendedProviderInfo{
				{
					PeerID:   ep1,
					Addrs:    ep1Addrs,
					Metadata: ep1Metadata,
				},
			},
			ContextualProviders: map[string]ContextualExtendedProviders{
				string(epContextId): {
					ContextID: epContextId,
					Override:  true,
					Providers: []ExtendedProviderInfo{
						{
							PeerID:   ep2,
							Addrs:    ep2Addrs,
							Metadata: ep2Metadata,
						},
					},
				},
			},
		},
		FrozenAt:     frozenAtCid,
		FrozenAtTime: frozenAtTime,
	}

	apiPI := RegToApiProviderInfo(&regPI)
	require.NotNil(t, apiPI)

	require.Equal(t, regPI.AddrInfo, apiPI.AddrInfo)
	require.Equal(t, regPI.LastAdvertisement, apiPI.LastAdvertisement)
	require.Equal(t, regPI.LastAdvertisementTime.Format(time.RFC3339), apiPI.LastAdvertisementTime)
	require.Equal(t, regPI.Publisher, apiPI.Publisher.ID)
	require.Equal(t, 1, len(apiPI.Publisher.Addrs))
	require.Equal(t, regPI.PublisherAddr, apiPI.Publisher.Addrs[0])
	require.Equal(t, regPI.FrozenAt, apiPI.FrozenAt)
	require.Equal(t, regPI.FrozenAtTime.Format(time.RFC3339), apiPI.FrozenAtTime)

	require.NotNil(t, apiPI.ExtendedProviders)
	apixp := apiPI.ExtendedProviders
	regxp := regPI.ExtendedProviders
	require.Equal(t, len(regxp.Providers), len(apixp.Providers))
	require.Equal(t, regxp.Providers[0].PeerID, apixp.Providers[0].ID)
	require.Equal(t, len(regxp.Providers), len(apixp.Metadatas))
	require.Equal(t, regxp.Providers[0].Metadata, apixp.Metadatas[0])

	require.Equal(t, len(regxp.ContextualProviders), len(apixp.Contextual))
	for _, apiCEP := range apixp.Contextual {
		regCEP, found := regxp.ContextualProviders[apiCEP.ContextID]
		require.True(t, found)
		require.Equal(t, string(regCEP.ContextID), apiCEP.ContextID)
		require.Equal(t, len(regCEP.Providers), len(apiCEP.Providers))
		require.Equal(t, regCEP.Providers[0].PeerID, apiCEP.Providers[0].ID)
		require.Equal(t, regCEP.Providers[0].Addrs, apiCEP.Providers[0].Addrs)
		require.Equal(t, len(regCEP.Providers), len(apiCEP.Metadatas))
		require.Equal(t, regCEP.Providers[0].Metadata, apiCEP.Metadatas[0])
		require.Equal(t, regCEP.Override, apiCEP.Override)
	}

	regPI2 := apiToRegProviderInfo(apiPI)
	require.NotNil(t, regPI2)

	var timeZero time.Time
	regPI.LastAdvertisement = cid.Undef
	regPI.LastAdvertisementTime = timeZero
	regPI.FrozenAt = cid.Undef
	regPI.FrozenAtTime = timeZero

	require.Equal(t, regPI, *regPI2)

	require.Nil(t, RegToApiProviderInfo(nil))
	require.Nil(t, apiToRegProviderInfo(nil))
}
