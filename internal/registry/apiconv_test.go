package registry

import (
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/stretchr/testify/require"
)

func TestRegToApiProviderInfo(t *testing.T) {
	rnd := random.New()
	addrInfos := rnd.AddrInfos(2, 1)
	provider := addrInfos[0]
	publisher := addrInfos[1]

	cids := rnd.Cids(2)
	lastAdCid := cids[0]
	lastAdTime := time.Now()

	frozenAtCid := cids[1]
	frozenAtTime := lastAdTime.Add(-time.Hour)

	peerIDs := rnd.Peers(2)
	maddrs := rnd.Multiaddrs(2)
	epContextId := []byte("ep-context-id")

	ep1 := peerIDs[0]
	ep1Addrs := maddrs[:1]
	ep1Metadata := []byte("ep1-metadata")

	ep2 := peerIDs[1]
	ep2Addrs := maddrs[1:]
	ep2Metadata := []byte("ep2-metadata")

	regPI := ProviderInfo{
		AddrInfo:              provider,
		LastAdvertisement:     lastAdCid,
		LastAdvertisementTime: lastAdTime,
		Publisher:             publisher.ID,
		PublisherAddr:         publisher.Addrs[0],
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
	require.Len(t, apiPI.Publisher.Addrs, 1)
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
