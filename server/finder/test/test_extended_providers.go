package test

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/storetheindex/api/v0/finder/client"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/test/util"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func ProvidersShouldBeUnaffectedByExtendedProvidersOfEachOtherTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})
	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})
	createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, provider1Id, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  ep1Addrs,
			},
		},
	})

	provider2Id, _, _ := util.RandomIdentity(t)
	ctxId2 := []byte("test-context-id-2")
	metadata2 := []byte("test-metadata-2")
	addrs2 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9998"})
	prov2, mhs2 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId2, metadata2, provider2Id, addrs2, nil)

	resp, err := c.FindBatch(ctx, mhs2[:10])
	require.NoError(t, err)
	err = checkResponse(resp, mhs2[:10], []model.ProviderResult{
		{
			ContextID: ctxId2,
			Provider: &peer.AddrInfo{
				ID:    prov2,
				Addrs: addrs2,
			},
			Metadata: metadata2,
		},
	})
	require.NoError(t, err)
}

func ExtendedProviderShouldHaveOwnMetadataTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})
	ep1Metadata := []byte("test-metadata-ep1")

	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})
	ep2Metadata := []byte("test-metadata-ep2")
	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, provider1Id, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID:   ep1,
				Addrs:    ep1Addrs,
				Metadata: ep1Metadata,
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{
			string(ctxId1): {
				ContextID: ctxId1,
				Providers: []registry.ExtendedProviderInfo{
					{
						PeerID:   ep2,
						Addrs:    ep2Addrs,
						Metadata: ep2Metadata,
					},
				},
			},
		},
	})

	resp, err := c.FindBatch(ctx, mhs1)
	require.NoError(t, err)
	err = checkResponse(resp, mhs1, []model.ProviderResult{
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: ep2Metadata,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: ep1Metadata,
		},
	})
	require.NoError(t, err)
}

func ExtendedProviderShouldInheritMetadataOfMainProviderTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})
	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})

	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, provider1Id, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  ep1Addrs,
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{
			string(ctxId1): {
				ContextID: ctxId1,
				Providers: []registry.ExtendedProviderInfo{
					{
						PeerID: ep2,
						Addrs:  ep2Addrs,
					},
				},
			},
		},
	})

	resp, err := c.FindBatch(ctx, mhs1)
	require.NoError(t, err)
	err = checkResponse(resp, mhs1, []model.ProviderResult{
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)
}

func ContextualExtendedProvidersShouldUnionUpWithChainLevelOnesTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})

	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})

	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, provider1Id, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  ep1Addrs,
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{string(ctxId1): {
			Override:  false,
			ContextID: ctxId1,
			Providers: []registry.ExtendedProviderInfo{
				{
					PeerID: ep2,
					Addrs:  ep2Addrs,
				},
			},
		}},
	})

	ctxId2 := []byte("test-context-id-2")
	mhs2 := util.RandomMultihashes(10, rng)

	v := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctxId2,
		MetadataBytes: metadata1,
	}
	populateIndex(ind, mhs2, v, t)

	resp, err := c.FindBatch(ctx, mhs1)
	require.NoError(t, err)
	err = checkResponse(resp, mhs1, []model.ProviderResult{
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)

	// for contextId2 we should get only chain-level extended providers
	resp, err = c.FindBatch(ctx, mhs2)
	require.NoError(t, err)
	err = checkResponse(resp, mhs2, []model.ProviderResult{
		{
			ContextID: ctxId2,
			Provider: &peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId2,
			Provider: &peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)
}

func ContextualExtendedProvidersShouldOverrideChainLevelOnesTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})

	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})

	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, provider1Id, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  ep1Addrs,
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{string(ctxId1): {
			Override:  true,
			ContextID: ctxId1,
			Providers: []registry.ExtendedProviderInfo{
				{
					PeerID: ep2,
					Addrs:  ep2Addrs,
				},
			},
		}},
	})

	resp, err := c.FindBatch(ctx, mhs1)
	require.NoError(t, err)
	err = checkResponse(resp, mhs1, []model.ProviderResult{
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)
}

func MainProviderChainRecordIsIncludedIfItsMetadataIsDifferentTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	providerId, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	providerMetadata := []byte("provider metadata")
	chainMetadata := []byte("chain level metadata")
	addrs := util.StringToMultiaddrs(t, util.RandomAddrs(2))
	chainAddrs := util.StringToMultiaddrs(t, util.RandomAddrs(2))

	_, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, providerMetadata, providerId, addrs, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID:   providerId,
				Metadata: chainMetadata,
				Addrs:    chainAddrs,
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{string(ctxId1): {
			ContextID: ctxId1,
			Providers: []registry.ExtendedProviderInfo{
				{
					PeerID:   providerId,
					Metadata: providerMetadata,
					Addrs:    util.StringToMultiaddrs(t, util.RandomAddrs(2)),
				},
			},
		}},
	})

	resp, err := c.FindBatch(ctx, mhs1)
	require.NoError(t, err)
	err = checkResponse(resp, mhs1, []model.ProviderResult{
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    providerId,
				Addrs: addrs,
			},
			Metadata: providerMetadata,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    providerId,
				Addrs: chainAddrs,
			},
			Metadata: chainMetadata,
		},
	})
	require.NoError(t, err)
}

func MainProviderContextRecordIsIncludedIfItsMetadataIsDifferentTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	providerId, _, _ := util.RandomIdentity(t)
	ctxId1 := []byte("test-context-id-1")
	providerMetadata := []byte("provider metadata")
	contextMetadata := []byte("context level metadata")
	addrs := util.StringToMultiaddrs(t, util.RandomAddrs(2))
	contextAddrs := util.StringToMultiaddrs(t, util.RandomAddrs(2))

	_, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, providerMetadata, providerId, addrs, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID:   providerId,
				Metadata: providerMetadata,
				Addrs:    util.StringToMultiaddrs(t, util.RandomAddrs(2)),
			},
		},
		ContextualProviders: map[string]registry.ContextualExtendedProviders{string(ctxId1): {
			ContextID: ctxId1,
			Providers: []registry.ExtendedProviderInfo{
				{
					PeerID:   providerId,
					Metadata: contextMetadata,
					Addrs:    contextAddrs,
				},
			},
		}},
	})

	resp, err := c.FindBatch(ctx, mhs1)
	require.NoError(t, err)
	err = checkResponse(resp, mhs1, []model.ProviderResult{
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    providerId,
				Addrs: addrs,
			},
			Metadata: providerMetadata,
		},
		{
			ContextID: ctxId1,
			Provider: &peer.AddrInfo{
				ID:    providerId,
				Addrs: contextAddrs,
			},
			Metadata: contextMetadata,
		},
	})
	require.NoError(t, err)
}

func createProviderAndPopulateIndexer(t *testing.T, ctx context.Context, ind indexer.Interface, reg *registry.Registry, contextID []byte, metadata []byte, providerID peer.ID, addrs []multiaddr.Multiaddr, extendedProviders *registry.ExtendedProviders) (peer.ID, []multihash.Multihash) {
	// Generate some multihashes and populate indexer
	mhs := util.RandomMultihashes(10, rng)

	v := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadata,
	}
	populateIndex(ind, mhs, v, t)

	provider := peer.AddrInfo{
		ID:    providerID,
		Addrs: addrs,
	}

	err := reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, extendedProviders, 0)
	require.NoError(t, err, "could not register provider info: %v", err)

	return providerID, mhs
}
