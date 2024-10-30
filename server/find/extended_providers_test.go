package find_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func providersShouldBeUnaffectedByExtendedProvidersOfEachOtherTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	maddrs := random.Multiaddrs(3)
	addrs1 := maddrs[:1]
	ep1Addrs := maddrs[1:2]
	ep1, _, _ := random.Identity()
	createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, provider1Id, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  ep1Addrs,
			},
		},
	})

	provider2Id, _, _ := random.Identity()
	ctxId2 := []byte("test-context-id-2")
	metadata2 := []byte("test-metadata-2")
	addrs2 := maddrs[2:3]
	prov2, mhs2 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId2, metadata2, provider2Id, addrs2, nil)

	resp, err := client.FindBatch(ctx, f, mhs2[:10])
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

func extendedProviderShouldHaveOwnMetadataTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	maddrs := random.Multiaddrs(3)
	addrs1 := maddrs[:1]

	ep1, _, _ := random.Identity()
	ep1Addrs := maddrs[1:2]
	ep1Metadata := []byte("test-metadata-ep1")

	ep2, _, _ := random.Identity()
	ep2Addrs := maddrs[2:3]
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

	resp, err := client.FindBatch(ctx, f, mhs1)
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

func extendedProviderShouldInheritMetadataOfMainProviderTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	maddrs := random.Multiaddrs(3)
	addrs1 := maddrs[:1]

	ep1, _, _ := random.Identity()
	ep1Addrs := maddrs[1:2]
	ep2, _, _ := random.Identity()
	ep2Addrs := maddrs[2:3]

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

	resp, err := client.FindBatch(ctx, f, mhs1)
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

func contextualExtendedProvidersShouldUnionUpWithChainLevelOnesTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	maddrs := random.Multiaddrs(3)
	addrs1 := maddrs[:1]

	ep1, _, _ := random.Identity()
	ep1Addrs := maddrs[1:2]

	ep2, _, _ := random.Identity()
	ep2Addrs := maddrs[2:3]

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
	mhs2 := random.Multihashes(10)

	v := indexer.Value{
		ProviderID:    prov1,
		ContextID:     ctxId2,
		MetadataBytes: metadata1,
	}
	populateIndex(ind, mhs2, v, t)

	resp, err := client.FindBatch(ctx, f, mhs1)
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
	resp, err = client.FindBatch(ctx, f, mhs2)
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

func contextualExtendedProvidersShouldOverrideChainLevelOnesTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	provider1Id, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	maddrs := random.Multiaddrs(3)
	addrs1 := maddrs[:1]

	ep1, _, _ := random.Identity()
	ep1Addrs := maddrs[1:2]

	ep2, _, _ := random.Identity()
	ep2Addrs := maddrs[2:3]

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

	resp, err := client.FindBatch(ctx, f, mhs1)
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

func mainProviderChainRecordIsIncludedIfItsMetadataIsDifferentTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	providerId, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	providerMetadata := []byte("provider metadata")
	chainMetadata := []byte("chain level metadata")
	maddrs := random.Multiaddrs(4)
	addrs := maddrs[0:2]
	chainAddrs := maddrs[2:4]

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
					Addrs:    random.Multiaddrs(2),
				},
			},
		}},
	})

	resp, err := client.FindBatch(ctx, f, mhs1)
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

func mainProviderContextRecordIsIncludedIfItsMetadataIsDifferentTest(ctx context.Context, t *testing.T, f client.Finder, ind indexer.Interface, reg *registry.Registry) {
	providerId, _, _ := random.Identity()
	ctxId1 := []byte("test-context-id-1")
	providerMetadata := []byte("provider metadata")
	contextMetadata := []byte("context level metadata")
	maddrs := random.Multiaddrs(4)
	addrs := maddrs[0:2]
	contextAddrs := maddrs[2:4]

	_, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, providerMetadata, providerId, addrs, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID:   providerId,
				Metadata: providerMetadata,
				Addrs:    random.Multiaddrs(2),
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

	resp, err := client.FindBatch(ctx, f, mhs1)
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
	mhs := random.Multihashes(10)

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
