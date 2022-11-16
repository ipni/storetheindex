package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/client"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func ProvidersShouldBeUnaffectedByExtendedProvidersOfEachOtherTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})
	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})
	createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, addrs1, &registry.ExtendedProviders{
		Providers: []registry.ExtendedProviderInfo{
			{
				PeerID: ep1,
				Addrs:  ep1Addrs,
			},
		},
	})

	ctxId2 := []byte("test-context-id-2")
	metadata2 := []byte("test-metadata-2")
	addrs2 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9998"})
	prov2, mhs2 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId2, metadata2, addrs2, nil)

	resp, err := c.FindBatch(ctx, mhs2[:10])
	require.NoError(t, err)
	err = checkResponse(resp, mhs2[:10], []model.ProviderResult{
		{
			ContextID: ctxId2,
			Provider: peer.AddrInfo{
				ID:    prov2,
				Addrs: addrs2,
			},
			Metadata: metadata2,
		},
	})
	require.NoError(t, err)
}

func ExtendedProviderShouldHaveOwnMetadataTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})
	ep1Metadata := []byte("test-metadata-ep1")

	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})
	ep2Metadata := []byte("test-metadata-ep2")
	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, addrs1, &registry.ExtendedProviders{
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
			Provider: peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: ep2Metadata,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: ep1Metadata,
		},
	})
	require.NoError(t, err)
}

func ExtendedProviderShouldInheritMetadataOfMainProviderTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})
	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})

	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, addrs1, &registry.ExtendedProviders{
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
			Provider: peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)
}

func ContextualExtendedProvidersShouldUnionUpWithChainLevelOnesTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})

	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})

	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, addrs1, &registry.ExtendedProviders{
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
			Provider: peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
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
			Provider: peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId2,
			Provider: peer.AddrInfo{
				ID:    ep1,
				Addrs: ep1Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)
}

func ContextualExtendedProvidersShouldOverrideChainLevelOnesTest(ctx context.Context, t *testing.T, c client.Finder, ind indexer.Interface, reg *registry.Registry) {
	ctxId1 := []byte("test-context-id-1")
	metadata1 := []byte("test-metadata-1")
	addrs1 := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9999"})

	ep1, _, _ := util.RandomIdentity(t)
	ep1Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9997"})

	ep2, _, _ := util.RandomIdentity(t)
	ep2Addrs := util.StringToMultiaddrs(t, []string{"/ip4/127.0.0.1/tcp/9996"})

	prov1, mhs1 := createProviderAndPopulateIndexer(t, ctx, ind, reg, ctxId1, metadata1, addrs1, &registry.ExtendedProviders{
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
			Provider: peer.AddrInfo{
				ID:    prov1,
				Addrs: addrs1,
			},
			Metadata: metadata1,
		},
		{
			ContextID: ctxId1,
			Provider: peer.AddrInfo{
				ID:    ep2,
				Addrs: ep2Addrs,
			},
			Metadata: metadata1,
		},
	})
	require.NoError(t, err)
}

func createProviderAndPopulateIndexer(t *testing.T, ctx context.Context, ind indexer.Interface, reg *registry.Registry, contextID []byte, metadata []byte, addrs []multiaddr.Multiaddr, extendedProviders *registry.ExtendedProviders) (peer.ID, []multihash.Multihash) {
	providerID, _, _ := util.RandomIdentity(t)

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

	err := reg.Update(ctx, provider, peer.AddrInfo{}, cid.Undef, extendedProviders)
	require.NoError(t, err, "could not register provider info: %v", err)

	return providerID, mhs
}
