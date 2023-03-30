package test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-indexer-core/engine"
	"github.com/ipni/go-indexer-core/store/memory"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/ingest/client"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/ipni/storetheindex/test/util"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var rng = rand.New(rand.NewSource(1413))

// InitIndex initialize a new indexer engine.
func InitIndex(t *testing.T, withCache bool) indexer.Interface {
	return engine.New(nil, memory.New())
}

// InitRegistry initializes a new registry
func InitRegistry(t *testing.T, trustedID string) *registry.Registry {
	var discoveryCfg = config.Discovery{
		Policy: config.Policy{
			Allow:         false,
			Except:        []string{trustedID},
			Publish:       false,
			PublishExcept: []string{trustedID},
		},
		PollInterval: config.Duration(time.Minute),
	}
	reg, err := registry.New(context.Background(), discoveryCfg, nil)
	require.NoError(t, err)
	return reg
}

func InitIngest(t *testing.T, indx indexer.Interface, reg *registry.Registry) *ingest.Ingester {
	cfg := config.NewIngest()
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)

	ing, err := ingest.NewIngester(cfg, host, indx, reg, ds)
	require.NoError(t, err)
	t.Cleanup(func() {
		ing.Close()
	})
	return ing
}

func RegisterProviderTest(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, addr string, reg *registry.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Log("registering provider")
	err := cl.Register(ctx, providerID, privateKey, []string{addr})
	require.NoError(t, err)

	require.True(t, reg.IsRegistered(providerID), "provider not registered")

	// Test signature fail
	t.Log("registering provider with bad signature")
	badPeerID, err := peer.Decode("12D3KooWD1XypSuBmhebQcvq7Sf1XJZ1hKSfYCED4w6eyxhzwqnV")
	require.NoError(t, err)

	err = cl.Register(ctx, badPeerID, privateKey, []string{addr})
	require.Error(t, err, "expected bad signature error")
}

func IndexContent(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := util.RandomMultihashes(1, rng)

	contextID := []byte("test-context-id")
	metadata := []byte("test-metadata")

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	require.NoError(t, err)

	vals, ok, err := ind.Get(mhs[0])
	require.NoError(t, err)
	require.True(t, ok, "did not find content")
	require.NotZero(t, len(vals), "no content values returned")

	expectValue := indexer.Value{
		ProviderID:    providerID,
		ContextID:     contextID,
		MetadataBytes: metadata,
	}
	ok = false
	for i := range vals {
		if vals[i].Equal(expectValue) {
			ok = true
			break
		}
	}
	require.True(t, ok, "did not get expected content")
}

func IndexContentNewAddr(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface, newAddr string, reg *registry.Registry) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := util.RandomMultihashes(1, rng)

	ctxID := []byte("test-context-id")
	metadata := []byte("test-metadata")
	addrs := []string{newAddr}

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], ctxID, metadata, addrs)
	require.NoError(t, err)

	info, allowed := reg.ProviderInfo(providerID)
	require.NotNil(t, info, "did not get infor for provider")
	require.True(t, allowed, "provider not allowed")

	maddr, err := multiaddr.NewMultiaddr(newAddr)
	require.NoError(t, err)

	require.True(t, info.AddrInfo.Addrs[0].Equal(maddr), "Did not update address")
}

func IndexContentFail(t *testing.T, cl client.Interface, providerID peer.ID, privateKey crypto.PrivKey, ind indexer.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mhs := util.RandomMultihashes(1, rng)

	contextID := make([]byte, schema.MaxContextIDLen+1)
	metadata := []byte("test-metadata")

	err := cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "context id too long")

	contextID = []byte("test-context-id")
	metadata = make([]byte, schema.MaxMetadataLen+1)
	err = cl.IndexContent(ctx, providerID, privateKey, mhs[0], contextID, metadata, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "metadata too long")

	var apierr *apierror.Error
	if errors.As(err, &apierr) {
		require.Equal(t, 400, apierr.Status())
	}
}

func AnnounceTest(t *testing.T, peerID peer.ID, sender announce.Sender) {
	ai, err := peer.AddrInfoFromString(fmt.Sprintf("/ip4/127.0.0.1/tcp/9999/p2p/%s", peerID))
	require.NoError(t, err)
	ai.ID = peerID

	p2pAddrs, err := peer.AddrInfoToP2pAddrs(ai)
	require.NoError(t, err)

	mhs := util.RandomMultihashes(1, rng)

	msg := message.Message{
		Cid: cid.NewCidV1(22, mhs[0]),
	}
	msg.SetAddrs(p2pAddrs)

	err = sender.Send(context.Background(), msg)
	require.NoError(t, err, "Failed to announce")
}
