package counter_test

import (
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/internal/counter"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/require"
)

func TestIndexCounts(t *testing.T) {
	providerPriv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	providerID1, err := peer.IDFromPrivateKey(providerPriv)
	require.NoError(t, err)
	providerPriv, _, err = test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	providerID2, err := peer.IDFromPrivateKey(providerPriv)
	require.NoError(t, err)

	c := counter.NewIndexCounts(datastore.NewMapDatastore())

	ctxid1 := []byte("ctxid1")
	ctxid2 := []byte("ctxid2")
	ctxid3 := []byte("ctxid3")
	ctxid4 := []byte("ctxid4")

	c.AddCount(providerID1, ctxid1, 5)
	c.AddCount(providerID1, ctxid2, 2)
	c.AddCount(providerID1, ctxid3, 3)
	total, err := c.Provider(providerID1)
	require.NoError(t, err)
	require.Equal(t, 10, int(total))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 10, int(total))

	c.AddCount(providerID2, ctxid4, 7)
	total, err = c.Provider(providerID2)
	require.NoError(t, err)
	require.Equal(t, 7, int(total))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 17, int(total))

	count, err := c.RemoveCtx(providerID1, ctxid2)
	require.NoError(t, err)
	require.Equal(t, 2, int(count))

	total, err = c.Provider(providerID1)
	require.NoError(t, err)
	require.Equal(t, 8, int(total))

	total, err = c.Provider(providerID2)
	require.NoError(t, err)
	require.Equal(t, 7, int(total))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 15, int(total))

	// Remove context that provider does not have should change nothing.
	count, err = c.RemoveCtx(providerID2, ctxid1)
	require.NoError(t, err)
	require.Zero(t, count)

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 15, int(total))

	// Remove first provider.
	count = c.RemoveProvider(providerID1)
	require.Equal(t, 8, int(count))
	count = c.RemoveProvider(providerID1)
	require.Zero(t, int(count))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 7, int(total))

	// Remove the last provider.
	count, err = c.RemoveCtx(providerID2, ctxid4)
	require.NoError(t, err)
	require.Equal(t, 7, int(count))

	total, err = c.Total()
	require.NoError(t, err)
	require.Zero(t, total)

	// Remove provider with total not yet in mem.
	c = counter.NewIndexCounts(datastore.NewMapDatastore())
	c.AddCount(providerID1, ctxid1, 5)
	c.AddCount(providerID2, ctxid4, 7)
	count = c.RemoveProvider(providerID1)
	require.Equal(t, 5, int(count))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 7, int(total))
}

func TestAddCount(t *testing.T) {
	providerPriv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	providerID, err := peer.IDFromPrivateKey(providerPriv)
	require.NoError(t, err)

	c := counter.NewIndexCounts(datastore.NewMapDatastore())

	ctxid1 := []byte("ctxid1")
	ctxid2 := []byte("ctxid2")

	c.AddCount(providerID, ctxid1, 5)
	c.AddCount(providerID, ctxid2, 2)
	total, err := c.Total()
	require.NoError(t, err)
	require.Equal(t, 7, int(total))

	// Show that AddCount adds to existing value.
	c.AddCount(providerID, ctxid2, 2)
	total, err = c.Provider(providerID)
	require.NoError(t, err)
	require.Equal(t, 9, int(total))

	c.AddCount(providerID, ctxid1, 3)
	total, err = c.Provider(providerID)
	require.NoError(t, err)
	require.Equal(t, 12, int(total))

	c.AddMissingCount(providerID, ctxid1, 5)
	total, err = c.Provider(providerID)
	require.NoError(t, err)
	require.Equal(t, 12, int(total))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 12, int(total))

	ctxid3 := []byte("ctxid3")
	c.AddMissingCount(providerID, ctxid3, 5)
	total, err = c.Provider(providerID)
	require.NoError(t, err)
	require.Equal(t, 17, int(total))

	total, err = c.Total()
	require.NoError(t, err)
	require.Equal(t, 17, int(total))
}
