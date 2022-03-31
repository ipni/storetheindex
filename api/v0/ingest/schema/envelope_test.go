package schema_test

import (
	"math/rand"
	"testing"

	stischema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
)

func TestAdvertisement_SignAndVerify(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	priv, pub, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	ec := stischema.EntryChunk{
		Entries: util.RandomMultihashes(10, rng),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	adv := stischema.Advertisement{
		Provider: "12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA",
		Addresses: []string{
			"/ip4/127.0.0.1/tcp/9999",
		},
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
	}
	err = adv.Sign(priv)
	require.NoError(t, err)

	signerID, err := adv.VerifySignature()
	require.NoError(t, err)
	require.Equal(t, peerID, signerID)

	// Show that signature can be valid even though advertisement not signed by
	// provider ID.  This is why it is necessary to check that the signer ID is
	// the expected signed after verifying the signature is valid.
	provID, err := peer.Decode(adv.Provider)
	require.NoError(t, err)
	require.NotEqual(t, signerID, provID)

	// Verification fails if something in the advertisement changes
	adv.Provider = ""
	_, err = adv.VerifySignature()
	require.NotNil(t, err)
}
