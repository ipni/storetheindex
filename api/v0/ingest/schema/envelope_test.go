package schema_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	stischema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/storetheindex/test/util"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/require"
)

func TestAdvertisement_SignAndVerify(t *testing.T) {
	testSignAndVerify(t, func(a *stischema.Advertisement, pk crypto.PrivKey) error {
		return a.Sign(pk)
	})
}

func TestAdWithoutExtendedProvidersCanBeSignedAndVerified(t *testing.T) {
	testSignAndVerify(t, func(a *stischema.Advertisement, pk crypto.PrivKey) error {
		return a.SignWithExtendedProviders(pk, func(s string) (crypto.PrivKey, error) {
			return nil, fmt.Errorf("there are no extended providers")
		})
	})
}

func testSignAndVerify(t *testing.T, signer func(*stischema.Advertisement, crypto.PrivKey) error) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
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
	err = signer(&adv, priv)
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

func TestSignShouldFailIfAdHasExtendedProviders(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
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

	_, ep1PeerID := generateIdentityAndKey(t)

	adv := stischema.Advertisement{
		Provider: peerID.String(),
		Addresses: []string{
			"/ip4/127.0.0.1/tcp/9999",
		},
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        ep1PeerID.String(),
					Addresses: randomAddrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
			},
		},
	}
	err = adv.Sign(priv)
	require.Error(t, err, "the ad can not be signed because it has extended providers")
}

func TestSignWithExtendedProviderAndVerify(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	ec := stischema.EntryChunk{
		Entries: util.RandomMultihashes(10, rng),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1Priv, ep1PeerID := generateIdentityAndKey(t)
	ep2Priv, ep2PeerID := generateIdentityAndKey(t)
	mpPriv, mpPeerID := generateIdentityAndKey(t)
	mpAddrs := randomAddrs(2)

	adv := stischema.Advertisement{
		Provider:  mpPeerID.String(),
		Addresses: mpAddrs,
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        ep1PeerID.String(),
					Addresses: randomAddrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
				{
					ID:        ep2PeerID.String(),
					Addresses: randomAddrs(2),
					Metadata:  []byte("ep2-metadata"),
				},
				{
					ID:        mpPeerID.String(),
					Addresses: mpAddrs,
					Metadata:  []byte("main-metadata"),
				},
			},
		},
	}

	err = adv.SignWithExtendedProviders(mpPriv, func(p string) (crypto.PrivKey, error) {
		switch p {
		case ep1PeerID.String():
			return ep1Priv, nil
		case ep2PeerID.String():
			return ep2Priv, nil
		default:
			return nil, fmt.Errorf("Unknown provider %s", p)
		}
	})

	require.NoError(t, err)

	signerID, err := adv.VerifySignature()
	require.NoError(t, err)
	require.Equal(t, mpPeerID, signerID)
	require.Equal(t, signerID, mpPeerID)
}

func TestSigVerificationFailsIfTheAdProviderIdentityIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		_, randomID := generateIdentityAndKey(t)
		adv.Provider = randomID.String()
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfTheExtendedProviderIdentityIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		// main provider is the first one on the list
		_, randomID := generateIdentityAndKey(t)
		adv.ExtendedProvider.Providers[1].ID = randomID.String()
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfTheExtendedProviderMetadataIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		rng := rand.New(rand.NewSource(time.Now().Unix()))
		meta := make([]byte, 10)
		rng.Read(meta)
		adv.ExtendedProvider.Providers[1].Metadata = meta
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfTheExtendedProviderAddrsAreIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		adv.ExtendedProvider.Providers[1].Addresses = randomAddrs(10)
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfOverrideIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		adv.ExtendedProvider.Override = !adv.ExtendedProvider.Override
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfContextIDIsIncorrect(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		adv.ContextID = []byte("ABC")
		_, err := adv.VerifySignature()
		require.Error(t, err)
	})
}

func TestSigVerificationFailsIfMainProviderIsNotInExtendedList(t *testing.T) {
	extendedSignatureTest(t, func(adv stischema.Advertisement) {
		// main provider is the first one on the list
		adv.ExtendedProvider.Providers = adv.ExtendedProvider.Providers[1:]
		_, err := adv.VerifySignature()
		require.Error(t, err)
		require.Equal(t, "extended providers must contain provider from the encapsulating advertisement", err.Error())
	})
}

func TestSignFailsIfMainProviderIsNotInExtendedList(t *testing.T) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	ec := stischema.EntryChunk{
		Entries: util.RandomMultihashes(10, rng),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1Priv, ep1PeerID := generateIdentityAndKey(t)
	mpPriv, mpPeerID := generateIdentityAndKey(t)
	mpAddrs := randomAddrs(2)

	adv := stischema.Advertisement{
		Provider:  mpPeerID.String(),
		Addresses: mpAddrs,
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        ep1PeerID.String(),
					Addresses: randomAddrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
			},
		},
	}

	err = adv.SignWithExtendedProviders(mpPriv, func(p string) (crypto.PrivKey, error) {
		switch p {
		case ep1PeerID.String():
			return ep1Priv, nil
		default:
			return nil, fmt.Errorf("Unknown provider %s", p)
		}
	})

	require.Error(t, err, "extended providers must contain provider from the encapsulating advertisement")
}

func extendedSignatureTest(t *testing.T, testFunc func(adv stischema.Advertisement)) {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	lsys := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)

	ec := stischema.EntryChunk{
		Entries: util.RandomMultihashes(10, rng),
	}

	node, err := ec.ToNode()
	require.NoError(t, err)
	elnk, err := lsys.Store(ipld.LinkContext{}, stischema.Linkproto, node)
	require.NoError(t, err)

	ep1Priv, ep1PeerID := generateIdentityAndKey(t)
	mpPriv, mpPeerID := generateIdentityAndKey(t)
	mpAddrs := randomAddrs(2)

	adv := stischema.Advertisement{
		Provider:  mpPeerID.String(),
		Addresses: mpAddrs,
		Entries:   elnk,
		ContextID: []byte("test-context-id"),
		Metadata:  []byte("test-metadata"),
		ExtendedProvider: &stischema.ExtendedProvider{
			Providers: []stischema.Provider{
				{
					ID:        mpPeerID.String(),
					Addresses: mpAddrs,
					Metadata:  []byte("main-metadata"),
				},
				{
					ID:        ep1PeerID.String(),
					Addresses: randomAddrs(2),
					Metadata:  []byte("ep1-metadata"),
				},
			},
		},
	}

	err = adv.SignWithExtendedProviders(mpPriv, func(p string) (crypto.PrivKey, error) {
		switch p {
		case ep1PeerID.String():
			return ep1Priv, nil
		default:
			return nil, fmt.Errorf("Unknown provider %s", p)
		}
	})

	require.NoError(t, err)

	_, err = adv.VerifySignature()
	require.NoError(t, err)

	testFunc(adv)
}

func randomAddrs(n int) []string {
	rng := rand.New(rand.NewSource(time.Now().Unix()))
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%255, rng.Int()%10751)
	}
	return addrs
}

func generateIdentityAndKey(t *testing.T) (crypto.PrivKey, peer.ID) {
	priv, pub, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	return priv, peerID
}
