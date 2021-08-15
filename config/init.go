package config

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

func Init(out io.Writer) (*Config, error) {
	identity, err := CreateIdentity(out)
	if err != nil {
		return nil, err
	}

	return InitWithIdentity(identity)
}

func InitWithIdentity(identity Identity) (*Config, error) {
	conf := &Config{
		// setup the node's default addresses.
		Addresses: Addresses{
			Admin:  defaultAdminAddr,
			Finder: defaultFinderAddr,
			Ingest: defaultIngestAddr,
		},

		Datastore: Datastore{
			Type: defaultDatastoreType,
			Dir:  defaultDatastoreDir,
		},

		Discovery: Discovery{
			Topic: defaultTopic,
		},

		Identity: identity,

		Indexer: Indexer{
			CacheSize:      defaultCacheSize,
			ValueStoreDir:  defaultValueStoreDir,
			ValueStoreType: defaultValueStoreType,
		},

		Providers: Providers{
			Policy:           defaultPolicy,
			Trust:            []string{identity.PeerID},
			PollInterval:     defaultPollInterval,
			DiscoveryTimeout: defaultDiscoveryTimeout,
			RediscoverWait:   defaultRediscoverWait,
		},
	}

	return conf, nil
}

// CreateIdentity initializes a new identity.
func CreateIdentity(out io.Writer) (Identity, error) {
	ident := Identity{}

	var sk crypto.PrivKey
	var pk crypto.PubKey

	fmt.Fprintf(out, "generating ED25519 keypair...")
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return ident, err
	}
	fmt.Fprintf(out, "done\n")

	sk = priv
	pk = pub

	// currently storing key unencrypted. in the future we need to encrypt it.
	// TODO(security)
	skbytes, err := crypto.MarshalPrivateKey(sk)
	if err != nil {
		return ident, err
	}
	ident.PrivKey = base64.StdEncoding.EncodeToString(skbytes)

	id, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return ident, err
	}
	ident.PeerID = id.Pretty()
	fmt.Fprintf(out, "peer identity: %s\n", ident.PeerID)
	return ident, nil
}
