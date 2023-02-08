package main

import (
	"fmt"
	"io"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Reads marshalled crypto.PrivKey from os.stdin and prints the peer.ID that corresponds to the key.
// See: crypto.UnmarshalPrivateKey, peer.IDFromPrivateKey.
//
// For example, you can directly get the peer.ID from a sops-encrypted identity file as long as
// your terminal session is authenticated to the storetheindex AWS account and has access
// to the relevant KMS keys like this:
//
//	sops -d indexer-0-identity.encrypted | go run <path>/<to>/peer_id_from_priv_key.go
func main() {
	all, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	key, err := crypto.UnmarshalPrivateKey(all)
	if err != nil {
		panic(err)
	}
	pid, err := peer.IDFromPrivateKey(key)
	if err != nil {
		panic(err)
	}
	fmt.Println(pid.String())
}
