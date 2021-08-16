package signature

import (
	"testing"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestVerify(t *testing.T) {
	var ident = config.Identity{
		PeerID:  "12D3KooWPw6bfQbJHfKa2o5XpusChoq67iZoqgfnhecygjKsQRmG",
		PrivKey: "CAESQEQliDSXbU/zR4hrGNgAM0crtmxcZ49F3OwjmptYEFuU0b0TwLTJz/OlSBBuK7QDV2PiyGOCjDkyxSXymuqLu18=",
	}

	privKey, err := ident.DecodePrivateKey("")
	if err != nil {
		t.Fatalf("could not decode private key: %s", err)
	}

	data, err := Nonce()
	if err != nil {
		t.Fatal(err)
	}

	sig, err := privKey.Sign(data)
	if err != nil {
		t.Fatalf("count not sign data: %s", err)
	}

	peerID, err := peer.Decode(ident.PeerID)
	if err != nil {
		t.Fatal("Could not decode peer ID")
	}

	err = Verify(peerID, data, sig)
	if err != nil {
		t.Fatal(err)
	}

	data[0]++
	err = Verify(peerID, sig, data)
	if err == nil {
		t.Fatal("signature should not verify")
	}

	err = Verify(peerID, sig, nil)
	if err == nil {
		t.Fatal("signature should not verify")
	}

	err = Verify(peerID, nil, data)
	if err == nil {
		t.Fatal("signature should not verify")
	}
}
