package model

import (
	"testing"

	"github.com/filecoin-project/storetheindex/config"
)

var providerIdent = config.Identity{
	PeerID:  "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ",
	PrivKey: "CAESQLypOCKYR7HGwVl4ngNhEqMZ7opchNOUA4Qc1QDpxsARGr2pWUgkXFXKU27TgzIHXqw0tXaUVx2GIbUuLitq22c=",
}

func TestRegisterRequest(t *testing.T) {
	addrs := []string{"/ip4/127.0.0.1/tcp/9999"}

	peerID, privKey, err := providerIdent.Decode()
	if err != nil {
		t.Fatal(err)
	}

	data, err := MakeRegisterRequest(peerID, privKey, addrs)
	if err != nil {
		t.Fatal(err)
	}

	peerRec, err := ReadRegisterRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	seq0 := peerRec.Seq

	data, err = MakeRegisterRequest(peerID, privKey, addrs)
	if err != nil {
		t.Fatal(err)
	}
	peerRec, err = ReadRegisterRequest(data)
	if err != nil {
		t.Fatal(err)
	}

	if seq0 >= peerRec.Seq {
		t.Fatal("sequence not greater than last seen")
	}
}
