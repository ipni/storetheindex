package config

import (
	"sort"
	"testing"
)

var testBootstrapAddresses = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
	"/ip4/104.131.131.82/udp/4001/quic/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
}

func TestBoostrapPeers(t *testing.T) {
	b := Bootstrap{
		Peers: testBootstrapAddresses,
	}
	addrs, err := b.PeerAddrs()
	if err != nil {
		t.Fatal(err)
	}

	var b2 Bootstrap
	b2.SetPeers(addrs)
	sort.Strings(b2.Peers)
	sort.Strings(b.Peers)

	for i := range b2.Peers {
		if b2.Peers[i] != b.Peers[i] {
			t.Fatalf("expected %s, %s", b.Peers[i], b2.Peers[i])
		}
	}
}
