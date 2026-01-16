package config

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	var b2 Bootstrap
	b2.SetPeers(addrs)
	slices.Sort(b2.Peers)
	slices.Sort(b.Peers)

	for i := range b2.Peers {
		require.Equal(t, b2.Peers[i], b.Peers[i])
	}
}
