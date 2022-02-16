package util

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/stretchr/testify/require"
)

func TestMemTransport(t *testing.T) {
	host, err := libp2p.New(libp2p.Transport(NewMemTransport), libp2p.ListenAddrStrings("/memtransport/0"), libp2p.Ping(true))
	require.NoError(t, err)
	require.NoError(t, err)
	hostAddrInfo := peer.AddrInfo{
		ID:    host.ID(),
		Addrs: host.Addrs(),
	}
	fmt.Println("Host has id", host.ID())
	fmt.Println("Host has addrs", host.Addrs())

	host2, err := libp2p.New(libp2p.Transport(NewMemTransport), libp2p.ListenAddrStrings("/memtransport/0"), libp2p.Ping(true))
	require.NoError(t, err)

	fmt.Println("Host 2 has id", host2.ID())
	fmt.Println("Host 2 has addrs", host2.Addrs())
	err = host2.Connect(context.Background(), hostAddrInfo)
	require.NoError(t, err)

	res := <-ping.Ping(context.Background(), host, host2.ID())
	require.NoError(t, res.Error)
	fmt.Println("Res", res)
}
