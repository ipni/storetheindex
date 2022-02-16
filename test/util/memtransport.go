package util

import (
	"context"
	"fmt"
	"math/rand"
	"net"

	"github.com/akutz/memconn"
	"github.com/libp2p/go-libp2p-core/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/transport"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

type MemTransport struct {
	localAddr  ma.Multiaddr
	remoteAddr ma.Multiaddr
	upgrader   transport.Upgrader
	rcmgr      network.ResourceManager
}

type MemConn struct {
	net.Conn
	localAddr  ma.Multiaddr
	remoteAddr ma.Multiaddr
}

type MemListener struct {
	innerListener net.Listener
	localAddr     ma.Multiaddr
	remoteAddr    ma.Multiaddr
}

func (l *MemListener) Accept() (manet.Conn, error) {
	c, err := l.innerListener.Accept()
	if err != nil {
		return nil, err
	}

	return &MemConn{c, l.localAddr, l.remoteAddr}, nil
}

func (l *MemListener) Close() error {
	return l.innerListener.Close()
}

func (l *MemListener) Addr() net.Addr {
	return l.innerListener.Addr()
}

func (l *MemListener) Multiaddr() ma.Multiaddr {
	return l.localAddr
}

const P_MEM = 0x300001

// func (c *MemConn) Transport() transport.Transport {
// 	return &MemTransport{}
// }

func (c *MemConn) LocalMultiaddr() ma.Multiaddr {
	return c.localAddr
}

func (c *MemConn) RemoteMultiaddr() ma.Multiaddr {
	return c.remoteAddr
}

func (t *MemTransport) Dial(ctx context.Context, raddr ma.Multiaddr, p peer.ID) (transport.CapableConn, error) {
	d, err := memconn.Dial("memu", fmt.Sprintf("%d", raddr))
	if err != nil {
		return nil, err
	}
	c := &MemConn{d, t.localAddr, t.remoteAddr}
	direction := network.DirOutbound
	connScope, err := t.rcmgr.OpenConnection(network.DirOutbound, true)

	if err != nil {
		return nil, err
	}

	return t.upgrader.Upgrade(ctx, t, c, direction, p, connScope)
}

func (t *MemTransport) Listen(laddr ma.Multiaddr) (transport.Listener, error) {
	fmt.Println("LISTENING on", t.localAddr)
	ml, err := memconn.Listen("memu", fmt.Sprintf("%d", t.localAddr))
	if err != nil {
		return nil, err
	}

	l := &MemListener{ml, t.localAddr, t.remoteAddr}
	ul := t.upgrader.UpgradeListener(t, l)

	return ul, nil
}

func (t *MemTransport) CanDial(addr ma.Multiaddr) bool {
	for _, p := range addr.Protocols() {
		if p.Code == P_MEM {
			return true
		}
	}
	return false
}

func (t *MemTransport) Protocols() []int {
	return []int{P_MEM}
}

func (t *MemTransport) Proxy() bool {
	return false
}

func init() {
	err := ma.AddProtocol(ma.Protocol{
		Name:       "memtransport",
		Code:       P_MEM,
		VCode:      ma.CodeToVarint(P_MEM),
		Size:       16, // bits
		Transcoder: ma.TranscoderPort,
	})
	if err != nil {
		panic("error adding mem transport protocol")
	}
}

func NewMemTransport(upgrader transport.Upgrader) (*MemTransport, error) {
	rand32 := rand.Int31()
	var remoteID int16 = int16(rand32 >> 16)
	var localID int16 = int16(rand32)
	remoteAddr, err := ma.NewMultiaddr(fmt.Sprintf("/memtransport/%d", remoteID))
	if err != nil {
		return nil, err
	}
	localAddr, err := ma.NewMultiaddr(fmt.Sprintf("/memtransport/%d", localID))
	if err != nil {
		return nil, err
	}

	return &MemTransport{
		upgrader:   upgrader,
		rcmgr:      network.NullResourceManager,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
	}, nil
}
