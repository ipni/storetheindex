// Package mautil provides multiaddr utility functions.
package mautil

import (
	"net"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// FilterPrivateIPs returns a new slice of multiaddrs with any private,
// loopback, or unspecified IP multiaddrs removed. If no multiaddrs are
// removed, then returns the original slice.
func FilterPrivateIPs(maddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	filtered := multiaddr.FilterAddrs(maddrs, func(target multiaddr.Multiaddr) bool {
		c, _ := multiaddr.SplitFirst(target)
		if c == nil {
			return false
		}
		switch c.Protocol().Code {
		case multiaddr.P_IP4, multiaddr.P_IP6, multiaddr.P_IP6ZONE, multiaddr.P_IPCIDR:
			return manet.IsPublicAddr(target)
		case multiaddr.P_DNS, multiaddr.P_DNS4, multiaddr.P_DNS6, multiaddr.P_DNSADDR:
			return c.Value() != "localhost"
		default:
			return true
		}
	})
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func notPrivateAddr(a multiaddr.Multiaddr) bool {
	if a == nil {
		return true
	}
	return !manet.IsPrivateAddr(a)
}

func MultiaddrStringToNetAddr(maddrStr string) (net.Addr, error) {
	maddr, err := multiaddr.NewMultiaddr(maddrStr)
	if err != nil {
		return nil, err
	}
	return manet.ToNetAddr(maddr)
}

// ParsePeers parses a list of multiaddr strings into a list of AddrInfo.
func ParsePeers(addrs []string) ([]peer.AddrInfo, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, addr := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, err
		}
	}
	return peer.AddrInfosFromP2pAddrs(maddrs...)
}
