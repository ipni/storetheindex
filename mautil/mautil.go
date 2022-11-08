// Package mautil provides multiaddr utility functions.
package mautil

import (
	"net"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// FilterPrivateIPs returns a new slice of multiaddrs with any private,
// loopback, or unspecified IP multiaddrs removed. If no multiaddrs are
// removed, then returns the original slice.
func FilterPrivateIPs(maddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	var pvt []int
	for i, maddr := range maddrs {
		if maddr == nil {
			continue
		}
		ip, err := manet.ToIP(maddr)
		if err != nil {
			continue
		}
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified() {
			pvt = append(pvt, i)
		}
	}

	// If no private addrs, return original slice.
	if len(pvt) == 0 {
		return maddrs
	}

	// If no non-private addrs, return nothing.
	notPrivLen := len(maddrs) - len(pvt)
	if notPrivLen == 0 {
		return nil
	}

	// Return new slice with non-private addrs.
	newAddrs := make([]multiaddr.Multiaddr, 0, notPrivLen)
	skip := pvt[0]
	for i, maddr := range maddrs {
		if i == skip {
			if len(pvt) > 1 {
				pvt = pvt[1:]
				skip = pvt[0]
			}
			continue
		}
		newAddrs = append(newAddrs, maddr)
	}
	return newAddrs
}

func MultiaddrStringToNetAddr(maddrStr string) (net.Addr, error) {
	maddr, err := multiaddr.NewMultiaddr(maddrStr)
	if err != nil {
		return nil, err
	}
	return manet.ToNetAddr(maddr)
}
