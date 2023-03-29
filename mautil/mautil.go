// Deprecated: The same functionality is provided by package
// github.com/ipni/go-libipni/mautil, and those implementations should be
// preferred in new code.
// See the specific function documentation for details.
package mautil

import (
	"net"

	"github.com/ipni/go-libipni/mautil"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// Depricated: Use github.com/ipni/go-libipni/mautil.FilterPrivateIPs
func FilterPrivateIPs(maddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	return mautil.FilterPrivateIPs(maddrs)
}

// Depricated: Use github.com/ipni/go-libipni/mautil.FindHTTPAddrs
func FindHTTPAddrs(maddrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	return mautil.FindHTTPAddrs(maddrs)
}

// Depricated: Use github.com/ipni/go-libipni/mautil.MultiaddrStringToNetAddr
func MultiaddrStringToNetAddr(maddrStr string) (net.Addr, error) {
	return mautil.MultiaddrStringToNetAddr(maddrStr)
}

// Depricated: Use github.com/ipni/go-libipni/mautil.ParsePeers
func ParsePeers(addrs []string) ([]peer.AddrInfo, error) {
	return mautil.ParsePeers(addrs)
}

// Depricated: Use github.com/ipni/go-libipni/mautil.StringsToMultiaddrs
func StringsToMultiaddrs(addrs []string) ([]multiaddr.Multiaddr, error) {
	return mautil.StringsToMultiaddrs(addrs)
}
