// Deprecated: The same functionality is provided by package
// github.com/ipni/go-libipni/maurl, and those implementations should be
// preferred in new code.
// See the specific function documentation for details.
package maconv

import (
	"net/url"

	"github.com/ipni/go-libipni/maurl"
	"github.com/multiformats/go-multiaddr"
)

// ToURL takes a multiaddr of the form:
// /dns/thing.com/http/urlescape<path/to/root>
//
// Depricated: This function simply calls github.com/ipni/go-libipni/maurl.ToURL.
func ToURL(ma multiaddr.Multiaddr) (*url.URL, error) {
	return maurl.ToURL(ma)
}

// ToMultiaddr takes a url and converts it into a multiaddr.
// converts scheme://host:port/path -> /ip/host/tcp/port/scheme/urlescape{path}
//
// Depricated: This function simply calls github.com/ipni/go-libipni/maurl.FromURL.
func ToMultiaddr(u *url.URL) (multiaddr.Multiaddr, error) {
	return maurl.FromURL(u)
}
