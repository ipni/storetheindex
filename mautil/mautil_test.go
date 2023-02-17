package mautil_test

import (
	"testing"

	"github.com/ipni/storetheindex/mautil"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestFilterPrivateIPs(t *testing.T) {
	addrs := []string{
		"/ip4/10.255.0.0/tcp/443",
		"/ip4/11.0.0.0/tcp/80",
		"/ip6/fc00::/tcp/1717",
		"/ip6/fe00::/tcp/8080",
		"/ip4/192.168.11.22/tcp/9999",
		"/dns4/example.net/tcp/1234",
		"/ip4/127.0.0.1/tcp/9999",
		"/dns4/localhost/tcp/1234",
	}
	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(addrs[i])
		require.NoError(t, err)
	}
	expected := make([]multiaddr.Multiaddr, 0, 3)
	expected = append(expected, maddrs[1])
	expected = append(expected, maddrs[3])
	expected = append(expected, maddrs[5])

	filtered := mautil.FilterPrivateIPs(maddrs)
	require.Equal(t, len(expected), len(filtered))

	for i := range filtered {
		require.Equal(t, expected[i], filtered[i])
	}

	filtered = mautil.FilterPrivateIPs(nil)
	require.Nil(t, filtered)
}

func TestFilterPrivateIPs_DoesNotPanicOnNilAddr(t *testing.T) {
	original := []multiaddr.Multiaddr{nil}
	got := mautil.FilterPrivateIPs(original)
	// According to the function documentation, it should return the original slice.
	require.Equal(t, original, got)
}
