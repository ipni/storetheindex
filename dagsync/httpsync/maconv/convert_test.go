package maconv

import (
	"net/url"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestRoundtrip(t *testing.T) {
	samples := []string{
		"http://www.google.com/path/to/rsrc",
		"https://protocol.ai",
		"http://192.168.0.1:8080/admin",
		"https://[2a00:1450:400e:80d::200e]:443/",
		"https://[2a00:1450:400e:80d::200e]/",
	}

	for _, s := range samples {
		u, _ := url.Parse(s)
		mu, err := ToMultiaddr(u)
		require.NoError(t, err)
		u2, err := ToURL(mu)
		require.NoError(t, err)
		require.Equal(t, u.Scheme, u2.Scheme, "scheme didn't roundtrip")
		require.Equal(t, u.Host, u2.Host, "host didn't roundtrip")
		require.Equal(t, u.Path, u2.Path, "path didn't roundtrip")
	}
}

func TestTLSProtos(t *testing.T) {
	samples := []string{
		"/ip4/192.169.0.1/tls/http",
		"/ip4/192.169.0.1/https",
		"/ip4/192.169.0.1/http",
		"/dns4/protocol.ai/tls/ws",
		"/dns4/protocol.ai/wss",
		"/dns4/protocol.ai/ws",
	}

	expect := []string{
		"https://192.169.0.1",
		"https://192.169.0.1",
		"http://192.169.0.1",
		"wss://protocol.ai",
		"wss://protocol.ai",
		"ws://protocol.ai",
	}

	for i := range samples {
		m, err := multiaddr.NewMultiaddr(samples[i])
		require.NoError(t, err)

		u, err := ToURL(m)
		require.NoError(t, err)
		require.Equal(t, expect[i], u.String(), "Did not convert to expected URL")
	}
}
