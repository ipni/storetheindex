package maconv

import (
	"net/url"
	"testing"

	"github.com/multiformats/go-multiaddr"
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
		if err != nil {
			t.Fatal(err)
		}
		u2, err := ToURL(mu)
		if err != nil {
			t.Fatal(err)
		}
		if u2.Scheme != u.Scheme {
			t.Fatalf("scheme didn't roundtrip. got %s expected %s", u2.Scheme, u.Scheme)
		}
		if u2.Host != u.Host {
			t.Fatalf("host didn't roundtrip. got %s, expected %s", u2.Host, u.Host)
		}
		if u2.Path != u.Path {
			t.Fatalf("path didn't roundtrip. got %s, expected %s", u2.Path, u.Path)
		}
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
		if err != nil {
			t.Fatal(err)
		}

		u, err := ToURL(m)
		if err != nil {
			t.Fatal(err)
		}
		if u.String() != expect[i] {
			t.Fatalf("expected %s to convert to url %s, got %s", m.String(), expect[i], u.String())
		}
	}
}
