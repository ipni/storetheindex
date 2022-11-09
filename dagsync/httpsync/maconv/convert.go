package maconv

import (
	"bytes"
	"fmt"
	"net"
	"net/url"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// register an 'httpath' component:
var transcodePath = multiaddr.NewTranscoderFromFunctions(pathStB, pathBtS, pathVal)

func pathVal(b []byte) error {
	if bytes.IndexByte(b, '/') >= 0 {
		return fmt.Errorf("encoded path '%s' contains a slash", string(b))
	}
	return nil
}

func pathStB(s string) ([]byte, error) {
	return []byte(s), nil
}

func pathBtS(b []byte) (string, error) {
	return string(b), nil
}

func init() {
	_ = multiaddr.AddProtocol(protoHTTPath)
}

var protoHTTPath = multiaddr.Protocol{
	Name:       "httpath",
	Code:       0x300200,
	VCode:      multiaddr.CodeToVarint(0x300200),
	Size:       multiaddr.LengthPrefixedVarSize,
	Transcoder: transcodePath,
}

// ToURL takes a multiaddr of the form:
// /dns/thing.com/http/urlescape<path/to/root>
// /ip/192.168.0.1/tcp/80/http
func ToURL(ma multiaddr.Multiaddr) (*url.URL, error) {
	// host should be either the dns name or the IP
	_, host, err := manet.DialArgs(ma)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.To4().Equal(ip) {
			// raw v6 IPs need `[ip]` encapsulation.
			host = fmt.Sprintf("[%s]", host)
		}
	}

	protos := ma.Protocols()
	pm := make(map[int]string, len(protos))
	for _, p := range protos {
		v, err := ma.ValueForProtocol(p.Code)
		if err == nil {
			pm[p.Code] = v
		}
	}

	scheme := "http"
	if _, ok := pm[multiaddr.P_HTTPS]; ok {
		scheme = "https"
	} else if _, ok = pm[multiaddr.P_HTTP]; ok {
		// /tls/http == /https
		if _, ok = pm[multiaddr.P_TLS]; ok {
			scheme = "https"
		}
	} else if _, ok = pm[multiaddr.P_WSS]; ok {
		scheme = "wss"
	} else if _, ok = pm[multiaddr.P_WS]; ok {
		scheme = "ws"
		// /tls/ws == /wss
		if _, ok = pm[multiaddr.P_TLS]; ok {
			scheme = "wss"
		}
	}

	path := ""
	if pb, ok := pm[protoHTTPath.Code]; ok {
		path, err = url.PathUnescape(pb)
		if err != nil {
			path = ""
		}
	}

	out := url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   path,
	}
	return &out, nil
}

// ToMultiaddr takes a url and converts it into a multiaddr.
// converts scheme://host:port/path -> /ip/host/tcp/port/scheme/urlescape{path}
func ToMultiaddr(u *url.URL) (multiaddr.Multiaddr, error) {
	h := u.Hostname()
	var addr *multiaddr.Multiaddr
	if n := net.ParseIP(h); n != nil {
		ipAddr, err := manet.FromIP(n)
		if err != nil {
			return nil, err
		}
		addr = &ipAddr
	} else {
		// domain name
		ma, err := multiaddr.NewComponent(multiaddr.ProtocolWithCode(multiaddr.P_DNS).Name, h)
		if err != nil {
			return nil, err
		}
		mab := multiaddr.Cast(ma.Bytes())
		addr = &mab
	}
	pv := u.Port()
	if pv != "" {
		port, err := multiaddr.NewComponent(multiaddr.ProtocolWithCode(multiaddr.P_TCP).Name, pv)
		if err != nil {
			return nil, err
		}
		wport := multiaddr.Join(*addr, port)
		addr = &wport
	}

	http, err := multiaddr.NewComponent(u.Scheme, "")
	if err != nil {
		return nil, err
	}

	joint := multiaddr.Join(*addr, http)
	if u.Path != "" {
		httpath, err := multiaddr.NewComponent(protoHTTPath.Name, url.PathEscape(u.Path))
		if err != nil {
			return nil, err
		}
		joint = multiaddr.Join(joint, httpath)
	}

	return joint, nil
}
