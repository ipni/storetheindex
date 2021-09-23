package libp2pclient

import (
	"fmt"
	"net"

	"github.com/libp2p/go-libp2p-core/host"
)

// NOTE: Placeholder to add client options

type clientConfig struct {
	hostname string
	netProto string
	port     int
	p2pHost  host.Host
}

const defaultLibp2pPort = 3003

// Option type for p2pclient
type Option func(*clientConfig) error

// apply applies the given options to this Option.
func (c *clientConfig) apply(opts ...Option) error {
	err := clientDefaults(c)
	if err != nil {
		// Fbailure of default option should panic
		panic("default option failed: " + err.Error())
	}
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("p2pclient option %d failed: %s", i, err)
		}
	}
	return nil
}

var clientDefaults = func(cfg *clientConfig) error {
	cfg.port = defaultLibp2pPort
	return nil
}

// Hostname configures the address or hostname of alibp2p host to connect to.
func Hostname(hostname string) Option {
	return func(cfg *clientConfig) error {
		ip := net.ParseIP(cfg.hostname)
		if ip == nil {
			cfg.netProto = "dns"
		} else if ip.To4() != nil {
			cfg.netProto = "ip4"
		} else if ip.To16() != nil {
			cfg.netProto = "ip6"
		} else {
			return fmt.Errorf("host %q does not appear to be a hostname or ip address", hostname)
		}
		cfg.hostname = hostname
		return nil
	}
}

// P2PHost configures a libp2p host to connect to use for the client if one
// already exists.  If this option is not specified, then a new host is created
// for the client.
func P2PHost(h host.Host) Option {
	return func(cfg *clientConfig) error {
		cfg.p2pHost = h
		return nil
	}
}

// Port configures a non-default port.  This is only used if the Hostname
// option is provided.
func Port(port int) Option {
	return func(cfg *clientConfig) error {
		if port < 1 || port > 65535 {
			return fmt.Errorf("invalid port: %d", port)
		}
		cfg.port = port
		return nil
	}
}
