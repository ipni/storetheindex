package announce

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring Subscriber.
type config struct {
	allowPeer AllowPeerFunc
	filterIPs bool
	resend    bool
	topic     *pubsub.Topic
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	var cfg config
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

// WithAllowPeer sets the function that determines whether to allow or reject
// messages from a peer.
func WithAllowPeer(allowPeer AllowPeerFunc) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
}

// WithFilterIPs sets whether or not IP filtering is enabled. When enabled it
// removes any private, loopback, or unspecified IP multiaddrs from addresses
// supplied in announce messages.
func WithFilterIPs(enable bool) Option {
	return func(c *config) error {
		c.filterIPs = enable
		return nil
	}
}

// WithResend determines whether to resend direct announce mesages (those that
// are not received via pubsub) over pubsub.
func WithResend(enable bool) Option {
	return func(c *config) error {
		c.resend = enable
		return nil
	}
}

// WithTopic provides an existing pubsub topic.
func WithTopic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}
