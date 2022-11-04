package announce

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type Option func(*config) error

// config contains all options for configuring Subscriber.
type config struct {
	allowPeer AllowPeerFunc
	filterIPs bool
	resend    bool
	topic     *pubsub.Topic
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
