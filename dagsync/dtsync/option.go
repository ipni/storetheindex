package dtsync

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	extraData []byte
	topic     *pubsub.Topic
	allowPeer func(peer.ID) bool
}

type Option func(*config) error

// apply applies the given options to this config.
func (c *config) apply(opts []Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

// WithExtraData sets the extra data to include in the pubsub message.
func WithExtraData(data []byte) Option {
	return func(c *config) error {
		if len(data) != 0 {
			c.extraData = data
		}
		return nil
	}
}

// Topic provides an existing pubsub topic.
func Topic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}

// AllowPeer sets the function that determines whether to allow or reject
// graphsync sessions from a peer.
func AllowPeer(allowPeer func(peer.ID) bool) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
}
