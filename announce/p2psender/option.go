package p2psender

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	topic *pubsub.Topic
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

// WithTopic provides an existing pubsub topic.
func WithTopic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}
