package p2psender

import (
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	topic *pubsub.Topic
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

// WithTopic provides an existing pubsub topic.
func WithTopic(topic *pubsub.Topic) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}
