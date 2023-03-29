package httpsync

import (
	"fmt"

	"github.com/ipni/go-libipni/announce"
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	extraData []byte
	senders   []announce.Sender
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

// WithAnnounceSenders sets announce.Senders to use for sending announcements.
func WithAnnounceSenders(senders ...announce.Sender) Option {
	return func(c *config) error {
		if len(senders) != 0 {
			c.senders = senders
		}
		return nil
	}
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
