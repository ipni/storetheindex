package dtsync

import (
	"fmt"

	"github.com/ipni/storetheindex/announce"
	"github.com/libp2p/go-libp2p/core/peer"
)

// config contains all options for configuring dtsync.publisher.
type config struct {
	extraData []byte
	allowPeer func(peer.ID) bool
	senders   []announce.Sender
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

// WithAllowPeer sets the function that determines whether to allow or reject
// graphsync sessions from a peer.
func WithAllowPeer(allowPeer func(peer.ID) bool) Option {
	return func(c *config) error {
		c.allowPeer = allowPeer
		return nil
	}
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
