package announce

import (
	"context"

	"github.com/ipni/storetheindex/announce/message"
)

type Sender interface {
	// Close closes the Sender.
	Close() error
	// Send sends the announce Message.
	Send(context.Context, message.Message) error
}
