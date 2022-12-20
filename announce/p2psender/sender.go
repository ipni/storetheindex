package p2psender

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ipni/storetheindex/announce/gossiptopic"
	"github.com/ipni/storetheindex/announce/message"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

const shutdownTime = 5 * time.Second

type Sender struct {
	cancelPubSub context.CancelFunc
	topic        *pubsub.Topic
}

// New creates a new Sender that sends announce messages over pubsub.
func New(p2pHost host.Host, topicName string, options ...Option) (*Sender, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	var cancelPubsub context.CancelFunc
	topic := opts.topic
	if topic == nil {
		if topicName != "" {
			topic, cancelPubsub, err = gossiptopic.MakeTopic(p2pHost, topicName)
			if err != nil {
				return nil, err
			}
		}
	}

	return &Sender{
		cancelPubSub: cancelPubsub,
		topic:        topic,
	}, nil
}

// Close stops the pubsub topic if the sender owns to topic.
func (s *Sender) Close() error {
	if s.cancelPubSub == nil {
		return nil
	}

	// If publisher owns the pubsub Topic, then leave topic and shutdown Pubsub.
	t := time.AfterFunc(shutdownTime, s.cancelPubSub)
	err := s.topic.Close()
	if err != nil {
		err = fmt.Errorf("failed to close pubsub topic: %w", err)
	}
	if t.Stop() {
		s.cancelPubSub()
	}
	s.cancelPubSub = nil
	return err
}

// Send sends the Message to the pubsub topic.
func (s *Sender) Send(ctx context.Context, msg message.Message) error {
	buf := bytes.NewBuffer(nil)
	if err := msg.MarshalCBOR(buf); err != nil {
		return err
	}
	return s.topic.Publish(ctx, buf.Bytes())
}

// TopicName returns the name of the topic that messages are sent to.
func (s *Sender) TopicName() string {
	return s.topic.String()
}
