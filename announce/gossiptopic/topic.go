package gossiptopic

import (
	"context"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"golang.org/x/crypto/blake2b"
)

var log = logging.Logger("gossiptopic")

// directConnectTicks makes pubsub check connections to peers every N seconds.
const directConnectTicks uint64 = 30

func makePubsub(h host.Host, topicName string) (*pubsub.PubSub, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())

	gossipSub, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerExchange(true),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			h, _ := blake2b.New256(nil)
			h.Write(pmsg.Data)
			return string(h.Sum(nil))
		}),
		pubsub.WithFloodPublish(true),
		pubsub.WithDirectConnectTicks(directConnectTicks),
		pubsub.WithRawTracer(&loggingTracer{log}),
	)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return gossipSub, cancel, nil
}

// MakeTopic creates a new PubSub object using the default GossipSubRouter as
// the router, and then joins the named topic. Returns a Topic handle for the
// joined topic and a CancelFunc to shutdown the PubSub object. Only one Topic
// handle should exist per topic, and MakeTopic will error if the Topic handle
// already exists.
func MakeTopic(h host.Host, topicName string) (*pubsub.Topic, context.CancelFunc, error) {
	gossipSub, cancel, err := makePubsub(h, topicName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gossip pubsub: %w", err)
	}

	topic, err := gossipSub.Join(topicName)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("failed to join topic %s: %w", topicName, err)
	}

	return topic, cancel, nil
}
