package gossiptopic

import (
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var _ pubsub.RawTracer = (*loggingTracer)(nil)

// loggingTracer is a pubsub.RawTracer that logs the internal operations of the pubsub subsystem
// for debugging purposes with debug log level.
type loggingTracer struct {
	log *logging.ZapEventLogger
}

func (l *loggingTracer) AddPeer(p peer.ID, proto protocol.ID) {
	l.log.Debugf("Peer added with ID %s and protocol %s", p, proto)
}

func (l *loggingTracer) RemovePeer(p peer.ID) {
	l.log.Debugf("Peer removed with ID %s", p)
}

func (l *loggingTracer) Join(topic string) {
	l.log.Debugf("Joined topic %s", topic)
}

func (l *loggingTracer) Leave(topic string) {
	l.log.Debugf("Left topic %s", topic)
}

func (l *loggingTracer) Graft(p peer.ID, topic string) {
	l.log.Debugf("Grafted peer with ID %s to topic %s", p, topic)
}

func (l *loggingTracer) Prune(p peer.ID, topic string) {
	l.log.Debugf("Pruned peer with ID %s from topic %s", p, topic)
}

func (l *loggingTracer) ValidateMessage(msg *pubsub.Message) {
	l.log.Debugf("Validating message from peer ID %s on topic %s with size %d bytes",
		msg.GetFrom(),
		msg.GetTopic(),
		msg.Size())
}

func (l *loggingTracer) DeliverMessage(msg *pubsub.Message) {
	l.log.Debugf("Delivered message from peer ID %s on topic %s with size %d bytes",
		msg.GetFrom(),
		msg.GetTopic(),
		msg.Size())
}

func (l *loggingTracer) RejectMessage(msg *pubsub.Message, reason string) {
	l.log.Debugf("Rejected message from peer ID %s on topic %s with size %d bytes: %s",
		msg.GetFrom(),
		msg.GetTopic(),
		msg.Size(),
		reason)
}

func (l *loggingTracer) DuplicateMessage(msg *pubsub.Message) {
	l.log.Debugf("Dropped duplicate message from peer ID %s on topic %s with size %d bytes",
		msg.GetFrom(),
		msg.GetTopic(),
		msg.Size())
}

func (l *loggingTracer) ThrottlePeer(p peer.ID) {
	l.log.Debugf("Throttled peer ID %s", p)
}

func (l *loggingTracer) RecvRPC(rpc *pubsub.RPC) {
	l.log.Debugf("Received RPC with size %d bytes", rpc.Size())
}

func (l *loggingTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	l.log.Debugf("Sent RPC with size %d bytes to peer ID %s", rpc.Size(), p)
}

func (l *loggingTracer) DropRPC(rpc *pubsub.RPC, p peer.ID) {
	l.log.Debugf("Dropped RPC with size %d bytes to peer ID %s", rpc.Size(), p)
}

func (l *loggingTracer) UndeliverableMessage(msg *pubsub.Message) {
	l.log.Debugf("Undeliverable message from peer ID %s on topic %s with size %d bytes",
		msg.GetFrom(),
		msg.GetTopic(),
		msg.Size())
}
