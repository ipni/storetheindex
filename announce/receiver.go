package announce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/storetheindex/announce/gossiptopic"
	"github.com/ipni/storetheindex/announce/message"
	"github.com/ipni/storetheindex/announce/p2psender"
	"github.com/ipni/storetheindex/mautil"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("announce")

const announceCacheSize = 64

// AllowPeerFunc is the signature of a function given to Subscriber that
// determines whether to allow or reject messages originating from a peer
// passed into the function. Returning true or false indicates that messages
// from that peer are allowed rejected, respectively.
type AllowPeerFunc func(peer.ID) bool

var (
	// ErrClosed is returned from Next and Direct when the Received is closed.
	ErrClosed = errors.New("closed")
	// errSourceNotAllowed is the error returned when a message source peer's
	// messages is not allowed to be processed. This is only used internally, and
	// pre-allocated here as it may occur frequently.
	errSourceNotAllowed = errors.New("message source not allowed")
	// errAlreadySeenCid is the error returned when an announce message is for a
	// CID has already been announced by a previous announce message.
	errAlreadySeenCid = errors.New("announcement for already seen CID")
)

// Receiver receives announce messages via gossip pubsub and HTTP. Receiver
// creates a single pubsub subscriber that receives messages from a gossip
// pubsub topic. Direct messages are received when the Receiver's Direct method
// is called.
type Receiver struct {
	allowPeer AllowPeerFunc
	filterIPs bool
	resend    bool
	hostID    peer.ID

	announceCache *stringLRU
	// announceMutex protects announceCache, and allowPeer, topicSub
	announceMutex sync.Mutex

	closed bool
	// cancelWatch stops the pubsub watcher
	cancelWatch context.CancelFunc
	// watchDone signals that the pubsub watch function exited.
	watchDone chan struct{}
	// does tells Next to stop waiting on the out channel.
	done chan struct{}

	cancelPubsub context.CancelFunc
	sender       *p2psender.Sender
	topic        *pubsub.Topic
	topicSub     *pubsub.Subscription

	outChan chan Announce
}

// Announce contains information about the announcement of an index
// advertisement.
type Announce struct {
	// Cid is the advertisement content identifier to announce.
	Cid cid.Cid
	// PeerID is the p2p peer ID hosting the announced advertisement.
	PeerID peer.ID
	// Addrs is the network location(s) hosting the announced advertisement.
	Addrs []multiaddr.Multiaddr
}

// NewReceiver creates a new Receiver that subscribes to the named pubsub topic
// and is listening for announce messages.
func NewReceiver(p2pHost host.Host, topicName string, options ...Option) (*Receiver, error) {
	cfg := config{}
	for i, opt := range options {
		if err := opt(&cfg); err != nil {
			return nil, fmt.Errorf("option %d failed: %s", i, err)
		}
	}

	var cancelPubsub context.CancelFunc
	var err error

	pubsubTopic := cfg.topic
	if pubsubTopic == nil && p2pHost != nil && topicName != "" {
		pubsubTopic, cancelPubsub, err = gossiptopic.MakeTopic(p2pHost, topicName)
		if err != nil {
			return nil, err
		}
		log.Infow("Created gossip pubsub and joined topic", "topic", topicName, "hostID", p2pHost.ID())
	}

	var sender *p2psender.Sender
	var topicSub *pubsub.Subscription
	if pubsubTopic != nil {
		topicSub, err = pubsubTopic.Subscribe()
		if err != nil {
			if cancelPubsub != nil {
				cancelPubsub()
			}
			return nil, err
		}

		sender, err = p2psender.New(nil, "", p2psender.WithTopic(pubsubTopic))
		if err != nil {
			return nil, err
		}
	} else {
		// Cannot republish if pubsub not available.
		cfg.resend = false
	}

	r := &Receiver{
		allowPeer: cfg.allowPeer,
		filterIPs: cfg.filterIPs,
		resend:    cfg.resend,

		announceCache: newStringLRU(announceCacheSize),

		done: make(chan struct{}),

		cancelPubsub: cancelPubsub,
		sender:       sender,
		topic:        pubsubTopic,
		topicSub:     topicSub,

		outChan: make(chan Announce, 1),
	}

	if p2pHost != nil {
		r.hostID = p2pHost.ID()
		watchCtx, cancelWatch := context.WithCancel(context.Background())
		r.cancelWatch = cancelWatch
		r.watchDone = make(chan struct{})

		// Start watcher to read pubsub messages.
		go r.watch(watchCtx)
	}

	return r, nil
}

// Next waits for and returns the next announce message that has passed
// filtering checks. Next also returns ErrClosed if the receiver is closed, or
// the context error if the given context is canceled.
func (r *Receiver) Next(ctx context.Context) (Announce, error) {
	select {
	case <-ctx.Done():
		return Announce{}, ctx.Err()
	case amsg := <-r.outChan:
		return amsg, nil
	case <-r.done:
		return Announce{}, ErrClosed
	}
}

// Close shuts down the Receiver.
func (r *Receiver) Close() error {
	r.announceMutex.Lock()
	if r.closed {
		return nil
	}
	r.closed = true

	if r.topicSub != nil {
		r.topicSub.Cancel()
	}

	r.announceMutex.Unlock()

	// Tell Next to stop waiting.
	close(r.done)

	// Cancel watch and wait for pubsub watch to exit.
	if r.cancelWatch != nil {
		r.cancelWatch()
		<-r.watchDone
	}

	var err error
	// If Receiver owns the pubsub topic, then close it.
	if r.cancelPubsub != nil {
		// Leave pubsub topic.
		if err = r.topic.Close(); err != nil {
			err = fmt.Errorf("failed to close pubsub topic: %w", err)
		}
		// Shutdown pubsub.
		r.cancelPubsub()
	} else if r.sender != nil {
		err = r.sender.Close()
	}

	return err
}

// SetAllowPeer configures Subscriber with a function to evaluate whether to
// allow or reject messages from a peer. Setting nil removes any filtering and
// allows messages from all peers. Calling SetAllowPeer replaces any previously
// configured AllowPeerFunc.
func (r *Receiver) SetAllowPeer(allowPeer AllowPeerFunc) {
	r.announceMutex.Lock()
	r.allowPeer = allowPeer
	r.announceMutex.Unlock()
}

// UncacheCid removes a CID from the announce cache.
func (r *Receiver) UncacheCid(adCid cid.Cid) {
	r.announceMutex.Lock()
	r.announceCache.remove(adCid.String())
	r.announceMutex.Unlock()
}

// TopicName returns the name of the topic the Receiver is listening on.
func (r *Receiver) TopicName() string {
	return r.topic.String()
}

// watch reads messages from a pubsub topic subscription and passes the message
// to a channel.
func (r *Receiver) watch(ctx context.Context) {
	for {
		msg, err := r.topicSub.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, pubsub.ErrSubscriptionCancelled) {
				// This is a normal result of shutting down the Subscriber.
				log.Debug("Canceled watching pubsub subscription")
				break
			}
			log.Errorw("Error reading from pubsub", "err", err)
			// Restart subscription.
			r.announceMutex.Lock()
			r.topicSub.Cancel()
			r.topicSub, err = r.topic.Subscribe()
			r.announceMutex.Unlock()
			if err != nil {
				log.Errorw("Cannot restart subscription", "err", err, "topic", r.TopicName())
				break
			}
			continue
		}

		srcPeer, err := peer.IDFromBytes(msg.From)
		if err != nil {
			continue
		}

		// Decode CID and originator addresses from message.
		m := message.Message{}
		if err = m.UnmarshalCBOR(bytes.NewBuffer(msg.Data)); err != nil {
			log.Errorw("Could not decode pubsub message", "err", err)
			continue
		}

		// Read publisher addresses from message.
		var addrs []multiaddr.Multiaddr
		if len(m.Addrs) != 0 {
			addrs, err = m.GetAddrs()
			if err != nil {
				log.Errorw("Could not decode pubsub message", "err", err)
				continue
			}
		}

		// If message has original peer set, then this is a republished message.
		if m.OrigPeer != "" {
			// Ignore re-published announce from this host.
			if srcPeer == r.hostID {
				log.Debug("Ignored rebuplished announce from self")
				continue
			}

			// Read the original publisher.
			relayPeer := srcPeer
			srcPeer, err = peer.Decode(m.OrigPeer)
			if err != nil {
				log.Errorw("Cannot read peerID from republished announce", "err", err)
				continue
			}
			log.Infow("Handling re-published pubsub announce", "originPeer", srcPeer, "relayPeer", relayPeer)
		} else {
			log.Infow("Handling pubsub announce", "peer", srcPeer)
		}

		amsg := Announce{
			Cid:    m.Cid,
			PeerID: srcPeer,
			Addrs:  addrs,
		}
		err = r.handleAnnounce(ctx, amsg, false)
		if err != nil {
			log.Errorw("Cannot process message", "err", err)
			continue
		}
	}

	close(r.watchDone)
}

// Direct handles a direct announce message, that was not arrived over pubsub.
// The message is resent over pubsub with the original peerID encoded into the
// message extra data.
func (r *Receiver) Direct(ctx context.Context, nextCid cid.Cid, peerID peer.ID, addrs []multiaddr.Multiaddr) error {
	log.Infow("Handling direct announce", "peer", peerID)
	amsg := Announce{
		Cid:    nextCid,
		PeerID: peerID,
		Addrs:  addrs,
	}
	return r.handleAnnounce(ctx, amsg, true)
}

func (r *Receiver) handleAnnounce(ctx context.Context, amsg Announce, direct bool) error {
	err := r.announceCheck(amsg)
	if err != nil {
		if err == ErrClosed {
			return err
		}
		log.Infow("Ignored announcement", "reason", err, "peer", amsg.PeerID)
		return nil
	}

	if r.filterIPs {
		amsg.Addrs = mautil.FilterPrivateIPs(amsg.Addrs)
		// Even if there are no addresses left after filtering, continue
		// because the others receiving the announce may be able to look up the
		// address in their peer store.
	}

	if direct && r.resend {
		err = r.republish(ctx, amsg)
		if err != nil {
			log.Errorw("Cannot republish announce message", "err", err)
		} else {
			log.Infow("Re-published direct announce message in pubsub channel", "cid", amsg.Cid, "originPeer", amsg.PeerID)
		}
	}

	select {
	case r.outChan <- amsg:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (r *Receiver) announceCheck(amsg Announce) error {
	r.announceMutex.Lock()
	defer r.announceMutex.Unlock()

	if r.closed {
		return ErrClosed
	}

	// Check callback to see if peer ID allowed.
	if r.allowPeer != nil && !r.allowPeer(amsg.PeerID) {
		return errSourceNotAllowed
	}

	// Check if a previous announce for this CID was already seen.
	if r.announceCache.update(amsg.Cid.String()) {
		return errAlreadySeenCid
	}

	return nil
}

func (r *Receiver) republish(ctx context.Context, amsg Announce) error {
	msg := message.Message{
		Cid:      amsg.Cid,
		OrigPeer: amsg.PeerID.String(),
	}
	msg.SetAddrs(amsg.Addrs)
	return r.sender.Send(ctx, msg)
}
