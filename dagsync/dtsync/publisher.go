package dtsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/storetheindex/announce/gossiptopic"
	"github.com/filecoin-project/storetheindex/dagsync/p2p/protocol/head"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
)

type publisher struct {
	cancelPubSub  context.CancelFunc
	closeOnce     sync.Once
	dtManager     dt.Manager
	dtClose       dtCloseFunc
	headPublisher *head.Publisher
	host          host.Host
	extraData     []byte
	topic         *pubsub.Topic
}

const shutdownTime = 5 * time.Second

// NewPublisher creates a new dagsync publisher.
func NewPublisher(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topic string, options ...Option) (*publisher, error) {
	cfg := config{}
	err := cfg.apply(options)
	if err != nil {
		return nil, err
	}

	var cancelPubsub context.CancelFunc
	t := cfg.topic
	if t == nil {
		t, cancelPubsub, err = gossiptopic.MakeTopic(host, topic)
		if err != nil {
			return nil, err
		}
	}

	dtManager, _, dtClose, err := makeDataTransfer(host, ds, lsys, cfg.allowPeer)
	if err != nil {
		if cancelPubsub != nil {
			cancelPubsub()
		}
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	p := &publisher{
		cancelPubSub:  cancelPubsub,
		dtManager:     dtManager,
		dtClose:       dtClose,
		headPublisher: headPublisher,
		host:          host,
		topic:         t,
	}

	if len(cfg.extraData) != 0 {
		p.extraData = cfg.extraData
	}
	return p, nil
}

func startHeadPublisher(host host.Host, topic string, headPublisher *head.Publisher) {
	go func() {
		log.Infow("Starting head publisher for topic", "topic", topic, "host", host.ID())
		err := headPublisher.Serve(host, topic)
		if err != http.ErrServerClosed {
			log.Errorw("Head publisher stopped serving on topic on host", "topic", topic, "host", host.ID(), "err", err)
		}
		log.Infow("Stopped head publisher", "host", host.ID(), "topic", topic)
	}()
}

// NewPublisherFromExisting instantiates publishing on an existing
// data transfer instance.
func NewPublisherFromExisting(dtManager dt.Manager, host host.Host, topic string, lsys ipld.LinkSystem, options ...Option) (*publisher, error) {
	cfg := config{}
	err := cfg.apply(options)
	if err != nil {
		return nil, err
	}

	var cancelPubsub context.CancelFunc
	t := cfg.topic
	if t == nil {
		t, cancelPubsub, err = gossiptopic.MakeTopic(host, topic)
		if err != nil {
			return nil, err
		}
	}

	err = configureDataTransferForDagsync(context.Background(), dtManager, lsys, cfg.allowPeer)
	if err != nil {
		if cancelPubsub != nil {
			cancelPubsub()
		}
		return nil, fmt.Errorf("cannot configure datatransfer: %w", err)
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topic, headPublisher)

	p := &publisher{
		cancelPubSub:  cancelPubsub,
		headPublisher: headPublisher,
		host:          host,
		topic:         t,
	}

	if len(cfg.extraData) != 0 {
		p.extraData = cfg.extraData
	}
	return p, nil
}

func (p *publisher) Addrs() []multiaddr.Multiaddr {
	return p.host.Addrs()
}

func (p *publisher) SetRoot(ctx context.Context, c cid.Cid) error {
	if c == cid.Undef {
		return errors.New("cannot update to an undefined cid")
	}
	log.Debugf("Setting root CID: %s", c)
	return p.headPublisher.UpdateRoot(ctx, c)
}

func (p *publisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return p.UpdateRootWithAddrs(ctx, c, p.host.Addrs())
}

func (p *publisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, addrs []multiaddr.Multiaddr) error {
	err := p.SetRoot(ctx, c)
	if err != nil {
		return err
	}
	log.Debugf("Publishing CID and addresses in pubsub channel: %s", c)
	msg := gossiptopic.Message{
		Cid:       c,
		ExtraData: p.extraData,
	}
	msg.SetAddrs(addrs)
	buf := bytes.NewBuffer(nil)
	if err := msg.MarshalCBOR(buf); err != nil {
		return err
	}
	return p.topic.Publish(ctx, buf.Bytes())
}

func (p *publisher) Close() error {
	var errs error
	p.closeOnce.Do(func() {
		err := p.headPublisher.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}

		if p.dtClose != nil {
			err = p.dtClose()
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}

		// If publisher owns the pubsub Topic, then leave topic and shutdown Pubsub.
		if p.cancelPubSub != nil {
			t := time.AfterFunc(shutdownTime, p.cancelPubSub)
			if err = p.topic.Close(); err != nil {
				log.Errorw("Failed to close pubsub topic", "err", err)
				errs = multierror.Append(errs, err)
			}
			if t.Stop() {
				p.cancelPubSub()
			}
		}
	})
	return errs
}
