package dtsync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/storetheindex/announce"
	"github.com/ipni/storetheindex/announce/message"
	"github.com/ipni/storetheindex/dagsync/p2p/protocol/head"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type publisher struct {
	closeOnce     sync.Once
	dtManager     dt.Manager
	dtClose       dtCloseFunc
	extraData     []byte
	headPublisher *head.Publisher
	host          host.Host
	senders       []announce.Sender
}

// NewPublisher creates a new dagsync publisher.
func NewPublisher(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, topicName string, options ...Option) (*publisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	dtManager, _, dtClose, err := makeDataTransfer(host, ds, lsys, opts.allowPeer)
	if err != nil {
		return nil, err
	}

	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topicName, headPublisher)

	return &publisher{
		dtManager:     dtManager,
		dtClose:       dtClose,
		extraData:     opts.extraData,
		headPublisher: headPublisher,
		host:          host,
		senders:       opts.senders,
	}, nil
}

func startHeadPublisher(host host.Host, topicName string, headPublisher *head.Publisher) {
	go func() {
		log := log.With("topic", topicName, "host", host.ID())
		log.Infow("Starting head publisher for topic")
		err := headPublisher.Serve(host, topicName)
		if err != http.ErrServerClosed {
			log.Errorw("Head publisher stopped serving on topic on host", "err", err)
		}
		log.Infow("Stopped head publisher")
	}()
}

// NewPublisherFromExisting instantiates publishing on an existing
// data transfer instance.
func NewPublisherFromExisting(dtManager dt.Manager, host host.Host, topicName string, lsys ipld.LinkSystem, options ...Option) (*publisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	err = configureDataTransferForDagsync(context.Background(), dtManager, lsys, opts.allowPeer)
	if err != nil {
		return nil, fmt.Errorf("cannot configure datatransfer: %w", err)
	}
	headPublisher := head.NewPublisher()
	startHeadPublisher(host, topicName, headPublisher)

	return &publisher{
		extraData:     opts.extraData,
		headPublisher: headPublisher,
		host:          host,
		senders:       opts.senders,
	}, nil
}

func (p *publisher) Addrs() []multiaddr.Multiaddr {
	return p.host.Addrs()
}

func (p *publisher) ID() peer.ID {
	return p.host.ID()
}

func (p *publisher) Protocol() int {
	return multiaddr.P_P2P
}

func (p *publisher) AnnounceHead(ctx context.Context) error {
	return p.announce(ctx, p.headPublisher.Root(), p.Addrs())
}

func (p *publisher) AnnounceHeadWithAddrs(ctx context.Context, addrs []multiaddr.Multiaddr) error {
	return p.announce(ctx, p.headPublisher.Root(), addrs)
}

func (p *publisher) announce(ctx context.Context, c cid.Cid, addrs []multiaddr.Multiaddr) error {
	// Do nothing if nothing to announce or no means to announce it.
	if c == cid.Undef || len(p.senders) == 0 {
		return nil
	}

	msg := message.Message{
		Cid:       c,
		ExtraData: p.extraData,
	}
	msg.SetAddrs(addrs)

	var errs error
	for _, sender := range p.senders {
		if err := sender.Send(ctx, msg); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

func (p *publisher) SetRoot(ctx context.Context, c cid.Cid) error {
	if c == cid.Undef {
		return errors.New("cannot update to an undefined cid")
	}
	log.Debugf("Setting root CID: %s", c)
	return p.headPublisher.UpdateRoot(ctx, c)
}

func (p *publisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return p.UpdateRootWithAddrs(ctx, c, p.Addrs())
}

func (p *publisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, addrs []multiaddr.Multiaddr) error {
	err := p.SetRoot(ctx, c)
	if err != nil {
		return err
	}
	return p.announce(ctx, c, addrs)
}

func (p *publisher) Close() error {
	var errs error
	p.closeOnce.Do(func() {
		err := p.headPublisher.Close()
		if err != nil {
			errs = multierror.Append(errs, err)
		}

		for _, sender := range p.senders {
			if err = sender.Close(); err != nil {
				errs = multierror.Append(errs, err)
			}
		}

		if p.dtClose != nil {
			err = p.dtClose()
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}

		for _, sender := range p.senders {
			if err = sender.Close(); err != nil {
				errs = multierror.Append(errs, err)
			}
		}
	})
	return errs
}
