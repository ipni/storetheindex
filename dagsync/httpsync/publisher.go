package httpsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipni/storetheindex/announce"
	"github.com/ipni/storetheindex/announce/message"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

type publisher struct {
	addr      multiaddr.Multiaddr
	closer    io.Closer
	lsys      ipld.LinkSystem
	peerID    peer.ID
	privKey   ic.PrivKey
	rl        sync.RWMutex
	root      cid.Cid
	senders   []announce.Sender
	extraData []byte
}

var _ http.Handler = (*publisher)(nil)

// NewPublisher creates a new http publisher, listening on the specified
// address.
func NewPublisher(address string, lsys ipld.LinkSystem, privKey ic.PrivKey, options ...Option) (*publisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	if privKey == nil {
		return nil, errors.New("private key required to sign head requests")
	}
	peerID, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer if from private key: %w", err)
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	maddr, err := manet.FromNetAddr(l.Addr())
	if err != nil {
		l.Close()
		return nil, err
	}
	proto, _ := multiaddr.NewMultiaddr("/http")

	pub := &publisher{
		addr:      multiaddr.Join(maddr, proto),
		closer:    l,
		lsys:      lsys,
		peerID:    peerID,
		privKey:   privKey,
		senders:   opts.senders,
		extraData: opts.extraData,
	}

	// Run service on configured port.
	server := &http.Server{
		Handler: pub,
		Addr:    l.Addr().String(),
	}
	go server.Serve(l)

	return pub, nil
}

// Addrs returns the addresses, as []multiaddress, that the publisher is
// listening on.
func (p *publisher) Addrs() []multiaddr.Multiaddr {
	return []multiaddr.Multiaddr{p.addr}
}

func (p *publisher) ID() peer.ID {
	return p.peerID
}

func (p *publisher) Protocol() int {
	return multiaddr.P_HTTP
}

func (p *publisher) AnnounceHead(ctx context.Context) error {
	p.rl.Lock()
	c := p.root
	p.rl.Unlock()
	return p.announce(ctx, c, p.Addrs())
}

func (p *publisher) AnnounceHeadWithAddrs(ctx context.Context, addrs []multiaddr.Multiaddr) error {
	p.rl.Lock()
	c := p.root
	p.rl.Unlock()
	return p.announce(ctx, c, addrs)
}

func (p *publisher) announce(ctx context.Context, c cid.Cid, addrs []multiaddr.Multiaddr) error {
	// Do nothing if nothing to announce or no means to announce it.
	if c == cid.Undef || len(p.senders) == 0 {
		return nil
	}

	log.Debugf("Publishing CID and addresses over HTTP: %s", c)
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
	p.rl.Lock()
	defer p.rl.Unlock()
	p.root = c
	return nil
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
	err := p.closer.Close()
	if err != nil {
		errs = multierror.Append(errs, err)
	}
	for _, sender := range p.senders {
		if err = sender.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

func (p *publisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ask := path.Base(r.URL.Path)
	if ask == "head" {
		// serve the head
		p.rl.RLock()
		defer p.rl.RUnlock()

		if p.root == cid.Undef {
			http.Error(w, "", http.StatusNoContent)
			return
		}
		marshalledMsg, err := newEncodedSignedHead(p.root, p.privKey)
		if err != nil {
			http.Error(w, "Failed to encode", http.StatusInternalServerError)
			log.Errorw("Failed to serve root", "err", err)
		} else {
			_, _ = w.Write(marshalledMsg)
		}
		return
	}
	// interpret `ask` as a CID to serve.
	c, err := cid.Parse(ask)
	if err != nil {
		http.Error(w, "invalid request: not a cid", http.StatusBadRequest)
		return
	}
	item, err := p.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	if err != nil {
		if errors.Is(err, ipld.ErrNotExists{}) {
			http.Error(w, "cid not found", http.StatusNotFound)
			return
		}
		http.Error(w, "unable to load data for cid", http.StatusInternalServerError)
		log.Errorw("Failed to load requested block", "err", err, "cid", c)
		return
	}
	// marshal to json and serve.
	_ = dagjson.Encode(item, w)

	// TODO: Sign message using publisher's private key.
}
