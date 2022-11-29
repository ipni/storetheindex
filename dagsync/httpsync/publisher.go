package httpsync

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

type publisher struct {
	addr    multiaddr.Multiaddr
	closer  io.Closer
	lsys    ipld.LinkSystem
	peerID  peer.ID
	privKey ic.PrivKey
	rl      sync.RWMutex
	root    cid.Cid
}

var _ http.Handler = (*publisher)(nil)

// NewPublisher creates a new http publisher, listening on the specified
// address.
func NewPublisher(address string, lsys ipld.LinkSystem, peerID peer.ID, privKey ic.PrivKey) (*publisher, error) {
	if privKey == nil {
		return nil, errors.New("private key required to sign head requests")
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
		addr:    multiaddr.Join(maddr, proto),
		closer:  l,
		lsys:    lsys,
		peerID:  peerID,
		privKey: privKey,
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

func (p *publisher) SetRoot(ctx context.Context, c cid.Cid) error {
	p.rl.Lock()
	defer p.rl.Unlock()
	p.root = c
	return nil
}

func (p *publisher) UpdateRoot(ctx context.Context, c cid.Cid) error {
	return p.SetRoot(ctx, c)
}

func (p *publisher) UpdateRootWithAddrs(ctx context.Context, c cid.Cid, _ []multiaddr.Multiaddr) error {
	return p.UpdateRoot(ctx, c)
}

func (p *publisher) Close() error {
	return p.closer.Close()
}

func (p *publisher) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ask := path.Base(r.URL.Path)
	if ask == "head" {
		// serve the
		p.rl.RLock()
		defer p.rl.RUnlock()

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
