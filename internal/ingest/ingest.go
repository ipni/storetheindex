package ingest

import (
	"context"
	"fmt"

	indexer "github.com/filecoin-project/go-indexer-core/engine"
	ingestion "github.com/filecoin-project/storetheindex/api/v0/ingest"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/im7mortal/kmutex"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/willscott/go-legs"
)

var _ ingestion.Ingester = &legIngester{}

var log = logging.Logger("indexer/ingest")

const (
	syncPrefix = "/sync/"
)

type legIngester struct {
	host     host.Host
	ds       datastore.Batching
	gs       graphsync.GraphExchange
	lsys     ipld.LinkSystem
	subTopic string
	indexer  *indexer.Engine

	subs  map[peer.ID]*sub
	sublk *kmutex.Kmutex
}

type sub struct {
	p       peer.ID
	ls      legs.LegSubscriber
	watcher chan cid.Cid
	cncl    context.CancelFunc
}

func (s *sub) cancelFunc(c1, c2 context.CancelFunc) context.CancelFunc {
	return func() {
		c1()
		c2()
	}
}

// NewLegIngester creates a new go-legs-backed ingester.
func NewLegIngester(ctx context.Context, cfg config.Ingest, h host.Host,
	i *indexer.Engine, ds datastore.Batching) (ingestion.Ingester, error) {

	lsys := mkVanillaLinkSystem(ds)
	gsnet := gsnet.NewFromLibp2pHost(h)
	gs := gsimpl.New(ctx, gsnet, lsys)

	return &legIngester{
		host:     h,
		ds:       ds,
		indexer:  i,
		lsys:     lsys,
		gs:       gs,
		subTopic: cfg.PubSubTopic,
		subs:     make(map[peer.ID]*sub),
		sublk:    kmutex.New(),
	}, nil
}

// Sync with a data provider up to latest ID
func (i *legIngester) Sync(ctx context.Context, p peer.ID) error {
	// Check latest sync for provider.
	c, err := i.getLatestAdvID(ctx, p)
	if err != nil {
		return err
	}

	// Check if we already have the advertisement.
	adv, err := i.ds.Get(datastore.NewKey(c.String()))
	if err != nil && err != datastore.ErrNotFound {
		return err
	}
	// If we have the advertisement do nothing, we are in sync.
	if adv != nil {
		return nil
	}

	// TODO: Figure out if graphsync should be shared or not before this.
	// Close current subscriber if any.
	// Sync with dedicated data transfer with stopAt in latestSync.
	// Start a new partiallySynced subscriber from the last advertisement.
	// NOTE: This may need changes over legs to allow dedicated transfers?
	panic("Not implemented")
}

func (i *legIngester) getLatestAdvID(ctx context.Context, p peer.ID) (cid.Cid, error) {
	// Query provider to get its latest sync.
	panic("not implemented")
}

// Subscribe to advertisements of a specific provider in the pubsub channel
func (i *legIngester) Subscribe(ctx context.Context, p peer.ID) error {
	sctx, cancel := context.WithCancel(ctx)
	s, err := i.newPeerSubscriber(sctx, p)
	if err != nil {
		cancel()
		return err
	}

	// If already subscribed do nothing.
	if s.watcher != nil {
		cancel()
		return nil
	}

	var cncl context.CancelFunc
	s.watcher, cncl = s.ls.OnChange()
	s.cncl = s.cancelFunc(cncl, cancel)

	// Listen updates persist latestSync when sync is done.
	go i.listenUpdates(ctx, s)
	return nil
}

func (i *legIngester) listenUpdates(ctx context.Context, s *sub) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Done?")
			return
		// Persist the latest sync
		case c := <-s.watcher:
			err := i.putLatestSync(s.p, c)
			if err != nil {
				log.Errorf("Error persisting latest sync: %w", err)
			}
		}
	}
}

// Unsubscribe to stop listening to advertisement from a specific provider.
func (i *legIngester) Unsubscribe(ctx context.Context, p peer.ID) error {
	i.sublk.Lock(p)
	defer i.sublk.Unlock(p)
	// Run cancel
	i.subs[p].cncl()
	// Close subscriber
	i.subs[p].ls.Close(ctx)
	// Delete from map
	delete(i.subs, p)

	return nil
}

// Creates a new subscriber for a peer according to its latest sync.
func (i *legIngester) newPeerSubscriber(ctx context.Context, p peer.ID) (*sub, error) {
	i.sublk.Lock(p)
	defer i.sublk.Unlock(p)
	s, ok := i.subs[p]
	// If there is already a subscriber for the peer, do nothing.
	if ok {
		return s, nil
	}

	// See if we already synced with this peer.
	c, err := i.getLatestSync(p)
	if err != nil {
		return nil, err
	}

	// If not synced start a brand new subscriber
	if c == cid.Undef {
		ls, err := legs.NewSubscriber(ctx, i.ds, i.host,
			i.gs, i.subTopic, legs.FilterPeerPolicy(p))
		if err != nil {
			return nil, err
		}
		s = &sub{p: p, ls: ls}
		i.subs[p] = s
		return s, nil
	}
	// If yes, start a partially synced subscriber.
	ls, err := legs.NewSubscriberPartiallySynced(ctx, i.ds, i.host,
		i.gs, i.subTopic, legs.FilterPeerPolicy(p), c)
	if err != nil {
		return nil, err
	}
	s = &sub{p: p, ls: ls}
	i.subs[p] = s
	return s, nil
}

func (i *legIngester) Close(ctx context.Context) error {
	// Unsubscribe from all peers
	for k := range i.subs {
		i.Unsubscribe(ctx, k)
	}
	return nil
}

// Get the latest cid synced for the peer.
func (i *legIngester) getLatestSync(p peer.ID) (cid.Cid, error) {
	b, err := i.ds.Get(datastore.NewKey(syncPrefix + p.String()))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

// Tracks latest sync for a specific peer.
func (i *legIngester) putLatestSync(p peer.ID, c cid.Cid) error {
	return i.ds.Put(datastore.NewKey(syncPrefix+p.String()), c.Bytes())
}
