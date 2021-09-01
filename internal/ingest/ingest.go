package ingest

import (
	"context"

	indexer "github.com/filecoin-project/go-indexer-core/engine"
	ingestclient "github.com/filecoin-project/indexer-reference-provider/api/v0/client"
	ingestclientimpl "github.com/filecoin-project/indexer-reference-provider/api/v0/client/libp2p"
	ingestion "github.com/filecoin-project/storetheindex/api/v0/ingest"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/im7mortal/kmutex"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/willscott/go-legs"
)

var log = logging.Logger("indexer/ingest")

var (
	_ LegIngester = &legIngester{}
)

// prefix used to track latest sync in datastore.
const (
	syncPrefix = "/sync/"
)

// LegIngester interface
type LegIngester interface {
	ingestion.Ingester
	Close(context.Context) error
}

// legIngester is an ingester type that leverages go-legs for the
// ingestion protocol.
type legIngester struct {
	host    host.Host
	ds      datastore.Batching
	lt      *legs.LegTransport
	indexer *indexer.Engine
	client  ingestclient.Provider

	subs  map[peer.ID]*sub
	sublk *kmutex.Kmutex
}

// Subscriber datastructure for a peer.
type sub struct {
	p       peer.ID
	ls      legs.LegSubscriber
	watcher chan cid.Cid
	cncl    context.CancelFunc
}

// NewLegIngester creates a new go-legs-backed ingester.
func NewLegIngester(ctx context.Context, cfg config.Ingest, h host.Host,
	i *indexer.Engine, ds datastore.Batching) (LegIngester, error) {

	lsys := mkVanillaLinkSystem(ds)
	lt, err := legs.MakeLegTransport(context.Background(), h, ds, lsys, cfg.PubSubTopic)
	if err != nil {
		return nil, err
	}

	cl, err := ingestclientimpl.NewProvider(ctx, h)
	if err != nil {
		return nil, err
	}
	li := &legIngester{
		host:    h,
		ds:      ds,
		indexer: i,
		lt:      lt,
		client:  cl,
		subs:    make(map[peer.ID]*sub),
		sublk:   kmutex.New(),
	}

	// Register storage hook to index data as we receive it.
	lt.Gs.RegisterIncomingBlockHook(li.storageHook())
	return li, nil
}

// Sync with a data provider up to latest ID
func (i *legIngester) Sync(ctx context.Context, p peer.ID, opts ...ingestion.SyncOption) (chan cid.Cid, error) {
	// Check latest sync for provider.
	c, err := i.getLatestAdvID(ctx, p)
	if err != nil {
		return nil, err
	}

	// Check if we already have the advertisement.
	adv, err := i.ds.Get(datastore.NewKey(c.String()))
	if err != nil && err != datastore.ErrNotFound {
		return nil, err
	}
	// If we have the advertisement do nothing, we already synced
	if adv != nil {
		return nil, nil
	}
	// Get subscriber for peer or create a new one
	s, err := i.newPeerSubscriber(ctx, p)
	if err != nil {
		return nil, err
	}

	// Apply options to syncConfig or use defaults
	var cfg ingestion.SyncConfig
	if err := cfg.Apply(append([]ingestion.SyncOption{ingestion.SyncDefaults}, opts...)...); err != nil {
		return nil, err
	}

	// Configure timeout for syncing process
	ctx, cancel := context.WithTimeout(ctx, cfg.SyncTimeout)
	// Start syncing. Notifications for the finished
	// sync will be done asynchronously.
	watcher, cncl, err := s.ls.Sync(ctx, p, c)
	if err != nil {
		cancel()
		return nil, err
	}
	// Merge cancelfuncs
	cncl = cancelFunc(cncl, cancel)
	// Notification channel.
	out := make(chan cid.Cid)
	// Listen when the sync is done to update latestSync and
	// notify the channel.
	go i.listenSyncUpdates(ctx, p, watcher, cncl, out)
	return out, nil
}

func (i *legIngester) getLatestAdvID(ctx context.Context, p peer.ID) (cid.Cid, error) {
	res, err := i.client.GetLatestAdv(ctx, p)
	if err != nil {
		return cid.Undef, err
	}
	return res.ID, nil
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
	s.cncl = cancelFunc(cncl, cancel)

	// Listen updates persist latestSync when sync is done.
	go i.listenSubUpdates(ctx, s)
	return nil
}

func (i *legIngester) listenSubUpdates(ctx context.Context, s *sub) {
	for {
		select {
		case <-ctx.Done():
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

func (i *legIngester) listenSyncUpdates(ctx context.Context, p peer.ID,
	watcher chan cid.Cid, cncl context.CancelFunc, out chan cid.Cid) {

	defer func() {
		cncl()
		close(out)
	}()

	select {
	case <-ctx.Done():
		return
	// Persist the latest sync
	case c := <-watcher:
		err := i.putLatestSync(p, c)
		if err != nil {
			log.Errorf("Error persisting latest sync: %w", err)
		}
		out <- c
	}
}

// Unsubscribe to stop listening to advertisement from a specific provider.
func (i *legIngester) Unsubscribe(ctx context.Context, p peer.ID) error {
	i.sublk.Lock(p)
	defer i.sublk.Unlock(p)
	// Close subscriber
	i.subs[p].ls.Close()
	// Check if we are subscribed
	if i.subs[p].cncl != nil {
		// If yes, run cancel
		i.subs[p].cncl()
	}
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

	// TODO: Make a request to provider to see if it has any new advertisement
	// and sync before initializing subscriber?

	// If not synced start a brand new subscriber
	if c == cid.Undef {
		ls, err := legs.NewSubscriber(ctx, i.lt, legs.FilterPeerPolicy(p))
		if err != nil {
			return nil, err
		}
		s = &sub{p: p, ls: ls}
		i.subs[p] = s
		return s, nil
	}
	// If yes, start a partially synced subscriber.
	ls, err := legs.NewSubscriberPartiallySynced(ctx, i.lt, legs.FilterPeerPolicy(p), c)
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
	// Close leg transport.
	return i.lt.Close(ctx)
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
	// Do not save if empty CIDs are received. Closing the channel
	// may lead to receiving empty CIDs.
	if c == cid.Undef {
		return nil
	}
	return i.ds.Put(datastore.NewKey(syncPrefix+p.String()), c.Bytes())
}

// cancelfunc for subscribers. Combines context cancel and LegSubscriber
// cancel function.
func cancelFunc(c1, c2 context.CancelFunc) context.CancelFunc {
	return func() {
		c1()
		c2()
	}
}
