package ingest

import (
	"context"

	indexer "github.com/filecoin-project/go-indexer-core/engine"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry"
	pclient "github.com/filecoin-project/storetheindex/providerclient"
	pclientp2p "github.com/filecoin-project/storetheindex/providerclient/libp2p"
	"github.com/gammazero/keymutex"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var log = logging.Logger("indexer/ingest")

var (
	_ LegIngester = &legIngester{}
)

// prefix used to track latest sync in datastore.
const (
	syncPrefix  = "/sync/"
	admapPrefix = "/admap/"
)

// LegIngester interface
type LegIngester interface {
	Ingester
	Close(context.Context) error
}

// legIngester is an ingester type that leverages go-legs for the
// ingestion protocol.
type legIngester struct {
	host    host.Host
	ds      datastore.Batching
	lt      *legs.LegTransport
	indexer *indexer.Engine

	newClient func(context.Context, host.Host, peer.ID) (pclient.Provider, error)

	subs  map[peer.ID]*subscriber
	sublk *keymutex.KeyMutex
}

// subscriber datastructure for a peer.
type subscriber struct {
	peerID  peer.ID
	ls      legs.LegSubscriber
	watcher <-chan cid.Cid
	cncl    context.CancelFunc
}

// NewLegIngester creates a new go-legs-backed ingester.
func NewLegIngester(ctx context.Context, cfg config.Ingest, h host.Host,
	idxr *indexer.Engine, reg *registry.Registry, ds datastore.Batching) (LegIngester, error) {

	lsys := mkLinkSystem(ds, reg)
	lt, err := legs.MakeLegTransport(context.Background(), h, ds, lsys, cfg.PubSubTopic)
	if err != nil {
		log.Errorf("Failed to state LegTransport in ingester: %s", err)
		return nil, err
	}

	// Function to create new client.  Setting the function allows this to be
	// mocked for testing.
	newClient := func(ctx context.Context, h host.Host, peerID peer.ID) (pclient.Provider, error) {
		return pclientp2p.New(h, peerID)
	}

	li := &legIngester{
		host:      h,
		ds:        ds,
		indexer:   idxr,
		newClient: newClient,
		lt:        lt,
		subs:      make(map[peer.ID]*subscriber),
		sublk:     keymutex.New(0),
	}

	// Register storage hook to index data as we receive it.
	lt.Gs.RegisterIncomingBlockHook(li.storageHook())
	log.Debugf("LegIngester started and all hooks and linksystem registered")
	return li, nil
}

// Sync with a data provider up to latest ID.
func (i *legIngester) Sync(ctx context.Context, peerID peer.ID, opts ...SyncOption) (<-chan multihash.Multihash, error) {
	log.Debugf("Syncing with peer %s", peerID)
	// Check latest sync for provider.
	c, err := i.getLatestAdvID(ctx, peerID)
	if err != nil {
		log.Errorf("Error getting latest advertisement for sync: %s", err)
		return nil, err
	}

	// Check if we already have the advertisement.
	adv, err := i.ds.Get(datastore.NewKey(c.String()))
	if err != nil && err != datastore.ErrNotFound {
		log.Errorf("Error fetching advertisement from datastore: %s", err)
		return nil, err
	}
	// If we have the advertisement do nothing, we already synced
	if adv != nil {
		log.Debugf("Alredy synced with provider %s", peerID)
		return nil, nil
	}
	// Get subscriber for peer or create a new one
	sub, err := i.newPeerSubscriber(ctx, peerID)
	if err != nil {
		log.Errorf("Error getting a subscriber instance for provider: %s", err)
		return nil, err
	}

	// Apply options to syncConfig or use defaults
	var cfg SyncConfig
	if err := cfg.Apply(append([]SyncOption{SyncDefaults}, opts...)...); err != nil {
		return nil, err
	}

	// Configure timeout for syncing process
	ctx, cancel := context.WithTimeout(ctx, cfg.SyncTimeout)
	// Start syncing. Notifications for the finished
	// sync will be done asynchronously.
	log.Debugf("Started syncing process with provider %s", sub)
	watcher, cncl, err := sub.ls.Sync(ctx, peerID, c)
	if err != nil {
		log.Errorf("Errored while syncing: %s", err)
		cancel()
		return nil, err
	}
	// Merge cancelfuncs
	cncl = cancelFunc(cncl, cancel)
	// Notification channel.
	out := make(chan multihash.Multihash)
	// Listen when the sync is done to update latestSync and notify the
	// channel. No need to pass ctx here, because if ctx is canceled, then
	// watcher is closed.
	go i.listenSyncUpdate(peerID, watcher, cncl, out)
	log.Infof("Waiting for sync to finish for provider %s", peerID)
	return out, nil
}

func (i *legIngester) getLatestAdvID(ctx context.Context, peerID peer.ID) (cid.Cid, error) {
	client, err := i.newClient(ctx, i.host, peerID)
	if err != nil {
		log.Errorf("Error creating new libp2p provider client in ingester: %s", err)
		return cid.Undef, err
	}
	defer client.Close()

	res, err := client.GetLatestAdv(ctx)
	if err != nil {
		return cid.Undef, err
	}
	return res.ID, nil
}

// Subscribe to advertisements of a specific provider in the pubsub channel.
func (i *legIngester) Subscribe(ctx context.Context, peerID peer.ID) error {
	log.Infow("Subscribing to advertisement pub-sub channel", "host_id", peerID)
	sctx, cancel := context.WithCancel(ctx)
	sub, err := i.newPeerSubscriber(sctx, peerID)
	if err != nil {
		log.Errorf("Error getting a subscriber instance for provider: %s", err)
		cancel()
		return err
	}

	// If already subscribed do nothing.
	if sub.watcher != nil {
		log.Infow("Already subscribed to provider", "id", peerID)
		cancel()
		return nil
	}

	var cncl context.CancelFunc
	sub.watcher, cncl = sub.ls.OnChange()
	sub.cncl = cancelFunc(cncl, cancel)

	// Listen updates persist latestSync when sync is done.
	go i.listenSubUpdates(sub)
	return nil
}

func (i *legIngester) listenSubUpdates(sub *subscriber) {
	for c := range sub.watcher {
		// Persist the latest sync
		if err := i.putLatestSync(sub.peerID, c); err != nil {
			log.Errorf("Error persisting latest sync: %s", err)
		}
	}
}

func (i *legIngester) listenSyncUpdate(peerID peer.ID, watcher <-chan cid.Cid, cncl context.CancelFunc, out chan<- multihash.Multihash) {

	defer func() {
		cncl()
		close(out)
	}()

	c, ok := <-watcher
	if ok {
		// Persist the latest sync
		err := i.putLatestSync(peerID, c)
		if err != nil {
			log.Errorf("Error persisting latest sync: %s", err)
		}
		out <- c.Hash()
	}
}

// Unsubscribe to stop listening to advertisement from a specific provider.
func (i *legIngester) Unsubscribe(ctx context.Context, peerID peer.ID) error {
	log.Debugf("Unsubscribing from provider %s", peerID)
	i.sublk.Lock(string(peerID))
	defer i.sublk.Unlock(string(peerID))
	// Check if subscriber exists.
	sub, ok := i.subs[peerID]
	if !ok {
		log.Infof("Not subscribed to provider %s. Nothing to do", peerID)
		// If not we have nothing to do.
		return nil
	}
	// Close subscriber
	sub.ls.Close()
	// Check if we are subscribed
	if sub.cncl != nil {
		// If yes, run cancel
		sub.cncl()
	}
	// Delete from map
	delete(i.subs, peerID)
	log.Infof("Unsubscribed from provider %s successfully", peerID)

	return nil
}

// Creates a new subscriber for a peer according to its latest sync.
func (i *legIngester) newPeerSubscriber(ctx context.Context, peerID peer.ID) (*subscriber, error) {
	i.sublk.Lock(string(peerID))
	defer i.sublk.Unlock(string(peerID))
	sub, ok := i.subs[peerID]
	// If there is already a subscriber for the peer, do nothing.
	if ok {
		return sub, nil
	}

	// See if we already synced with this peer.
	c, err := i.getLatestSync(peerID)
	if err != nil {
		return nil, err
	}

	// TODO: Make a request to provider to see if it has any new advertisement
	// and sync before initializing subscriber?

	// If not synced start a brand new subscriber
	var ls legs.LegSubscriber
	if c == cid.Undef {
		ls, err = legs.NewSubscriber(ctx, i.lt, legs.FilterPeerPolicy(peerID))
	} else {
		// If yes, start a partially synced subscriber.
		ls, err = legs.NewSubscriberPartiallySynced(ctx, i.lt, legs.FilterPeerPolicy(peerID), c)
	}
	if err != nil {
		return nil, err
	}
	sub = &subscriber{
		peerID: peerID,
		ls:     ls,
	}
	i.subs[peerID] = sub
	return sub, nil
}

func (i *legIngester) Close(ctx context.Context) error {
	// Unsubscribe from all peers
	for k := range i.subs {
		err := i.Unsubscribe(ctx, k)
		if err != nil {
			return err
		}
	}
	// Close leg transport.
	return i.lt.Close(ctx)
}

// Get the latest cid synced for the peer.
func (i *legIngester) getLatestSync(peerID peer.ID) (cid.Cid, error) {
	b, err := i.ds.Get(datastore.NewKey(syncPrefix + peerID.String()))
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
func (i *legIngester) putLatestSync(peerID peer.ID, c cid.Cid) error {
	// Do not save if empty CIDs are received. Closing the channel
	// may lead to receiving empty CIDs.
	if c == cid.Undef {
		return nil
	}
	_ = stats.RecordWithOptions(context.Background(),
		stats.WithTags(tag.Insert(metrics.Method, "libp2p2")),
		stats.WithMeasurements(metrics.IngestChange.M(1)))

	return i.ds.Put(datastore.NewKey(syncPrefix+peerID.String()), c.Bytes())
}

// cancelfunc for subscribers. Combines context cancel and LegSubscriber
// cancel function.
func cancelFunc(c1, c2 context.CancelFunc) context.CancelFunc {
	return func() {
		c1()
		c2()
	}
}
