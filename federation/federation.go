package federation

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/maurl"
	"github.com/ipni/go-libipni/mautil"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	headSnapshotKey = datastore.NewKey("head-snapshot")
	logger          = logging.Logger("federation")
)

type (
	Federation struct {
		*options
		vc vectorClock

		mu            sync.Mutex
		headNodeCache ipld.Node
		httpServer    http.Server
		shutdown      chan struct{}

		// lastSeenSnapshotByMember is the cache of already reconciled snapshots by federation member.
		// An in-memory data structure is enough, because upon indexer restart we want to check if
		// the local node has missed anything since reconciliation process is cheap enough.
		lastSeenSnapshotByMember map[peer.ID]cid.Cid
	}
)

func New(o ...Option) (*Federation, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &Federation{
		options:                  opts,
		vc:                       newVectorClock(),
		shutdown:                 make(chan struct{}),
		lastSeenSnapshotByMember: make(map[peer.ID]cid.Cid),
	}, nil
}

func (f *Federation) Start(_ context.Context) error {

	listen, err := net.Listen("tcp", f.httpListenAddr)
	if err != nil {
		return err
	}
	go func() {
		f.httpServer.Handler = f.ServeMux()
		switch err := f.httpServer.Serve(listen); {
		case errors.Is(err, http.ErrServerClosed):
			logger.Infow("Federation HTTP server stopped")
		default:
			logger.Errorw("Federation HTTP server stopped unexpectedly")
		}
	}()

	go func() {
		snapshotTicker := time.NewTicker(f.snapshotInterval)
		reconciliationTicker := time.NewTicker(f.reconciliationInterval)
		defer func() {
			snapshotTicker.Stop()
			reconciliationTicker.Stop()
		}()
		for {
			select {
			case <-f.shutdown:
				logger.Warnw("Stopping federation event loop")
				return
			case t := <-snapshotTicker.C:
				if err := f.snapshot(context.TODO(), t); err != nil {
					logger.Errorw("Failed to take snapshot", "err", err)
				}
			case t := <-reconciliationTicker.C:
				if err := f.reconcile(context.TODO(), t); err != nil {
					logger.Errorw("Failed to reconcile", "err", err)
				}
			}
		}
	}()
	return nil
}

func (f *Federation) ServeMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/ipni/v1/fed/head", f.handleV1FedHead)
	mux.HandleFunc("/ipni/v1/fed/", f.handleV1FedSubtree)
	return mux
}

func (f *Federation) snapshot(ctx context.Context, t time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	previous, err := f.getHeadLink(ctx)
	if err != nil {
		return err
	}
	newSnapshot := &Snapshot{
		// TODO: consider reducing the granularity of epoch. Millisecond increments in terms of unix clock may be too
		//       fine for the purposes of IPNI federation.
		Epoch:       uint64(t.UTC().Unix()),
		VectorClock: f.vc.tick(f.host.ID()),
		Providers: ProvidersIngestStatusMap{
			Values: make(map[string]IngestStatus),
		},
		Previous: previous,
	}
	for _, info := range f.registry.AllProviderInfo() {
		var latestProcessedAdLink ipld.Link
		if !info.LastAdvertisement.Equals(cid.Undef) {
			latestProcessedAdLink = cidlink.Link{Cid: info.LastAdvertisement}
		}
		key := info.AddrInfo.ID.String()
		value := IngestStatus{
			LastAdvertisement: latestProcessedAdLink,
			// TODO: Add height hint once it is measured by indexers.
		}
		newSnapshot.Providers.Put(key, value)
	}

	node := bindnode.Wrap(newSnapshot, Prototypes.Snapshot.Type()).Representation()
	headSnapshotLink, err := f.linkSystem.Store(ipld.LinkContext{Ctx: ctx}, Prototypes.link, node)
	if err != nil {
		return err
	}
	if err := f.setHeadLink(ctx, headSnapshotLink); err != nil {
		return err
	}
	return nil
}

func (f *Federation) getHeadNode(ctx context.Context) (ipld.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.headNodeCache != nil {
		return f.headNodeCache, nil
	}

	headLink, err := f.getHeadLink(ctx)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			// Set headNodeCache to an empty node to avoid hitting datastore every time head is fetched.
			f.headNodeCache = basicnode.Prototype.Any.NewBuilder().Build()
			return f.headNodeCache, nil
		}
		return nil, err
	}
	key, err := f.host.ID().ExtractPublicKey()
	if err != nil {
		logger.Errorw("Failed to get public key", "err", err)
		return nil, err
	}
	marshalledPubKey, err := crypto.MarshalPublicKey(key)
	if err != nil {
		logger.Errorw("Failed to marshal public key", "err", err)
		return nil, err
	}
	head := &Head{
		Head:      headLink,
		PublicKey: marshalledPubKey,
	}
	if err := head.Sign(f.host.Peerstore().PrivKey(f.host.ID())); err != nil {
		logger.Errorw("Failed to sign head", "err", err)
		return nil, err
	}
	f.headNodeCache = bindnode.Wrap(head, Prototypes.Head.Type()).Representation()
	return f.headNodeCache, nil
}

func (f *Federation) getHeadLink(ctx context.Context) (ipld.Link, error) {
	v, err := f.datastore.Get(ctx, headSnapshotKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	_, c, err := cid.CidFromBytes(v)
	if err != nil {
		return nil, err
	}
	return cidlink.Link{Cid: c}, nil
}

func (f *Federation) setHeadLink(ctx context.Context, head ipld.Link) error {
	return f.datastore.Put(ctx, headSnapshotKey, head.(cidlink.Link).Cid.Bytes())
}

func (f *Federation) reconcile(ctx context.Context, t time.Time) error {
	for _, member := range f.members {
		logger := logger.With("id", member.ID)
		memberPubKey, err := member.ID.ExtractPublicKey()
		if err != nil {
			logger.Errorw("Failed to extract public key fom federation member peer ID", "err", err)
			continue
		}

		var endpoint *url.URL
		{ // Get URL of federation member server

			httpAddrs := mautil.FindHTTPAddrs(member.Addrs)
			switch len(httpAddrs) {
			case 0:
				logger.Errorw("No HTTP(S) address found for federation member", "addrs", member.Addrs)
				continue
			case 1:
			// Exactly one HTTP(S) multiaddr was found. As you were; proceed.
			default:
				logger.Warnw("Multiple HTTP(S) multiaddrs found for federation member. Picking the first one.", "httpAddrs", httpAddrs)
			}
			endpoint, err = maurl.ToURL(httpAddrs[0])
			if err != nil {
				logger.Errorw("Failed to extract URL from HTTP(S) multiaddr", "httpAddr", httpAddrs[0], "err", err)
				continue
			}
		}

		var memberProviders map[peer.ID]*model.ProviderInfo
		{ // List providers known by the federation member.
			findClient, err := client.New(endpoint.String())
			if err != nil {
				logger.Errorw("Failed to instantiate find client for member", "endpoint", endpoint, "err", err)
				continue
			}
			providers, err := findClient.ListProviders(ctx)
			if err != nil {
				logger.Errorw("Failed to list providers for member", "endpoint", endpoint, "err", err)
				continue
			}
			memberProviders = make(map[peer.ID]*model.ProviderInfo)
			for _, provider := range providers {
				memberProviders[provider.AddrInfo.ID] = provider
			}
		}

		var head *Head
		{ // Get federation head from member

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.JoinPath("ipni", "v1", "fed", "head").String(), nil)
			if err != nil {
				logger.Errorw("Failed instantiate GET request", "err", err)
				continue
			}
			// TODO: accept other kinds too, like dagcbor.
			req.Header.Set("Accept", "application/json")
			resp, err := f.reconciliationHttpClient.Do(req)
			if err != nil {
				logger.Errorw("Failed to GET federation state head from member", "err", err)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				logger.Errorw("GET federation state head response status code is not OK", "status", resp.Status, "err", err)
				continue
			}
			defer func() { _ = resp.Body.Close() }()
			// TODO: add checks for Content-Type.
			builder := Prototypes.Head.NewBuilder()
			if err := dagjson.Decode(builder, resp.Body); err != nil {
				logger.Errorw("Failed to decode ", "status", resp.Status, "err", err)
				continue
			}
			headNode := builder.Build()
			head := bindnode.Unwrap(headNode).(*Head)
			if err := head.Verify(memberPubKey); err != nil {
				logger.Errorw("Invalid federation state head", "err", err)
				continue
			}

			if head.Head == nil {
				logger.Debug("Head has no link to snapshot.")
				continue
			}
		}

		var (
			snapshot    *Snapshot
			snapshotCid cid.Cid
		)
		{ // Get the federation member snapshot
			snapshotCid := head.Head.(cidlink.Link).Cid
			logger = logger.With("snapshotCid", snapshotCid)

			if seenCid, ok := f.lastSeenSnapshotByMember[member.ID]; ok && seenCid.Equals(snapshotCid) {
				logger.Debugw("Snapshot already seen; skipping reconciliation")
				continue
			}

			sDecoder, err := multicodec.LookupDecoder(snapshotCid.Prefix().Codec)
			if err != nil {
				logger.Errorw("Failed to find decoder for snapshot link ", "err", err)
				continue
			}

			sReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.JoinPath("ipni", "v1", "fed", snapshotCid.String()).String(), nil)
			if err != nil {
				logger.Errorw("Failed instantiate GET snapshot request request", "err", err)
				continue
			}
			sResp, err := f.reconciliationHttpClient.Do(sReq)
			if err != nil {
				logger.Errorw("Failed to GET snapshot from member", "err", err)
				continue
			}
			if sResp.StatusCode != http.StatusOK {
				logger.Errorw("GET snapshot  response status code is not OK", "status", sResp.Status, "err", err)
				continue
			}
			defer func() { _ = sResp.Body.Close() }()
			// TODO: add checks for Content-Type.
			sBuilder := Prototypes.Snapshot.NewBuilder()
			if err := sDecoder(sBuilder, sResp.Body); err != nil {
				logger.Errorw("Failed to decode ", "status", sResp.Status, "err", err)
				continue
			}
			snapshotNode := sBuilder.Build()
			snapshot = bindnode.Unwrap(snapshotNode).(*Snapshot)
			gotLink, err := f.linkSystem.ComputeLink(cidlink.LinkPrototype{Prefix: snapshotCid.Prefix()}, snapshotNode)
			if err != nil {
				logger.Errorw("Failed to compute link for snapshot", "err", err)
				continue
			}
			gotSnapshotCid := gotLink.(cidlink.Link).Cid
			if !snapshotCid.Equals(gotSnapshotCid) {
				logger.Errorw("Snapshot link mismatch", "got", gotSnapshotCid)
				continue
			}
		}

		{ // Reconcile local registry based on snapshot
			if f.vc.reconcile(member.ID, snapshot.VectorClock) {
				for providerID, snapshotIngestStatus := range snapshot.Providers.Values {
					pid, err := peer.Decode(providerID)
					if err != nil {
						logger.Warnw("Failed to decode provider ID in snapshot", "id", providerID)
						continue
					}

					providerInfo, ok := memberProviders[pid]
					if !ok {
						logger.Warnw("Provider in snapshot os not known by the member; skipping reconciliation for provider", "provider", pid)
						continue
					}

					// TODO: confirm this is the right way to deduce publiser addrinfo from model.ProviderInfo
					var pai peer.AddrInfo
					if providerInfo.Publisher != nil {
						pai = *providerInfo.Publisher
					} else {
						pai = providerInfo.AddrInfo
					}

					info, found := f.registry.ProviderInfo(pid)
					// If local indexer does not know about the provider in snapshot, then
					// simply announce it to the local ingester.
					if !found {
						if err := f.ingester.Announce(ctx, snapshotIngestStatus.LastAdvertisement.(cidlink.Link).Cid, pai); err != nil {
							logger.Warnw("Failed to announce newly discovered provider to ingester", "provider", pid, "err", err)
						}
						continue
					}

					// If the latest processed ad CID by both local and remote indexer are the same, then
					// they are in-sync and there is nothing to do.
					if info.LastAdvertisement.Equals(snapshotIngestStatus.LastAdvertisement.(cidlink.Link).Cid) {
						continue
					}

					// TODO: Optimise what CID we announce by finding which is most recent.
					//       For now, simply announcing an ad would work since indexers keep the chain of ads they have processed.
					if err := f.ingester.Announce(ctx, snapshotIngestStatus.LastAdvertisement.(cidlink.Link).Cid, pai); err != nil {
						logger.Warnw("Failed to announce newly discovered provider to ingester", "provider", pid, "err", err)
					}
				}
			}

			// Remember the latest processed snapshot for member to avoid re-processing already seen snapshots by that member.
			f.lastSeenSnapshotByMember[member.ID] = snapshotCid
		}
	}

	logger.Infow("Finished reconciliation cycle", "triggeredAt", t)
	return nil
}

func (f *Federation) Shutdown(ctx context.Context) error {
	close(f.shutdown)
	return f.httpServer.Shutdown(ctx)
}
