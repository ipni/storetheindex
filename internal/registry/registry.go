package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry/discovery"
	"github.com/filecoin-project/storetheindex/internal/registry/policy"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
)

const (
	// providerKeyPath is where provider info is stored in to indexer repo.
	providerKeyPath = "/registry/pinfo"
	// registryVersionKey is where registry version number is stored.
	registryVersionKey = "/registry/version"

	registryVersion = "v1"
)

var log = logging.Logger("indexer/registry")

// Registry stores information about discovered providers
type Registry struct {
	actions   chan func()
	closed    chan struct{}
	closeOnce sync.Once
	closing   chan struct{}
	dstore    datastore.Datastore
	providers map[peer.ID]*ProviderInfo
	sequences *sequences

	discoverer discovery.Discoverer
	discoWait  sync.WaitGroup
	discoTimes map[string]time.Time
	policy     *policy.Policy

	discoveryTimeout time.Duration
	rediscoverWait   time.Duration

	syncChan chan *ProviderInfo
}

// ProviderInfo is an immutable data structure that holds information about a
// provider.  A ProviderInfo instance is never modified, but rather a new one
// is created to update its contents.  This means existing references remain
// valid.
type ProviderInfo struct {
	// AddrInfo contains a peer.ID and set of Multiaddr addresses.
	AddrInfo peer.AddrInfo
	// DiscoveryAddr is the address that is used for discovery of the provider.
	DiscoveryAddr string `json:",omitempty"`
	// LastAdvertisement identifies the latest advertisement the indexer has ingested.
	LastAdvertisement cid.Cid `json:",omitempty"`
	// LastAdvertisementTime is the time the latest advertisement was received.
	LastAdvertisementTime time.Time `json:",omitempty"`
	// Publisher contains the ID and multiaddrs of the provider info publisher.
	Publisher *peer.AddrInfo `json:",omitempty"`

	// lastContactTime is the last time the publisher contexted the
	// indexer. This is not persisted, so that the time since last contact is
	// reset when the indexer is started. If not reset, then it would appear
	// the publisher was unreachable for the indexer downtime.
	lastContactTime time.Time
}

func (p *ProviderInfo) dsKey() datastore.Key {
	return datastore.NewKey(path.Join(providerKeyPath, p.AddrInfo.ID.String()))
}

// NewRegistry creates a new provider registry, giving it provider policy
// configuration, a datastore to persist provider data, and a Discoverer
// interface.  The context is only used for cancellation of this function.
//
// TODO: It is probably necessary to have multiple discoverer interfaces
func NewRegistry(ctx context.Context, cfg config.Discovery, dstore datastore.Datastore, disco discovery.Discoverer) (*Registry, error) {
	// Create policy from config
	discoPolicy, err := policy.New(cfg.Policy)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		actions:   make(chan func()),
		closed:    make(chan struct{}),
		closing:   make(chan struct{}),
		policy:    discoPolicy,
		providers: map[peer.ID]*ProviderInfo{},
		sequences: newSequences(0),

		rediscoverWait:   time.Duration(cfg.RediscoverWait),
		discoveryTimeout: time.Duration(cfg.Timeout),

		discoverer: disco,
		discoTimes: map[string]time.Time{},

		dstore:   dstore,
		syncChan: make(chan *ProviderInfo, 1),
	}

	count, err := r.loadPersistedProviders(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot load provider data from datastore: %w", err)
	}
	log.Infow("loaded providers into registry", "count", count)

	go r.run()
	go r.runPollCheck(
		time.Duration(cfg.PollInterval),
		time.Duration(cfg.PollRetryAfter),
		time.Duration(cfg.PollStopAfter))

	return r, nil
}

// Close waits for any pending discoverer to finish and then stops the registry
func (r *Registry) Close() error {
	var err error
	r.closeOnce.Do(func() {
		close(r.closing)
		<-r.closed

		if r.dstore != nil {
			err = r.dstore.Close()
		}
	})
	<-r.closed
	return err
}

func (r *Registry) SyncChan() <-chan *ProviderInfo {
	return r.syncChan
}

// run executes functions that need to be executed on the same goroutine
//
// Running actions here is a substitute for mutex-locking the sections of code
// run as an action and allows the caller to decide whether or not to wait for
// the code to finish running.
//
// All functions named using the prefix "sync" must be run on this goroutine.
func (r *Registry) run() {
	defer close(r.closed)

	for action := range r.actions {
		action()
	}
}

func (r *Registry) runPollCheck(pollInterval, pollRetryAfter, pollStopAfter time.Duration) {
	if pollRetryAfter < time.Minute {
		pollRetryAfter = time.Minute
	}
	timer := time.NewTimer(pollRetryAfter)
running:
	for {
		select {
		case <-timer.C:
			r.cleanup()
			r.pollProviders(pollInterval, pollRetryAfter, pollStopAfter)
			timer.Reset(pollRetryAfter)
		case <-r.closing:
			break running
		}
	}

	// Check that pollProviders is finished and close sync channel.
	done := make(chan struct{})
	r.actions <- func() {
		close(done)
	}
	<-done
	close(r.syncChan)

	// Wait for any pending discoveries to complete, then stop the main run
	// goroutine.
	r.discoWait.Wait()
	close(r.actions)
}

// Discover begins the process of discovering and verifying a provider.  The
// discovery address us used to lookup the provider's information.
//
// TODO: To support multiple discoverer methods (lotus, IPFS, etc.) there need
// to be information that is part of, or in addition to, the discoveryAddr to
// indicate where/how discovery is done.
func (r *Registry) Discover(peerID peer.ID, discoveryAddr string, sync bool) error {
	// If provider is not allowed, then ignore request
	allowed, _ := r.policy.Check(peerID)
	if !allowed {
		return v0.NewError(ErrNotAllowed, http.StatusForbidden)
	}

	// If provider is already trusted, then discovery is being done only to get
	// the provider's address.

	errCh := make(chan error, 1)
	r.actions <- func() {
		r.syncStartDiscover(peerID, discoveryAddr, errCh)
	}
	if sync {
		return <-errCh
	}
	return nil
}

// Register is used to directly register a provider, bypassing discovery and
// adding discovered data directly to the registry.
func (r *Registry) Register(ctx context.Context, info *ProviderInfo) error {
	if len(info.AddrInfo.Addrs) == 0 {
		return errors.New("missing provider address")
	}

	err := info.AddrInfo.ID.Validate()
	if err != nil {
		return err
	}

	allowed, trusted := r.policy.Check(info.AddrInfo.ID)

	// If provider is not allowed, then ignore request.
	if !allowed {
		return v0.NewError(ErrNotAllowed, http.StatusForbidden)
	}

	// If allowed provider is not trusted, then they require authentication
	// before being registered.  This means going through discovery and looking
	// up a miner ID on-chain.
	if !trusted {
		return v0.NewError(ErrNotTrusted, http.StatusForbidden)
	}

	// If publisher is valid and different than the provider, check if the
	// publisher is allowed.
	if info.Publisher != nil && info.Publisher.ID != info.AddrInfo.ID {
		allowed, trusted := r.policy.Check(info.Publisher.ID)
		if !allowed {
			return v0.NewError(ErrNotAllowed, http.StatusForbidden)
		}
		if !trusted {
			return v0.NewError(ErrNotTrusted, http.StatusForbidden)
		}
	}

	// If allowed provider and publisher are trusted, register immediately
	// without verification.
	errCh := make(chan error, 1)
	r.actions <- func() {
		errCh <- r.syncRegister(ctx, info)
	}

	err = <-errCh
	if err != nil {
		return err
	}

	log.Infow("registered provider", "id", info.AddrInfo.ID, "addrs", info.AddrInfo.Addrs)
	return nil
}

// Check if the peer is trusted by policy, or if it has been previously
// verified and registered as a provider.
func (r *Registry) Authorized(peerID peer.ID) (bool, error) {
	allowed, trusted := r.policy.Check(peerID)

	if !allowed {
		return false, nil
	}

	// Peer is allowed but not trusted, see if it is a registered provider.
	if !trusted {
		regOk := make(chan bool)
		r.actions <- func() {
			_, ok := r.providers[peerID]
			regOk <- ok
		}
		return <-regOk, nil
	}

	return true, nil
}

func (r *Registry) SetPolicy(policyCfg config.Policy) error {
	return r.policy.Config(policyCfg)
}

// AllowPeer configures the policy to allow messages published by the
// identified peer, or allow the peer to register as a provider.
func (r *Registry) AllowPeer(peerID peer.ID) bool {
	return r.policy.Allow(peerID)
}

// BlockPeer configures the policy to block messages published by the
// identified peer, and block the peer from registering as a provider.
func (r *Registry) BlockPeer(peerID peer.ID) bool {
	return r.policy.Block(peerID)
}

// RegisterOrUpdate attempts to register an unregistered provider, or updates
// the addresses and latest advertisement of an already registered provider.
func (r *Registry) RegisterOrUpdate(ctx context.Context, providerID peer.ID, addrs []string, adID cid.Cid, publisher peer.AddrInfo) error {
	var fullRegister bool
	// Check that the provider has been discovered and validated
	info := r.ProviderInfo(providerID)
	if info != nil {
		info = &ProviderInfo{
			AddrInfo: peer.AddrInfo{
				ID:    providerID,
				Addrs: info.AddrInfo.Addrs,
			},
			DiscoveryAddr:         info.DiscoveryAddr,
			LastAdvertisement:     info.LastAdvertisement,
			LastAdvertisementTime: info.LastAdvertisementTime,
			Publisher:             info.Publisher,
		}

		if publisher.ID.Validate() == nil {
			if info.Publisher == nil || publisher.ID != info.Publisher.ID {
				// Publisher ID changed.
				info.Publisher = &peer.AddrInfo{
					ID:    publisher.ID,
					Addrs: publisher.Addrs,
				}
				fullRegister = true
			} else if len(publisher.Addrs) != 0 && !equalAddrs(publisher.Addrs, info.Publisher.Addrs) {
				// Publisher addrs changes.
				info.Publisher = &peer.AddrInfo{
					ID:    publisher.ID,
					Addrs: publisher.Addrs,
				}
			}
		}
	} else {
		fullRegister = true
		info = &ProviderInfo{
			AddrInfo: peer.AddrInfo{
				ID: providerID,
			},
		}
		if publisher.ID.Validate() == nil {
			info.Publisher = &peer.AddrInfo{
				ID:    publisher.ID,
				Addrs: publisher.Addrs,
			}
		}
	}

	if len(addrs) != 0 {
		maddrs, err := stringsToMultiaddrs(addrs)
		if err != nil {
			panic(err)
		}
		info.AddrInfo.Addrs = maddrs
	}

	if info.Publisher != nil && len(info.Publisher.Addrs) == 0 && info.Publisher.ID == info.AddrInfo.ID {
		info.Publisher = &info.AddrInfo
	}

	now := time.Now()

	if adID != info.LastAdvertisement && adID != cid.Undef {
		info.LastAdvertisement = adID
		info.LastAdvertisementTime = now
	}
	info.lastContactTime = now

	// If there is a new providerID or publisherID then do a full Register that
	// checks the allow policy.
	if fullRegister {
		return r.Register(ctx, info)
	}

	// If laready registered and no new IDs, register without verification.
	errCh := make(chan error, 1)
	r.actions <- func() {
		errCh <- r.syncRegister(ctx, info)
	}
	err := <-errCh
	if err != nil {
		return err
	}

	log.Debugw("Updated registered provider info", "id", info.AddrInfo.ID, "addrs", info.AddrInfo.Addrs)
	return nil
}

// IsRegistered checks if the provider is in the registry
func (r *Registry) IsRegistered(providerID peer.ID) bool {
	done := make(chan struct{})
	var found bool
	r.actions <- func() {
		_, found = r.providers[providerID]
		close(done)
	}
	<-done
	return found
}

// ProviderInfoByAddr finds a registered provider using its discovery address
func (r *Registry) ProviderInfoByAddr(discoAddr string) *ProviderInfo {
	infoChan := make(chan *ProviderInfo)
	r.actions <- func() {
		// TODO: consider adding a map of discoAddr->providerID
		for _, info := range r.providers {
			if info.DiscoveryAddr == discoAddr {
				infoChan <- info
				break
			}
		}
		close(infoChan)
	}

	return <-infoChan
}

// ProviderInfo returns information for a registered provider
func (r *Registry) ProviderInfo(providerID peer.ID) *ProviderInfo {
	infoChan := make(chan *ProviderInfo)
	r.actions <- func() {
		stats.Record(context.Background(), metrics.ProviderCount.M(int64(len(r.providers))))
		info, ok := r.providers[providerID]
		if ok {
			infoChan <- info
		}
		close(infoChan)
	}

	return <-infoChan
}

// AllProviderInfo returns information for all registered providers
func (r *Registry) AllProviderInfo() []*ProviderInfo {
	var infos []*ProviderInfo
	done := make(chan struct{})
	r.actions <- func() {
		infos = make([]*ProviderInfo, len(r.providers))
		var i int
		for _, info := range r.providers {
			infos[i] = info
			i++
		}
		close(done)
	}
	<-done
	return infos
}

func (r *Registry) CheckSequence(peerID peer.ID, seq uint64) error {
	return r.sequences.check(peerID, seq)
}

func (r *Registry) syncStartDiscover(peerID peer.ID, discoAddr string, errCh chan<- error) {
	err := r.syncNeedDiscover(discoAddr)
	if err != nil {
		errCh <- err
		return
	}

	// Mark discovery as in progress
	r.discoTimes[discoAddr] = time.Time{}
	r.discoWait.Add(1)

	// Do discovery asynchronously; do not block other discovery requests
	go func() {
		ctx := context.Background()
		if r.discoveryTimeout != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, r.discoveryTimeout)
			defer cancel()
		}

		discoData, discoErr := r.discover(ctx, peerID, discoAddr)
		r.actions <- func() {
			defer close(errCh)
			defer r.discoWait.Done()

			// Update discovery completion time.
			r.discoTimes[discoAddr] = time.Now()
			if discoErr != nil {
				errCh <- discoErr
				return
			}

			info := &ProviderInfo{
				AddrInfo:      discoData.AddrInfo,
				DiscoveryAddr: discoAddr,
			}
			if err := r.syncRegister(ctx, info); err != nil {
				errCh <- err
				return
			}
		}
	}()
}

func (r *Registry) syncRegister(ctx context.Context, info *ProviderInfo) error {
	r.providers[info.AddrInfo.ID] = info
	err := r.syncPersistProvider(ctx, info)
	if err != nil {
		err = fmt.Errorf("could not persist provider: %s", err)
		return v0.NewError(err, http.StatusInternalServerError)
	}
	return nil
}

func (r *Registry) syncNeedDiscover(discoAddr string) error {
	completed, ok := r.discoTimes[discoAddr]
	if ok {
		// Check if discovery already in progress
		if completed.IsZero() {
			return ErrInProgress
		}

		// Check if last discovery completed too recently
		if r.rediscoverWait != 0 && time.Since(completed) < r.rediscoverWait {
			return ErrTooSoon
		}
	}
	return nil
}

func (r *Registry) syncPersistProvider(ctx context.Context, info *ProviderInfo) error {
	if r.dstore == nil {
		return nil
	}
	value, err := json.Marshal(info)
	if err != nil {
		return err
	}

	dsKey := info.dsKey()
	if err = r.dstore.Put(ctx, dsKey, value); err != nil {
		return err
	}
	if err = r.dstore.Sync(ctx, dsKey); err != nil {
		return fmt.Errorf("cannot sync provider info: %s", err)
	}
	return nil
}

func (r *Registry) loadPersistedProviders(ctx context.Context) (int, error) {
	if r.dstore == nil {
		return 0, nil
	}

	err := r.migrate()
	if err != nil {
		return 0, err
	}

	// Load all providers from the datastore.
	q := query.Query{
		Prefix: providerKeyPath,
	}
	results, err := r.dstore.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer results.Close()

	var count int
	for result := range results.Next() {
		if result.Error != nil {
			return 0, fmt.Errorf("cannot read provider data: %v", result.Error)
		}
		ent := result.Entry

		peerID, err := peer.Decode(path.Base(ent.Key))
		if err != nil {
			return 0, fmt.Errorf("cannot decode provider ID: %s", err)
		}

		// If provider is not allowed, then do not load into registry.
		allowed, _ := r.policy.Check(peerID)
		if !allowed {
			log.Warnw("Refusing to load registry data for forbidden peer", "peer", peerID)
			continue
		}

		pinfo := new(ProviderInfo)
		err = json.Unmarshal(ent.Value, pinfo)
		if err != nil {
			return 0, err
		}

		if pinfo.Publisher != nil && pinfo.Publisher.ID == pinfo.AddrInfo.ID && equalAddrs(pinfo.Publisher.Addrs, pinfo.AddrInfo.Addrs) {
			pinfo.Publisher = &pinfo.AddrInfo
		}

		r.providers[peerID] = pinfo
		count++
	}
	return count, nil
}

func (r *Registry) migrate() error {
	ctx := context.Background() // do not use context that can be canceled
	var count int
	var fromVer string
	regVerDsKey := datastore.NewKey(registryVersionKey)
	regVerData, err := r.dstore.Get(ctx, regVerDsKey)
	if err != nil && err != datastore.ErrNotFound {
		return err
	}
	if len(regVerData) != 0 {
		fromVer = string(regVerData)
	}

	switch fromVer {
	case registryVersion:
		return nil
	case "":
		fromVer = "v0" // for logging
		type v0ProviderInfo struct {
			AddrInfo              peer.AddrInfo
			DiscoveryAddr         string    `json:",omitempty"`
			LastAdvertisement     cid.Cid   `json:",omitempty"`
			LastAdvertisementTime time.Time `json:",omitempty"`
			Publisher             peer.ID   `json:",omitempty"`
		}

		// Load all providers from the datastore.
		q := query.Query{
			Prefix: providerKeyPath,
		}
		results, err := r.dstore.Query(ctx, q)
		if err != nil {
			return err
		}
		defer results.Close()

		for result := range results.Next() {
			if result.Error != nil {
				log.Errorw("Cannot read provider data", "err", result.Error)
				continue
			}

			v0Info := new(v0ProviderInfo)
			err = json.Unmarshal(result.Entry.Value, v0Info)
			if err != nil {
				log.Errorw("Cannot unmarshal v0 provider data", "err", err, "value", string(result.Entry.Value))
				continue
			}

			v1Info := ProviderInfo{
				AddrInfo:              v0Info.AddrInfo,
				DiscoveryAddr:         v0Info.DiscoveryAddr,
				LastAdvertisement:     v0Info.LastAdvertisement,
				LastAdvertisementTime: v0Info.LastAdvertisementTime,
			}

			if v0Info.Publisher == v1Info.AddrInfo.ID {
				v1Info.Publisher = &v1Info.AddrInfo
			} else if v0Info.Publisher.Validate() == nil {
				v1Info.Publisher = &peer.AddrInfo{
					ID: v0Info.Publisher,
				}
			}

			value, err := json.Marshal(v1Info)
			if err != nil {
				return fmt.Errorf("cannot marshal v1 provider data: %w", err)
			}

			if err = r.dstore.Put(ctx, v1Info.dsKey(), value); err != nil {
				return fmt.Errorf("could not write v1 provider data: %w", err)
			}
			count++
		}

	default:
		return fmt.Errorf("cannot migrate from unsupported registry version %s", fromVer)
	}

	if count != 0 {
		if err = r.dstore.Sync(ctx, datastore.NewKey(providerKeyPath)); err != nil {
			return err
		}
		log.Infow("Migrated registry datastore", "from", fromVer, "to", registryVersion)
	}

	if err = r.dstore.Put(ctx, regVerDsKey, []byte(registryVersion)); err != nil {
		return fmt.Errorf("could not write registry version: %w", err)
	}
	return r.dstore.Sync(ctx, datastore.NewKey(providerKeyPath))
}

func (r *Registry) discover(ctx context.Context, peerID peer.ID, discoAddr string) (*discovery.Discovered, error) {
	if r.discoverer == nil {
		return nil, ErrNoDiscovery
	}

	discoData, err := r.discoverer.Discover(ctx, peerID, discoAddr)
	if err != nil {
		return nil, fmt.Errorf("cannot discover provider: %s", err)
	}

	return discoData, nil
}

func (r *Registry) cleanup() {
	r.discoWait.Add(1)
	r.sequences.retire()
	r.actions <- func() {
		now := time.Now()
		for id, completed := range r.discoTimes {
			if completed.IsZero() {
				continue
			}
			if r.rediscoverWait != 0 && now.Sub(completed) < r.rediscoverWait {
				continue
			}
			delete(r.discoTimes, id)
		}
		if len(r.discoTimes) == 0 {
			r.discoTimes = make(map[string]time.Time)
		}
	}
	r.discoWait.Done()
}

func (r *Registry) pollProviders(pollInterval, pollRetryAfter, pollStopAfter time.Duration) {
	pollStopAfter += pollInterval
	r.actions <- func() {
		now := time.Now()
		for _, info := range r.providers {
			if info.Publisher == nil {
				// No publisher.
				continue
			}
			if info.lastContactTime.IsZero() {
				// There has been no contact since startup.  Poll during next
				// call to this function if no updated for provider.
				info.lastContactTime = now.Add(-pollInterval)
				continue
			}
			noContactTime := now.Sub(info.lastContactTime)
			if noContactTime < pollInterval {
				// Not enough time since last contact.
				continue
			}
			if noContactTime > pollStopAfter {
				// Too much time since last contact.
				log.Warnw("Lost contact with provider's publisher", "publisher", info.Publisher.ID, "provider", info.AddrInfo.ID, "since", info.lastContactTime)
				// Remove the non-responsive publisher.
				info = &ProviderInfo{
					AddrInfo:              info.AddrInfo,
					DiscoveryAddr:         info.DiscoveryAddr,
					LastAdvertisement:     info.LastAdvertisement,
					LastAdvertisementTime: info.LastAdvertisementTime,
					lastContactTime:       info.lastContactTime,
					Publisher:             nil,
				}
				if err := r.syncRegister(context.Background(), info); err != nil {
					log.Errorw("Failed to update provider info", "err", err)
				}
				continue
			}
			select {
			case r.syncChan <- info:
			default:
				log.Debugw("Sync channel blocked, skipping auto-sync", "publisher", info.Publisher.ID)
			}
		}
	}
}

func equalAddrs(addrs, others []multiaddr.Multiaddr) bool {
	if len(addrs) != len(others) {
		return false
	}

	for _, a := range addrs {
		var found bool
		for _, o := range others {
			if a.Equal(o) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// stringsToMultiaddrs converts a slice of string into a slice of Multiaddr
func stringsToMultiaddrs(addrs []string) ([]multiaddr.Multiaddr, error) {
	if len(addrs) == 0 {
		return nil, nil
	}

	maddrs := make([]multiaddr.Multiaddr, len(addrs))
	for i, m := range addrs {
		var err error
		maddrs[i], err = multiaddr.NewMultiaddr(m)
		if err != nil {
			return nil, fmt.Errorf("bad address: %s", err)
		}
	}
	return maddrs, nil
}
