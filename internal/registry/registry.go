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

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/metrics"
	"github.com/filecoin-project/storetheindex/internal/registry/discovery"
	"github.com/filecoin-project/storetheindex/internal/registry/policy"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
)

const (
	// providerKeyPath is where provider info is stored in to indexer repo
	providerKeyPath = "/registry/pinfo"
)

var log = logging.Logger("indexer/registry")

// Registry stores information about discovered providers
type Registry struct {
	actions   chan func()
	closed    chan struct{}
	closeOnce sync.Once
	dstore    datastore.Datastore
	providers map[peer.ID]*ProviderInfo
	sequences *sequences

	discoverer discovery.Discoverer
	discoWait  sync.WaitGroup
	discoTimes map[string]time.Time
	policy     *policy.Policy

	discoveryTimeout time.Duration
	pollInterval     time.Duration
	rediscoverWait   time.Duration

	periodicTimer *time.Timer
}

// ProviderInfo is an immutable data sturcture that holds information about a
// provider.  A ProviderInfo instance is never modified, but rather a new one
// is created to update its contents.  This means existing references remain
// valid.
type ProviderInfo struct {
	// AddrInfo contains a peer.ID and set of Multiaddr addresses.
	AddrInfo peer.AddrInfo
	// DiscoveryAddr is the address that is used for discovery of the provider.
	DiscoveryAddr string
	// LastAdvertisement identifies the latest advertizement the indexer has ingested.
	LastAdvertisement cid.Cid
	// LastAdvertisementTime is the time the latest advertisement was received.
	LastAdvertisementTime time.Time

	lastContactTime time.Time
}

func (p *ProviderInfo) dsKey() datastore.Key {
	return datastore.NewKey(path.Join(providerKeyPath, p.AddrInfo.ID.String()))
}

// NewRegistry creates a new provider registry, giving it provider policy
// configuration, a datastore to persist provider data, and a Discoverer
// interface.
//
// TODO: It is probably necessary to have multiple discoverer interfaces
func NewRegistry(cfg config.Discovery, dstore datastore.Datastore, disco discovery.Discoverer) (*Registry, error) {
	// Create policy from config
	discoPolicy, err := policy.New(cfg.Policy)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		actions:   make(chan func()),
		closed:    make(chan struct{}),
		policy:    discoPolicy,
		providers: map[peer.ID]*ProviderInfo{},
		sequences: newSequences(0),

		pollInterval:     time.Duration(cfg.PollInterval),
		rediscoverWait:   time.Duration(cfg.RediscoverWait),
		discoveryTimeout: time.Duration(cfg.Timeout),

		discoverer: disco,
		discoTimes: map[string]time.Time{},

		dstore: dstore,
	}

	count, err := r.loadPersistedProviders()
	if err != nil {
		return nil, err
	}
	log.Infow("loaded providers into registry", "count", count)

	r.periodicTimer = time.AfterFunc(r.pollInterval/2, func() {
		r.cleanup()
		r.pollProviders()
		r.periodicTimer.Reset(r.pollInterval / 2)
	})

	go r.run()
	return r, nil
}

// Close waits for any pending discoverer to finish and then stops the registry
func (r *Registry) Close() error {
	var err error
	r.closeOnce.Do(func() {
		r.periodicTimer.Stop()
		// Wait for any pending discoveries to complete, then stop the main run
		// goroutine
		r.discoWait.Wait()
		close(r.actions)

		if r.dstore != nil {
			err = r.dstore.Close()
		}
	})
	<-r.closed
	return err
}

// run executs functions that need to be executed on the same goroutine
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
		return syserr.New(ErrNotAllowed, http.StatusForbidden)
	}

	// It does not matter if the provider is trusted or not, since verification
	// is necessary to get the provider's address

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
func (r *Registry) Register(info *ProviderInfo) error {
	if len(info.AddrInfo.Addrs) == 0 {
		return syserr.New(errors.New("missing provider address"), http.StatusBadRequest)
	}

	allowed, trusted := r.policy.Check(info.AddrInfo.ID)

	// If provider is not allowed, then ignore request.
	if !allowed {
		return syserr.New(ErrNotAllowed, http.StatusForbidden)
	}

	// If allowed provider is not trusted, then they require authorization
	// before being registered.
	if !trusted {
		return syserr.New(ErrNotTrusted, http.StatusUnauthorized)
	}

	// If allowed provider is trusted, register immediately without verification.
	errCh := make(chan error, 1)
	r.actions <- func() {
		r.syncRegister(info, errCh)
	}

	err := <-errCh
	if err != nil {
		return err
	}

	log.Infow("registered provider", "id", info.AddrInfo.ID, "addrs", info.AddrInfo.Addrs)
	return nil
}

// Check if the provider is trusted by policy, or if it has been previously
// verified and registered.
func (r *Registry) Authorized(providerID peer.ID) (bool, error) {
	allowed, trusted := r.policy.Check(providerID)

	if !allowed {
		return false, nil
	}

	// Provider is allowed but not trusted, see if they are already registered.
	if !trusted {
		regOk := make(chan bool)
		r.actions <- func() {
			_, ok := r.providers[providerID]
			regOk <- ok
		}
		return <-regOk, nil
	}

	return true, nil
}

func (r *Registry) SetPolicy(policyCfg config.Policy) error {
	return r.policy.Config(policyCfg)
}

func (r *Registry) AllowProvider(providerID peer.ID) bool {
	return r.policy.Allow(providerID)
}

func (r *Registry) BlockProvider(providerID peer.ID) bool {
	return r.policy.Block(providerID)
}

// RegisterOrUpdate attempts to register an unregistered provider, or updates
// the addresses and latest advertisement of an already registered provider.
func (r *Registry) RegisterOrUpdate(providerID peer.ID, addrs []string, adID cid.Cid) error {
	// Check that the provider has been discovered and validated
	info := r.ProviderInfo(providerID)
	if info == nil {
		if len(addrs) == 0 {
			return errors.New("cannot regiser provider with no address")
		}

		maddrs, err := stringsToMultiaddrs(addrs)
		if err != nil {
			return err
		}

		now := time.Now()
		info = &ProviderInfo{
			AddrInfo: peer.AddrInfo{
				ID:    providerID,
				Addrs: maddrs,
			},
			lastContactTime: now,
		}

		if adID != cid.Undef {
			info.LastAdvertisement = adID
			info.LastAdvertisementTime = now
		}

		return r.Register(info)
	}

	var update bool

	if len(addrs) != 0 {
		maddrs, err := stringsToMultiaddrs(addrs)
		if err != nil {
			return err
		}

		// If the registered addresses are different than those provided, then
		// re-register with new address.
		if len(addrs) != len(info.AddrInfo.Addrs) {
			info.AddrInfo.Addrs = maddrs
			update = true
		} else {
			for i := range maddrs {
				if !maddrs[i].Equal(info.AddrInfo.Addrs[i]) {
					info.AddrInfo.Addrs = maddrs
					update = true
					break
				}
			}
		}
	}

	if !update && adID == cid.Undef {
		return nil
	}

	now := time.Now()

	if adID != cid.Undef {
		info.LastAdvertisement = adID
		info.LastAdvertisementTime = now
	}
	info.lastContactTime = now

	return r.Register(info)
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
		r.syncEndDiscover(discoAddr, nil, err, errCh)
		return
	}

	// Mark discovery as in progress
	r.discoTimes[discoAddr] = time.Time{}
	r.discoWait.Add(1)

	// Do discovery asynchronously; do not block other discovery requests
	go func() {
		discoData, discoErr := r.discover(peerID, discoAddr)
		r.actions <- func() {
			r.syncEndDiscover(discoAddr, discoData, discoErr, errCh)
			r.discoWait.Done()
		}
	}()
}

func (r *Registry) syncEndDiscover(discoAddr string, discoData *discovery.Discovered, err error, errCh chan<- error) {
	r.discoTimes[discoAddr] = time.Now()

	if err != nil {
		errCh <- err
		close(errCh)
		return
	}

	info := &ProviderInfo{
		AddrInfo:      discoData.AddrInfo,
		DiscoveryAddr: discoAddr,
	}

	r.syncRegister(info, errCh)
}

func (r *Registry) syncRegister(info *ProviderInfo, errCh chan<- error) {
	r.providers[info.AddrInfo.ID] = info
	err := r.syncPersistProvider(info)
	if err != nil {
		err = fmt.Errorf("could not persist provider: %s", err)
		errCh <- syserr.New(err, http.StatusInternalServerError)
	}
	close(errCh)
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

func (r *Registry) syncPersistProvider(info *ProviderInfo) error {
	if r.dstore == nil {
		return nil
	}
	value, err := json.Marshal(info)
	if err != nil {
		return err
	}

	dsKey := info.dsKey()
	if err = r.dstore.Put(dsKey, value); err != nil {
		return err
	}
	if err = r.dstore.Sync(dsKey); err != nil {
		return fmt.Errorf("cannot sync provider info: %s", err)
	}
	return nil
}

func (r *Registry) loadPersistedProviders() (int, error) {
	if r.dstore == nil {
		return 0, nil
	}

	// Load all providers from the datastore.
	q := query.Query{
		Prefix: providerKeyPath,
	}
	results, err := r.dstore.Query(q)
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

		pinfo := new(ProviderInfo)
		err = json.Unmarshal(ent.Value, pinfo)
		if err != nil {
			return 0, err
		}

		r.providers[peerID] = pinfo
		count++
	}
	return count, nil
}

func (r *Registry) discover(peerID peer.ID, discoAddr string) (*discovery.Discovered, error) {
	if r.discoverer == nil {
		return nil, ErrNoDiscovery
	}

	ctx := context.Background()
	discoTimeout := r.discoveryTimeout
	if discoTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, discoTimeout)
		defer cancel()
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

func (r *Registry) pollProviders() {
	// TODO: Poll providers that have not been contacted for more than pollInterval.
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
