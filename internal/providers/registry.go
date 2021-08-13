package providers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/storetheindex/config"
	"github.com/filecoin-project/storetheindex/internal/providers/discovery"
	"github.com/filecoin-project/storetheindex/internal/providers/policy"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("providers")

// Registry stores information about discovered providers
type Registry struct {
	actions   chan func()
	providers map[peer.ID]*ProviderInfo
	policy    *policy.Policy

	discovery    discovery.Discovery
	discoTimes   map[string]time.Time
	closeOnce    sync.Once
	closed       chan struct{}
	pendingDisco int

	dstore datastore.Datastore
}

// ProviderInfo is an immutable data sturcture that holds information about a
// provider.  A ProviderInfo instance is never modified, but rather a new one
// is created to update its contents.  This means existing references remain
// valid.
type ProviderInfo struct {
	// AddrInfo contains a peer.ID and set of Multiaddr addresses
	AddrInfo peer.AddrInfo
	// DiscoveryAddr is the address that is used for discovery of the provider
	DiscoveryAddr string
	// LastIndex identifies the latest advertised data the indexer has ingested
	LastIndex cid.Cid
	// LastIndexTime is the time indexed data was added to the indexer
	LastIndexTime time.Time
}

// NewRegistry creates a new provider registry, giving it provider policy
// configuration, a datastore to persist provider data, and a Discovery
// interface.
//
// TODO: It is probably necessary to have multiple discovery interfaces
func NewRegistry(providerCfg config.Providers, dstore datastore.Datastore, disco discovery.Discovery) (*Registry, error) {
	// Create policy from config
	providerPolicy, err := policy.New(providerCfg)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		actions:   make(chan func()),
		closed:    make(chan struct{}),
		policy:    providerPolicy,
		providers: map[peer.ID]*ProviderInfo{},

		discovery:  disco,
		discoTimes: map[string]time.Time{},

		dstore: dstore,
	}

	err = r.loadPersistedProviders()
	if err != nil {
		return nil, err
	}

	go r.run()
	return r, nil
}

// Close waits for any pending discovery to finish and then stops the registry
func (r *Registry) Close() error {
	var err error
	r.closeOnce.Do(func() {
		// Wait for any pending discoveries to complete
		doneChan := make(chan bool)
		for {
			r.actions <- func() {
				doneChan <- r.pendingDisco == 0
			}
			done := <-doneChan
			if done {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}

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

// Discovery begins the process of discovering and verifying a provider.  The
// discovery address us used to lookup the provider's information.
//
// TODO: To support multiple discovery methods (lotus, IPFS, etc.) there need
// to be information that is part of, or in addition to, the discoveryAddr to
// indicate where/how discovery is done.
func (r *Registry) Discover(discoveryAddr string, signature, signed []byte, sync bool) error {
	errCh := make(chan error, 1)
	r.actions <- func() {
		r.syncStartDiscover(discoveryAddr, signature, signed, errCh)
	}
	if sync {
		return <-errCh
	}
	return nil
}

// Register is used to directly register a provider, bypassing discovery and
// adding discovered data directly to the registry.
func (r *Registry) Register(info *ProviderInfo) error {
	if len(info.AddrInfo.ID) == 0 {
		return errors.New("missing peer id")
	}

	// If provider is trusted, register immediately
	if !r.policy.Trusted(info.AddrInfo.ID) {
		return errors.New("provider must be trusted to register without on-chain verification")
	}

	errCh := make(chan error, 1)
	r.actions <- func() {
		r.syncRegister(info, errCh)
	}

	return <-errCh
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

// ProciverInfoByAddr finds a registered provider using its discovery address
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

func (r *Registry) syncStartDiscover(discoAddr string, signature, signed []byte, errCh chan<- error) {
	err := r.syncNeedDiscover(discoAddr)
	if err != nil {
		r.syncEndDiscover(discoAddr, nil, err, errCh)
		return
	}

	// Mark discovery as in progress
	r.discoTimes[discoAddr] = time.Time{}

	r.pendingDisco++

	// Do discovery asynchronously; do not block other discovery requests
	go func() {
		discoData, discoErr := r.discover(discoAddr, signed, signature)
		r.actions <- func() {
			r.syncEndDiscover(discoAddr, discoData, discoErr, errCh)
			r.pendingDisco--
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
		log.Errorw("could not persiste provider", "err", err)
		errCh <- err
	}
	close(errCh)
}

func (r *Registry) syncPersistProvider(info *ProviderInfo) error {
	if r.dstore == nil {
		return nil
	}
	value, err := json.Marshal(info)
	if err != nil {
		return err
	}
	dsKey := datastore.NewKey(info.AddrInfo.ID.String())

	return r.dstore.Put(dsKey, value)
}

func (r *Registry) syncNeedDiscover(discoAddr string) error {
	completed, ok := r.discoTimes[discoAddr]
	if ok {
		// Check if discovery already in progress
		if completed.IsZero() {
			return ErrInProgress
		}

		// Check if last discovery completed too recently
		if !r.policy.CanRediscover(completed) {
			return ErrTooSoon
		}
	}
	return nil
}

func (r *Registry) discover(discoAddr string, signature, signed []byte) (*discovery.Discovered, error) {
	if r.discovery == nil {
		return nil, errors.New("miner discovery not available")
	}

	ctx := context.Background()
	discoTimeout := r.policy.DiscoveryTimeout()
	if discoTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, discoTimeout)
		defer cancel()
	}

	discoData, err := r.discovery.Discover(ctx, discoAddr, signature, signed)
	if err != nil {
		return nil, fmt.Errorf("cannot discover provider: %s", err)
	}

	// If provider is not allowed, then ignore request
	if !r.policy.Allowed(discoData.AddrInfo.ID) {
		return nil, ErrNotAllowed
	}

	return discoData, nil
}

// VerifySignature verifies the signature over the given data using the public
// key from the given peerID
func VerifySignature(peerID peer.ID, signature, data []byte) (bool, error) {
	if len(signature) == 0 {
		return false, errors.New("empty signature")
	}
	if len(data) == 0 {
		return false, errors.New("no data to sign")
	}

	pubKey, err := peerID.ExtractPublicKey()
	if err != nil {
		return false, err
	}

	return pubKey.Verify(data, signature)
}

func (r *Registry) loadPersistedProviders() error {
	// TODO: implement
	if r.dstore == nil {
		return nil
	}
	return nil
}
