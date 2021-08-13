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
	AddrInfo      peer.AddrInfo
	FcAddress     string
	LastIndex     cid.Cid
	LastIndexTime time.Time
}

// NewRegistry creates a new provider registry, giving it a Discovery interface
// and provider policy configuration.
func NewRegistry(providerCfg config.Providers, disco discovery.Discovery, dstore datastore.Datastore) (*Registry, error) {
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

func (r *Registry) run() {
	defer close(r.closed)

	for action := range r.actions {
		action()
	}
}

func (r *Registry) Discover(fcAddr string, signature, signed []byte, sync bool) error {
	errCh := make(chan error, 1)
	r.actions <- func() {
		r.syncStartDiscover(fcAddr, signature, signed, errCh)
	}
	if sync {
		return <-errCh
	}
	return nil
}

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

func (r *Registry) ProviderInfoByAddr(fcAddr string) *ProviderInfo {
	infoChan := make(chan *ProviderInfo)
	r.actions <- func() {
		// TODO: consider adding a map of fcAddr->providerID
		for _, info := range r.providers {
			if info.FcAddress == fcAddr {
				infoChan <- info
				break
			}
		}
		close(infoChan)
	}

	return <-infoChan
}

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

func (r *Registry) syncStartDiscover(fcAddr string, signature, signed []byte, errCh chan<- error) {
	err := r.syncNeedDiscover(fcAddr)
	if err != nil {
		r.syncEndDiscover(fcAddr, nil, err, errCh)
		return
	}

	// Mark discovery as in progress
	r.discoTimes[fcAddr] = time.Time{}

	r.pendingDisco++

	// Do discovery asynchronously; do not block other discovery requests
	go func() {
		discoData, discoErr := r.discover(fcAddr, signed, signature)
		r.actions <- func() {
			r.syncEndDiscover(fcAddr, discoData, discoErr, errCh)
			r.pendingDisco--
		}
	}()
}

func (r *Registry) syncEndDiscover(fcAddr string, discoData *discovery.Discovered, err error, errCh chan<- error) {
	r.discoTimes[fcAddr] = time.Now()

	if err != nil {
		log.Errorw("could not discover provider", "err", err)
		errCh <- err
		close(errCh)
		return
	}

	info := &ProviderInfo{
		AddrInfo:  discoData.AddrInfo,
		FcAddress: fcAddr,
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

func (r *Registry) syncNeedDiscover(fcAddr string) error {
	completed, ok := r.discoTimes[fcAddr]
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

func (r *Registry) discover(fcAddr string, signature, signed []byte) (*discovery.Discovered, error) {
	if r.discovery == nil {
		return nil, errors.New("miner discovery not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.policy.DiscoveryTimeout())
	defer cancel()

	discoData, err := r.discovery.Discover(ctx, fcAddr, signature, signed)
	if err != nil {
		return nil, fmt.Errorf("cannot discover provider: %s", err)
	}

	// If provider is not allowed, then ignore request
	if !r.policy.Allowed(discoData.AddrInfo.ID) {
		return nil, ErrNotAllowed
	}

	return discoData, nil
}

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
