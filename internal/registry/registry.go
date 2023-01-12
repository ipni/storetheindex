package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	v0 "github.com/ipni/storetheindex/api/v0"
	httpclient "github.com/ipni/storetheindex/api/v0/finder/client/http"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/internal/disk"
	"github.com/ipni/storetheindex/internal/freeze"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry/discovery"
	"github.com/ipni/storetheindex/internal/registry/policy"
	"github.com/ipni/storetheindex/mautil"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"go.opencensus.io/stats"
)

const (
	// providerKeyPath is where provider info is stored in to indexer repo.
	providerKeyPath       = "/registry/pinfo"
	assignmentsKeyPath    = "/assignments-v2"
	oldAssignmentsKeyPath = "/assignments-v1"
)

var log = logging.Logger("indexer/registry")

// Registry stores information about discovered providers
type Registry struct {
	actions   chan func()
	closed    chan struct{}
	closeOnce sync.Once
	closing   chan struct{}
	dstore    datastore.Datastore
	filterIPs bool
	freezer   *freeze.Freezer
	providers map[peer.ID]*ProviderInfo
	sequences *sequences

	discoverer    discovery.Discoverer
	discoverWait  sync.WaitGroup
	discoverTimes map[string]time.Time
	policy        *policy.Policy

	// assigned tracks peers assigned by assigner service.
	assigned map[peer.ID]peer.ID
	// assignMutex protects assigner.
	assignMutex sync.Mutex
	// preferred tracks unassigned peers that indexer has previously received
	// index data from.
	preferred map[peer.ID]struct{}

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
	LastAdvertisementTime time.Time
	// Publisher contains the ID of the provider info publisher.
	Publisher peer.ID `json:",omitempty"`
	// PublisherAddr contains the last seen publisher multiaddr.
	PublisherAddr multiaddr.Multiaddr `json:",omitempty"`
	// ExtendedProviders registered for that provider
	ExtendedProviders *ExtendedProviders `json:",omitempty"`

	// FrozenAt identifies the last advertisement that was received before the
	// indexer became frozen.
	FrozenAt cid.Cid `json:",omitempty"`
	// FrozenAtTime is the time that the FrozenAt advertisement was received.
	FrozenAtTime time.Time

	// lastContactTime is the last time the publisher contacted the indexer.
	// This is not persisted, so that the time since last contact is reset when
	// the indexer is started. If not reset, then it would appear the publisher
	// was unreachable for the indexer downtime.
	lastContactTime time.Time

	// deleted is used as a signal to the ingester to delete the provider's data.
	deleted bool
	// inactive means polling the publisher with no response yet.
	inactive bool
	// handoffCid is used to tell the autosync goroutine to set the latest CID
	// to stop the sync at.
	stopCid cid.Cid
}

// ExtendedProviderInfo is an immutable data structure that holds information
// about an extended provider.
type ExtendedProviderInfo struct {
	// PeerID contains a peer.ID of the extended provider.
	PeerID peer.ID
	// Metadata contains a metadata override for this provider within the
	// extended provider context. If extended provider's metadata hasn't been
	// specified - the main provider's metadata is going to be used instead.
	Metadata []byte `json:",omitempty"`
	// Addrs contains advertised multiaddresses for this extended provider.
	Addrs []multiaddr.Multiaddr
}

// ContextualExtendedProviders holds information about a context-level extended
// providers. These can either replace or compliment (union) the chain-level
// extended providers, which is driven by the Override flag.
type ContextualExtendedProviders struct {
	// Providers contains a list of context-level extended providers
	Providers []ExtendedProviderInfo
	// Override defines whether chain-level extended providers should be used
	// for this ContextID. If true, then the chain-level extended providers are
	// going to be ignored.
	Override bool
	// ContextID defines the context ID that the extended providers have been
	// published for
	ContextID []byte `json:",omitempty"`
}

// ExtendedProviders contains chain-level and context-level extended provider
// sets.
type ExtendedProviders struct {
	// Providers contains a chain-level set of extended providers.
	Providers []ExtendedProviderInfo `json:",omitempty"`
	// ContextualProviders contains a context-level sets of extended providers.
	ContextualProviders map[string]ContextualExtendedProviders `json:",omitempty"`
}

type polling struct {
	interval        time.Duration
	retryAfter      time.Duration
	stopAfter       time.Duration
	deactivateAfter time.Duration
}

func (p *ProviderInfo) Deleted() bool {
	return p.deleted
}

func (p *ProviderInfo) Inactive() bool {
	return p.inactive
}

func (p *ProviderInfo) StopCid() cid.Cid {
	return p.stopCid
}

func (p *ProviderInfo) dsKey() datastore.Key {
	return peerIDToDsKey(providerKeyPath, p.AddrInfo.ID)
}

func peerIDToDsKey(keyPath string, peerID peer.ID) datastore.Key {
	return datastore.NewKey(path.Join(keyPath, peerID.String()))
}

func (p *ProviderInfo) MarshalJSON() ([]byte, error) {
	var pubAddr string
	if p.PublisherAddr != nil {
		pubAddr = p.PublisherAddr.String()
	}
	type Alias ProviderInfo
	return json.Marshal(&struct {
		PublisherAddr string `json:",omitempty"`
		*Alias
	}{
		PublisherAddr: pubAddr,
		Alias:         (*Alias)(p),
	})
}

func (p *ProviderInfo) UnmarshalJSON(data []byte) error {
	type Alias ProviderInfo
	aux := &struct {
		PublisherAddr string `json:",omitempty"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	if aux.PublisherAddr != "" {
		p.PublisherAddr, err = multiaddr.NewMultiaddr(aux.PublisherAddr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *ExtendedProviderInfo) MarshalJSON() ([]byte, error) {
	addrs := make([]string, len(p.Addrs))
	for i, addr := range p.Addrs {
		addrs[i] = addr.String()
	}
	type Alias ExtendedProviderInfo
	return json.Marshal(&struct {
		Addrs []string `json:",omitempty"`
		*Alias
	}{
		Addrs: addrs,
		Alias: (*Alias)(p),
	})
}

func (p *ExtendedProviderInfo) UnmarshalJSON(data []byte) error {
	type Alias ExtendedProviderInfo
	aux := &struct {
		Addrs []string `json:",omitempty"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}
	err := json.Unmarshal(data, &aux)
	if err != nil {
		return err
	}
	maddrs := make([]multiaddr.Multiaddr, len(aux.Addrs))
	for i, addr := range aux.Addrs {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return err
		}
		maddrs[i] = maddr
	}
	p.Addrs = maddrs
	return nil
}

// New creates a new provider registry, giving it provider policy
// configuration, a datastore to persist provider data, and a Discoverer
// interface. The context is only used for cancellation of this function.
func New(ctx context.Context, cfg config.Discovery, dstore datastore.Datastore, options ...Option) (*Registry, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	// Create policy from config.
	regPolicy, err := policy.New(cfg.Policy)
	if err != nil {
		return nil, err
	}
	// Log warning if no peers are allowed.
	if regPolicy.NoneAllowed() {
		log.Warn("Policy does not allow any peers to index content")
	}

	r := &Registry{
		actions:   make(chan func()),
		closed:    make(chan struct{}),
		closing:   make(chan struct{}),
		filterIPs: cfg.FilterIPs,
		policy:    regPolicy,
		sequences: newSequences(0),

		rediscoverWait:   time.Duration(cfg.RediscoverWait),
		discoveryTimeout: time.Duration(cfg.Timeout),

		discoverer: opts.discoverer,

		dstore:   dstore,
		syncChan: make(chan *ProviderInfo, 1),
	}

	r.providers, err = loadPersistedProviders(ctx, dstore, cfg.FilterIPs)
	if err != nil {
		return nil, fmt.Errorf("cannot load provider data from datastore: %w", err)
	}
	log.Infow("Loaded providers into registry", "count", len(r.providers))

	if cfg.UseAssigner {
		r.assigned, err = loadPersistedAssignments(ctx, dstore, cfg.RemoveOldAssignments)
		if err != nil {
			return nil, err
		}
		log.Infow("Loaded assignments into registry", "count", len(r.assigned))
		r.preferred = loadPreferredAssignments(r.providers, r.assigned)
	}

	pollOverrides, err := makePollOverrideMap(cfg.PollOverrides)
	if err != nil {
		return nil, err
	}
	poll := polling{
		interval:        time.Duration(cfg.PollInterval),
		retryAfter:      time.Duration(cfg.PollRetryAfter),
		stopAfter:       time.Duration(cfg.PollStopAfter),
		deactivateAfter: time.Duration(cfg.DeactivateAfter),
	}

	go r.run()

	if opts.freezeAtPercent >= 0 {
		r.freezer, err = freeze.New(opts.valueStoreDir, opts.freezeAtPercent, dstore, r.freeze)
		if err != nil {
			return nil, fmt.Errorf("cannot create freezer: %s", err)
		}
	}

	go r.runPollCheck(poll, pollOverrides)

	return r, nil
}

func makePollOverrideMap(cfgPollOverrides []config.Polling) (map[peer.ID]polling, error) {
	if len(cfgPollOverrides) == 0 {
		return nil, nil
	}

	pollOverrides := make(map[peer.ID]polling, len(cfgPollOverrides))
	for _, poll := range cfgPollOverrides {
		peerID, err := peer.Decode(poll.ProviderID)
		if err != nil {
			return nil, fmt.Errorf("cannot decode provider ID %q in PollOverrides: %s", poll.ProviderID, err)
		}
		pollOverrides[peerID] = polling{
			interval:        time.Duration(poll.Interval),
			retryAfter:      time.Duration(poll.RetryAfter),
			stopAfter:       time.Duration(poll.StopAfter),
			deactivateAfter: time.Duration(poll.DeactivateAfter),
		}
	}
	return pollOverrides, nil
}

// Close waits for polling and actions to finish and then stops the registry.
func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		if r.freezer != nil {
			r.freezer.Close()
		}
		close(r.closing)
	})
	<-r.closed
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

func (r *Registry) runPollCheck(poll polling, pollOverrides map[peer.ID]polling) {
	retryAfter := poll.retryAfter
	for i := range pollOverrides {
		if pollOverrides[i].retryAfter < retryAfter {
			retryAfter = pollOverrides[i].retryAfter
		}
	}

	if retryAfter < time.Minute {
		retryAfter = time.Minute
	}
	timer := time.NewTimer(retryAfter)
running:
	for {
		select {
		case <-timer.C:
			r.cleanup()
			r.pollProviders(poll, pollOverrides)
			timer.Reset(retryAfter)
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
	r.discoverWait.Wait()
	close(r.actions)
}

// Discover begins the process of discovering and verifying a provider.  The
// discovery address is used to lookup the provider's information.
//
// TODO: To support multiple discoverer methods (lotus, IPFS, etc.) there need
// to be information that is part of, or in addition to, the discoveryAddr to
// indicate where/how discovery is done.
func (r *Registry) Discover(peerID peer.ID, discoveryAddr string, sync bool) error {
	// If provider is not allowed, then ignore request
	if !r.policy.Allowed(peerID) {
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

// Saw indicates that a provider was seen.
func (r *Registry) Saw(provider peer.ID) {
	done := make(chan struct{})
	r.actions <- func() {
		if _, ok := r.providers[provider]; ok {
			pinfo := r.providers[provider]
			pinfo.lastContactTime = time.Now()
			pinfo.inactive = false
		}
		close(done)
	}
	<-done
}

// Allowed checks if the peer is allowed by policy. If configured to work with
// an assigner service, then the peer must also be assigned to the indexer to
// be allowed.
func (r *Registry) Allowed(peerID peer.ID) bool {
	if r.assigned != nil {
		r.assignMutex.Lock()
		_, ok := r.assigned[peerID]
		r.assignMutex.Unlock()
		if !ok {
			return false
		}
	}
	return r.policy.Allowed(peerID)
}

// PublishAllowed checks if a peer is allowed to publish for other providers.
func (r *Registry) PublishAllowed(publisherID, providerID peer.ID) bool {
	return r.policy.PublishAllowed(publisherID, providerID)
}

func (r *Registry) SetPolicy(policyCfg config.Policy) error {
	newPol, err := policy.New(policyCfg)
	if err != nil {
		return err
	}
	// Log warning if no peers are allowed.
	if newPol.NoneAllowed() {
		log.Warn("Policy does not allow any peers to index content")
	}

	r.policy.Copy(newPol)
	return nil
}

// ListAssignedPeers returns list of assigned peer IDs, when the indexer is
// configured to work with an assigner service. Otherwise returns error.
func (r *Registry) ListAssignedPeers() ([]peer.ID, []peer.ID, error) {
	if r.assigned == nil {
		return nil, nil, ErrNoAssigner
	}

	r.assignMutex.Lock()
	defer r.assignMutex.Unlock()

	publisherIDs := make([]peer.ID, len(r.assigned))
	continuedIDs := make([]peer.ID, len(r.assigned))
	var i int
	for publisher, continued := range r.assigned {
		publisherIDs[i] = publisher
		continuedIDs[i] = continued
		i++
	}
	return publisherIDs, continuedIDs, nil
}

// ListPreferredPeers returns list of unassigned peer IDs, that the indexer has
// already retrieved advertisements from. Only available when the indexer is
// configured to work with an assigner service. Otherwise returns error.
func (r *Registry) ListPreferredPeers() ([]peer.ID, error) {
	if r.assigned == nil {
		return nil, ErrNoAssigner
	}

	r.assignMutex.Lock()
	defer r.assignMutex.Unlock()

	if len(r.preferred) == 0 {
		return nil, nil
	}

	peers := make([]peer.ID, 0, len(r.preferred))
	for peerID := range r.preferred {
		if !r.policy.Allowed(peerID) {
			continue
		}
		peers = append(peers, peerID)
	}

	return peers, nil
}

// AssignPeer assigns the peer to this indexer if using an assigner service.
// This allows the indexer to accept announce messages having this peer as a
// publisher. The assignment is persisted in the datastore.
func (r *Registry) AssignPeer(publisherID peer.ID) error {
	if r.assigned == nil {
		return ErrNoAssigner
	}
	if !r.policy.Allowed(publisherID) {
		return ErrNotAllowed
	}
	if r.Frozen() {
		return fmt.Errorf("cannot assign publisher: %w", ErrFrozen)
	}

	return r.assignPeer(publisherID, peer.ID(""))
}

func (r *Registry) assignPeer(publisherID, frozenID peer.ID) error {
	r.assignMutex.Lock()
	defer r.assignMutex.Unlock()

	if _, ok := r.assigned[publisherID]; ok {
		return ErrAlreadyAssigned
	}

	err := r.saveAssignedPeer(publisherID, frozenID)
	if err != nil {
		return fmt.Errorf("cannot save assignment: %w", err)
	}

	r.assigned[publisherID] = frozenID
	delete(r.preferred, publisherID)
	return nil
}

// UnassignPeer removes a peer assignment from this indexer if using an
// assigner service. The assignment removal is persisted in the datastore.
func (r *Registry) UnassignPeer(peerID peer.ID) error {
	if r.assigned == nil {
		return ErrNoAssigner
	}

	r.assignMutex.Lock()
	defer r.assignMutex.Unlock()

	err := r.deleteAssignedPeer(peerID)
	if err != nil {
		return fmt.Errorf("cannot save assignment: %w", err)
	}
	delete(r.assigned, peerID)
	return nil
}

// AllowPeer configures the policy to allow messages published by the
// identified peer. Returns true if policy changed. This configuration is not
// persisted across indexer restarts. Update the indexer config file to persist
// the change.
func (r *Registry) AllowPeer(peerID peer.ID) bool {
	return r.policy.Allow(peerID)
}

// BlockPeer configures the policy to block messages published by the
// identified peer. Returns true if policy changed. This configuration is not
// persisted across indexer restarts. Update the indexer config file to persist
// the change.
func (r *Registry) BlockPeer(peerID peer.ID) bool {
	return r.policy.Block(peerID)
}

// FilterIPsEnabled returns true if IP address filtering is enabled.
func (r *Registry) FilterIPsEnabled() bool {
	return r.filterIPs
}

// Update attempts to update the registry's provider information. If publisher
// has a valid ID, then the supplied publisher data replaces the provider's
// previous publisher information.
func (r *Registry) Update(ctx context.Context, provider, publisher peer.AddrInfo, adCid cid.Cid, extendedProviders *ExtendedProviders) error {
	// Do not accept update if provider is not allowed.
	if !r.policy.Allowed(provider.ID) {
		return ErrNotAllowed
	}

	if r.filterIPs {
		provider.Addrs = mautil.FilterPrivateIPs(provider.Addrs)
		publisher.Addrs = mautil.FilterPrivateIPs(publisher.Addrs)
	}

	var newPublisher bool

	// Check that the provider has been discovered and validated
	info, _ := r.ProviderInfo(provider.ID)
	if info != nil {
		info = &ProviderInfo{
			AddrInfo:              info.AddrInfo,
			DiscoveryAddr:         info.DiscoveryAddr,
			LastAdvertisement:     info.LastAdvertisement,
			LastAdvertisementTime: info.LastAdvertisementTime,
			Publisher:             info.Publisher,
			PublisherAddr:         info.PublisherAddr,
			ExtendedProviders:     info.ExtendedProviders,

			FrozenAt:     info.FrozenAt,
			FrozenAtTime: info.FrozenAtTime,
		}

		// If new addrs provided, update to use these.
		if len(provider.Addrs) != 0 {
			info.AddrInfo.Addrs = provider.Addrs
		}

		if extendedProviders != nil {
			info.ExtendedProviders = extendedProviders
		}

		// If publisher ID changed.
		if publisher.ID.Validate() == nil && publisher.ID != info.Publisher {
			newPublisher = true
		}
	} else {
		if r.Frozen() {
			return fmt.Errorf("cannot register new provider: %w", ErrFrozen)
		}

		err := provider.ID.Validate()
		if err != nil {
			return err
		}
		if len(provider.Addrs) == 0 {
			return errors.New("missing provider address")
		}
		info = &ProviderInfo{
			AddrInfo:          provider,
			ExtendedProviders: extendedProviders,
		}
		// It is possible to have a provider with no publisher, in the case of
		// a provider that is only manually synced.
		if publisher.ID.Validate() == nil {
			newPublisher = true
		}
	}

	if newPublisher {
		// Check if new publisher is allowed.
		if !r.policy.Allowed(publisher.ID) {
			return v0.NewError(ErrPublisherNotAllowed, http.StatusForbidden)
		}
		if !r.policy.PublishAllowed(publisher.ID, info.AddrInfo.ID) {
			return v0.NewError(ErrCannotPublish, http.StatusForbidden)
		}
		info.Publisher = publisher.ID
	}

	if info.Publisher.Validate() == nil {
		// Use new publisher addrs if any given. Otherwise, keep existing.
		// If no existing publisher addrs, and publisher ID is same as
		// provider ID, then use provider addresses if any.
		if len(publisher.Addrs) != 0 {
			info.PublisherAddr = publisher.Addrs[0]
		} else if info.PublisherAddr == nil && publisher.ID == info.AddrInfo.ID {
			info.PublisherAddr = info.AddrInfo.Addrs[0]
		}
	}

	now := time.Now()
	if adCid != info.LastAdvertisement && adCid != cid.Undef {
		info.LastAdvertisement = adCid
		info.LastAdvertisementTime = now
	}
	info.lastContactTime = now

	err := r.register(ctx, info)
	if err != nil {
		return err
	}
	log.Debugw("Updated registered provider info", "id", info.AddrInfo.ID, "addrs", info.AddrInfo.Addrs)
	return nil
}

func (r *Registry) register(ctx context.Context, info *ProviderInfo) error {
	errCh := make(chan error, 1)
	r.actions <- func() {
		errCh <- r.syncRegister(ctx, info)
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

// ProviderInfo returns information for a registered provider.
func (r *Registry) ProviderInfo(providerID peer.ID) (*ProviderInfo, bool) {
	infoChan := make(chan *ProviderInfo)
	r.actions <- func() {
		info, ok := r.providers[providerID]
		if ok {
			infoChan <- info
		}
		close(infoChan)
	}
	pinfo := <-infoChan
	if pinfo == nil {
		return nil, false
	}

	return pinfo, r.policy.Allowed(providerID)
}

// AllProviderInfo returns information for all registered providers that are
// active and allowed.
func (r *Registry) AllProviderInfo() []*ProviderInfo {
	var infos []*ProviderInfo
	done := make(chan struct{})
	r.actions <- func() {
		infos = make([]*ProviderInfo, 0, len(r.providers))
		for _, info := range r.providers {
			if !info.Inactive() && r.policy.Allowed(info.AddrInfo.ID) {
				infos = append(infos, info)
			}
		}
		close(done)
	}
	<-done
	// Stats tracks the number of active, allowed providers.
	stats.Record(context.Background(), metrics.ProviderCount.M(int64(len(infos))))
	return infos
}

func (r *Registry) Handoff(ctx context.Context, publisherID, frozenID peer.ID, frozenURL *url.URL) error {
	if r.assigned == nil {
		return ErrNoAssigner
	}
	if !r.policy.Allowed(publisherID) {
		return ErrPublisherNotAllowed
	}

	r.assignMutex.Lock()
	_, ok := r.assigned[publisherID]
	r.assignMutex.Unlock()
	if ok {
		return ErrAlreadyAssigned
	}

	// Get the providers from the frozen indexer.
	cl, err := httpclient.New(frozenURL.String())
	if err != nil {
		return err
	}
	provs, err := cl.ListProviders(ctx, true)
	if err != nil {
		return err
	}

	// Iterate through the providers to find the one with the publisher being handed off.
	var provInfo *model.ProviderInfo
	for _, pInfo := range provs {
		if pInfo.Publisher.ID == publisherID {
			provInfo = pInfo
			break
		}
	}

	if provInfo == nil {
		// None of the providers has publisher that is being handed off. The
		// provider changed publishers at some point after the publisher was
		// assigned to the indexer. There is nothing to hand off for this
		// publisher. With no associated provider, there is no way to know
		// where indexing was frozen, so do not assign the publisher to this
		// indexer.
		log.Infow("handoff publisher no longer in use on frozen indexer")
		return nil
	}

	// If this indexer does not allow this provider, then this publisher cannot
	// be handed off to this indexer.
	if !r.policy.Allowed(provInfo.AddrInfo.ID) {
		return ErrNotAllowed
	}
	// If this indexer does not allow the publisher to publish for the
	// provider, then this publisher cannot be handed off to this indexer.
	if !r.policy.PublishAllowed(publisherID, provInfo.AddrInfo.ID) {
		return ErrCannotPublish
	}

	regInfo := apiToRegProviderInfo(provInfo)
	err = r.register(ctx, regInfo)
	if err != nil {
		return fmt.Errorf("cannot register provider from frozen indexer: %w", err)
	}

	// If source indexer has ingested any ads for the provider, then start a
	// sync with the frozen at ad as the ad to stop at.
	if provInfo.FrozenAt != cid.Undef {
		regInfo.stopCid = provInfo.FrozenAt

		select {
		case r.syncChan <- regInfo:
		case <-r.closing:
			return errors.New("indexer shutdown")
		}
	}

	return r.assignPeer(publisherID, frozenID)
}

// ImportProviders reads providers from another indexer and registers any that
// are not already registered. Returns the count of newly registered providers.
func (r *Registry) ImportProviders(ctx context.Context, fromURL *url.URL) (int, error) {
	cl, err := httpclient.New(fromURL.String())
	if err != nil {
		return 0, err
	}

	provs, err := cl.ListProviders(ctx, true)
	if err != nil {
		return 0, err
	}

	var newProvs []*ProviderInfo
	for _, pInfo := range provs {
		if r.IsRegistered(pInfo.AddrInfo.ID) {
			continue
		}

		regInfo := apiToRegProviderInfo(pInfo)
		if regInfo.PublisherAddr == nil {
			log.Infow("Publisher missing address",
				"provider", regInfo.AddrInfo.ID, "publisher", regInfo.Publisher)
		}

		if !r.policy.Allowed(regInfo.Publisher) {
			log.Infow("Cannot register provider", "err", ErrPublisherNotAllowed,
				"provider", regInfo.AddrInfo.ID, "publisher", regInfo.Publisher)
			continue
		} else if !r.policy.PublishAllowed(regInfo.Publisher, regInfo.AddrInfo.ID) {
			log.Infow("Cannot register provider", "err", ErrCannotPublish,
				"provider", regInfo.AddrInfo.ID, "publisher", regInfo.Publisher)
			continue
		}

		err = r.register(ctx, regInfo)
		if err != nil {
			log.Infow("Cannot register provider", "err", err,
				"provider", regInfo.AddrInfo.ID, "publisher", regInfo.Publisher)
			continue
		}

		newProvs = append(newProvs, regInfo)
	}
	log.Infow("Imported new providers from other indexer", "from", fromURL.String(), "count", len(newProvs))

	// Start goroutine to sync with all the new providers.
	go func() {
		for _, pinfo := range newProvs {
			select {
			case r.syncChan <- pinfo:
			case <-r.closing:
				return
			}
		}
	}()

	return len(newProvs), nil
}

func (r *Registry) RemoveProvider(ctx context.Context, providerID peer.ID) error {
	var pinfo *ProviderInfo
	errChan := make(chan error)
	r.actions <- func() {
		pinfo = r.providers[providerID]
		// Remove provider from datastore and memory.
		errChan <- r.syncRemoveProvider(ctx, providerID)
	}
	err := <-errChan
	if err != nil {
		return err
	}
	if pinfo != nil {
		// Tell ingester to delete its provider data.
		pinfo.deleted = true
		r.syncChan <- pinfo
	}
	return nil
}

func (r *Registry) CheckSequence(peerID peer.ID, seq uint64) error {
	return r.sequences.check(peerID, seq)
}

// Freeze puts the indexer into forzen mode.
//
// The registry in not frozen directly, but the Freezer is triggered instead.
func (r *Registry) Freeze() error {
	if r.freezer == nil {
		return ErrNoFreeze
	}
	return r.freezer.Freeze()
}

// freeze is called by the Freezer to record the last advertisement ingested
// for each provider at the time the indexer becomes frozen.
func (r *Registry) freeze() error {
	now := time.Now()
	errCh := make(chan error)
	r.actions <- func() {
		errCh <- r.syncFreeze(now)
	}
	err := <-errCh
	if err != nil {
		return fmt.Errorf("cannot freeze providers: %w", err)
	}
	return nil
}

// Frozen returns true if indexer is frozen.
func (r *Registry) Frozen() bool {
	if r.freezer == nil {
		return false
	}
	return r.freezer.Frozen()
}

func (r *Registry) ValueStoreUsage() (*disk.UsageStats, error) {
	if r.freezer == nil {
		return nil, ErrNoFreeze
	}
	return r.freezer.Usage()
}

// Unfreeze reverts the freezer and provider information back to its unfrozen
// state. This must only be called when the registry is not running.
func Unfreeze(ctx context.Context, vstoreDir string, freezeAtPercent float64, dstore datastore.Datastore) (map[peer.ID]cid.Cid, error) {
	if dstore == nil {
		return nil, nil
	}

	err := freeze.Unfreeze(ctx, vstoreDir, freezeAtPercent, dstore)
	if err != nil {
		return nil, fmt.Errorf("cannot unfreeze freezer: %w", err)
	}

	providers, err := loadPersistedProviders(ctx, dstore, false)
	if err != nil {
		return nil, fmt.Errorf("cannot load provider info: %w", err)
	}

	unfrozen := make(map[peer.ID]cid.Cid, len(providers))

	for _, info := range providers {
		if info.FrozenAtTime.IsZero() {
			// Already unforzen. Frozen provider will have non-zero time.
			continue
		}
		unfrozen[info.Publisher] = info.FrozenAt

		info.LastAdvertisement = info.FrozenAt
		info.LastAdvertisementTime = info.FrozenAtTime
		info.FrozenAt = cid.Undef
		info.FrozenAtTime = time.Time{}

		value, err := json.Marshal(info)
		if err != nil {
			return nil, err
		}

		if err = dstore.Put(ctx, info.dsKey(), value); err != nil {
			return nil, err
		}
	}
	if len(unfrozen) == 0 {
		return nil, nil
	}
	if err = dstore.Sync(ctx, datastore.NewKey(providerKeyPath)); err != nil {
		return nil, err
	}
	return unfrozen, nil
}

func (r *Registry) syncFreeze(now time.Time) error {
	ctx := context.Background()
	for id, info := range r.providers {
		frozenInfo := *info
		frozenInfo.FrozenAt = info.LastAdvertisement
		if info.LastAdvertisementTime.IsZero() {
			frozenInfo.FrozenAtTime = now
		} else {
			frozenInfo.FrozenAtTime = info.LastAdvertisementTime
		}
		r.providers[id] = &frozenInfo

		if r.dstore == nil {
			continue
		}

		value, err := json.Marshal(&frozenInfo)
		if err != nil {
			return err
		}

		dsKey := info.dsKey()
		if err = r.dstore.Put(ctx, dsKey, value); err != nil {
			return err
		}
	}
	if r.dstore != nil {
		return r.dstore.Sync(ctx, datastore.NewKey(providerKeyPath))
	}
	return nil
}

func (r *Registry) syncStartDiscover(peerID peer.ID, spID string, errCh chan<- error) {
	err := r.syncNeedDiscover(spID)
	if err != nil {
		errCh <- err
		return
	}

	// Mark discovery as in progress.
	if r.discoverTimes == nil {
		r.discoverTimes = make(map[string]time.Time)
	}
	r.discoverTimes[spID] = time.Time{}
	r.discoverWait.Add(1)

	// Do discovery asynchronously; do not block other discovery requests.
	go func() {
		ctx := context.Background()
		if r.discoveryTimeout != 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, r.discoveryTimeout)
			defer cancel()
		}

		discoverData, discoverErr := r.discover(ctx, peerID, spID)
		r.actions <- func() {
			defer close(errCh)
			defer r.discoverWait.Done()

			// Update discovery completion time.
			r.discoverTimes[spID] = time.Now()
			if discoverErr != nil {
				errCh <- discoverErr
				return
			}

			info := &ProviderInfo{
				AddrInfo:      discoverData.AddrInfo,
				DiscoveryAddr: spID,
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

func (r *Registry) syncNeedDiscover(spID string) error {
	completed, ok := r.discoverTimes[spID]
	if ok {
		// Check if discovery already in progress.
		if completed.IsZero() {
			return ErrInProgress
		}

		// Check if last discovery completed too recently.
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
	return r.dstore.Sync(ctx, dsKey)
}

func (r *Registry) discover(ctx context.Context, peerID peer.ID, spID string) (*discovery.Discovered, error) {
	if r.discoverer == nil {
		return nil, ErrNoDiscovery
	}

	discoverData, err := r.discoverer.Discover(ctx, peerID, spID)
	if err != nil {
		return nil, fmt.Errorf("cannot discover provider: %s", err)
	}

	return discoverData, nil
}

func (r *Registry) cleanup() {
	r.discoverWait.Add(1)
	r.sequences.retire()
	r.actions <- func() {
		if len(r.discoverTimes) == 0 {
			return
		}
		now := time.Now()
		for spID, completed := range r.discoverTimes {
			if completed.IsZero() {
				continue
			}
			if r.rediscoverWait != 0 && now.Sub(completed) < r.rediscoverWait {
				continue
			}
			delete(r.discoverTimes, spID)
		}
		if len(r.discoverTimes) == 0 {
			r.discoverTimes = nil // remove empty map
		}
	}
	r.discoverWait.Done()
}

func (r *Registry) pollProviders(poll polling, pollOverrides map[peer.ID]polling) {
	r.actions <- func() {
		now := time.Now()
		for peerID, info := range r.providers {
			// If the provider is not allowed, then do not poll or de-list.
			if !r.policy.Allowed(peerID) {
				continue
			}
			if info.Publisher.Validate() != nil || !r.policy.Allowed(info.Publisher) {
				// No publisher.
				continue
			}
			// If using assigner service, and the provider's publisher is not
			// assigned, then do not poll.
			if r.assigned != nil {
				r.assignMutex.Lock()
				_, ok := r.assigned[info.Publisher]
				r.assignMutex.Unlock()
				if !ok {
					continue
				}
			}
			override, ok := pollOverrides[peerID]
			if ok {
				poll = override
			}
			if info.lastContactTime.IsZero() {
				// There has been no contact since startup.  Poll during next
				// call to this function if no update for provider.
				info.lastContactTime = now.Add(-poll.interval)
				continue
			}
			noContactTime := now.Sub(info.lastContactTime)
			if noContactTime < poll.interval {
				// Had recent enough contact, no need to poll.
				continue
			}
			sincePollingStarted := noContactTime - poll.interval
			// If more than stopAfter time has elapsed since polling started,
			// then the publisher is considered permanently unresponsive, so
			// remove it.
			if sincePollingStarted >= poll.stopAfter {
				// Too much time since last contact.
				log.Warnw("Lost contact with provider, too long with no updates",
					"publisher", info.Publisher,
					"provider", info.AddrInfo.ID,
					"since", info.lastContactTime)
				// Remove the dead provider from the registry.
				if err := r.syncRemoveProvider(context.Background(), peerID); err != nil {
					log.Errorw("Failed to update deleted provider info", "err", err)
				}
				// Tell the ingester to remove data for the provider.
				info.deleted = true
			} else if sincePollingStarted >= poll.deactivateAfter {
				// Still polling after deactivateAfter, so mark inactive.
				// This will exclude the provider from find responses.
				info.inactive = true
			}
			select {
			case r.syncChan <- info:
			default:
				log.Debugw("Sync channel blocked, skipping auto-sync", "publisher", info.Publisher)
			}
		}
	}
}

func (r *Registry) syncRemoveProvider(ctx context.Context, providerID peer.ID) error {
	// Remove the provider from the registry.
	delete(r.providers, providerID)

	if r.dstore == nil {
		return nil
	}

	dsKey := peerIDToDsKey(providerKeyPath, providerID)
	err := r.dstore.Delete(ctx, dsKey)
	if err != nil {
		return err
	}
	if err = r.dstore.Sync(ctx, dsKey); err != nil {
		return err
	}
	return nil
}

func (r *Registry) saveAssignedPeer(peerID, frozenID peer.ID) error {
	if r.dstore == nil {
		return nil
	}
	ctx := context.Background()
	dsKey := peerIDToDsKey(assignmentsKeyPath, peerID)
	var valData []byte
	var err error
	if frozenID.Validate() == nil {
		valData, err = json.Marshal(frozenID)
		if err != nil {
			return err
		}
	} else {
		valData = []byte{}
	}

	err = r.dstore.Put(ctx, dsKey, valData)
	if err != nil {
		return err
	}
	return r.dstore.Sync(ctx, dsKey)
}

func (r *Registry) deleteAssignedPeer(peerID peer.ID) error {
	if r.dstore == nil {
		return nil
	}
	dsKey := peerIDToDsKey(assignmentsKeyPath, peerID)
	return r.dstore.Delete(context.Background(), dsKey)
}

func migrateOldAssignments(ctx context.Context, dstore datastore.Datastore, prefix string, deleteOld bool) error {
	q := query.Query{
		Prefix:   prefix,
		KeysOnly: true,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return err
	}

	ents, err := results.Rest()
	if err != nil {
		return err
	}

	for i := range ents {
		key := ents[i].Key
		if !deleteOld {
			peerID, err := peer.Decode(path.Base(key))
			if err != nil {
				log.Errorw("cannot decode assigned peer ID, removing")
			} else {
				dsKey := peerIDToDsKey(assignmentsKeyPath, peerID)
				log.Debugw("Renamed assignment", "from", key, "to", dsKey)
				err := dstore.Put(ctx, dsKey, []byte{})
				if err != nil {
					return err
				}
			}
		}
		err = dstore.Delete(ctx, datastore.NewKey(key))
		if err != nil {
			return err
		}
	}

	return nil
}

func loadPersistedProviders(ctx context.Context, dstore datastore.Datastore, filterIPs bool) (map[peer.ID]*ProviderInfo, error) {
	providers := make(map[peer.ID]*ProviderInfo)

	if dstore == nil {
		return providers, nil
	}

	// Load all providers from the datastore.
	q := query.Query{
		Prefix: providerKeyPath,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	for result := range results.Next() {
		if result.Error != nil {
			return nil, fmt.Errorf("cannot read provider data: %v", result.Error)
		}
		ent := result.Entry

		peerID, err := peer.Decode(path.Base(ent.Key))
		if err != nil {
			return nil, fmt.Errorf("cannot decode provider ID: %s", err)
		}

		pinfo := new(ProviderInfo)
		err = json.Unmarshal(ent.Value, pinfo)
		if err != nil {
			log.Errorw("Cannot load provider info", "err", err, "provider", peerID)
			pinfo.AddrInfo.ID = peerID
			// Add the provider to the set of registered providers so that it
			// does not get delisted. The next update should fix the addresses.
		}

		if filterIPs {
			pinfo.AddrInfo.Addrs = mautil.FilterPrivateIPs(pinfo.AddrInfo.Addrs)
			if pinfo.Publisher.Validate() == nil && pinfo.PublisherAddr != nil {
				pubAddrs := mautil.FilterPrivateIPs([]multiaddr.Multiaddr{pinfo.PublisherAddr})
				if len(pubAddrs) == 0 {
					pinfo.PublisherAddr = nil
				} else {
					pinfo.PublisherAddr = pubAddrs[0]
				}
			}
		}

		if pinfo.Publisher.Validate() == nil && pinfo.PublisherAddr == nil && pinfo.Publisher == pinfo.AddrInfo.ID &&
			len(pinfo.AddrInfo.Addrs) != 0 {
			pinfo.PublisherAddr = pinfo.AddrInfo.Addrs[0]
		}

		providers[peerID] = pinfo
	}

	return providers, nil
}

func loadPersistedAssignments(ctx context.Context, dstore datastore.Datastore, deleteOld bool) (map[peer.ID]peer.ID, error) {
	assigned := make(map[peer.ID]peer.ID)

	if dstore == nil {
		return assigned, nil
	}

	err := migrateOldAssignments(ctx, dstore, oldAssignmentsKeyPath, deleteOld)
	if err != nil {
		return nil, err
	}

	// Load all assigned publishers from datastore.
	q := query.Query{
		Prefix:   assignmentsKeyPath,
		KeysOnly: true,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	for result := range results.Next() {
		if result.Error != nil {
			return nil, fmt.Errorf("cannot read assignment data: %w", result.Error)
		}
		ent := result.Entry

		peerID, err := peer.Decode(path.Base(ent.Key))
		if err != nil {
			return nil, fmt.Errorf("cannot decode assigned peer ID: %w", err)
		}

		var fromID peer.ID
		if len(ent.Value) != 0 {
			err = json.Unmarshal(ent.Value, &fromID)
			if err != nil {
				log.Errorw("Cannot load assignment info", "err", err, "publisher", peerID)
			}
		}
		assigned[peerID] = fromID
	}

	return assigned, nil
}

func loadPreferredAssignments(providers map[peer.ID]*ProviderInfo, assigned map[peer.ID]peer.ID) map[peer.ID]struct{} {
	var preferred map[peer.ID]struct{}
	for _, pinfo := range providers {
		if pinfo.Publisher.Validate() != nil {
			continue
		}
		if _, ok := assigned[pinfo.Publisher]; ok {
			continue
		}
		if pinfo.LastAdvertisement == cid.Undef {
			continue
		}
		if preferred == nil {
			preferred = make(map[peer.ID]struct{})
		}
		preferred[pinfo.Publisher] = struct{}{}
	}
	return preferred
}
