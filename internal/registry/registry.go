package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/apierror"
	findclient "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/mautil"
	"github.com/ipni/storetheindex/config"
	"github.com/ipni/storetheindex/fsutil/disk"
	"github.com/ipni/storetheindex/internal/freeze"
	"github.com/ipni/storetheindex/internal/metrics"
	"github.com/ipni/storetheindex/internal/registry/policy"
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
	closeOnce sync.Once
	closing   chan struct{}
	dstore    datastore.Datastore
	filterIPs bool
	freezer   *freeze.Freezer
	maxPoll   int
	provMutex sync.Mutex
	pollDone  chan struct{}
	providers map[peer.ID]*ProviderInfo
	sequences *sequences

	policy *policy.Policy

	// assigned tracks peers assigned by assigner service.
	assigned map[peer.ID]peer.ID
	// assignMutex protects assigner.
	assignMutex sync.Mutex
	// preferred tracks unassigned peers that indexer has previously received
	// index data from.
	preferred map[peer.ID]struct{}

	syncChan chan *ProviderInfo
}

// ProviderInfo is an immutable data structure that holds information about a
// provider.  A ProviderInfo instance is never modified, but rather a new one
// is created to update its contents.  This means existing references remain
// valid.
type ProviderInfo struct {
	// AddrInfo contains a peer.ID and set of Multiaddr addresses.
	AddrInfo peer.AddrInfo
	// LastAdvertisement identifies the latest advertisement the indexer has ingested.
	LastAdvertisement cid.Cid `json:",omitempty"`
	// LastAdvertisementTime is the time the latest advertisement was received.
	LastAdvertisementTime time.Time
	// Lag is how far the indexer is behing in processing the ad chain.
	Lag int `json:",omitempty"`

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

	// LastError is a description of the last ingestion error to occur for this
	// provider.
	LastError string `json:",omitempty"`
	// LastErrorTime is the time that LastError occurred.
	LastErrorTime time.Time

	// lastContactTime is the last time the publisher contacted the indexer.
	// This is not persisted, so that the time since last contact is reset when
	// the indexer is started. If not reset, then it would appear the publisher
	// was unreachable for the indexer downtime.
	lastContactTime time.Time

	// lastPoll is a sequence number recording when the provider was last polled
	lastPoll int

	// deleted is used as a signal to the ingester to delete the provider's data.
	deleted bool
	// inactive means polling the publisher with no response yet.
	inactive bool
	// stopCid is used to tell the autosync goroutine to set the latest CID
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

// StopCid returns the CID of the advertisement to stop at when handling
// handoff from another indexer that is frozen.
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
// configuration, a datastore to persist provider data. The context is only
// used for cancellation of this function.
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
		closing:   make(chan struct{}),
		filterIPs: cfg.FilterIPs,
		policy:    regPolicy,
		sequences: newSequences(0),

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
		unassigned := unassignedPublishers(r.providers, r.assigned)
		if cfg.UnassignedPublishers {
			// Leave unassigned publishers unassigned, but indicate preference
			// that they get assigned to this indexer.
			r.preferred = unassigned
		} else {
			// Immediately assign all unassigned publishers to this indexer.
			if err = r.assignUnassigned(unassigned); err != nil {
				return nil, err
			}
		}
	}

	if opts.freezeAtPercent >= 0 {
		r.freezer, err = freeze.New(opts.freezeDirs, opts.freezeAtPercent, dstore, r.freeze)
		if err != nil {
			return nil, fmt.Errorf("cannot create freezer: %s", err)
		}
	}

	if cfg.PollInterval != 0 {
		poll := polling{
			interval:        time.Duration(cfg.PollInterval),
			retryAfter:      time.Duration(cfg.PollRetryAfter),
			stopAfter:       time.Duration(cfg.PollStopAfter),
			deactivateAfter: time.Duration(cfg.DeactivateAfter),
		}
		if poll.deactivateAfter == 0 {
			poll.deactivateAfter = poll.stopAfter
		}
		if err = validatePolling(poll); err != nil {
			return nil, fmt.Errorf("invalid polling config: %s", err)
		}

		pollOverrides, err := makePollOverrideMap(poll, cfg.PollOverrides)
		if err != nil {
			return nil, err
		}
		r.pollDone = make(chan struct{})
		go r.runPollCheck(poll, pollOverrides)
	}

	return r, nil
}

func validatePolling(poll polling) error {
	if poll.interval == 0 {
		return errors.New("zero value for PollInterval")
	}
	if poll.retryAfter == 0 {
		return errors.New("zero value for PollRetryAfter")
	}
	if poll.stopAfter == 0 {
		return errors.New("zero value for PollStopAfter")
	}
	if poll.deactivateAfter == 0 {
		return errors.New("zero value for DeactivateAfter")
	}
	return nil
}

func makePollOverrideMap(poll polling, cfgPollOverrides []config.Polling) (map[peer.ID]polling, error) {
	if len(cfgPollOverrides) == 0 {
		return nil, nil
	}

	pollOverrides := make(map[peer.ID]polling, len(cfgPollOverrides))
	for _, ovCfg := range cfgPollOverrides {
		peerID, err := peer.Decode(ovCfg.ProviderID)
		if err != nil {
			return nil, fmt.Errorf("cannot decode provider ID %q in PollOverrides: %s", ovCfg.ProviderID, err)
		}
		override := polling{
			interval:        time.Duration(ovCfg.Interval),
			retryAfter:      time.Duration(ovCfg.RetryAfter),
			stopAfter:       time.Duration(ovCfg.StopAfter),
			deactivateAfter: time.Duration(ovCfg.DeactivateAfter),
		}
		if override.interval == 0 {
			override.interval = poll.interval
		}
		if override.retryAfter == 0 {
			override.retryAfter = poll.retryAfter
		}
		if override.stopAfter == 0 {
			override.stopAfter = poll.stopAfter
		}
		if override.deactivateAfter == 0 {
			override.deactivateAfter = poll.deactivateAfter
		}
		pollOverrides[peerID] = override
	}
	return pollOverrides, nil
}

// Close stops the registry and waits for polling to finish.
func (r *Registry) Close() {
	r.closeOnce.Do(func() {
		if r.freezer != nil {
			r.freezer.Close()
		}
		close(r.closing)
		if r.pollDone != nil {
			<-r.pollDone
		}
	})
}

func (r *Registry) SyncChan() <-chan *ProviderInfo {
	return r.syncChan
}

func (r *Registry) runPollCheck(poll polling, pollOverrides map[peer.ID]polling) {
	retryAfter := poll.retryAfter
	for i := range pollOverrides {
		if pollOverrides[i].retryAfter < retryAfter {
			retryAfter = pollOverrides[i].retryAfter
		}
	}

	var pollSeq int
	if retryAfter < time.Minute {
		retryAfter = time.Minute
	}
	timer := time.NewTimer(retryAfter)
running:
	for {
		select {
		case <-timer.C:
			pollSeq++
			r.pollProviders(poll, pollOverrides, pollSeq)
			timer.Reset(retryAfter)
		case <-r.closing:
			break running
		}
	}
	close(r.syncChan)
	close(r.pollDone)
	timer.Stop()
}

// Saw indicates that a provider was seen.
func (r *Registry) Saw(providerID peer.ID, clearError bool) {
	r.provMutex.Lock()
	defer r.provMutex.Unlock()

	pinfo, ok := r.providers[providerID]
	if ok {
		pinfo.lastContactTime = time.Now()
		pinfo.inactive = false
		log.Infow("Saw provider", "provider", providerID, "publisher", pinfo.Publisher, "time", pinfo.lastContactTime)
		if clearError && pinfo.LastError != "" {
			pinfoCpy := *pinfo
			pinfoCpy.LastError = ""
			pinfoCpy.LastErrorTime = time.Time{}
			r.providers[providerID] = &pinfoCpy
		}
	}
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
// Returns true if the specified peer was assigned.
func (r *Registry) UnassignPeer(peerID peer.ID) (bool, error) {
	if r.assigned == nil {
		return false, ErrNoAssigner
	}

	r.assignMutex.Lock()
	defer r.assignMutex.Unlock()

	_, ok := r.assigned[peerID]
	if !ok {
		return false, nil
	}

	if r.dstore != nil {
		dsKey := peerIDToDsKey(assignmentsKeyPath, peerID)
		err := r.dstore.Delete(context.Background(), dsKey)
		if err != nil {
			return false, fmt.Errorf("cannot save assignment: %w", err)
		}
	}

	delete(r.assigned, peerID)
	return true, nil
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

func validateExtProviderInfos(xpInfos []ExtendedProviderInfo) error {
	for i := range xpInfos {
		if len(xpInfos[i].Addrs) == 0 {
			return errors.New("missing address")
		}
	}
	return nil
}

func validateExtProviders(extendedProviders *ExtendedProviders) error {
	err := validateExtProviderInfos(extendedProviders.Providers)
	if err != nil {
		return fmt.Errorf("extended providers error: %s", err)
	}
	for _, cxp := range extendedProviders.ContextualProviders {
		if err = validateExtProviderInfos(cxp.Providers); err != nil {
			return fmt.Errorf("context extended providers error: %s", err)
		}
	}
	return nil
}

// Update attempts to update the registry's provider information. If publisher
// has a valid ID, then the supplied publisher data replaces the provider's
// previous publisher information.
func (r *Registry) Update(ctx context.Context, provider, publisher peer.AddrInfo, adCid cid.Cid, extendedProviders *ExtendedProviders, lag int) error {
	// Do not accept update if provider is not allowed.
	if !r.policy.Allowed(provider.ID) {
		return ErrNotAllowed
	}

	if r.filterIPs {
		provider.Addrs = mautil.FilterPublic(provider.Addrs)
		publisher.Addrs = mautil.FilterPublic(publisher.Addrs)
	}

	var newPublisher bool

	info, ok := r.ProviderInfo(provider.ID)
	if ok {
		info = &ProviderInfo{
			AddrInfo:              info.AddrInfo,
			LastAdvertisement:     info.LastAdvertisement,
			LastAdvertisementTime: info.LastAdvertisementTime,
			Publisher:             info.Publisher,
			PublisherAddr:         info.PublisherAddr,
			ExtendedProviders:     info.ExtendedProviders,

			FrozenAt:     info.FrozenAt,
			FrozenAtTime: info.FrozenAtTime,

			LastError:     info.LastError,
			LastErrorTime: info.LastErrorTime,

			lastPoll: info.lastPoll,
		}

		// If new addrs provided, update to use these.
		if len(provider.Addrs) != 0 {
			info.AddrInfo.Addrs = provider.Addrs
		}

		if extendedProviders != nil {
			if err := validateExtProviders(extendedProviders); err != nil {
				return err
			}
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
			return ErrMissingProviderAddr
		}
		info = &ProviderInfo{
			AddrInfo: provider,
		}
		if extendedProviders != nil {
			if err = validateExtProviders(extendedProviders); err != nil {
				return err
			}
			info.ExtendedProviders = extendedProviders
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
			return apierror.New(ErrPublisherNotAllowed, http.StatusForbidden)
		}
		if !r.policy.PublishAllowed(publisher.ID, info.AddrInfo.ID) {
			return apierror.New(ErrCannotPublish, http.StatusForbidden)
		}
		info.Publisher = publisher.ID
	}

	if info.Publisher.Validate() == nil {
		// Use new publisher addrs if any given. Otherwise, keep existing. If
		// no existing publisher addrs, and publisher ID is same as provider
		// ID, then use provider addresses if any.
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
		info.Lag = lag
	}
	info.lastContactTime = now

	return r.register(ctx, info)
}

func (r *Registry) register(ctx context.Context, info *ProviderInfo) error {
	r.provMutex.Lock()
	defer r.provMutex.Unlock()

	r.providers[info.AddrInfo.ID] = info
	err := r.syncPersistProvider(ctx, info)
	if err != nil {
		err = fmt.Errorf("could not persist provider: %s", err)
		return apierror.New(err, http.StatusInternalServerError)
	}
	return nil
}

// IsRegistered checks if the provider is in the registry
func (r *Registry) IsRegistered(providerID peer.ID) bool {
	r.provMutex.Lock()
	_, found := r.providers[providerID]
	r.provMutex.Unlock()
	return found
}

// ProviderInfo returns information for a registered provider.
func (r *Registry) ProviderInfo(providerID peer.ID) (*ProviderInfo, bool) {
	r.provMutex.Lock()
	pinfo, ok := r.providers[providerID]
	r.provMutex.Unlock()
	if !ok {
		return nil, false
	}
	return pinfo, r.policy.Allowed(providerID)
}

// AllProviderInfo returns information for all registered providers that are
// active and allowed.
func (r *Registry) AllProviderInfo() []*ProviderInfo {
	r.provMutex.Lock()
	infos := make([]*ProviderInfo, 0, len(r.providers))
	for _, info := range r.providers {
		if r.assigned != nil {
			r.assignMutex.Lock()
			_, ok := r.assigned[info.Publisher]
			r.assignMutex.Unlock()
			if !ok {
				// Skip providers whose publisher is not assigned, if using
				// assigner service.
				continue
			}
		}
		infos = append(infos, info)
	}
	r.provMutex.Unlock()

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
	cl, err := findclient.New(frozenURL.String())
	if err != nil {
		return err
	}
	provs, err := cl.ListProviders(ctx)
	if err != nil {
		return err
	}

	// Iterate through providers to find the one with the publisher being
	// handed off.
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
	if r.assigned != nil {
		return 0, errors.New("feature not available when using assigner")
	}

	cl, err := findclient.New(fromURL.String())
	if err != nil {
		return 0, err
	}

	provs, err := cl.ListProviders(ctx)
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
	var err error

	r.provMutex.Lock()
	pinfo, ok := r.providers[providerID]
	if !ok {
		r.provMutex.Unlock()
		return nil
	}
	// Remove provider from datastore and memory.
	err = r.syncRemoveProvider(ctx, providerID)
	r.provMutex.Unlock()

	if err != nil {
		return err
	}

	// Tell ingester to delete its provider data.
	pinfo.deleted = true
	select {
	case r.syncChan <- pinfo:
	case <-r.closing:
		return errors.New("shutdown")
	}
	return nil
}

func (r *Registry) SetLastError(providerID peer.ID, err error) {
	var now time.Time
	if err != nil {
		now = time.Now()
	}

	r.provMutex.Lock()
	defer r.provMutex.Unlock()

	pinfo, ok := r.providers[providerID]
	if !ok {
		return
	}

	var errMsg string
	if err != nil {
		errMsg = err.Error()
	} else if pinfo.LastError == "" {
		// Last error is also empty; nothing to update.
		return
	}
	pinfoCpy := *pinfo
	pinfoCpy.LastError = errMsg
	pinfoCpy.LastErrorTime = now
	r.providers[providerID] = &pinfoCpy
}

// SetMaxPoll sets the maximum number of providers to poll at each polling retry.
func (r *Registry) SetMaxPoll(maxPoll int) {
	r.provMutex.Lock()
	r.maxPoll = maxPoll
	r.provMutex.Unlock()
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
	if err := r.freezeProviders(); err != nil {
		return fmt.Errorf("cannot freeze providers: %w", err)
	}
	return nil
}

func (r *Registry) freezeProviders() error {
	ctx := context.Background()
	now := time.Now()

	r.provMutex.Lock()
	defer r.provMutex.Unlock()

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
func Unfreeze(ctx context.Context, freezeDirs []string, freezeAtPercent float64, dstore datastore.Datastore) (map[peer.ID]cid.Cid, error) {
	if dstore == nil {
		return nil, nil
	}

	err := freeze.Unfreeze(ctx, freezeDirs, freezeAtPercent, dstore)
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

func (r *Registry) pollProviders(normalPoll polling, pollOverrides map[peer.ID]polling, pollSeq int) {
	var needPoll []*ProviderInfo
	var maxPoll int
	now := time.Now()

	r.provMutex.Lock()
	for peerID, info := range r.providers {
		// Reset poll in case previously overridden.
		poll := normalPoll
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
			// There has been no contact since startup. Poll during next call
			// to this function if no update for provider.
			info.lastContactTime = now.Add(-poll.interval)
			continue
		}
		noContactTime := now.Sub(info.lastContactTime)
		if noContactTime < poll.interval {
			// Had recent enough contact, no need to poll.
			continue
		}
		sincePollingStarted := noContactTime - poll.interval
		// If more than stopAfter time has elapsed since polling started, then
		// the publisher is considered permanently unresponsive, so remove it.
		if sincePollingStarted >= poll.stopAfter {
			// Too much time since last contact.
			log.Warnw("Lost contact with provider, too long with no updates",
				"publisher", info.Publisher,
				"provider", info.AddrInfo.ID,
				"since", info.lastContactTime,
				"sincePollingStarted", sincePollingStarted,
				"stopAfter", poll.stopAfter)
			// Remove the dead provider from the registry.
			if err := r.syncRemoveProvider(context.Background(), peerID); err != nil {
				log.Errorw("Failed to update deleted provider info", "err", err)
			}
			// Tell the ingester to remove data for the provider.
			info.deleted = true
		} else if sincePollingStarted >= poll.deactivateAfter {
			// Still polling after deactivateAfter, so mark inactive. This will
			// exclude the provider from find responses.
			log.Infow("Deactivating provider, too long with no updates",
				"publisher", info.Publisher,
				"provider", info.AddrInfo.ID,
				"since", info.lastContactTime,
				"sincePollingStarted", sincePollingStarted,
				"deactivateAfter", poll.deactivateAfter)
			info.inactive = true
		}
		needPoll = append(needPoll, info)
	}
	maxPoll = r.maxPoll
	r.provMutex.Unlock()

	if len(needPoll) == 0 {
		return
	}

	// Sort from least to most recent.
	sort.Sort(pollSortableInfos(needPoll))
	// Do not poll more than the max, if it is set to non-zero. This selects
	// the n least recently used.
	if maxPoll != 0 && len(needPoll) > maxPoll {
		needPoll = needPoll[:maxPoll]
	}
	// Record last poll sequence for infos getting polled
	r.provMutex.Lock()
	for _, info := range needPoll {
		info.lastPoll = pollSeq
	}
	r.provMutex.Unlock()

	// If system is too slow to write all within a second, then stop.
	timer := time.NewTimer(time.Second)
	defer timer.Stop()

	for _, info := range needPoll {
		select {
		case r.syncChan <- info:
		case <-r.closing:
			return
		case <-timer.C:
			log.Warn("Timeout sending providers to auto-sync")
			return
		}
	}
}

// Make provider info sortable by poll sequence.
type pollSortableInfos []*ProviderInfo

func (p pollSortableInfos) Len() int           { return len(p) }
func (p pollSortableInfos) Less(i, j int) bool { return p[i].lastPoll < p[j].lastPoll }
func (p pollSortableInfos) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

	var fixes []*ProviderInfo

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
			fixes = append(fixes, pinfo)
		}

		if filterIPs {
			pinfo.AddrInfo.Addrs = mautil.FilterPublic(pinfo.AddrInfo.Addrs)
			if pinfo.Publisher.Validate() == nil && pinfo.PublisherAddr != nil {
				pubAddrs := mautil.FilterPublic([]multiaddr.Multiaddr{pinfo.PublisherAddr})
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

	if len(fixes) != 0 {
		for _, pinfo := range fixes {
			value, err := json.Marshal(pinfo)
			if err != nil {
				log.Errorw("Cannot marshal provider info", "err", err, "provider", pinfo.AddrInfo.ID)
				continue
			}
			if err = dstore.Put(ctx, pinfo.dsKey(), value); err != nil {
				log.Errorw("Cannot store provider info", "err", err, "provider", pinfo.AddrInfo.ID)
				continue
			}
		}
		_ = dstore.Sync(ctx, datastore.NewKey(providerKeyPath))
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

func unassignedPublishers(providers map[peer.ID]*ProviderInfo, assigned map[peer.ID]peer.ID) map[peer.ID]struct{} {
	var unassigned map[peer.ID]struct{}
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
		if unassigned == nil {
			unassigned = make(map[peer.ID]struct{})
		}
		unassigned[pinfo.Publisher] = struct{}{}
	}
	return unassigned
}

func (r *Registry) assignUnassigned(unassigned map[peer.ID]struct{}) error {
	var noFrom peer.ID
	ctx := context.Background()
	for peerID := range unassigned {
		if r.dstore != nil {
			dsKey := peerIDToDsKey(assignmentsKeyPath, peerID)
			err := r.dstore.Put(ctx, dsKey, []byte{})
			if err != nil {
				return err
			}
		}
		r.assigned[peerID] = noFrom
	}
	if r.dstore != nil {
		return r.dstore.Sync(ctx, datastore.NewKey(assignmentsKeyPath))
	}
	return nil
}
