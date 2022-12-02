package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/filecoin-project/storetheindex/announce"
	adminclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	ingestclient "github.com/filecoin-project/storetheindex/api/v0/ingest/client/http"
	"github.com/filecoin-project/storetheindex/assigner/config"
	"github.com/filecoin-project/storetheindex/peerutil"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("assigner/core")

// Assigner is responsible for assigning publishers to indexers.
type Assigner struct {
	// assigned maps a publisher to a set of indexers.
	assigned map[peer.ID]*assignment
	// indexerPool is the set of indexers to assign publishers to.
	indexerPool []indexerInfo
	// mutex protects assigned.
	mutex   sync.Mutex
	p2pHost host.Host
	// policy decides what publisher to accept announce messages from.
	policy peerutil.Policy
	// presets maps publisher ID to pre-assigned indexers.
	presets map[peer.ID][]int
	// receiver receives announce messages.
	receiver *announce.Receiver
	// replication is the number of indexers to assign a publisher to.
	replication int
	// watchDone signals that the watch function exited.
	watchDone chan struct{}
	// waitingNotice are channels waiting for a specific peer to be assigned
	waitingNotice map[peer.ID]chan int
	noticeMutex   sync.Mutex
}

// assignment holds the indexers that a publisher is assigned to. The indexer
// values are positions in the Assigner's indexerPool.
type assignment struct {
	indexers   []int
	preferred  []int
	processing bool
}

// addIndexer adds an indexer, identified by its number in the pool, to this
// assignment.
func (asmt *assignment) addIndexer(x int) {
	i := sort.SearchInts(asmt.indexers, x)
	if i < len(asmt.indexers) && asmt.indexers[i] == x {
		return
	}
	// Insert indexer number into correct index in sorted slice.
	asmt.indexers = append(asmt.indexers, 0)
	copy(asmt.indexers[i+1:], asmt.indexers[i:])
	asmt.indexers[i] = x
}

// hasIndexer returns true if indexer is in this assignment.
func (asmt *assignment) hasIndexer(x int) bool {
	i := sort.SearchInts(asmt.indexers, x)
	return i < len(asmt.indexers) && asmt.indexers[i] == x
}

// indexerInfo describes an indexer in the indexer pool.
type indexerInfo struct {
	adminURL  string
	ingestURL string
	assigned  int32
}

// assignedCount returns the number of publishers assigned to this indexer.
func (ii *indexerInfo) assignedCount() int {
	return int(atomic.LoadInt32(&ii.assigned))
}

// addAssignedCount adds delta to the number of publishers assigned to this
// indexer, and returns the new value.
func (ii *indexerInfo) addAssignedCount(delta int) int {
	return int(atomic.AddInt32(&ii.assigned, int32(delta)))
}

// NewAssigner created a new assigner core that handles announce messages and
// assigns them to the indexers configured in the inderer pool.
func NewAssigner(ctx context.Context, cfg config.Assignment, p2pHost host.Host) (*Assigner, error) {
	if cfg.Replication < 0 {
		return nil, errors.New("bad replication value, must be 0 or positive")
	}
	if len(cfg.IndexerPool) < 1 {
		return nil, errors.New("no indexers configured to assign to")
	}

	indexerPool, presets, err := indexersFromConfig(cfg.IndexerPool)
	if err != nil {
		return nil, err
	}

	// Get the publishers currently assigned to each indexer in the pool.
	assigned := initAssignments(ctx, indexerPool, presets)

	policy, err := peerutil.NewPolicyStrings(cfg.Policy.Allow, cfg.Policy.Except)
	if err != nil {
		return nil, fmt.Errorf("bad allow policy: %s", err)
	}

	rcvr, err := announce.NewReceiver(p2pHost, cfg.PubSubTopic,
		announce.WithAllowPeer(policy.Eval),
		announce.WithFilterIPs(cfg.FilterIPs),
		announce.WithResend(true),
	)
	if err != nil {
		return nil, err
	}

	log.Infof("Assigner operating with %d indexers", len(indexerPool))

	a := &Assigner{
		assigned:    assigned,
		indexerPool: indexerPool,
		p2pHost:     p2pHost,
		policy:      policy,
		presets:     presets,
		receiver:    rcvr,
		replication: cfg.Replication,
		watchDone:   make(chan struct{}),
	}

	go a.watch()

	return a, nil
}

// IndexerAssignedCounts returns a slice of counts, one for each indexer in the
// pool. Each count is the number of publishers assigned to the indexer. The
// position of each count corresponds to the position of the indexer in the
// pool.
func (a *Assigner) IndexerAssignedCounts() []int {
	counts := make([]int, len(a.indexerPool))
	for i := range a.indexerPool {
		counts[i] = a.indexerPool[i].assignedCount()
	}
	return counts
}

func initAssignments(ctx context.Context, indexerPool []indexerInfo, presets map[peer.ID][]int) map[peer.ID]*assignment {
	assigned := make(map[peer.ID]*assignment)

	// If a publisher is pre-assigned to specific indexers, then ignore
	// any indexer that is not one of those pre-assigned.
	wrongPreset := func(pubID peer.ID, indexerNum int) bool {
		preset, usesPreset := presets[pubID]
		if usesPreset {
			for _, p := range preset {
				if p == indexerNum {
					// Indexer is a preset, keep assignment.
					return false
				}
			}
			// Publisher uses presets, but indexer is not one of them; ignore.
			return true
		}
		// Publisher does not use presets, keep assignment.
		return false
	}

	for i := range indexerPool {
		pubs, prefPubs, err := getAssignments(ctx, indexerPool[i].adminURL)
		if err != nil {
			log.Errorw("Could not get assignments from indexer", "err", err, "indexer", i, "adminURL", indexerPool[i].adminURL)
			continue
		}

		// Add this indexer to each publisher's assignments.
		for _, pubID := range pubs {
			if wrongPreset(pubID, i) {
				continue
			}
			asmt, found := assigned[pubID]
			if !found {
				asmt = &assignment{
					indexers: []int{},
				}
				assigned[pubID] = asmt
			}
			asmt.addIndexer(i)
			indexerPool[i].assigned++
		}
		log.Infof("Indexer %d has %d assignments", i, indexerPool[i].assigned)

		// Add this indexer to each publisher's preferred assignments.
		for _, pubID := range prefPubs {
			if wrongPreset(pubID, i) {
				continue
			}
			asmt, found := assigned[pubID]
			if !found {
				asmt = &assignment{
					indexers: []int{},
				}
				assigned[pubID] = asmt
			} else if asmt.hasIndexer(i) {
				log.Errorw("Publisher assigned to indexer cannot be listed as preferred", "indexer", i, "publisher", pubID)
				continue
			}
			asmt.preferred = append(asmt.preferred, i)
		}
	}
	return assigned
}

// indexersFromConfig reads the indexer pool config to create the indexer pool.
func indexersFromConfig(cfgIndexerPool []config.Indexer) ([]indexerInfo, map[peer.ID][]int, error) {
	seen := make(map[string]struct{}, len(cfgIndexerPool))
	indexers := make([]indexerInfo, 0, len(cfgIndexerPool))
	var presets map[peer.ID][]int

	for i := range cfgIndexerPool {
		var iInfo indexerInfo

		u, err := url.Parse(cfgIndexerPool[i].AdminURL)
		if err != nil {
			return nil, nil, fmt.Errorf("indexer %d has bad admin url: %s: %w", i, cfgIndexerPool[i].AdminURL, err)
		}
		iInfo.adminURL = u.String()
		if _, found := seen[iInfo.adminURL]; found {
			return nil, nil, fmt.Errorf("indexer %d has non-unique admin url %s", i, iInfo.adminURL)
		}

		u, err = url.Parse(cfgIndexerPool[i].IngestURL)
		if err != nil {
			return nil, nil, fmt.Errorf("indexer %d has bad ingest url: %s: %w", i, cfgIndexerPool[i].IngestURL, err)
		}
		iInfo.ingestURL = u.String()
		if _, found := seen[iInfo.ingestURL]; found {
			return nil, nil, fmt.Errorf("indexer %d has non-unique ingest url %s", i, iInfo.ingestURL)
		}

		indexers = append(indexers, iInfo)

		seen[iInfo.adminURL] = struct{}{}
		seen[iInfo.ingestURL] = struct{}{}

		// Add indexer to each publisher's preset list.
		preset := cfgIndexerPool[i].PresetPeers
		if len(preset) != 0 {
			if presets == nil {
				presets = make(map[peer.ID][]int)
			}
			for _, pubIDStr := range preset {
				pubID, err := peer.Decode(pubIDStr)
				if err != nil {
					return nil, nil, fmt.Errorf("indexer %d has bad preset peer id %s", i, pubIDStr)
				}
				presets[pubID] = append(presets[pubID], i)
			}
		}
	}

	return indexers, presets, nil
}

// Allowed determines whether or not the assigner is accepting announce
// messages from the specified publisher.
func (a *Assigner) Allowed(peerID peer.ID) bool {
	return a.policy.Eval(peerID)
}

// Announce sends a direct announce message to the assigner. This publisher in
// the message will be assigned to one or more indexers.
func (a *Assigner) Announce(ctx context.Context, nextCid cid.Cid, addrInfo peer.AddrInfo) error {
	return a.receiver.Direct(ctx, nextCid, addrInfo.ID, addrInfo.Addrs)
}

// Assigned returns the indexers that the given peer is assigned to.
func (a *Assigner) Assigned(peerID peer.ID) []int {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	asmt, found := a.assigned[peerID]
	if !found {
		return nil
	}
	n := len(asmt.indexers)
	if n == 0 {
		return nil
	}
	cpy := make([]int, n)
	copy(cpy, asmt.indexers)
	return cpy
}

// Presets returns preset indexer assignments for the given peer.
func (a *Assigner) Presets(peerID peer.ID) []int {
	return a.presets[peerID]
}

// Close shuts down the Subscriber.
func (a *Assigner) Close() error {
	a.noticeMutex.Lock()
	for _, ch := range a.waitingNotice {
		close(ch)
	}
	a.waitingNotice = nil
	a.noticeMutex.Unlock()

	// Close receiver and wait for watch to exit.
	err := a.receiver.Close()
	<-a.watchDone

	return err
}

// OnAssignment returns a channel that reports the number of the indexer that
// the specified peer ID was assigned to, each time that peer is assigned to an
// indexer. The channel is closed when there are no more indexers required to
// assign the peer to. Multiple calls using the same pubID return the same
// channel.
func (a *Assigner) OnAssignment(pubID peer.ID) (<-chan int, context.CancelFunc) {
	a.noticeMutex.Lock()
	defer a.noticeMutex.Unlock()

	var noticeChan chan int
	var ok bool
	if a.waitingNotice == nil {
		a.waitingNotice = make(map[peer.ID]chan int)
	} else {
		noticeChan, ok = a.waitingNotice[pubID]
	}

	if !ok {
		noticeChan = make(chan int, 1)
		a.waitingNotice[pubID] = noticeChan
	}

	return noticeChan, func() { a.closeNotifyAssignment(pubID) }
}

// notifyAssignment is called when a publisher is assigned to an indexer, to
// send a notification to any channel waiting for notification.
func (a *Assigner) notifyAssignment(pubID peer.ID, indexerNum int) {
	a.noticeMutex.Lock()
	defer a.noticeMutex.Unlock()

	noticeChan, ok := a.waitingNotice[pubID]
	if !ok {
		return
	}
	select {
	case noticeChan <- indexerNum:
	default:
		// Do not stall because there is no reader.
	}
}

func (a *Assigner) closeNotifyAssignment(pubID peer.ID) {
	a.noticeMutex.Lock()
	defer a.noticeMutex.Unlock()

	noticeChan, ok := a.waitingNotice[pubID]
	if !ok {
		return
	}

	close(noticeChan) // signal no more data on channel.

	delete(a.waitingNotice, pubID)
	if len(a.waitingNotice) == 0 {
		a.waitingNotice = nil
	}
}

// watch fetches announce messages from the Receiver.
func (a *Assigner) watch() {
	defer close(a.watchDone)

	// Cancel any pending messages if this function exits.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		amsg, err := a.receiver.Next(context.Background())
		if err != nil {
			// This is a normal result of shutting down the Receiver.
			log.Infow("Done handling announce messages", "reason", err)
			break
		}
		log.Debugw("Received announce", "publisher", amsg.PeerID)

		asmt, need := a.checkAssignment(amsg.PeerID)
		if need == 0 {
			continue
		}

		go a.makeAssignments(ctx, amsg, asmt, need)
	}
}

// checkAssignment checks if a publisher is assigned to sufficient indexers.
func (a *Assigner) checkAssignment(pubID peer.ID) (*assignment, int) {
	var required int
	preset, usesPreset := a.presets[pubID]
	if usesPreset {
		required = len(preset)
	} else {
		required = a.replication
		if required == 0 {
			required = len(a.indexerPool)
		}
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	asmt, found := a.assigned[pubID]
	if found {
		if asmt.processing {
			log.Debug("Publisher assignment already being processed")
			return nil, 0
		}

		if len(asmt.indexers) >= required {
			log.Debug("Publisher already assigned to all required indexers")
			return nil, 0
		}
		asmt.processing = true
		return asmt, required - len(asmt.indexers)
	}

	// Publisher not yet assigned to an indexer, so make assignment.
	asmt = &assignment{
		processing: true,
		indexers:   []int{},
	}
	a.assigned[pubID] = asmt

	return asmt, required
}

func (a *Assigner) makeAssignments(ctx context.Context, amsg announce.Announce, asmt *assignment, need int) {
	log := log.With("publisher", amsg.PeerID)

	defer func() {
		a.mutex.Lock()
		asmt.processing = false
		a.mutex.Unlock()
	}()

	var candidates []int
	var required int

	preset, usesPresets := a.presets[amsg.PeerID]
	if usesPresets {
		candidates = make([]int, 0, need)
		for _, indexerNum := range preset {
			if !asmt.hasIndexer(indexerNum) {
				candidates = append(candidates, indexerNum)
			}
		}
		required = len(preset)
	} else {
		candidates = make([]int, 0, len(a.indexerPool)-len(asmt.indexers))
		for i := range a.indexerPool {
			if !asmt.hasIndexer(i) {
				candidates = append(candidates, i)
			}
		}
		required = a.replication
		a.orderCandidates(candidates, asmt.preferred)
	}

	// There are no remaining indexers to assign publisher to.
	if len(candidates) == 0 {
		log.Warnw("Insufficient indexers to assign publisher to", "indexersAssigned", len(asmt.indexers), "required", required)
		return
	}

	for _, indexerNum := range candidates {
		err := assignIndexer(ctx, a.indexerPool[indexerNum], amsg)
		if err != nil {
			log.Errorw("Could not assign publisher to indexer", "indexer", indexerNum, "adminURL", a.indexerPool[indexerNum].adminURL)
			continue
		}
		asmt.addIndexer(indexerNum)
		a.indexerPool[indexerNum].addAssignedCount(1)
		a.notifyAssignment(amsg.PeerID, indexerNum)
		need--
		if need == 0 {
			a.closeNotifyAssignment(amsg.PeerID)
			log.Infow("Publisher assigned to required number of indexers", "required", required)
			return
		}
	}
	log.Warnf("Publisher assigned to %d out of %d required indexers", len(asmt.indexers), required)
}

type indexerSlice struct {
	indexers []int
	counts   map[int]int
	prefs    map[int]bool
}

func (x indexerSlice) Len() int { return len(x.indexers) }

func (x indexerSlice) Less(i, j int) bool {
	ni := x.indexers[i]
	nj := x.indexers[j]
	pi := x.prefs[ni]
	if pi == x.prefs[nj] {
		// Both preferred or both not preferred, sort by assigned count.
		return x.counts[ni] < x.counts[nj]
	}
	return pi
}

func (x indexerSlice) Swap(i, j int) { x.indexers[i], x.indexers[j] = x.indexers[j], x.indexers[i] }

func (a *Assigner) orderCandidates(indexers []int, preferred []int) {
	// Sort indexer list by preferred, then least-assigned-first.
	counts := map[int]int{}
	for _, n := range indexers {
		counts[n] = a.indexerPool[n].assignedCount()
	}
	prefs := map[int]bool{}
	for _, p := range preferred {
		prefs[p] = true
	}
	iSlice := indexerSlice{
		indexers: indexers,
		counts:   counts,
		prefs:    prefs,
	}
	sort.Sort(&iSlice)
}

func assignIndexer(ctx context.Context, indexer indexerInfo, amsg announce.Announce) error {
	cl, err := adminclient.New(indexer.adminURL)
	if err != nil {
		return err
	}
	err = cl.Assign(ctx, amsg.PeerID)
	if err != nil {
		return err
	}
	log.Infow("Assigned publisher to indexer, sending direct announce",
		"adminURL", indexer.adminURL,
		"ingestURL", indexer.ingestURL,
		"publisher", amsg.PeerID)

	// Send announce instead of sync request in case indexer is already syncing
	// due to receiving announce after immediately allowing the publisher.
	icl, err := ingestclient.New(indexer.ingestURL)
	if err != nil {
		log.Errorw("Error creating ingest client", "err", err)
		return nil
	}
	pubInfo := peer.AddrInfo{
		ID:    amsg.PeerID,
		Addrs: amsg.Addrs,
	}
	if err = icl.Announce(ctx, &pubInfo, amsg.Cid); err != nil {
		log.Errorw("Error sending announce message", "err", err)
	}
	return nil
}

func getAssignments(ctx context.Context, adminURL string) ([]peer.ID, []peer.ID, error) {
	cl, err := adminclient.New(adminURL)
	if err != nil {
		return nil, nil, err
	}
	assigned, err := cl.ListAssignedPeers(ctx)
	if err != nil {
		return nil, nil, err
	}
	preferred, err := cl.ListPreferredPeers(ctx)
	if err != nil {
		log.Errorw("Cannot get preferred assignments from indexer", "err", err, "adminURL", adminURL)
	}
	return assigned, preferred, nil
}
