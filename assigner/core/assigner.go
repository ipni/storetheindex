package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-libipni/announce"
	ingestclient "github.com/ipni/go-libipni/ingest/client"
	adminclient "github.com/ipni/storetheindex/admin/client"
	"github.com/ipni/storetheindex/assigner/config"
	"github.com/ipni/storetheindex/peerutil"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("assigner/core")

const pollFrozenTimeout = 2 * time.Minute

// Assigner is responsible for assigning publishers to indexers.
type Assigner struct {
	// assigned maps a publisher to a set of indexers.
	assigned map[peer.ID]*assignment
	// indexerPool is the set of indexers to assign publishers to.
	indexerPool []indexerInfo
	// initDone is true when assignments have been read from all indexers.
	initDone bool
	// mutex protects assigned.
	mutex   sync.Mutex
	p2pHost host.Host
	// policy decides what publisher to accept announce messages from.
	policy peerutil.Policy
	// pollCancel cancels the poll goroutine and any polling in progress.
	pollCancel context.CancelFunc
	// pollDone signals that the poll goroutine has exited.
	pollDone chan struct{}
	// pollNow signals the poll goroutine to poll immediately.
	pollNow chan struct{}
	// presets maps publisher ID to pre-assigned indexers.
	presets map[peer.ID][]int
	// presetRepl is number of the preset indexers to assign a publisher to.
	presetRepl int
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
	indexers  []int
	preferred []int
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

// removeIndexer removes an indexer, identified by its number in the pool, from
// this assignment.
func (asmt *assignment) removeIndexer(x int) bool {
	i := sort.SearchInts(asmt.indexers, x)
	if i < len(asmt.indexers) && asmt.indexers[i] == x {
		asmt.indexers = append(asmt.indexers[:i], asmt.indexers[i+1:]...)
		return true
	}
	return false
}

// hasIndexer returns true if indexer is in this assignment.
func (asmt *assignment) hasIndexer(x int) bool {
	i := sort.SearchInts(asmt.indexers, x)
	return i < len(asmt.indexers) && asmt.indexers[i] == x
}

// indexerInfo describes an indexer in the indexer pool.
type indexerInfo struct {
	adminURL  string
	findURL   string
	ingestURL string

	assigned    int32
	frozen      bool
	id          peer.ID
	initDone    bool
	needHandoff map[peer.ID]struct{}
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
// assigns them to the indexers configured in the indexer pool.
func NewAssigner(ctx context.Context, cfg config.Assignment, p2pHost host.Host) (*Assigner, error) {
	if cfg.Replication < 0 {
		return nil, errors.New("bad replication value, must be 0 or positive")
	}

	indexerPool, presets, err := indexersFromConfig(cfg.IndexerPool)
	if err != nil {
		return nil, err
	}

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

	presetRepl := cfg.PresetReplication
	if presetRepl <= 0 {
		presetRepl = 1
	}

	replication := cfg.Replication
	if replication <= 0 {
		replication = 1
	}
	if replication > len(indexerPool) {
		replication = len(indexerPool)
	}

	a := &Assigner{
		assigned:    make(map[peer.ID]*assignment),
		indexerPool: indexerPool,
		p2pHost:     p2pHost,
		policy:      policy,
		presets:     presets,
		presetRepl:  presetRepl,
		receiver:    rcvr,
		replication: replication,
		watchDone:   make(chan struct{}),
	}

	// Get the publishers currently assigned to each indexer in the pool. If
	// assignments cannot be read from all, then retry later when an unassigned
	// publisher is seen. Reduce the needed assignments for the publisher by
	// the number of offline indexers to prevent over-assigning indexers in the
	// pool.
	downCount := a.initAssignments(ctx)
	if downCount != 0 {
		log.Warnw("Could not get existing assignments for all indexers in pool, will retry later")
	} else {
		log.Infow("Read assignments from all indexers")
	}

	if len(indexerPool) != 0 {
		a.pollDone = make(chan struct{})
		a.pollNow = make(chan struct{})
		pollCtx, pollCancel := context.WithCancel(context.Background())
		a.pollCancel = pollCancel
		go a.poll(pollCtx, time.Duration(cfg.PollInterval))
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

func (a *Assigner) InitDone() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.initDone
}

func (a *Assigner) PollNow() {
	if a.pollNow != nil {
		a.pollNow <- struct{}{}
	}
}

func (a *Assigner) initAssignments(ctx context.Context) int {
	// If a publisher is pre-assigned to specific indexers, then ignore
	// any indexer that is not one of those pre-assigned.
	wrongPreset := func(pubID peer.ID, indexerNum int) bool {
		preset, usesPreset := a.presets[pubID]
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

	var frozenIndexers []int
	indexerAssigned := make(map[int]map[peer.ID]peer.ID)

	var needInit int
	for i := range a.indexerPool {
		if a.indexerPool[i].initDone {
			continue
		}

		id, frozen, assigned, prefPubs, err := a.getAssignments(ctx, i)
		if err != nil {
			needInit++
			log.Errorw("Could not get assignments from indexer", "err", err, "indexer", i)
			continue
		}
		a.indexerPool[i].id = id

		// Add this indexer to each publisher's assignments.
		for pubID := range assigned {
			if wrongPreset(pubID, i) {
				continue
			}
			asmt, found := a.assigned[pubID]
			if !found {
				asmt = &assignment{
					indexers: []int{},
				}
				a.assigned[pubID] = asmt
			}
			asmt.addIndexer(i)
			a.indexerPool[i].assigned++
		}
		indexerAssigned[i] = assigned
		log.Infof("Indexer %d has %d assignments", i, a.indexerPool[i].assigned)

		if frozen {
			needHandoff := make(map[peer.ID]struct{})
			for pubID := range assigned {
				needHandoff[pubID] = struct{}{}
			}
			a.indexerPool[i].needHandoff = needHandoff
			frozenIndexers = append(frozenIndexers, i)
		}

		// Add this indexer to each publisher's preferred assignments.
		for _, pubID := range prefPubs {
			if wrongPreset(pubID, i) {
				continue
			}
			asmt, found := a.assigned[pubID]
			if !found {
				asmt = &assignment{
					indexers: []int{},
				}
				a.assigned[pubID] = asmt
			} else if asmt.hasIndexer(i) {
				log.Errorw("Publisher assigned to indexer cannot be listed as preferred", "indexer", i, "publisher", pubID)
				continue
			}
			asmt.preferred = append(asmt.preferred, i)
		}

		a.indexerPool[i].initDone = true
		log.Infow("Initialized indexer", "number", i, "id", id)
	}

	// Check that handoff is complete for each frozen indexer.
	for _, frozenIndexer := range frozenIndexers {
		needHandoff := a.indexerPool[frozenIndexer].needHandoff
		frozenID := a.indexerPool[frozenIndexer].id

		// Check if each publisher from the frozen indexer has been handed off
		// to another indexer.
		for handoffPub := range needHandoff {
			// Look at each indexer's assignments.
			for indexer, assigned := range indexerAssigned {
				if indexer == frozenIndexer {
					continue // skip self
				}
				// If an indexer has this publisher assigned to it, and it was
				// a handoff from the frozen indexer, then handoff of this
				// publisher is done.
				fromID, ok := assigned[handoffPub]
				if ok && fromID == frozenID {
					delete(needHandoff, handoffPub)
					// Remove frozen indexer from assignments for this
					// publisher. An assignment will only have frozen indexer
					// is handoff was incomplete for that frozen indexer.
					asmt, found := a.assigned[handoffPub]
					if found {
						asmt.removeIndexer(indexer)
						a.indexerPool[indexer].assigned--
					}
					break
				}
			}
		}
		if len(needHandoff) == 0 {
			a.indexerPool[frozenIndexer].needHandoff = nil
			a.indexerPool[frozenIndexer].frozen = true
		} else {
			log.Infow("Frozen indexer has incomplete handoff", "indexer", frozenIndexer)
		}
	}

	if needInit == 0 {
		a.initDone = true
	}

	return needInit
}

func (a *Assigner) handoffPublisher(ctx context.Context, publisher peer.ID, fromIndexer, toIndexer int) error {
	toURL := a.indexerPool[toIndexer].adminURL
	cl, err := adminclient.New(toURL)
	if err != nil {
		return err
	}

	fromID := a.indexerPool[fromIndexer].id
	fromURL := a.indexerPool[fromIndexer].findURL
	return cl.Handoff(ctx, publisher, fromID, fromURL)
}

// indexersFromConfig reads the indexer pool config to create the indexer pool.
func indexersFromConfig(cfgIndexerPool []config.Indexer) ([]indexerInfo, map[peer.ID][]int, error) {
	seen := make(map[string]struct{}, len(cfgIndexerPool))
	indexers := make([]indexerInfo, 0, len(cfgIndexerPool))
	var presets map[peer.ID][]int

	for i := range cfgIndexerPool {
		var iInfo indexerInfo
		var err error

		iInfo.adminURL, err = configURL(cfgIndexerPool[i].AdminURL, "admin", i, seen)
		if err != nil {
			return nil, nil, err
		}
		iInfo.findURL, err = configURL(cfgIndexerPool[i].FindURL, "find", i, seen)
		if err != nil {
			return nil, nil, err
		}
		iInfo.ingestURL, err = configURL(cfgIndexerPool[i].IngestURL, "ingest", i, seen)
		if err != nil {
			return nil, nil, err
		}

		indexers = append(indexers, iInfo)

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

func configURL(urlStr, name string, indexerNum int, seen map[string]struct{}) (string, error) {
	if !strings.HasPrefix(urlStr, "http://") && !strings.HasPrefix(urlStr, "https://") {
		urlStr = "http://" + urlStr
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("indexer %d has bad %s url: %s: %w", indexerNum, name, urlStr, err)
	}
	urlStr = u.String()
	if _, found := seen[urlStr]; found {
		return "", fmt.Errorf("indexer %d has non-unique %s url %s", indexerNum, name, urlStr)
	}
	seen[urlStr] = struct{}{}
	return urlStr, nil
}

// Allowed determines whether the assigner is accepting announce messages from
// the specified publisher.
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

	if a.pollDone != nil {
		a.mutex.Lock()
		if a.pollCancel != nil {
			a.pollCancel()
			a.pollCancel = nil
		}
		a.mutex.Unlock()
		<-a.pollDone
	}

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
//
// The notification channel is closed when assignment is complete. A new
// channel will need to be created to receive notification of future
// assignments/handoff.
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
		noticeChan = make(chan int, len(a.indexerPool))
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
	// Will never block because channel size is same as pool size.
	noticeChan <- indexerNum
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

func (a *Assigner) poll(ctx context.Context, interval time.Duration) {
	defer close(a.pollDone)

	var timerCh <-chan time.Time
	var timer *time.Timer

	if interval != 0 {
		timer = time.NewTimer(interval)
		defer timer.Stop()
		timerCh = timer.C
	}

	for {
		select {
		case <-timerCh:
			a.pollFrozen(ctx)
			timer.Reset(interval)
		case <-ctx.Done():
			return
		case <-a.pollNow:
			a.pollFrozen(ctx)
		}
	}
}

// watch fetches announce messages from the Receiver.
func (a *Assigner) watch() {
	defer close(a.watchDone)
	var pending sync.WaitGroup

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

		// If no indexers to assign to, ignore announcement.
		if len(a.indexerPool) == 0 {
			continue
		}

		a.mutex.Lock()

		asmt, need := a.checkAssignment(amsg.PeerID)
		if need != 0 {
			a.makeAssignments(ctx, amsg, asmt, need)
		}

		a.mutex.Unlock()
	}
	// Wait for pending assignments in to complete.
	pending.Wait()
}

// checkAssignment checks if a publisher is assigned to sufficient indexers.
func (a *Assigner) checkAssignment(pubID peer.ID) (*assignment, int) {
	required := a.replication
	if _, usesPreset := a.presets[pubID]; usesPreset {
		required = a.presetRepl
	}

	asmt, found := a.assigned[pubID]
	if found {
		if len(asmt.indexers) >= required {
			log.Debug("Publisher already assigned to all required indexers")
			return nil, 0
		}

		need := required - len(asmt.indexers)
		if !a.initDone {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			downCount := a.initAssignments(ctx)
			cancel()
			// If the number of unavailable indexers is as many or more than
			// needed, then do not do assignment since peer could already be
			// assigned to offline indexers.
			need -= downCount
			if need <= 0 {
				return nil, 0
			}
		}

		return asmt, need
	}

	if !a.initDone {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		downCount := a.initAssignments(ctx)
		cancel()
		// If the number of unavailable indexers is as many or more than
		// needed, then do not do assignment since peer could already be
		// assigned to offline indexers.
		required -= downCount
		if required <= 0 {
			return nil, 0
		}
	}

	// Publisher not yet assigned to an indexer, so make assignment.
	asmt = &assignment{
		indexers: []int{},
	}
	a.assigned[pubID] = asmt

	return asmt, required
}

func (a *Assigner) makeAssignments(ctx context.Context, amsg announce.Announce, asmt *assignment, need int) {
	log := log.With("publisher", amsg.PeerID)

	var candidates []int
	var required int

	preset, usesPresets := a.presets[amsg.PeerID]
	if usesPresets {
		candidates = make([]int, 0, len(preset)-len(asmt.indexers))
		for _, indexerNum := range preset {
			if !a.indexerPool[indexerNum].frozen && !asmt.hasIndexer(indexerNum) {
				candidates = append(candidates, indexerNum)
			}
		}
		required = a.presetRepl
	} else {
		candidates = make([]int, 0, len(a.indexerPool)-len(asmt.indexers))
		for i := range a.indexerPool {
			if !a.indexerPool[i].frozen && !asmt.hasIndexer(i) {
				candidates = append(candidates, i)
			}
		}
		required = a.replication
	}

	// There are no remaining indexers to assign publisher to.
	if len(candidates) == 0 {
		log.Warnw("Insufficient indexers to assign publisher to", "indexersAssigned", len(asmt.indexers), "required", required)
		return
	}

	a.orderCandidates(candidates, asmt.preferred)

	for _, indexerNum := range candidates {
		err := a.assignIndexer(ctx, indexerNum, amsg)
		if err != nil {
			log.Errorw("Could not assign publisher to indexer", "err", err,
				"indexer", indexerNum, "adminURL", a.indexerPool[indexerNum].adminURL)
			continue
		}
		asmt.addIndexer(indexerNum)
		a.indexerPool[indexerNum].addAssignedCount(1)
		a.notifyAssignment(amsg.PeerID, indexerNum)
		need--
		if need == 0 {
			// Close the notification channel to signal to reader that
			// assignment is complete. A new channel will need to be created to
			// receive notification of future assignments/handoff.
			a.closeNotifyAssignment(amsg.PeerID)
			log.Infow("Publisher assigned to required number of indexers", "required", required)
			return
		}
	}
	log.Warnf("Publisher assigned to %d out of %d required indexers", len(asmt.indexers), required)
}

func (a *Assigner) pollFrozen(ctx context.Context) {
	if len(a.indexerPool) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, pollFrozenTimeout)
	defer cancel()

	newFrozen := make(chan int, len(a.indexerPool))
	var reqCount int

	for i := range a.indexerPool {
		if !a.indexerPool[i].initDone {
			continue // ignore offline
		}
		if a.indexerPool[i].frozen {
			continue // ignore already frozen
		}

		reqCount++

		// If incomplete handoff, indexer must be frozen so report it as frozen
		// without actually requesting status.
		if len(a.indexerPool[i].needHandoff) != 0 {
			newFrozen <- i
			continue
		}

		// Send status requests concurrently.
		go func(indexerNum int) {
			frozen, err := a.checkFrozen(ctx, indexerNum)
			if err != nil {
				log.Errorw("Cannot get indexer status", "err", err, "indexer", indexerNum)
				newFrozen <- -1
				return
			}
			if !frozen {
				newFrozen <- -1
				return
			}
			// Indexer has become frozen since last check.
			newFrozen <- indexerNum
		}(i)
	}

	for ; reqCount > 0; reqCount-- {
		i := <-newFrozen
		if i != -1 {
			if err := a.handoffFrozen(ctx, i); err != nil {
				log.Errorw("Handoff incomplete", "err", err, "frozenIndexer", i)
			}
		}
	}
}

func (a *Assigner) checkFrozen(ctx context.Context, indexerNum int) (bool, error) {
	adminURL := a.indexerPool[indexerNum].adminURL
	cl, err := adminclient.New(adminURL)
	if err != nil {
		return false, fmt.Errorf("cannot create admin client: %w", err)
	}

	status, err := cl.Status(ctx)
	if err != nil {
		return false, fmt.Errorf("error requesting status: %w", err)
	}

	return status.Frozen, nil
}

// handoffFrozen does a handoff of all the publishers assigned to an indexer.
func (a *Assigner) handoffFrozen(ctx context.Context, indexerNum int) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Get assignments from previous incomplete handoff, or if none, from frozen indexer.
	needHandoff := a.indexerPool[indexerNum].needHandoff
	a.indexerPool[indexerNum].needHandoff = nil

	candidates := make([]int, 0, len(a.indexerPool))

	log := log.With("frozenIndexer", indexerNum)

	a.mutex.Lock()
	defer a.mutex.Unlock()

	if needHandoff == nil {
		needHandoff = make(map[peer.ID]struct{})
		// Build list of publishers that are assigned to the frozen indexer.
		for pubID, asmt := range a.assigned {
			if asmt.hasIndexer(indexerNum) {
				needHandoff[pubID] = struct{}{}
			}
		}
	}

	// Try to assign each publisher to a non-frozen indexer.
	for pubID := range needHandoff {
		asmt, found := a.assigned[pubID]
		if !found {
			// At a minimum, the publisher should be assigned to the frozen indexer.
			log.Warnw("Frozen indexer has publisher that is unknown to assigner", "publisher", pubID)
			asmt = &assignment{
				indexers: []int{indexerNum},
			}
			a.assigned[pubID] = asmt
		}

		candidates = candidates[:0]

		preset, usesPresets := a.presets[pubID]
		if usesPresets {
			// Find an indexer, that has this publisher as a preset, that the
			// publisher is not already assigned to.
			for _, i := range preset {
				if i != indexerNum && !a.indexerPool[i].frozen && !asmt.hasIndexer(i) {
					candidates = append(candidates, i)
				}
			}
		} else {
			// Find an indexer that the publisher is not already assigned to.
			for i := range a.indexerPool {
				if i != indexerNum && !a.indexerPool[i].frozen && !asmt.hasIndexer(i) {
					candidates = append(candidates, i)
				}
			}
		}
		// There are no remaining indexers to handoff publisher to.
		if len(candidates) == 0 {
			a.indexerPool[indexerNum].needHandoff = needHandoff
			return errors.New("insufficient indexers to handoff publishers")
		}

		a.orderCandidates(candidates, nil)

		handoffTo := -1
		for _, candNum := range candidates {
			err := a.handoffPublisher(ctx, pubID, indexerNum, candNum)
			if err != nil {
				log.Errorw("Could not handoff publisher to indexer", "err", err, "targetIndexer", candNum)
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					a.indexerPool[indexerNum].needHandoff = needHandoff
					return err
				}
				continue // try another candidate
			}
			handoffTo = candNum
			break
		}

		// Handoff incomplete, save the publishers that need handoff for later.
		if handoffTo == -1 {
			a.indexerPool[indexerNum].needHandoff = needHandoff
			return errors.New("could not handoff publisher to any indexer")
		}

		delete(needHandoff, pubID)

		asmt.removeIndexer(indexerNum)
		asmt.addIndexer(handoffTo)
		a.notifyAssignment(pubID, handoffTo)

		log.Infow("Publisher handoff done", "publisher", pubID, "targetIndexer", handoffTo)
	}

	// Now that handoff is complete, mark indexer as frozen.
	a.indexerPool[indexerNum].frozen = true

	log.Info("Handoff complete for frozen indexer")
	return nil
}

type indexerSlice struct {
	indexers []int
	counts   map[int]int
	prefs    map[int]bool
}

// Len is part of sort.Interface.
func (x indexerSlice) Len() int { return len(x.indexers) }

// Less is part of sort.Interface.
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

// Swap is part of sort.Interface.
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

func (a *Assigner) assignIndexer(ctx context.Context, indexerNum int, amsg announce.Announce) error {
	indexer := a.indexerPool[indexerNum]

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

func (a *Assigner) getAssignments(ctx context.Context, indexerNum int) (peer.ID, bool, map[peer.ID]peer.ID, []peer.ID, error) {
	adminURL := a.indexerPool[indexerNum].adminURL

	cl, err := adminclient.New(adminURL)
	if err != nil {
		return "", false, nil, nil, fmt.Errorf("cannot create admin client: %w", err)
	}

	assigned, err := cl.ListAssignedPeers(ctx)
	if err != nil {
		return "", false, nil, nil, fmt.Errorf("cannot get assignments: %w", err)
	}

	status, err := cl.Status(ctx)
	if err != nil {
		return "", false, nil, nil, fmt.Errorf("cannot get indexer status: %w", err)
	}
	if status.Frozen {
		return status.ID, true, assigned, nil, nil
	}

	preferred, err := cl.ListPreferredPeers(ctx)
	if err != nil {
		log.Errorw("Cannot get preferred assignments from indexer", "err", err, "indexer", indexerNum)
	}
	return status.ID, false, assigned, preferred, nil
}
