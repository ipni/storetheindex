package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"

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

// assignment holds the indexers that a publisher is assigned to.
type assignment struct {
	indexers   []int
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

func (asmt *assignment) hasIndexer(x int) bool {
	i := sort.SearchInts(asmt.indexers, x)
	return i < len(asmt.indexers) && asmt.indexers[i] == x
}

// indexerInfo describes an indexer in the indexer pool.
type indexerInfo struct {
	adminURL  string
	ingestURL string
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

	assigned := make(map[peer.ID]*assignment)
	indexerPool, presets, err := indexersFromConfig(cfg.IndexerPool)
	if err != nil {
		return nil, err
	}

	// Get the publishers currently assigned to each indexer in the pool.
	for i := range indexerPool {
		pubs, err := getAssignments(ctx, indexerPool[i].adminURL)
		if err != nil {
			log.Errorw("could not get assignments from indexer", "err", err, "indexer", i, "adminURL", indexerPool[i].adminURL)
			continue
		}

		// Add this indexer to each publisher assignments.
		for _, pubID := range pubs {
			asmt, found := assigned[pubID]
			if !found {
				asmt = &assignment{
					indexers: []int{},
				}
				assigned[pubID] = asmt
			}
			asmt.addIndexer(i)
		}

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
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.noticeMutex.Lock()
	for _, ch := range a.waitingNotice {
		close(ch)
	}
	a.noticeMutex.Unlock()

	if a.receiver == nil {
		return errors.New("already closed")
	}
	// Close receiver and wait for watch to exit.
	a.receiver.Close()
	<-a.watchDone
	a.receiver = nil

	return nil
}

// OnAssignment returns a channel that reports the number of the indexer that
// the specified peer ID was assigned to, each time that peer is assigned to an
// indexer. The channel is closed when there are no more indexers required to
// assign the peer to.
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

		asmt, need, preset := a.checkAssignment(amsg.PeerID)
		if need == 0 {
			continue
		}

		if preset != nil {
			go a.makePresetAssignments(ctx, amsg, asmt, preset)
		} else {
			go a.makeAssignments(ctx, amsg, asmt, need)
		}
	}
}

// checkAssignment checks if a publisher is assigned to sufficient indexers.
func (a *Assigner) checkAssignment(pubID peer.ID) (*assignment, int, []int) {
	repl := a.replication
	if repl == 0 {
		repl = len(a.indexerPool)
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	asmt, found := a.assigned[pubID]
	if found {
		if asmt.processing {
			log.Debug("Publisher assignment already being processed")
			return nil, 0, nil
		}

		preset, usesPreset := a.presets[pubID]
		if usesPreset {
			if len(preset) == 0 {
				log.Debug("Publisher already assigned to all preset indexers, ignoring announce")
				return nil, 0, nil
			}
			asmt.processing = true
			return asmt, len(preset), preset
		}

		if len(asmt.indexers) >= repl {
			log.Debug("Publisher already assigned, ignoring announce")
			return nil, 0, nil
		}
		asmt.processing = true
		return asmt, repl - len(asmt.indexers), nil
	}

	// Publisher not yet assigned to an indexer, so make assignment.
	asmt = &assignment{
		processing: true,
		indexers:   []int{},
	}
	a.assigned[pubID] = asmt

	preset, usesPreset := a.presets[pubID]
	if usesPreset {
		return asmt, len(preset), preset
	}

	return asmt, repl, nil
}

func (a *Assigner) makePresetAssignments(ctx context.Context, amsg announce.Announce, asmt *assignment, preset []int) {
	var unassigned []int
	for _, indexerNum := range preset {
		err := assignIndexer(ctx, a.indexerPool[indexerNum], amsg)
		if err != nil {
			unassigned = append(unassigned, indexerNum)
			log.Errorw("Could not assign publisher to indexer", "indexer", indexerNum, "adminURL", a.indexerPool[indexerNum].adminURL)
			continue
		}
		asmt.addIndexer(indexerNum)
		a.notifyAssignment(amsg.PeerID, indexerNum)
	}

	if len(unassigned) == 0 {
		a.closeNotifyAssignment(amsg.PeerID)
	}

	a.mutex.Lock()
	a.presets[amsg.PeerID] = unassigned
	asmt.processing = false
	a.mutex.Unlock()
}

func (a *Assigner) makeAssignments(ctx context.Context, amsg announce.Announce, asmt *assignment, need int) {
	log := log.With("publisher", amsg.PeerID)

	defer func() {
		a.mutex.Lock()
		asmt.processing = false
		a.mutex.Unlock()
	}()

	candidates := make([]int, 0, len(a.indexerPool)-len(asmt.indexers))
	for i := range a.indexerPool {
		if !asmt.hasIndexer(i) {
			candidates = append(candidates, i)
		}
	}

	// There are no remaining indexers to assign publisher to.
	if len(candidates) == 0 {
		log.Warnw("Insufficient indexers to assign publisher to", "indexersAssigned", len(asmt.indexers), "replication", a.replication)
		return
	}

	a.orderCandidates(candidates)

	for _, indexerNum := range candidates {
		err := assignIndexer(ctx, a.indexerPool[indexerNum], amsg)
		if err != nil {
			log.Errorw("Could not assign publisher to indexer", "indexer", indexerNum, "adminURL", a.indexerPool[indexerNum].adminURL)
			continue
		}
		asmt.addIndexer(indexerNum)
		a.notifyAssignment(amsg.PeerID, indexerNum)
		need--
		if need == 0 {
			a.closeNotifyAssignment(amsg.PeerID)
			log.Info("Publisher assigned to required number of indexers")
			return
		}
	}
	log.Warnf("Publisher assigned to %d out of %d required indexers", len(asmt.indexers), a.replication)
}

func (a *Assigner) orderCandidates(indexers []int) {
	// TODO: order candidates by available storage and number of providers.
}

func assignIndexer(ctx context.Context, indexer indexerInfo, amsg announce.Announce) error {
	cl, err := adminclient.New(indexer.adminURL)
	if err != nil {
		return err
	}
	err = cl.Allow(ctx, amsg.PeerID)
	if err != nil {
		return err
	}
	log.Infow("Assigned publisher to indexer, sending direct announce", "adminURL", indexer.adminURL, "ingestURL", indexer.ingestURL, "publisher", amsg.PeerID)

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

func getAssignments(ctx context.Context, adminURL string) ([]peer.ID, error) {
	cl, err := adminclient.New(adminURL)
	if err != nil {
		return nil, err
	}
	return cl.ListAssignedPeers(ctx)
}
