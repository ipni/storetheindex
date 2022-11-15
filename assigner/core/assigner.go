package core

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/filecoin-project/storetheindex/announce"
	adminclient "github.com/filecoin-project/storetheindex/api/v0/admin/client/http"
	findclient "github.com/filecoin-project/storetheindex/api/v0/finder/client/http"
	"github.com/filecoin-project/storetheindex/assigner/config"
	"github.com/filecoin-project/storetheindex/peerutil"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("assigner/core")

// Core contains the core data and logic for the assigner.
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
	// receiver receives announce messages.
	receiver *announce.Receiver
	// replication is the number of indexers to assign a publisher to.
	replication int
	// watchDone signals that the watch function exited.
	watchDone chan struct{}
	// waitingNotice are channels waiting for a specific peer to be assigned
	waitingNotice map[peer.ID]chan string
	noticeMutex   sync.Mutex
}

type assignment struct {
	indexers   map[int]struct{}
	processing bool
}

type indexerInfo struct {
	adminURL  string
	findURL   string
	ingestURL string
}

func NewAssigner(ctx context.Context, cfg config.Assignment, p2pHost host.Host) (*Assigner, error) {
	if cfg.Replication < 0 {
		return nil, errors.New("bad replication value, must be 0 or positive")
	}
	if len(cfg.IndexerPool) < 1 {
		return nil, errors.New("no indexers configured to assign to")
	}

	assigned := make(map[peer.ID]*assignment)
	indexerPool, err := indexersFromConfig(cfg.IndexerPool)
	if err != nil {
		return nil, err
	}

	// Get the publishers currently assigned to each indexer in the pool.
	for i := range indexerPool {
		pubs, err := getAssignments(ctx, indexerPool[i].findURL)
		if err != nil {
			log.Errorw("could not get assignments from indexer", "err", err, "indexer", i, "findURL", indexerPool[i].findURL)
			continue
		}
		// Add this indexer to each publishers assignments.
		for _, pubID := range pubs {
			asmt, found := assigned[pubID]
			if !found {
				asmt = &assignment{}
				assigned[pubID] = asmt
			}
			asmt.indexers[i] = struct{}{}
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
		receiver:    rcvr,
		replication: cfg.Replication,
		watchDone:   make(chan struct{}),
	}

	go a.watch()

	return a, nil
}

func indexersFromConfig(cfgIndexerPool []config.Indexer) ([]indexerInfo, error) {
	seen := make(map[string]struct{}, len(cfgIndexerPool))
	indexers := make([]indexerInfo, 0, len(cfgIndexerPool))
	for i := range cfgIndexerPool {
		var iInfo indexerInfo

		u, err := url.Parse(cfgIndexerPool[i].AdminURL)
		if err != nil {
			return nil, fmt.Errorf("indexer %d has bad admin url: %s: %w", i, cfgIndexerPool[i].AdminURL, err)
		}
		iInfo.adminURL = u.String()
		if _, found := seen[iInfo.adminURL]; found {
			return nil, fmt.Errorf("indexer %d has non-unique admin url %s", i, iInfo.adminURL)
		}

		u, err = url.Parse(cfgIndexerPool[i].FindURL)
		if err != nil {
			return nil, fmt.Errorf("indexer %d has bad find url: %s: %w", i, cfgIndexerPool[i].FindURL, err)
		}
		iInfo.findURL = u.String()
		if _, found := seen[iInfo.findURL]; found {
			return nil, fmt.Errorf("indexer %d has non-unique find url %s", i, iInfo.findURL)
		}

		u, err = url.Parse(cfgIndexerPool[i].IngestURL)
		if err != nil {
			return nil, fmt.Errorf("indexer %d has bad ingest url: %s: %w", i, cfgIndexerPool[i].IngestURL, err)
		}
		iInfo.ingestURL = u.String()
		if _, found := seen[iInfo.ingestURL]; found {
			return nil, fmt.Errorf("indexer %d has non-unique ingest url %s", i, iInfo.ingestURL)
		}

		indexers = append(indexers, iInfo)

		seen[iInfo.adminURL] = struct{}{}
		seen[iInfo.findURL] = struct{}{}
		seen[iInfo.ingestURL] = struct{}{}
	}

	return indexers, nil
}

func (a *Assigner) Allowed(peerID peer.ID) bool {
	return a.policy.Eval(peerID)
}

func (a *Assigner) Announce(ctx context.Context, nextCid cid.Cid, addrInfo peer.AddrInfo) error {
	return a.receiver.Direct(ctx, nextCid, addrInfo.ID, addrInfo.Addrs)
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

func (a *Assigner) OnAssignment(pubID peer.ID) <-chan string {
	a.noticeMutex.Lock()
	defer a.noticeMutex.Unlock()

	if a.waitingNotice == nil {
		a.waitingNotice = make(map[peer.ID]chan string)
	}
	noticeChan := make(chan string, 1)
	a.waitingNotice[pubID] = noticeChan

	return noticeChan
}

func (a *Assigner) notifyAssignment(pubID peer.ID, indexerNum int) {
	a.noticeMutex.Lock()
	defer a.noticeMutex.Unlock()

	noticeChan, ok := a.waitingNotice[pubID]
	if !ok {
		return
	}
	noticeChan <- a.indexerPool[indexerNum].adminURL
	close(noticeChan) // signal not more data on channel

	delete(a.waitingNotice, pubID)
	if len(a.waitingNotice) == 0 {
		a.waitingNotice = nil
	}
}

// watch fetches announce messages from the Reciever.
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

func (a *Assigner) makeAssignments(ctx context.Context, amsg announce.Announce, asmt *assignment, need int) {
	log := log.With("publisher", amsg.PeerID)

	defer func() {
		a.mutex.Lock()
		asmt.processing = false
		a.mutex.Unlock()
	}()

	candidates := make([]int, 0, len(a.indexerPool)-len(asmt.indexers))
	for i := range a.indexerPool {
		if _, already := asmt.indexers[i]; !already {
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
		err := a.assignIndexer(ctx, a.indexerPool[indexerNum].adminURL, amsg)
		if err != nil {
			log.Errorw("Could not assign publisher to indexer", "indexer", indexerNum, "adminURL", a.indexerPool[indexerNum].adminURL)
			continue
		}
		asmt.indexers[indexerNum] = struct{}{}
		a.notifyAssignment(amsg.PeerID, indexerNum)
		need--
		if need == 0 {
			log.Info("Publisher assigned to required number of indexers")
			return
		}
	}
	log.Warnf("Publisher assigned to %d out of %d required indexers", len(asmt.indexers), a.replication)
}

// checkAssignment checks if a publisher is assigned to sufficient indexers.
func (a *Assigner) checkAssignment(pubID peer.ID) (*assignment, int) {
	repl := a.replication
	if repl == 0 {
		repl = len(a.indexerPool)
	}

	var need int

	a.mutex.Lock()
	defer a.mutex.Unlock()

	asmt, found := a.assigned[pubID]
	if found {
		if asmt.processing {
			log.Debug("Publisher assignment already being processed")
			return nil, 0
		}
		if len(asmt.indexers) >= repl {
			log.Debug("Publisher already assigned, ignoring announce")
			return nil, 0
		}
		need = repl - len(asmt.indexers)
		asmt.processing = true
	} else {
		// Publisher not yet assigned to an indexer, so make assignment.
		asmt = &assignment{
			processing: true,
			indexers:   make(map[int]struct{}),
		}
		a.assigned[pubID] = asmt
		need = repl
	}

	return asmt, need
}

func (a *Assigner) orderCandidates(indexers []int) {
	// TODO: order candidates by available storage and number of providers.
	return
}

func (a *Assigner) assignIndexer(ctx context.Context, adminURL string, amsg announce.Announce) error {
	cl, err := adminclient.New(adminURL)
	if err != nil {
		return err
	}
	err = cl.Allow(ctx, amsg.PeerID)
	if err != nil {
		return err
	}

	err = cl.Sync(ctx, amsg.PeerID, amsg.Addrs[0], 0, false)
	if err != nil {
		// Do not consider this a failure to assign since allowing the
		// publisher effectively assigns it.
		log.Errorw("Error starting sync to new assigned publisher", "err", err, "publisher", amsg.PeerID)
	}

	log.Infow("Assigned publisher to indexer", "publisher", amsg.PeerID, "adminURL", adminURL)
	return nil
}

func getAssignments(ctx context.Context, findURL string) ([]peer.ID, error) {
	cl, err := findclient.New(findURL)
	if err != nil {
		return nil, err
	}
	provs, err := cl.ListProviders(ctx)
	if err != nil {
		return nil, err
	}
	if len(provs) == 0 {
		return nil, nil
	}

	// Get set of unique publishers, since multiple providers may have the same
	// publisher.
	pubSet := make(map[peer.ID]struct{})
	for _, pinfo := range provs {
		if pinfo.Publisher == nil {
			continue
		}
		pubSet[pinfo.Publisher.ID] = struct{}{}
	}

	pubs := make([]peer.ID, len(pubSet))
	var i int
	for p := range pubSet {
		pubs[i] = p
		i++
	}

	return pubs, nil
}
