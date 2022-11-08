package dtsync

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

var log = logging.Logger("dagsync/dtsync")

const hitRateLimitErrStr = "hitRateLimit"

type inProgressSyncKey struct {
	c    cid.Cid
	peer peer.ID
}

// Sync provides sync functionality for use with all datatransfer syncs.
type Sync struct {
	dtManager   dt.Manager
	dtClose     dtCloseFunc
	host        host.Host
	ls          *ipld.LinkSystem
	unsubEvents dt.Unsubscribe
	unregHook   graphsync.UnregisterHookFunc

	// Used to signal CIDs that are found locally.
	// Note, blockhook is called in 2 ways:
	// 1. via graphsync hook registered here for blocks that are not found locally.
	// 2. via Syncer.signalLocallyFoundCids for blockhooks thar are found locally.
	blockHook func(peer.ID, cid.Cid)

	// Map of CID of in-progress sync to sync done channel.
	syncDoneChans map[inProgressSyncKey]chan<- error
	syncDoneMutex sync.Mutex

	rateLimiters map[peer.ID]*rate.Limiter
	rateMutex    sync.Mutex
}

// NewSyncWithDT creates a new Sync with a datatransfer.Manager provided by the
// caller.
func NewSyncWithDT(host host.Host, dtManager dt.Manager, gs graphsync.GraphExchange, ls *ipld.LinkSystem, blockHook func(peer.ID, cid.Cid)) (*Sync, error) {
	err := registerVoucher(dtManager, &Voucher{}, nil)
	if err != nil {
		return nil, err
	}

	s := &Sync{
		host:         host,
		dtManager:    dtManager,
		ls:           ls,
		rateLimiters: map[peer.ID]*rate.Limiter{},
		blockHook:    blockHook,
	}

	if blockHook != nil {
		s.unregHook = gs.RegisterIncomingBlockHook(s.addRateLimiting(addIncomingBlockHook(nil, blockHook), s.getRateLimiter, gs))
	}

	s.unsubEvents = dtManager.SubscribeToEvents(s.onEvent)
	return s, nil
}

// NewSync creates a new Sync with its own datatransfer.Manager.
func NewSync(host host.Host, ds datastore.Batching, lsys ipld.LinkSystem, blockHook func(peer.ID, cid.Cid)) (*Sync, error) {
	dtManager, gs, dtClose, err := makeDataTransfer(host, ds, lsys, nil)
	if err != nil {
		return nil, err
	}

	s := &Sync{
		host:         host,
		dtManager:    dtManager,
		ls:           &lsys,
		dtClose:      dtClose,
		rateLimiters: make(map[peer.ID]*rate.Limiter),
		blockHook:    blockHook,
	}

	if blockHook != nil {
		s.unregHook = gs.RegisterIncomingBlockHook(s.addRateLimiting(addIncomingBlockHook(nil, blockHook), s.getRateLimiter, gs))
	}

	s.unsubEvents = dtManager.SubscribeToEvents(s.onEvent)
	return s, nil
}

func (s *Sync) clearRateLimiter(peerID peer.ID) {
	s.rateMutex.Lock()
	delete(s.rateLimiters, peerID)
	s.rateMutex.Unlock()
}

func (s *Sync) setRateLimiter(peerID peer.ID, rateLimiter *rate.Limiter) {
	s.rateMutex.Lock()
	s.rateLimiters[peerID] = rateLimiter
	s.rateMutex.Unlock()
}

func (s *Sync) getRateLimiter(peerID peer.ID) *rate.Limiter {
	s.rateMutex.Lock()
	limiter := s.rateLimiters[peer.ID(peerID)]
	s.rateMutex.Unlock()
	return limiter
}

func (s *Sync) addRateLimiting(bFn graphsync.OnIncomingBlockHook, rateLimiter func(peer.ID) *rate.Limiter, gs graphsync.GraphExchange) graphsync.OnIncomingBlockHook {
	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		isLocalBlock := blockData.BlockSizeOnWire() == 0

		if !isLocalBlock {
			limiter := rateLimiter(p)
			if limiter != nil && !limiter.Allow() {
				// We've hit a rate limit. We'll terminate this sync with a rate limit
				// err along with the cid of the block that we didn't process. When we
				// restart the sync after the rate limit we should continue from this
				// block.
				hookActions.TerminateWithError(fmt.Errorf("%s(%s)", hitRateLimitErrStr, blockData.Link().(cidlink.Link).Cid.String()))
				return
			}
		}

		if bFn != nil {
			bFn(p, responseData, blockData, hookActions)
		}
	}
}

func addIncomingBlockHook(bFn graphsync.OnIncomingBlockHook, blockHook func(peer.ID, cid.Cid)) graphsync.OnIncomingBlockHook {
	return func(p peer.ID, responseData graphsync.ResponseData, blockData graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
		blockHook(peer.ID(p), blockData.Link().(cidlink.Link).Cid)
		if bFn != nil {
			bFn(p, responseData, blockData, hookActions)
		}
	}
}

// Close unregisters datatransfer event notification. If this Sync owns the
// datatransfer.Manager then the Manager is stopped.
func (s *Sync) Close() error {
	s.unsubEvents()
	if s.unregHook != nil {
		s.unregHook()
	}

	var err error
	if s.dtClose != nil {
		err = s.dtClose()
	}

	// Dismiss any handlers waiting completion of sync.
	s.syncDoneMutex.Lock()
	if len(s.syncDoneChans) != 0 {
		log.Warnf("Closing datatransfer sync with %d syncs in progress", len(s.syncDoneChans))
	}
	for _, ch := range s.syncDoneChans {
		ch <- errors.New("sync closed")
		close(ch)
	}
	s.syncDoneChans = nil
	s.syncDoneMutex.Unlock()

	return err
}

// NewSyncer creates a new Syncer to use for a single sync operation against a peer.
func (s *Sync) NewSyncer(peerID peer.ID, topicName string, rateLimiter *rate.Limiter) *Syncer {
	return &Syncer{
		peerID:      peerID,
		sync:        s,
		topicName:   topicName,
		rateLimiter: rateLimiter,
		ls:          s.ls,
	}
}

// notifyOnSyncDone returns a channel that sync done notification is sent on.
func (s *Sync) notifyOnSyncDone(k inProgressSyncKey) <-chan error {
	syncDone := make(chan error, 1)

	s.syncDoneMutex.Lock()
	defer s.syncDoneMutex.Unlock()

	if s.syncDoneChans == nil {
		s.syncDoneChans = make(map[inProgressSyncKey]chan<- error)
	}
	s.syncDoneChans[k] = syncDone

	return syncDone
}

// signalSyncDone removes and closes the channel when the pending sync has
// completed.  Returns true if a channel was found.
func (s *Sync) signalSyncDone(k inProgressSyncKey, err error) bool {
	s.syncDoneMutex.Lock()
	defer s.syncDoneMutex.Unlock()

	syncDone, ok := s.syncDoneChans[k]
	if !ok {
		return false
	}
	if len(s.syncDoneChans) == 1 {
		s.syncDoneChans = nil
	} else {
		delete(s.syncDoneChans, k)
	}

	if err != nil {
		syncDone <- err
	}
	close(syncDone)
	return true
}

// signalLocallyFoundCids calls the syncer blockhook if present with any CIDs that are
// traversed during a sync but not transported using graphsync exchange.
func (s *Sync) signalLocallyFoundCids(id peer.ID, cids []cid.Cid) {
	if s.blockHook != nil {
		for _, c := range cids {
			s.blockHook(id, c)
		}
	}
}

type rateLimitErr struct {
	msg          string
	stoppedAtCid cid.Cid
}

func (e rateLimitErr) Error() string { return e.msg }

// onEvent is called by the datatransfer manager to send events.
func (s *Sync) onEvent(event dt.Event, channelState dt.ChannelState) {
	var err error
	switch channelState.Status() {
	case dt.Completed:
		// Tell the waiting handler that the sync has finished successfully.
		log.Debugw("datatransfer completed successfully", "cid", channelState.BaseCID(), "peer", channelState.OtherPeer())
	case dt.Cancelled:
		// The request was canceled; inform waiting handler.
		err = fmt.Errorf("datatransfer cancelled")
		log.Warnw(err.Error(), "cid", channelState.BaseCID(), "peer", channelState.OtherPeer(), "message", channelState.Message())
	case dt.Failed:
		// Communicate the error back to the waiting handler.
		msg := channelState.Message()
		if idx := strings.Index(msg, hitRateLimitErrStr); idx != -1 {
			// This is a rate limit error, let's pull out the cid of the block we
			// didn't process to include it in the error.
			stoppedAtCidStr := msg[idx+len(hitRateLimitErrStr)+1:]
			lastParen := strings.Index(stoppedAtCidStr, ")")
			stoppedAtCidStr = stoppedAtCidStr[:lastParen]

			var stoppedAtCid cid.Cid
			stoppedAtCid, err = cid.Decode(stoppedAtCidStr)
			if err == nil {
				err = rateLimitErr{msg, stoppedAtCid}
			}
		} else {
			err = fmt.Errorf("datatransfer failed: %s", msg)
		}

		log.Errorw(err.Error(), "cid", channelState.BaseCID(), "peer", channelState.OtherPeer(), "message", msg)

		if strings.HasSuffix(msg, "content not found") {
			err = errors.New(err.Error() + ": content not found")
		}
	default:
		// Ignore non-terminal channel states.
		return
	}

	// Send the FinishTransfer signal to the handler.  This will allow its
	// handle goroutine to distribute the update and exit.
	//
	// It is not necessary to return the channelState CID, since we already
	// know it is the correct on since it was used to look up this syncDone
	// channel.
	if !s.signalSyncDone(inProgressSyncKey{channelState.BaseCID(), peer.ID(channelState.OtherPeer())}, err) {
		log.Errorw("Could not find channel for completed transfer notice", "cid", channelState.BaseCID())
		return
	}
}
