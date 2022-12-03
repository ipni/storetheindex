package dtsync

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/storetheindex/dagsync/p2p/protocol/head"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

// Syncer handles a single sync with a provider.
type Syncer struct {
	peerID      peer.ID
	rateLimiter *rate.Limiter
	sync        *Sync
	ls          *ipld.LinkSystem
	topicName   string
}

// GetHead queries a provider for the latest CID.
func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	return head.QueryRootCid(ctx, s.sync.host, s.topicName, s.peerID)
}

// Sync opens a datatransfer data channel and uses the selector to pull data
// from the provider.
func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	if s.rateLimiter != nil {
		// Set the rate limiter to use for this sync of the peer. This limiter
		// is retrieved by getRateLimiter, called from wrapped block hook.
		s.sync.setRateLimiter(s.peerID, s.rateLimiter)
		// Remove rate limiter set above.
		defer s.sync.clearRateLimiter(s.peerID)
	}

	// See if we already have the requested data first.
	// TODO: The check here is equivalent to "all or nothing": if a DAG is partially available
	//       The entire thing will be re-downloaded even if we are missing only a single link.
	//       Consider a further optimisation where we would only sync the portion of DAG that
	//       is absent.
	//       Hint: be careful with the selector; this can become tricky depending on what the
	//             given selector is after. We could accept arguments that ask the user to
	//             help with determining what the "next" CID would be if a DAG is partially
	//             present. Similar to what SegmentSyncActions does.
	if cids, ok := s.has(ctx, nextCid, sel); ok {
		s.sync.signalLocallyFoundCids(s.peerID, cids)
		inProgressSyncK := inProgressSyncKey{nextCid, s.peerID}
		s.sync.signalSyncDone(inProgressSyncK, nil)
		return nil
	}

	for {
		inProgressSyncK := inProgressSyncKey{nextCid, s.peerID}
		// For loop to retry if we get rate limited.
		syncDone := s.sync.notifyOnSyncDone(inProgressSyncK)

		log.Debugw("Starting data channel for message source", "cid", nextCid, "source_peer", s.peerID)

		v := Voucher{&nextCid}
		// Do not pass cancelable context into OpenPullDataChannel because a
		// canceled context causes it to hang.
		_, err := s.sync.dtManager.OpenPullDataChannel(context.Background(), s.peerID, &v, nextCid, sel)
		if err != nil {
			s.sync.signalSyncDone(inProgressSyncK, nil)
			return fmt.Errorf("cannot open data channel: %w", err)
		}

		// Wait for transfer finished signal.
		select {
		case err = <-syncDone:
		case <-ctx.Done():
			s.sync.signalSyncDone(inProgressSyncK, ctx.Err())
			err = <-syncDone
		}
		if err, ok := err.(rateLimitErr); ok {
			// Wait until the rate limit bucket is fully refilled since this is
			// a relatively heavy operation (essentially restarting the sync).
			// Note, cannot use s.rateLimiter.WaitN here because that waits,
			// but also consumes n tokens.
			waitMsec := 1000.0 * float64(s.rateLimiter.Burst()) / float64(s.rateLimiter.Limit())
			var waitTime time.Duration
			waitTime = time.Duration(waitMsec) * time.Millisecond
			if waitTime == 0 {
				waitTime = time.Duration(1000*waitMsec) * time.Microsecond
			}
			log.Infow("Hit rate limit. Waiting and will retry later", "cid", nextCid, "source_peer", s.peerID, "delay", waitTime.String())
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return ctx.Err()
			}
			// Need to consume one token, since the stopped to make up for the
			// previous Allow that did not consume a token and triggered rate
			// limiting, even though the block was still downloaded. At next
			// restart the stopped at block will be local and will not count
			// toward rate limiting.
			s.rateLimiter.Allow()

			// Set the nextCid to be the cid that we stopped at because of rate
			// limiting. This lets us pick up where we left off
			nextCid = err.stoppedAtCid
			continue
		}
		return err
	}
}

// has determines if a given CID and selector is stored in the linksystem for a syncer already.
//
// If stored, returns true along with the list of CIDs that were encountered during traversal
// in order of traversal. Otherwise, returns false with no CIDs.
func (s *Syncer) has(ctx context.Context, nextCid cid.Cid, sel ipld.Node) ([]cid.Cid, bool) {
	getMissingLs := cidlink.DefaultLinkSystem()
	// trusted because it'll be hashed/verified on the way into the link system when fetched.
	getMissingLs.TrustedStorage = true

	var traversed []cid.Cid
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		r, err := s.ls.StorageReadOpener(lc, l)
		if err != nil {
			return nil, fmt.Errorf("not available locally: %w", err)
		}
		// Found block read opener, so return it.
		traversed = append(traversed, l.(cidlink.Link).Cid)
		return r, nil
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     getMissingLs,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
		Path: datamodel.NewPath([]datamodel.PathSegment{}),
	}
	csel, err := selector.CompileSelector(sel)
	if err != nil {
		return nil, false
	}

	// get the direct node.
	rootNode, err := getMissingLs.Load(ipld.LinkContext{}, cidlink.Link{Cid: nextCid}, basicnode.Prototype.Any)
	if err != nil {
		return nil, false
	}
	if err := progress.WalkMatching(rootNode, csel, func(p traversal.Progress, n datamodel.Node) error {
		return nil
	}); err != nil {
		return nil, false
	}
	return traversed, true
}
