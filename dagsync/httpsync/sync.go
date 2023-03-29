package httpsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/go-libipni/maurl"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"golang.org/x/time/rate"
)

const defaultHttpTimeout = 10 * time.Second

var log = logging.Logger("dagsync/httpsync")

// Sync provides sync functionality for use with all http syncs.
type Sync struct {
	blockHook func(peer.ID, cid.Cid)
	client    *http.Client
	lsys      ipld.LinkSystem
}

func NewSync(lsys ipld.LinkSystem, client *http.Client, blockHook func(peer.ID, cid.Cid)) *Sync {
	if client == nil {
		client = &http.Client{
			Timeout: defaultHttpTimeout,
		}
	}
	return &Sync{
		blockHook: blockHook,
		client:    client,
		lsys:      lsys,
	}
}

// NewSyncer creates a new Syncer to use for a single sync operation against a peer.
func (s *Sync) NewSyncer(peerID peer.ID, peerAddrs []multiaddr.Multiaddr, rateLimiter *rate.Limiter) (*Syncer, error) {
	urls := make([]*url.URL, len(peerAddrs))
	for i := range peerAddrs {
		var err error
		urls[i], err = maurl.ToURL(peerAddrs[i])
		if err != nil {
			return nil, err
		}
	}

	return &Syncer{
		peerID:      peerID,
		rateLimiter: rateLimiter,
		rootURL:     *urls[0],
		urls:        urls[1:],
		sync:        s,
	}, nil
}

func (s *Sync) Close() {
	s.client.CloseIdleConnections()
}

var errHeadFromUnexpectedPeer = errors.New("found head signed from an unexpected peer")

type Syncer struct {
	peerID      peer.ID
	rateLimiter *rate.Limiter
	rootURL     url.URL
	urls        []*url.URL
	sync        *Sync
}

func (s *Syncer) GetHead(ctx context.Context) (cid.Cid, error) {
	var head cid.Cid
	var pubKey ic.PubKey

	err := s.fetch(ctx, "head", func(msg io.Reader) error {
		var err error
		pubKey, head, err = openSignedHeadWithIncludedPubKey(msg)
		return err
	})
	if err != nil {
		return cid.Undef, err
	}

	peerIDFromSig, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return cid.Undef, err
	}

	if peerIDFromSig != s.peerID {
		return cid.Undef, errHeadFromUnexpectedPeer
	}

	return head, nil
}

func (s *Syncer) Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error {
	xsel, err := selector.CompileSelector(sel)
	if err != nil {
		return fmt.Errorf("failed to compile selector: %w", err)
	}

	cids, err := s.walkFetch(ctx, nextCid, xsel)
	if err != nil {
		return fmt.Errorf("failed to traverse requested dag: %w", err)
	}

	// We run the block hook to emulate the behavior of graphsync's
	// `OnIncomingBlockHook` callback (gets called even if block is already stored
	// locally).
	//
	// We are purposefully not doing this in the StorageReadOpener because the
	// hook can do anything, including deleting the block from the block store. If
	// it did that then we would not be able to continue our traversal. So instead
	// we remember the blocks seen during traversal and then call the hook at the
	// end when we no longer care what it does with the blocks.
	if s.sync.blockHook != nil {
		for _, c := range cids {
			s.sync.blockHook(s.peerID, c)
		}
	}

	s.sync.client.CloseIdleConnections()
	return nil
}

// walkFetch is run by a traversal of the selector.  For each block that the
// selector walks over, walkFetch will look to see if it can find it in the
// local data store. If it cannot, it will then go and get it over HTTP.  This
// emulates way libp2p/graphsync fetches data, but the actual fetch of data is
// done over HTTP.
func (s *Syncer) walkFetch(ctx context.Context, rootCid cid.Cid, sel selector.Selector) ([]cid.Cid, error) {
	// Track the order of cids we've seen during our traversal so we can call the
	// block hook function in the same order. We emulate the behavior of
	// graphsync's `OnIncomingBlockHook`, this means we call the blockhook even if
	// we have the block locally.
	var traversalOrder []cid.Cid
	getMissingLs := cidlink.DefaultLinkSystem()
	// trusted because it'll be hashed/verified on the way into the link system when fetched.
	getMissingLs.TrustedStorage = true
	getMissingLs.StorageReadOpener = func(lc ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		c := l.(cidlink.Link).Cid
		r, err := s.sync.lsys.StorageReadOpener(lc, l)
		if err == nil {
			// Found block read opener, so return it.
			traversalOrder = append(traversalOrder, c)
			return r, nil
		}

		// Did not find block read opener, so fetch block via HTTP with re-try in case rate limit is
		// reached.
		for {
			if err = s.fetchBlock(ctx, c); err != nil {
				var errRateLimit rateLimitErr
				if errors.As(err, &errRateLimit) {
					// TODO: implement backoff to avoid potentially exhausting the HTTP source.
					log.Info("Fetch request was rate-limited")
					continue
				}
				return nil, fmt.Errorf("failed to fetch block for cid %s: %w", c, err)
			}
			break
		}

		r, err = s.sync.lsys.StorageReadOpener(lc, l)
		if err == nil {
			traversalOrder = append(traversalOrder, c)
		}
		return r, err
	}

	progress := traversal.Progress{
		Cfg: &traversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     getMissingLs,
			LinkTargetNodePrototypeChooser: basicnode.Chooser,
		},
		Path: datamodel.NewPath([]datamodel.PathSegment{}),
	}
	// get the direct node.
	rootNode, err := getMissingLs.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: rootCid}, basicnode.Prototype.Any)
	if err != nil {
		return nil, fmt.Errorf("failed to load node for root cid %s: %w", rootCid, err)
	}
	err = progress.WalkMatching(rootNode, sel, func(p traversal.Progress, n datamodel.Node) error {
		return nil
	})
	if err != nil {
		return nil, err
	}
	return traversalOrder, nil
}

type rateLimitErr struct {
	resource string
	rootURL  url.URL
	source   peer.ID
}

func (r rateLimitErr) Error() string {
	return fmt.Sprintf("rate limit reached when fetching %s from %s at %s", r.resource, r.source, r.rootURL.String())
}

func (s *Syncer) fetch(ctx context.Context, rsrc string, cb func(io.Reader) error) error {
	if s.rateLimiter != nil {
		err := s.rateLimiter.Wait(ctx)
		if err != nil {
			return &rateLimitErr{
				resource: rsrc,
				rootURL:  s.rootURL,
				source:   s.peerID,
			}
		}
	}

nextURL:
	localURL := s.rootURL
	localURL.Path = path.Join(s.rootURL.Path, rsrc)

	req, err := http.NewRequestWithContext(ctx, "GET", localURL.String(), nil)
	if err != nil {
		return err
	}

	resp, err := s.sync.client.Do(req)
	if err != nil {
		if len(s.urls) != 0 {
			s.rootURL = *s.urls[0]
			s.urls = s.urls[1:]
			goto nextURL
		}
		return fmt.Errorf("fetch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non success http fetch response at %s: %d", localURL.String(), resp.StatusCode)
	}

	return cb(resp.Body)
}

// fetchBlock fetches an item into the datastore at c if not locally available.
func (s *Syncer) fetchBlock(ctx context.Context, c cid.Cid) error {
	n, err := s.sync.lsys.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, basicnode.Prototype.Any)
	// node is already present.
	if n != nil && err == nil {
		return nil
	}

	return s.fetch(ctx, c.String(), func(data io.Reader) error {
		writer, committer, err := s.sync.lsys.StorageWriteOpener(ipld.LinkContext{Ctx: ctx})
		if err != nil {
			log.Errorw("Failed to get write opener", "err", err)
			return err
		}
		tee := io.TeeReader(data, writer)
		sum, err := multihash.SumStream(tee, c.Prefix().MhType, c.Prefix().MhLength)
		if err != nil {
			return err
		}
		if !bytes.Equal(c.Hash(), sum) {
			err := fmt.Errorf("hash digest mismatch; expected %s but got %s", c.Hash().B58String(), sum.B58String())
			log.Errorw("Failed to persist fetched block with mismatching digest", "cid", c, "err", err)
			return err
		}
		if err = committer(cidlink.Link{Cid: c}); err != nil {
			log.Errorw("Failed to commit", "err", err)
			return err
		}
		return nil
	})
}
