package dagsync

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/multiformats/go-multiaddr"
)

// Publisher is an interface for updating the published dag.
type Publisher interface {
	// Addrs returns the addresses that the publisher is listening on.
	Addrs() []multiaddr.Multiaddr
	// AnnounceHead sends an announce messag, via all senders, to announce the
	// current head advertisement CID. If there is no head, then does nothing.
	AnnounceHead(context.Context) error
	// AnnounceHead sends an announce messag containing the specified
	// addresses, via all senders, to announce the current head advertisement
	// CID. If there is no head, then does nothing.
	AnnounceHeadWithAddrs(context.Context, []multiaddr.Multiaddr) error
	// SetRoot sets the root CID without publishing it.
	SetRoot(context.Context, cid.Cid) error
	// UpdateRoot sets the root CID and publishes its update via all senders.
	UpdateRoot(context.Context, cid.Cid) error
	// UpdateRootWithAddrs publishes an update for the DAG, using custom
	// multiaddrs, via all senders.
	UpdateRootWithAddrs(context.Context, cid.Cid, []multiaddr.Multiaddr) error
	// Close publisher.
	Close() error
}

// Syncer is the interface used to sync with a data source.
type Syncer interface {
	GetHead(context.Context) (cid.Cid, error)
	Sync(ctx context.Context, nextCid cid.Cid, sel ipld.Node) error
}
