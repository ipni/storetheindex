package federation

import (
	"errors"
	"net/http"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/dsadapter"
	"github.com/ipni/storetheindex/internal/ingest"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type (
	Option func(*options) error

	options struct {
		datastore                datastore.Datastore
		host                     host.Host
		httpListenAddr           string
		linkSystem               *ipld.LinkSystem
		members                  []peer.AddrInfo
		registry                 *registry.Registry
		ingester                 *ingest.Ingester
		reconciliationHttpClient *http.Client
		reconciliationInterval   time.Duration
		snapshotInterval         time.Duration
	}
)

func newOptions(o ...Option) (*options, error) {
	opt := options{
		httpListenAddr:           "0.0.0.0:3004",
		reconciliationHttpClient: http.DefaultClient,
		reconciliationInterval:   1 * time.Hour,
		snapshotInterval:         30 * time.Minute,
	}
	for _, apply := range o {
		if err := apply(&opt); err != nil {
			return nil, err
		}
	}
	// Check required options
	if opt.ingester == nil {
		return nil, errors.New("ingester must be specified")
	}
	if opt.registry == nil {
		return nil, errors.New("registry must be specified")
	}

	// Set defaults
	if opt.datastore == nil {
		opt.datastore = sync.MutexWrap(datastore.NewMapDatastore())
	}
	if opt.host == nil {
		var err error
		opt.host, err = libp2p.New()
		if err != nil {
			return nil, err
		}
	}
	if opt.linkSystem == nil {
		ls := cidlink.DefaultLinkSystem()
		storage := dsadapter.Adapter{
			Wrapped: opt.datastore,
		}
		ls.SetReadStorage(&storage)
		ls.SetWriteStorage(&storage)
		opt.linkSystem = &ls
	}

	return &opt, nil
}

func WithDatastore(v datastore.Datastore) Option {
	return func(o *options) error {
		o.datastore = v
		return nil
	}
}

func WithHost(v host.Host) Option {
	return func(o *options) error {
		o.host = v
		return nil
	}
}
func WithHttpListenAddr(v string) Option {
	return func(o *options) error {
		o.httpListenAddr = v
		return nil
	}
}

func WithMembers(v ...peer.AddrInfo) Option {
	return func(o *options) error {
		o.members = append(o.members, v...)
		return nil
	}
}
func WithLinkSystem(v *ipld.LinkSystem) Option {
	return func(o *options) error {
		o.linkSystem = v
		return nil
	}
}

func WithIngester(v *ingest.Ingester) Option {
	return func(o *options) error {
		o.ingester = v
		return nil
	}
}

func WithRegistry(v *registry.Registry) Option {
	return func(o *options) error {
		o.registry = v
		return nil
	}
}

func WithReconciliationInterval(v time.Duration) Option {
	return func(o *options) error {
		o.reconciliationInterval = v
		return nil
	}
}

func WithSnapshotInterval(v time.Duration) Option {
	return func(o *options) error {
		o.snapshotInterval = v
		return nil
	}
}
