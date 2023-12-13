package reaper

import (
	"errors"
	"fmt"
	"time"

	"github.com/ipni/go-libipni/pcache"
	"github.com/ipni/storetheindex/carstore"
	"github.com/libp2p/go-libp2p/core/host"
)

const (
	defaultHttpTimeout = 10 * time.Second
	defaultTopic       = "/indexer/ingest/mainnet"
)

type config struct {
	carCompAlg        string
	carDelete         bool
	carRead           bool
	commit            bool
	deleteNotFound    bool
	dstoreDir         string
	dstoreTmpDir      string
	entriesDepthLimit int64
	entsFromPub       bool
	httpTimeout       time.Duration
	p2pHost           host.Host
	pcache            *pcache.ProviderCache
	topic             string
}

// Option is a function that sets a value in a config.
type Option func(*config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		carCompAlg:  carstore.Gzip,
		carRead:     true,
		entsFromPub: true,
		httpTimeout: defaultHttpTimeout,
		topic:       defaultTopic,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return cfg, nil
}

// WithCarCompress configures CAR file compression.
func WithCarCompress(alg string) Option {
	return func(c *config) error {
		if alg != "" {
			c.carCompAlg = alg
		}
		return nil
	}
}

// WithCarRead sets whether or not entries are read from CAR files. Only set to
// false if CAR file exist, but do not contain needed entries data.
func WithCarRead(rd bool) Option {
	return func(c *config) error {
		c.carRead = rd
		return nil
	}
}

// WithCarDelete deletes CAR files that have no multihash content, including
// CAR file for removal and address update advertisements.
func WithCarDelete(del bool) Option {
	return func(c *config) error {
		c.carDelete = del
		return nil
	}
}

// WithCommit tells GC to commit changes to storage. Otherwise, GC only reports
// information about what would have been collected.
func WithCommit(commit bool) Option {
	return func(c *config) error {
		c.commit = commit
		return nil
	}
}

func WithDatastoreDir(dir string) Option {
	return func(c *config) error {
		c.dstoreDir = dir
		return nil
	}
}

func WithDatastoreTempDir(dir string) Option {
	return func(c *config) error {
		c.dstoreTmpDir = dir
		return nil
	}
}

// WithDeleteNotFound causes all index content for a provider to be deleted if
// that provider is not found in any of the sources of provider information.
func WithDeleteNotFound(dnf bool) Option {
	return func(c *config) error {
		c.deleteNotFound = dnf
		return nil
	}
}

// WithEntriesFromPublisher allows fetching advertisement entries from the
// publisher if they cannot be fetched from a CAR file.
func WithEntriesFromPublisher(entsFromPub bool) Option {
	return func(c *config) error {
		c.entsFromPub = entsFromPub
		return nil
	}
}

// WithLibp2pHost configures gc to use an existing libp2p host to connect to
// publishers.
func WithLibp2pHost(h host.Host) Option {
	return func(c *config) error {
		c.p2pHost = h
		return nil
	}
}

func WithPCache(pc *pcache.ProviderCache) Option {
	return func(c *config) error {
		c.pcache = pc
		return nil
	}
}

// WithTopicName sets the topic name on which the provider announces advertised
// content. Defaults to '/indexer/ingest/mainnet'.
func WithTopicName(topic string) Option {
	return func(c *config) error {
		c.topic = topic
		return nil
	}
}

// WithEntriesDepthLimit sets the depth limit when syncing an
// advertisement entries chain. Setting to 0 means no limit.
func WithEntriesDepthLimit(depthLimit int64) Option {
	return func(c *config) error {
		if depthLimit < 0 {
			return errors.New("ad entries depth limit cannot be negative")
		}
		c.entriesDepthLimit = depthLimit
		return nil
	}
}

// WithHttpTimeout sets the timeout for http and libp2phttp connections.
func WithHttpTimeout(to time.Duration) Option {
	return func(c *config) error {
		if to != 0 {
			c.httpTimeout = to
		}
		return nil
	}
}
