package ingest

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/storetheindex/internal/counter"
)

// configIngest contains all options for the ingester.
type configIngest struct {
	idxCounts *counter.IndexCounts
	dsAds     datastore.Batching
}

// Option is a function that sets a value in a config.
type Option func(*configIngest) error

// getOpts creates a configIngest and applies Options to it.
func getOpts(opts []Option) (configIngest, error) {
	var cfg configIngest
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return configIngest{}, fmt.Errorf("option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithAdsDatastore configures a separate datastore for advertizements.
func WithAdsDatastore(ds datastore.Batching) Option {
	return func(c *configIngest) error {
		c.dsAds = ds
		return nil
	}
}

// WithIndexCounts configures counting indexes using an IndexCounts instance.
func WithIndexCounts(ic *counter.IndexCounts) Option {
	return func(c *configIngest) error {
		c.idxCounts = ic
		return nil
	}
}
