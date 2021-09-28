package client

import (
	"context"

	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/multiformats/go-multihash"
)

// Finder is the interface implemented by all finder client protocols
type Finder interface {
	// Get record for single multihash from indexer
	Find(context.Context, multihash.Multihash) (*model.FindResponse, error)
	// Get info from a batch of multihashes
	FindBatch(context.Context, []multihash.Multihash) (*model.FindResponse, error)
}
