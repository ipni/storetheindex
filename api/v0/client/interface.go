package client

import (
	"context"

	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/ipfs/go-cid"
)

// Finder is the interface implemented by all finder client protocols
type Finder interface {
	// Get record for single CID from indexer
	Get(context.Context, cid.Cid) (*models.Response, error)
	// Get info from a batch of CIDs
	GetBatch(context.Context, []cid.Cid) (*models.Response, error)
}
