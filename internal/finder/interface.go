package finder

import (
	"context"

	"github.com/filecoin-project/storetheindex/api/v1/finder/models"
	"github.com/filecoin-project/storetheindex/server/net"
	"github.com/ipfs/go-cid"
)

// Interface implemented by all client protocols.
type Interface interface {
	// Get record for single CID from indexer
	Get(context.Context, cid.Cid, net.Endpoint) (*models.Response, error)
	// Get info from a batch of CIDs
	GetBatch(context.Context, []cid.Cid, net.Endpoint) (*models.Response, error)
}

// Server side interface for entities serving client requests.
type Server interface {
	Endpoint() net.Endpoint
}
