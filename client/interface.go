package client

import (
	"context"

	"github.com/filecoin-project/storetheindex/client/models"
	"github.com/ipfs/go-cid"
)

// Interface implemented by all client protocols.
type Interface interface {
	// Get record for single CID from indexer
	Get(context.Context, cid.Cid, Endpoint) (*models.Response, error)
	// Get info from a batch of CIDs
	GetBatch(context.Context, []cid.Cid, Endpoint) (*models.Response, error)
}

// Endpoint of the indexer the client makes the request to.
// According to the protocol implementation, endpoints may
// differ. Clients are not exclusive for a single endpoint,
// they should be able to dynamically request form differrent indexer
// nodes.
type Endpoint interface {
	Addr() interface{}
}

// Server side interface for entities serving client requests.
type Server interface {
	Endpoint() Endpoint
}
