package handler

import (
	"fmt"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

// FinderHandler provides request handling functionality for the finder server
// that is common to all protocols
type FinderHandler struct {
	indexer  indexer.Interface
	registry *providers.Registry
}

func NewFinderHandler(indexer indexer.Interface, registry *providers.Registry) *FinderHandler {
	return &FinderHandler{
		indexer:  indexer,
		registry: registry,
	}
}

// MakeFindResponse reads from indexer core to populate a response from a
// list of multihashes.
func (h *FinderHandler) MakeFindResponse(mhashes []multihash.Multihash) (*models.FindResponse, error) {
	results := make([]models.IndexResult, 0, len(mhashes))
	var providerResults []peer.AddrInfo
	providerSeen := map[peer.ID]struct{}{}

	for i := range mhashes {
		values, found, err := h.indexer.Get(mhashes[i])
		if err != nil {
			return nil, syserr.New(fmt.Errorf("failed to query %q: %s", mhashes[i], err), 500)
		}
		if !found {
			continue
		}
		// Add the result to the list of index results
		results = append(results, models.IndexResult{
			Multihash: mhashes[i],
			Values:    values,
		})

		// Lookup provider info for each unique provider
		for j := range values {
			provID := values[j].ProviderID
			if _, found = providerSeen[provID]; found {
				continue
			}
			providerSeen[provID] = struct{}{}

			pinfo := h.registry.ProviderInfo(provID)
			if pinfo == nil {
				continue
			}

			providerResults = append(providerResults, pinfo.AddrInfo)
		}
	}

	return &models.FindResponse{
		IndexResults: results,
		Providers:    providerResults,
	}, nil
}
