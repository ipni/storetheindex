package handler

import (
	"fmt"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/models"
	"github.com/filecoin-project/storetheindex/internal/providers"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
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

// PopulateResponse reads from indexer core to populate a response from a
// list of CIDs.
func (h *FinderHandler) PopulateResponse(cids []cid.Cid) (*models.Response, error) {
	cidResults := make([]models.CidResult, 0, len(cids))
	var providerResults []peer.AddrInfo
	providerSeen := map[peer.ID]struct{}{}

	for i := range cids {
		values, found, err := h.indexer.Get(cids[i])
		if err != nil {
			return nil, syserr.New(fmt.Errorf("failed to query cid %q: %s", cids[i], err), 500)
		}
		if !found {
			continue
		}
		// Add the response to the list of CID responses
		cidResults = append(cidResults, models.CidResult{Cid: cids[i], Values: values})

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

	return &models.Response{
		CidResults: cidResults,
		Providers:  providerResults,
	}, nil
}
