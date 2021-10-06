package handler

import (
	"fmt"

	indexer "github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

// FinderHandler provides request handling functionality for the finder server
// that is common to all protocols.
type FinderHandler struct {
	indexer  indexer.Interface
	registry *registry.Registry
}

func NewFinderHandler(indexer indexer.Interface, registry *registry.Registry) *FinderHandler {
	return &FinderHandler{
		indexer:  indexer,
		registry: registry,
	}
}

// MakeFindResponse reads from indexer core to populate a response from a list
// of multihashes.
func (h *FinderHandler) MakeFindResponse(mhashes []multihash.Multihash) (*model.FindResponse, error) {
	results := make([]model.MultihashResult, 0, len(mhashes))
	provAddrs := map[peer.ID][]multiaddr.Multiaddr{}

	for i := range mhashes {
		values, found, err := h.indexer.Get(mhashes[i])
		if err != nil {
			return nil, syserr.New(fmt.Errorf("failed to query %q: %s", mhashes[i], err), 500)
		}
		if !found {
			continue
		}

		provResults := make([]model.ProviderResult, len(values))
		for j := range values {
			addrInfo := peer.AddrInfo{
				ID: values[j].ProviderID,
			}
			// Lookup provider info for each unique provider, look in local map
			// before going to registry.
			addrs, ok := provAddrs[addrInfo.ID]
			if !ok {
				pinfo := h.registry.ProviderInfo(addrInfo.ID)
				if pinfo != nil {
					addrs = pinfo.AddrInfo.Addrs
					provAddrs[addrInfo.ID] = addrs
				}
			}
			addrInfo.Addrs = addrs

			provResults[j] = model.ProviderResult{
				ContextID: values[j].ContextID,
				Metadata:  values[j].Metadata,
				Provider:  addrInfo,
			}
		}

		// Add the result to the list of index results.
		results = append(results, model.MultihashResult{
			Multihash:       mhashes[i],
			ProviderResults: provResults,
		})
	}

	return &model.FindResponse{
		MultihashResults: results,
	}, nil
}
