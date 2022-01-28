package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/internal/registry"
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
			err = fmt.Errorf("failed to query %q: %s", mhashes[i], err)
			return nil, v0.NewError(err, http.StatusInternalServerError)
		}
		if !found {
			continue
		}

		provResults := make([]model.ProviderResult, len(values))
		for j := range values {
			provID := values[j].ProviderID
			// Lookup provider info for each unique provider, look in local map
			// before going to registry.
			addrs, ok := provAddrs[provID]
			if !ok {
				pinfo := h.registry.ProviderInfo(provID)
				if pinfo != nil {
					addrs = pinfo.AddrInfo.Addrs
					provAddrs[provID] = addrs
				}
			}

			provResults[j], err = providerResultFromValue(values[j], addrs)
			if err != nil {
				return nil, err
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

func (h *FinderHandler) ListProviders() ([]byte, error) {
	infos := h.registry.AllProviderInfo()

	responses := make([]model.ProviderInfo, len(infos))
	for i := range infos {
		responses[i] = model.MakeProviderInfo(infos[i].AddrInfo, infos[i].LastAdvertisement, infos[i].LastAdvertisementTime)
	}

	return json.Marshal(responses)
}

func (h *FinderHandler) GetProvider(providerID peer.ID) ([]byte, error) {
	info := h.registry.ProviderInfo(providerID)
	if info == nil {
		return nil, nil
	}

	rsp := model.MakeProviderInfo(info.AddrInfo, info.LastAdvertisement, info.LastAdvertisementTime)

	return json.Marshal(&rsp)
}

func providerResultFromValue(value indexer.Value, addrs []multiaddr.Multiaddr) (model.ProviderResult, error) {
	var metadata v0.Metadata
	err := metadata.UnmarshalBinary(value.MetadataBytes)
	if err != nil {
		return model.ProviderResult{}, fmt.Errorf("could not decode metadata: %s", err)
	}

	return model.ProviderResult{
		ContextID: value.ContextID,
		Metadata:  metadata,
		Provider: peer.AddrInfo{
			ID:    value.ProviderID,
			Addrs: addrs,
		},
	}, nil
}
