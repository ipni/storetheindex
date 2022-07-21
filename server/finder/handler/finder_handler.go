package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/filecoin-project/go-indexer-core"
	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/finder/model"
	"github.com/filecoin-project/storetheindex/internal/registry"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("indexer/finder")

// avg_mh_size is a slight overcount over the expected size of a multihash as a
// way of estimating the number of entries in the primary value store.
const avg_mh_size = 40

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

// Find reads from indexer core to populate a response from a list of
// multihashes.
func (h *FinderHandler) Find(mhashes []multihash.Multihash) (*model.FindResponse, error) {
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

		provResults := make([]model.ProviderResult, 0, len(values))
		for j := range values {
			provID := values[j].ProviderID
			// Lookup provider info for each unique provider, look in local map
			// before going to registry.
			addrs, ok := provAddrs[provID]
			if !ok {
				pinfo := h.registry.ProviderInfo(provID)
				if pinfo == nil {
					// If provider not in registry, then provider was deleted.
					// Tell the indexed core to delete the contextID for the
					// deleted provider. Delete the contextID from the core,
					// because there is no way to delete all records for the
					// provider without a scan of the entire core valuestore.
					go func(value indexer.Value) {
						err := h.indexer.RemoveProviderContext(value.ProviderID, value.ContextID)
						if err != nil {
							log.Errorw("Error removing provider context", "err", err)
						}
					}(values[j])
					// If provider not in registry, do not return in result.
					continue
				}
				addrs = pinfo.AddrInfo.Addrs
				provAddrs[provID] = addrs
			}

			provResult, err := providerResultFromValue(values[j], addrs)
			if err != nil {
				return nil, err
			}
			provResults = append(provResults, provResult)
		}

		// If there are no providers for this multihash, then do not return a
		// result for it.
		if len(provResults) == 0 {
			continue
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
		responses[i] = model.MakeProviderInfo(infos[i].AddrInfo, infos[i].LastAdvertisement,
			infos[i].LastAdvertisementTime, infos[i].Publisher, infos[i].PublisherAddr)
	}

	return json.Marshal(responses)
}

func (h *FinderHandler) GetProvider(providerID peer.ID) ([]byte, error) {
	info := h.registry.ProviderInfo(providerID)
	if info == nil {
		return nil, nil
	}

	rsp := model.MakeProviderInfo(info.AddrInfo, info.LastAdvertisement, info.LastAdvertisementTime, info.Publisher, info.PublisherAddr)

	return json.Marshal(&rsp)
}

func (h *FinderHandler) GetStats() ([]byte, error) {
	size, err := h.indexer.Size()
	if err != nil {
		return nil, err
	}

	s := model.Stats{
		EntriesEstimate: size / avg_mh_size,
	}

	return model.MarshalStats(&s)
}

func providerResultFromValue(value indexer.Value, addrs []multiaddr.Multiaddr) (model.ProviderResult, error) {
	return model.ProviderResult{
		ContextID: value.ContextID,
		Metadata:  value.MetadataBytes,
		Provider: peer.AddrInfo{
			ID:    value.ProviderID,
			Addrs: addrs,
		},
	}, nil
}
