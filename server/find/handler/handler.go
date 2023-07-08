package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	"github.com/ipni/go-libipni/apierror"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("indexer/find")

// avg_mh_size is a slight overcount over the expected size of a multihash as a
// way of estimating the number of entries in the primary value store.
const avg_mh_size = 40

// handler provides request handling functionality for the find server
// that is common to all protocols.
type Handler struct {
	indexer  indexer.Interface
	registry *registry.Registry
	stats    *cachedStats
}

func New(indexer indexer.Interface, reg *registry.Registry) *Handler {
	return &Handler{
		indexer:  indexer,
		registry: reg,
		stats:    newCachedStats(indexer, time.Hour),
	}
}

// Find reads from indexer core to populate a response from a list of
// multihashes.
func (h *Handler) Find(mhashes []multihash.Multihash) (*model.FindResponse, error) {
	results := make([]model.MultihashResult, 0, len(mhashes))
	provInfos := map[peer.ID]*registry.ProviderInfo{}

	for i := range mhashes {
		values, found, err := h.indexer.Get(mhashes[i])
		if err != nil {
			err = fmt.Errorf("failed to query multihash %s: %s", mhashes[i].B58String(), err)
			return nil, apierror.New(err, http.StatusInternalServerError)
		}
		if !found {
			continue
		}

		provResults := make([]model.ProviderResult, 0, len(values))
		for j := range values {
			iVal := values[j]
			provID := iVal.ProviderID
			pinfo := h.fetchProviderInfo(provID, iVal.ContextID, provInfos, true)

			if pinfo == nil {
				continue
			}

			// Adding the main provider
			provResult := providerResultFromValue(provID, iVal.ContextID, iVal.MetadataBytes, pinfo.AddrInfo.Addrs)
			provResults = append(provResults, provResult)

			if pinfo.ExtendedProviders == nil {
				continue
			}

			epRecord := pinfo.ExtendedProviders

			// If override is set to true at the context level then the chain
			// level EPs should be ignored for this context ID
			override := false

			// Adding context-level EPs if they exist
			if contextualEpRecord, ok := epRecord.ContextualProviders[string(iVal.ContextID)]; ok {
				override = contextualEpRecord.Override
				for _, epInfo := range contextualEpRecord.Providers {
					// Skippng the main provider's record if its metadata is
					// nil or is the same as the one retrieved from the
					// indexer, because such EP record does not advertise any
					// new protocol.
					if epInfo.PeerID == provID &&
						(len(epInfo.Metadata) == 0 || bytes.Equal(epInfo.Metadata, iVal.MetadataBytes)) {
						continue
					}
					provResult := createExtendedProviderResult(epInfo, iVal)
					provResults = append(provResults, *provResult)

				}
			}

			if override {
				continue
			}

			// Adding chain-level EPs if such exist
			for _, epInfo := range epRecord.Providers {
				// Skippng the main provider's record if its metadata is nil or
				// is the same as the one retrieved from the indexer, because
				// such EP record does not advertise any new protocol.
				if epInfo.PeerID == provID &&
					(len(epInfo.Metadata) == 0 || bytes.Equal(epInfo.Metadata, iVal.MetadataBytes)) {
					continue
				}
				provResult := createExtendedProviderResult(epInfo, iVal)
				provResults = append(provResults, *provResult)
			}

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

func (h *Handler) fetchProviderInfo(provID peer.ID, contextID []byte, provAddrs map[peer.ID]*registry.ProviderInfo, removeProviderContext bool) *registry.ProviderInfo {
	// Lookup provider info for each unique provider, look in local map
	// before going to registry.
	pinfo, ok := provAddrs[provID]
	if ok {
		return pinfo
	}
	pinfo, allowed := h.registry.ProviderInfo(provID)
	if pinfo == nil && removeProviderContext {
		// If provider not in registry, then provider was deleted.
		// Tell the indexed core to delete the contextID for the
		// deleted provider. Delete the contextID from the core,
		// because there is no way to delete all records for the
		// provider without a scan of the entire core valuestore.
		go func(provID peer.ID, contextID []byte) {
			err := h.indexer.RemoveProviderContext(provID, contextID)
			if err != nil {
				log.Errorw("Error removing provider context", "err", err)
			}
		}(provID, contextID)
		// If provider not in registry, do not return in result.
		return nil
	}
	// Omit provider info if not allowed or marked as inactive.
	if !allowed || pinfo.Inactive() {
		return nil
	}
	provAddrs[provID] = pinfo
	return pinfo
}

func (h *Handler) ListProviders() ([]byte, error) {
	infos := h.registry.AllProviderInfo()

	responses := make([]model.ProviderInfo, len(infos))
	for i, pInfo := range infos {
		responses[i] = *registry.RegToApiProviderInfo(pInfo)
	}

	return json.Marshal(responses)
}

func (h *Handler) GetProvider(providerID peer.ID) ([]byte, error) {
	info, allowed := h.registry.ProviderInfo(providerID)
	if info == nil || !allowed || info.Inactive() {
		return nil, nil
	}
	rsp := registry.RegToApiProviderInfo(info)
	return json.Marshal(rsp)
}

func (h *Handler) RefreshStats() {
	h.stats.refresh()
}

func (h *Handler) GetStats() ([]byte, error) {
	stats, err := h.stats.get()
	if err != nil {
		return nil, err
	}
	return model.MarshalStats(&stats)
}

func (h *Handler) Close() {
	h.stats.close()
}

func providerResultFromValue(provID peer.ID, contextID []byte, metadata []byte, addrs []multiaddr.Multiaddr) model.ProviderResult {
	return model.ProviderResult{
		ContextID: contextID,
		Metadata:  metadata,
		Provider: &peer.AddrInfo{
			ID:    provID,
			Addrs: addrs,
		},
	}
}

func createExtendedProviderResult(epInfo registry.ExtendedProviderInfo, iVal indexer.Value) *model.ProviderResult {
	metadata := epInfo.Metadata
	if metadata == nil {
		metadata = iVal.MetadataBytes
	}

	provResult := providerResultFromValue(epInfo.PeerID, iVal.ContextID, metadata, epInfo.Addrs)
	return &provResult
}
