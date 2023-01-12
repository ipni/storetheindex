package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipni/go-indexer-core"
	v0 "github.com/ipni/storetheindex/api/v0"
	"github.com/ipni/storetheindex/api/v0/finder/model"
	"github.com/ipni/storetheindex/internal/counter"
	"github.com/ipni/storetheindex/internal/registry"
	"github.com/libp2p/go-libp2p/core/peer"
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
	indexer     indexer.Interface
	registry    *registry.Registry
	indexCounts *counter.IndexCounts
	stats       *cachedStats
}

func NewFinderHandler(indexer indexer.Interface, registry *registry.Registry, indexCounts *counter.IndexCounts) *FinderHandler {
	return &FinderHandler{
		indexer:     indexer,
		registry:    registry,
		indexCounts: indexCounts,
		stats:       newCachedStats(indexer, time.Hour),
	}
}

// Find reads from indexer core to populate a response from a list of
// multihashes.
func (h *FinderHandler) Find(mhashes []multihash.Multihash) (*model.FindResponse, error) {
	resp := &model.FindResponse{}
	provInfos := map[peer.ID]*registry.ProviderInfo{}
	for _, mh := range mhashes {
		dmh, err := multihash.Decode(mh)
		if err != nil {
			log.Infow("Error decoding a multihash", "multihash", mh, "err", err)
			continue
		}
		if dmh.Code == multihash.DBL_SHA2_256 {
			mhr, err := h.encFind(mh)
			if err != nil {
				log.Infow("Error processing encfind request", "multihash", mh, "err", err)
				continue
			}
			if mhr != nil {
				resp.EncMultihashResults = append(resp.EncMultihashResults, *mhr)
			}
		} else {
			mhr, err := h.find(mh, provInfos)
			if err != nil {
				log.Infow("Error processing find request", "multihash", mh, "err", err)
				continue
			}
			if mhr != nil {
				resp.MultihashResults = append(resp.MultihashResults, *mhr)
			}
		}
	}

	return resp, nil
}

func (h *FinderHandler) find(mh multihash.Multihash, provInfos map[peer.ID]*registry.ProviderInfo) (*model.MultihashResult, error) {
	values, found, err := h.indexer.Get(mh)
	if err != nil {
		err = fmt.Errorf("failed to query multihash %s: %s", mh.B58String(), err)
		return nil, v0.NewError(err, http.StatusInternalServerError)
	}
	if !found {
		return nil, nil
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

		// If override is set to true at the context level then the chain level EPs should be ignored for this context ID
		override := false

		// Adding context-level EPs if they exist
		if contextualEpRecord, ok := epRecord.ContextualProviders[string(iVal.ContextID)]; ok {
			override = contextualEpRecord.Override
			for _, epInfo := range contextualEpRecord.Providers {
				// Skippng the main provider's record if its metadata is nil or the same to the one retrieved from the indexer,
				// because such EP record doesn't advertise any new protocol.
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
			// Skippng the main provider's record if its metadata is nil or the same to the one retrieved from the indexer,
			// because such EP record doesn't advertise any new protocol.
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
		return nil, nil
	}

	return &model.MultihashResult{
		Multihash:       mh,
		ProviderResults: provResults,
	}, nil
}

func (h *FinderHandler) encFind(mh multihash.Multihash) (*model.EncMultihashResult, error) {
	mhr := &model.EncMultihashResult{
		Multihash: mh,
	}
	vks, hasResult, err := h.indexer.GetValueKeys(mh)
	if err != nil {
		return nil, err
	}
	if !hasResult {
		return nil, nil
	}
	mhr.ValueKeys = append(mhr.ValueKeys, vks...)
	return mhr, nil
}

func (h *FinderHandler) fetchProviderInfo(provID peer.ID,
	contextID []byte,
	provAddrs map[peer.ID]*registry.ProviderInfo,
	removeProviderContext bool) *registry.ProviderInfo {
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

func (h *FinderHandler) ListProviders() ([]byte, error) {
	infos := h.registry.AllProviderInfo()

	responses := make([]model.ProviderInfo, len(infos))
	for i := range infos {
		var indexCount uint64
		if h.indexCounts != nil {
			var err error
			indexCount, err = h.indexCounts.Provider(infos[i].AddrInfo.ID)
			if err != nil {
				log.Errorw("Could not get provider index count", "err", err)
			}
		}
		pInfo := infos[i]
		responses[i] = model.MakeProviderInfo(pInfo.AddrInfo, pInfo.LastAdvertisement,
			pInfo.LastAdvertisementTime, pInfo.Publisher, pInfo.PublisherAddr, indexCount)

		responses[i].ExtendedProviders = makeExtendedProviders(pInfo)
	}

	return json.Marshal(responses)
}

func makeExtendedProviders(pInfo *registry.ProviderInfo) *model.ExtendedProviders {
	if pInfo.ExtendedProviders == nil {
		return nil
	}

	xProvs := &model.ExtendedProviders{}
	if len(pInfo.ExtendedProviders.Providers) > 0 {
		xProvs.Providers = make([]peer.AddrInfo, 0, len(pInfo.ExtendedProviders.Providers))
		for _, xp := range pInfo.ExtendedProviders.Providers {
			xProvs.Providers = append(xProvs.Providers, peer.AddrInfo{
				ID:    xp.PeerID,
				Addrs: xp.Addrs,
			})
		}
	}

	if len(pInfo.ExtendedProviders.ContextualProviders) == 0 {
		return xProvs
	}

	xProvs.Contextual = make([]model.ContextualExtendedProviders, 0, len(pInfo.ExtendedProviders.ContextualProviders))
	for _, registerXp := range pInfo.ExtendedProviders.ContextualProviders {
		modelXp := model.ContextualExtendedProviders{
			Override:  registerXp.Override,
			ContextID: string(registerXp.ContextID),
			Providers: make([]peer.AddrInfo, 0, len(registerXp.Providers)),
		}
		for _, p := range registerXp.Providers {
			modelXp.Providers = append(modelXp.Providers, peer.AddrInfo{
				ID:    p.PeerID,
				Addrs: p.Addrs,
			})
		}
		xProvs.Contextual = append(xProvs.Contextual, modelXp)
	}

	return xProvs
}

func (h *FinderHandler) GetProvider(providerID peer.ID) ([]byte, error) {
	info, allowed := h.registry.ProviderInfo(providerID)
	if info == nil || !allowed || info.Inactive() {
		return nil, nil
	}

	var indexCount uint64
	if h.indexCounts != nil {
		var err error
		indexCount, err = h.indexCounts.Provider(providerID)
		if err != nil {
			log.Errorw("Could not get provider index count", "err", err)
		}
	}
	rsp := model.MakeProviderInfo(info.AddrInfo, info.LastAdvertisement, info.LastAdvertisementTime, info.Publisher, info.PublisherAddr, indexCount)
	rsp.ExtendedProviders = makeExtendedProviders(info)

	return json.Marshal(&rsp)
}

func (h *FinderHandler) RefreshStats() {
	h.stats.refresh()
}

func (h *FinderHandler) GetStats() ([]byte, error) {
	stats, err := h.stats.get()
	if err != nil {
		return nil, err
	}
	return model.MarshalStats(&stats)
}

func (h *FinderHandler) Close() {
	h.stats.close()
}

func providerResultFromValue(provID peer.ID, contextID []byte, metadata []byte, addrs []multiaddr.Multiaddr) model.ProviderResult {
	return model.ProviderResult{
		ContextID: contextID,
		Metadata:  metadata,
		Provider: peer.AddrInfo{
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
