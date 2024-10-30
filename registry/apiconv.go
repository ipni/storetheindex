package registry

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// RegToApiProviderInfo converts provider info from registry to api objects.
func RegToApiProviderInfo(pi *ProviderInfo) *model.ProviderInfo {
	if pi == nil {
		return nil
	}

	apiPI := &model.ProviderInfo{
		AddrInfo: pi.AddrInfo,
		Lag:      pi.Lag,
		Inactive: pi.Inactive(),
	}
	if pi.LastAdvertisement != cid.Undef {
		apiPI.LastAdvertisement = pi.LastAdvertisement
		if !pi.LastAdvertisementTime.IsZero() {
			apiPI.LastAdvertisementTime = pi.LastAdvertisementTime.Format(time.RFC3339)
		}
	}
	if pi.LastError != "" {
		apiPI.LastError = pi.LastError
		if !pi.LastErrorTime.IsZero() {
			apiPI.LastErrorTime = pi.LastErrorTime.Format(time.RFC3339)
		}
	}

	if pi.Publisher.Validate() == nil && pi.PublisherAddr != nil {
		apiPI.Publisher = &peer.AddrInfo{
			ID:    pi.Publisher,
			Addrs: []multiaddr.Multiaddr{pi.PublisherAddr},
		}
	}

	if pi.FrozenAt != cid.Undef {
		apiPI.FrozenAt = pi.FrozenAt
	}
	// Provider is still frozen even if there is no FrozenAt CID.
	if !pi.FrozenAtTime.IsZero() {
		apiPI.FrozenAtTime = pi.FrozenAtTime.Format(time.RFC3339)
	}

	xpiToApi := func(xpis []ExtendedProviderInfo) ([]peer.AddrInfo, [][]byte) {
		if len(xpis) == 0 {
			return nil, nil
		}

		apiProvs := make([]peer.AddrInfo, len(xpis))
		var apiMetas = make([][]byte, len(xpis))

		for i := range xpis {
			apiProvs[i] = peer.AddrInfo{
				ID:    xpis[i].PeerID,
				Addrs: xpis[i].Addrs,
			}
			apiMetas[i] = xpis[i].Metadata
		}

		return apiProvs, apiMetas
	}

	if pi.ExtendedProviders != nil {
		xp := pi.ExtendedProviders
		apiProvs, apiMetas := xpiToApi(xp.Providers)

		var apiCtxProvs []model.ContextualExtendedProviders
		if len(xp.ContextualProviders) != 0 {
			apiCtxProvs = make([]model.ContextualExtendedProviders, len(xp.ContextualProviders))
			var i int
			for contextID, cxp := range xp.ContextualProviders {
				provs, metas := xpiToApi(cxp.Providers)
				apiCtxProvs[i] = model.ContextualExtendedProviders{
					Override:  cxp.Override,
					ContextID: contextID,
					Providers: provs,
					Metadatas: metas,
				}
				i++
			}
		}

		apiPI.ExtendedProviders = &model.ExtendedProviders{
			Providers:  apiProvs,
			Contextual: apiCtxProvs,
			Metadatas:  apiMetas,
		}
	}

	return apiPI
}

// apiToRegProviderInfo converts provider info from api to registry objects.
func apiToRegProviderInfo(apiPI *model.ProviderInfo) *ProviderInfo {
	if apiPI == nil {
		return nil
	}

	regInfo := &ProviderInfo{
		AddrInfo: apiPI.AddrInfo,
	}

	if apiPI.Publisher != nil && apiPI.Publisher.ID.Validate() == nil {
		regInfo.Publisher = apiPI.Publisher.ID
		if len(apiPI.Publisher.Addrs) == 0 {
			if apiPI.Publisher.ID == regInfo.AddrInfo.ID {
				// Publisher does not have addresses, so use provider address
				// if the publisher and provider ID are the same.
				regInfo.PublisherAddr = regInfo.AddrInfo.Addrs[0]
			}
		} else {
			regInfo.PublisherAddr = apiPI.Publisher.Addrs[0]
		}
	} else {
		regInfo.Publisher = regInfo.AddrInfo.ID
		regInfo.PublisherAddr = regInfo.AddrInfo.Addrs[0]
	}

	if apiPI.ExtendedProviders != nil {
		apiXP := apiPI.ExtendedProviders

		xpiToReg := func(providers []peer.AddrInfo, metadatas [][]byte) []ExtendedProviderInfo {
			if len(providers) == 0 {
				return nil
			}
			xpis := make([]ExtendedProviderInfo, len(providers))
			mdOk := len(metadatas) == len(providers)
			for i, addrInfo := range providers {
				var md []byte
				if mdOk {
					md = metadatas[i]
				}
				xpis[i] = ExtendedProviderInfo{
					PeerID:   addrInfo.ID,
					Addrs:    addrInfo.Addrs,
					Metadata: md,
				}
			}
			return xpis
		}

		regProvs := xpiToReg(apiXP.Providers, apiXP.Metadatas)
		var regCtxProvs map[string]ContextualExtendedProviders

		if len(apiXP.Contextual) != 0 {
			regCtxProvs = make(map[string]ContextualExtendedProviders, len(apiXP.Contextual))
			for _, cep := range apiXP.Contextual {
				regCtxProvs[cep.ContextID] = ContextualExtendedProviders{
					ContextID: []byte(cep.ContextID),
					Providers: xpiToReg(cep.Providers, cep.Metadatas),
					Override:  cep.Override,
				}
			}
		}

		if regProvs != nil || regCtxProvs != nil {
			regInfo.ExtendedProviders = &ExtendedProviders{
				Providers:           regProvs,
				ContextualProviders: regCtxProvs,
			}
		}
	}

	return regInfo
}
