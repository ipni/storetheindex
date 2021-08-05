package providers

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type Registry struct {
	rwmutex   sync.RWMutex
	providers map[peer.ID]*ProviderInfo
}

type ProviderInfo struct {
	Addresses []ma.Multiaddr
}

func NewRegistry() *Registry {
	return &Registry{
		providers: map[peer.ID]*ProviderInfo{},
	}
}

func (r *Registry) AddProviderAddress(providerID peer.ID, addr ma.Multiaddr) bool {
	r.rwmutex.Lock()
	defer r.rwmutex.Unlock()
	info, ok := r.providers[providerID]
	if !ok {
		r.providers[providerID] = &ProviderInfo{
			Addresses: []ma.Multiaddr{addr},
		}
		return true
	}
	for i := range info.Addresses {
		if addr == info.Addresses[i] {
			return false
		}
	}
	info.Addresses = append(info.Addresses, addr)
	return true
}

func (r *Registry) ProviderInfo(providerID peer.ID) (ProviderInfo, bool) {
	r.rwmutex.RLock()
	defer r.rwmutex.RUnlock()
	info, ok := r.providers[providerID]
	if !ok {
		return ProviderInfo{}, false
	}
	return *info, true
}
