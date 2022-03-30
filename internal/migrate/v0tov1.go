package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/filecoin-project/storetheindex/internal/registry"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-core/peer"
)

const v0ProviderKeyPath = "/registry/pinfo"

type v0ProviderInfo struct {
	AddrInfo              peer.AddrInfo
	DiscoveryAddr         string    `json:",omitempty"`
	LastAdvertisement     cid.Cid   `json:",omitempty"`
	LastAdvertisementTime time.Time `json:",omitempty"`
	Publisher             peer.ID   `json:",omitempty"`
}

func (p *v0ProviderInfo) dsKey() datastore.Key {
	return datastore.NewKey(path.Join(v0ProviderKeyPath, p.AddrInfo.ID.String()))
}

func migrateV0ToV1(ctx context.Context, dstore datastore.Datastore) error {
	// Load all providers from the datastore.
	q := query.Query{
		Prefix: v0ProviderKeyPath,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	var converted []string
	for result := range results.Next() {
		if result.Error != nil {
			return fmt.Errorf("cannot read provider info: %s", result.Error)
		}

		v1Info, err := convertV0ToV1ProviderInfo(result.Entry.Value)
		if err != nil {
			return err
		}

		value, err := json.Marshal(v1Info)
		if err != nil {
			return fmt.Errorf("cannot marshal v1 provider data: %w", err)
		}

		if err = dstore.Put(ctx, v1Info.DsKey(), value); err != nil {
			return fmt.Errorf("could not write v1 provider data: %w", err)
		}

		converted = append(converted, result.Entry.Key)
	}
	results.Close()

	if len(converted) != 0 {
		if err = dstore.Sync(ctx, datastore.NewKey(registry.ProviderKeyPath)); err != nil {
			return err
		}

		// Delete source records.
		for _, k := range converted {
			dstore.Delete(ctx, datastore.RawKey(k))
		}

		log.Debugw("Converted records from version 0 to 1", "records", len(converted))
		return dstore.Sync(ctx, datastore.NewKey(v0ProviderKeyPath))
	}
	return nil
}

func convertV0ToV1ProviderInfo(value []byte) (*registry.ProviderInfo, error) {
	v0Info := new(v0ProviderInfo)
	err := json.Unmarshal(value, v0Info)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal version 0 provider data: %w", err)
	}

	v1Info := registry.ProviderInfo{
		AddrInfo:              v0Info.AddrInfo,
		DiscoveryAddr:         v0Info.DiscoveryAddr,
		LastAdvertisement:     v0Info.LastAdvertisement,
		LastAdvertisementTime: v0Info.LastAdvertisementTime,
	}

	if v0Info.Publisher == v1Info.AddrInfo.ID {
		v1Info.Publisher = &v1Info.AddrInfo
	} else if v0Info.Publisher.Validate() == nil {
		v1Info.Publisher = &peer.AddrInfo{
			ID: v0Info.Publisher,
		}
	}

	return &v1Info, nil
}

func revertV1ToV0(ctx context.Context, dstore datastore.Datastore) error {
	// Load all providers from the datastore.
	q := query.Query{
		Prefix: registry.ProviderKeyPath,
	}
	results, err := dstore.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	var converted []string
	for result := range results.Next() {
		if result.Error != nil {
			return fmt.Errorf("cannot read provider info: %s", result.Error)
		}

		v0Info, err := convertV1ToV0ProviderInfo(result.Entry.Value)
		if err != nil {
			return err
		}

		value, err := json.Marshal(v0Info)
		if err != nil {
			return fmt.Errorf("cannot marshal version 0 provider data: %w", err)
		}

		if err = dstore.Put(ctx, v0Info.dsKey(), value); err != nil {
			return fmt.Errorf("could not write version 0 provider data: %w", err)
		}

		converted = append(converted, result.Entry.Key)
	}
	results.Close()

	if len(converted) != 0 {
		if err = dstore.Sync(ctx, datastore.NewKey(registry.ProviderKeyPath)); err != nil {
			return err
		}

		// Delete source records.
		for _, k := range converted {
			dstore.Delete(ctx, datastore.RawKey(k))
		}

		log.Debugw("Converted records from version 1 to 0", "records", len(converted))
		return dstore.Sync(ctx, datastore.NewKey(registry.ProviderKeyPath))
	}
	return nil
}

func convertV1ToV0ProviderInfo(value []byte) (*v0ProviderInfo, error) {
	v1Info := new(registry.ProviderInfo)
	err := json.Unmarshal(value, v1Info)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal version 1 provider data: %w", err)
	}

	v0Info := v0ProviderInfo{
		AddrInfo:              v1Info.AddrInfo,
		DiscoveryAddr:         v1Info.DiscoveryAddr,
		LastAdvertisement:     v1Info.LastAdvertisement,
		LastAdvertisementTime: v1Info.LastAdvertisementTime,
	}

	if v1Info.Publisher != nil {
		v0Info.Publisher = v1Info.Publisher.ID
	}

	return &v0Info, nil
}
