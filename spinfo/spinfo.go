package spinfo

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	jrpc "github.com/ybbus/jsonrpc/v2"
)

type ExpTipSet struct {
	Cids []cid.Cid
	//Blocks []*BlockHeader
	//Height abi.ChainEpoch
	Blocks []interface{}
	Height int64
}

type MinerInfo struct {
	Owner                      address.Address
	Worker                     address.Address
	NewWorker                  address.Address
	ControlAddresses           []address.Address
	WorkerChangeEpoch          int64
	PeerId                     *peer.ID
	Multiaddrs                 [][]byte
	WindowPoStProofType        int64
	SectorSize                 uint64
	WindowPoStPartitionSectors uint64
	ConsensusFaultElapsed      int64
}

func SPAddrInfo(ctx context.Context, gateway, spID string) (peer.AddrInfo, error) {
	if gateway == "" {
		return peer.AddrInfo{}, errors.New("empty gateway")
	}

	// Get SP info from lotus
	spAddr, err := address.NewFromString(spID)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("invalid storage provider id: %w", err)
	}

	gwURL := url.URL{
		Host:   gateway,
		Scheme: "https",
		Path:   "/rpc/v1",
	}
	jrpcClient := jrpc.NewClient(gwURL.String())

	var ets ExpTipSet
	err = jrpcClient.CallFor(&ets, "Filecoin.ChainHead")
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("cannot get chain head from gateway: %w", err)
	}

	var minerInfo MinerInfo
	err = jrpcClient.CallFor(&minerInfo, "Filecoin.StateMinerInfo", spAddr, ets.Cids)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("cannot get miner infor from gateway: %w", err)
	}

	if minerInfo.PeerId == nil {
		return peer.AddrInfo{}, errors.New("no peer id for miner")
	}

	multiaddrs := make([]multiaddr.Multiaddr, 0, len(minerInfo.Multiaddrs))
	for _, a := range minerInfo.Multiaddrs {
		maddr, err := multiaddr.NewMultiaddrBytes(a)
		if err != nil {
			continue
		}
		multiaddrs = append(multiaddrs, maddr)
	}

	return peer.AddrInfo{
		ID:    *minerInfo.PeerId,
		Addrs: multiaddrs,
	}, nil
}
