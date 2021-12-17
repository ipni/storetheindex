package lotus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/storetheindex/internal/registry/discovery"
	"github.com/filecoin-project/storetheindex/internal/syserr"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	jrpc "github.com/ybbus/jsonrpc/v2"
)

type Discoverer struct {
	gatewayURL string
}

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

// New creates a new lotus Discoverer
func NewDiscoverer(gateway string) (*Discoverer, error) {
	if gateway == "" {
		return nil, errors.New("empty gateway")
	}

	u := url.URL{
		Host:   gateway,
		Scheme: "https",
		Path:   "/rpc/v1",
	}

	return &Discoverer{
		gatewayURL: u.String(),
	}, nil
}

func (d *Discoverer) Discover(ctx context.Context, peerID peer.ID, minerAddr string) (*discovery.Discovered, error) {
	// Get miner info from lotus
	minerAddress, err := address.NewFromString(minerAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid provider filecoin address: %s", err)
	}

	jrpcClient := jrpc.NewClient(d.gatewayURL)

	var ets ExpTipSet
	err = jrpcClient.CallFor(&ets, "Filecoin.ChainHead")
	if err != nil {
		return nil, syserr.New(err, http.StatusBadGateway)
	}

	var minerInfo MinerInfo
	err = jrpcClient.CallFor(&minerInfo, "Filecoin.StateMinerInfo", minerAddress, ets.Cids)
	if err != nil {
		return nil, syserr.New(err, http.StatusBadGateway)
	}

	if minerInfo.PeerId == nil {
		return nil, errors.New("no peer id for miner")
	}
	if *minerInfo.PeerId != peerID {
		return nil, errors.New("provider id mismatch")
	}

	// Get miner peer ID and addresses from miner info
	addrInfo, err := d.getMinerPeerAddr(minerInfo)
	if err != nil {
		return nil, err
	}

	return &discovery.Discovered{
		AddrInfo: addrInfo,
		Type:     discovery.MinerType,
	}, nil
}

func (d *Discoverer) getMinerPeerAddr(minerInfo MinerInfo) (peer.AddrInfo, error) {
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
