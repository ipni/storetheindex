package lotus

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/storetheindex/internal/providers/discovery"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multiaddr"
	// If it is necessary to have lotus verify a signature
	//"github.com/filecoin-project/go-state-types/crypto"
	//"github.com/filecoin-project/lotus/tree/master/lib/sigs"
)

type Discovery struct {
	node   api.Gateway
	closer *nodeCloser
}

func (d *Discovery) Close() error {
	return d.closer.Close()
}

func (d *Discovery) Discover(ctx context.Context, peerID peer.ID, minerAddr string, data, signature []byte) (*discovery.Discovered, error) {
	tipSet, err := d.node.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot get filecoin chain head: %s", err)
	}
	tipSetKey := tipSet.Key()

	// Get miner info from lotus
	minerAddress, err := address.NewFromString(minerAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid provider filecoin address: %s", err)
	}
	minerInfo, err := d.node.StateMinerInfo(ctx, minerAddress, tipSetKey)
	if err != nil {
		return nil, err
	}

	if minerInfo.PeerId == nil {
		return nil, errors.New("no peer id for miner")
	}
	if *minerInfo.PeerId != peerID {
		return nil, fmt.Errorf("id mismatch for provider %q, requested: %q, response: %q", minerAddr, peerID, minerInfo.PeerId)
	}

	// Get miner peer ID and addresses from miner info
	addrInfo, err := d.getMinerPeerAddr(minerInfo)
	if err != nil {
		return nil, err
	}

	// TODO: Determine if this is needed
	//
	// Verifying the miner's signature is commented out as it may not be
	// needed, so long as the peerID matches the one sent in the request, and
	// the signature made by the peerID is valid.
	/*
		sig := new(crypto.Signature)
		err = sig.UnmarshalBinary(signature)
		if err != nil {
			return err
		}

		fcAddr, err := d.node.StateAccountKey(ctx, minerInfo.Worker, tipSetKey)
		if err != nil {
			return fmt.Errorf("cannot get miner worker key: %s", err)
		}

		err = sigs.Verify(sig, fcAddr, data)
		if err != nil {
			return fmt.Errorf("cannot verify miner signature: %s", err)
		}
	*/
	return &discovery.Discovered{
		AddrInfo: addrInfo,
		Type:     discovery.MinerType,
	}, nil
}

func (d *Discovery) getMinerPeerAddr(minerInfo miner.MinerInfo) (peer.AddrInfo, error) {
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
