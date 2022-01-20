package model

import (
	"encoding/json"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

// SyncRequest is the client request send by end user clients
type SyncRequest struct {
	PeerAddr multiaddr.Multiaddr
	SyncCid  cid.Cid
}

// Serialization crrently uses JSON, but could use anything else.
//
// NOTE: Consider using other serialization formats?  We could maybe use IPLD
// schemas instead of structs for requests and response so we have any codec by
// design.
func MarshalSyncRequest(r *SyncRequest) ([]byte, error) {
	return json.Marshal(r)
}

// UnmarshalSyncRequest de-serializes the request.
func UnmarshalSyncRequest(b []byte) (*SyncRequest, error) {
	ma, _ := multiaddr.NewMultiaddr("/tcp/0")
	r := &SyncRequest{
		PeerAddr: ma,
	}
	err := json.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}
