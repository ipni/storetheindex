package message_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/storetheindex/announce/message"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const (
	cidStr     = "QmPNHBy5h7f19yJDt7ip9TvmMRbqmYsa6aetkrsc1ghjLB"
	origPeerID = "12D3KooWBckWLKiYoUX4k3HTrbrSe4DD5SPNTKgP6vKTva1NaRkJ"
)

var adCid cid.Cid
var maddr1, maddr2 multiaddr.Multiaddr

func init() {
	var err error
	adCid, err = cid.Decode(cidStr)
	if err != nil {
		panic(err)
	}

	maddr1, err = multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/2701")
	if err != nil {
		panic(err)
	}

	maddr2, err = multiaddr.NewMultiaddr("/dns4/localhost/tcp/7663")
	if err != nil {
		panic(err)
	}
}

func TestCBOR(t *testing.T) {
	msg := message.Message{
		Cid:       adCid,
		ExtraData: []byte("t01000"),
		OrigPeer:  origPeerID,
	}
	msg.SetAddrs([]multiaddr.Multiaddr{maddr1, maddr2})

	buf := bytes.NewBuffer(nil)
	err := msg.MarshalCBOR(buf)
	require.NoError(t, err)

	var newMsg message.Message
	err = newMsg.UnmarshalCBOR(buf)
	require.NoError(t, err)

	require.Equal(t, msg, newMsg)
}

func TestJSON(t *testing.T) {
	msg := message.Message{
		Cid:       adCid,
		ExtraData: []byte("t01000"),
		OrigPeer:  origPeerID,
	}
	msg.SetAddrs([]multiaddr.Multiaddr{maddr1, maddr2})

	data, err := json.Marshal(&msg)
	require.NoError(t, err)

	var newMsg message.Message
	err = json.Unmarshal(data, &newMsg)
	require.NoError(t, err)

	require.Equal(t, msg, newMsg)
}
