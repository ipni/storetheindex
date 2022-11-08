package gossiptopic

import (
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multiaddr"
)

var ErrBadEncoding = errors.New("invalid message encoding")

// Message is the payload of a gossip pubsub message.
type Message struct {
	Cid       cid.Cid
	Addrs     [][]byte
	ExtraData []byte
	// The OrigPeer field may or may not be present in the serialized data, and
	// the CBOR serializer/deserializer is able to detect that. Only messages
	// that are re-published by an indexer, for consumption by othen indexers,
	// contain this field.
	OrigPeer string
}

// SetAddrs writes a slice of Multiaddr into the Message as a slice of []byte.
func (m *Message) SetAddrs(addrs []multiaddr.Multiaddr) {
	m.Addrs = make([][]byte, len(addrs))
	for i, a := range addrs {
		m.Addrs[i] = a.Bytes()
	}
}

// GetAddrs reads the slice of Multiaddr that is stored in the Message as a
// slice of []byte.
func (m *Message) GetAddrs() ([]multiaddr.Multiaddr, error) {
	addrs := make([]multiaddr.Multiaddr, len(m.Addrs))
	for i := range m.Addrs {
		var err error
		addrs[i], err = multiaddr.NewMultiaddrBytes(m.Addrs[i])
		if err != nil {
			return nil, err
		}
	}
	return addrs, nil
}
