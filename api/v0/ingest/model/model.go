package model

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/record"
)

func makeRequestEnvelop(rec record.Record, privateKey crypto.PrivKey) ([]byte, error) {
	envelope, err := record.Seal(rec, privateKey)
	if err != nil {
		return nil, fmt.Errorf("could not sign request: %s", err)
	}

	data, err := envelope.Marshal()
	if err != nil {
		return nil, fmt.Errorf("could not marshal request envelop: %s", err)
	}

	return data, nil
}
