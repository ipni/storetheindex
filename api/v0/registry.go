package v0

import (
	"fmt"

	"github.com/multiformats/go-multicodec"
)

type metadataRegistry map[multicodec.Code]func() ProtocolMetadata

var defaultRegistry metadataRegistry

// ErrProtocolAlreadyRegistered is returned when a protocol has already been registered
var ErrProtocolAlreadyRegistered = fmt.Errorf("protocol already registered")

func init() {
	defaultRegistry = make(metadataRegistry)
}

// RegisterMetadataProtocol allows interpretation of a given metadata transport protocol
func RegisterMetadataProtocol(factory func() ProtocolMetadata) error {
	instance := factory()
	code := instance.Protocol()
	if _, ok := defaultRegistry[code]; ok {
		return ErrProtocolAlreadyRegistered
	}
	defaultRegistry[code] = factory
	return nil
}
