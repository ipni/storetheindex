package schema

import (
	"errors"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/multiformats/go-multihash"
)

type (
	Advertisement struct {
		PreviousID ipld.Link
		Provider   string
		Addresses  []string
		Signature  []byte
		Entries    ipld.Link
		ContextID  []byte
		Metadata   []byte
		IsRm       bool
	}
	EntryChunk struct {
		Entries []multihash.Multihash
		Next    ipld.Link
	}
)

// ToNode converts this advertisement to its representation as an IPLD typed node.
//
// See: bindnode.Wrap.
func (a Advertisement) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&a, AdvertisementPrototype.Type()).Representation()
	return
}

// UnwrapAdvertisement unwraps the given node as an advertisement.
//
// Note that the node is reassigned to AdvertisementPrototype if its prototype is different.
// Therefore, it is recommended to load the node using the correct prototype initially
// function to avoid unnecessary node assignment.
func UnwrapAdvertisement(node ipld.Node) (*Advertisement, error) {
	// When an IPLD node is loaded using `Prototype.Any` unwrap with bindnode will not work.
	// Here we defensively check the prototype and wrap if needed, since:
	//   - linksystem in sti is passed into other libraries, and
	//   - for whatever reason clients of this package may load nodes using Prototype.Any.
	//
	// The code in this repo, however should load nodes with appropriate prototype and never trigger
	// this if statement.
	if node.Prototype() != AdvertisementPrototype {
		adBuilder := AdvertisementPrototype.NewBuilder()
		err := adBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = adBuilder.Build()
	}

	ad, ok := bindnode.Unwrap(node).(*Advertisement)
	if !ok || ad == nil {
		return nil, fmt.Errorf("unwrapped node does not match schema.Advertisement")
	}
	return ad, nil
}

func (a Advertisement) Validate() error {
	if len(a.ContextID) > MaxContextIDLen {
		return errors.New("context id too long")
	}

	if len(a.Metadata) > MaxMetadataLen {
		return errors.New("metadata too long")
	}

	return nil
}

// UnwrapEntryChunk unwraps the given node as an entry chunk.
//
// Note that the node is reassigned to EntryChunkPrototype if its prototype is different.
// Therefore, it is recommended to load the node using the correct prototype initially
// function to avoid unnecessary node assignment.
func UnwrapEntryChunk(node ipld.Node) (*EntryChunk, error) {
	// When an IPLD node is loaded using `Prototype.Any` unwrap with bindnode will not work.
	// Here we defensively check the prototype and wrap if needed, since:
	//   - linksystem in sti is passed into other libraries, and
	//   - for whatever reason clients of this package may load nodes using Prototype.Any.
	//
	// The code in this repo, however should load nodes with appropriate prototype and never trigger
	// this if statement.
	if node.Prototype() != EntryChunkPrototype {
		adBuilder := EntryChunkPrototype.NewBuilder()
		err := adBuilder.AssignNode(node)
		if err != nil {
			return nil, fmt.Errorf("faild to convert node prototype: %w", err)
		}
		node = adBuilder.Build()
	}
	chunk, ok := bindnode.Unwrap(node).(*EntryChunk)
	if !ok || chunk == nil {
		return chunk, fmt.Errorf("unwrapped node does not match schema.EntryChunk")
	}
	return chunk, nil
}

// ToNode converts this entry chunk to its representation as an IPLD typed node.
//
// See: bindnode.Wrap.
func (e EntryChunk) ToNode() (n ipld.Node, err error) {
	// TODO: remove the panic recovery once IPLD bindnode is stabilized.
	defer func() {
		if r := recover(); r != nil {
			err = toError(r)
		}
	}()
	n = bindnode.Wrap(&e, EntryChunkPrototype.Type()).Representation()
	return
}

func toError(r interface{}) error {
	switch x := r.(type) {
	case string:
		return errors.New(x)
	case error:
		return x
	default:
		return fmt.Errorf("unknown panic: %v", r)
	}
}
