package schema

import (
	_ "embed"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

const (
	// MaxContextIDLen specifies the maximum number of bytes accepted as Advertisement.ContextID.
	MaxContextIDLen = 64
	// MaxMetadataLen specifies the maximum number of bytes an advertisement metadata can contain.
	MaxMetadataLen = 1024 // 1KiB
)

var (
	// NoEntries is a special value used to explicitly indicate that an
	// advertisement does not have any entries. When isRm is true it and serves to
	// remove content by context ID, and when isRm is false it serves to update
	// metadata only.
	NoEntries cidlink.Link

	// Linkproto is the ipld.LinkProtocol used for the ingestion protocol.
	// Refer to it if you have encoding questions.
	Linkproto = cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagJson),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: 16,
		},
	}

	// AdvertisementPrototype represents the IPLD node prototype of Advertisement.
	//
	// See: bindnode.Prototype.
	AdvertisementPrototype schema.TypedPrototype

	// EntryChunkPrototype represents the IPLD node prototype of EntryChunk.
	//
	// See: bindnode.Prototype.
	EntryChunkPrototype schema.TypedPrototype

	//go:embed schema.ipldsch
	schemaBytes []byte
)

func init() {
	typeSystem, err := ipld.LoadSchemaBytes(schemaBytes)
	if err != nil {
		panic(fmt.Errorf("failed to load schema: %w", err))
	}
	AdvertisementPrototype = bindnode.Prototype((*Advertisement)(nil), typeSystem.TypeByName("Advertisement"))
	EntryChunkPrototype = bindnode.Prototype((*EntryChunk)(nil), typeSystem.TypeByName("EntryChunk"))

	// Define NoEntries as the CID of a sha256 hash of nil.
	m, err := multihash.Sum(nil, multihash.SHA2_256, 16)
	if err != nil {
		panic(fmt.Errorf("failed to sum NoEntries multihash: %w", err))
	}
	NoEntries = cidlink.Link{Cid: cid.NewCidV1(cid.Raw, m)}
}
