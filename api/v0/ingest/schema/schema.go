package schema

import (
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/schema"
	ns "github.com/ipni/go-libipni/ingest/schema"
)

const (
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.MaxContextIDLen instead
	MaxContextIDLen = ns.MaxContextIDLen
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.MaxMetadataLen instead
	MaxMetadataLen = ns.MaxMetadataLen
)

var (
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.NoEntries instead
	NoEntries cidlink.Link

	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.NoEntries instead
	Linkproto cidlink.LinkPrototype

	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.AdvertisementPrototype instead
	AdvertisementPrototype schema.TypedPrototype

	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.EntryChunkPrototype instead
	EntryChunkPrototype schema.TypedPrototype
)

func init() {
	NoEntries = ns.NoEntries
	Linkproto = ns.Linkproto
	AdvertisementPrototype = ns.AdvertisementPrototype
	EntryChunkPrototype = ns.EntryChunkPrototype
}
