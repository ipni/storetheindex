package schema

import (
	"github.com/ipld/go-ipld-prime"
	ns "github.com/ipni/go-libipni/ingest/schema"
)

type (
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.ExtendedProvider instead
	ExtendedProvider = ns.ExtendedProvider
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.Provider instead
	Provider = ns.Provider
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.Advertisement instead
	Advertisement = ns.Advertisement
	// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.EntryChunk instead
	EntryChunk = ns.EntryChunk
)

// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.UnwrapAdvertisement instead
func UnwrapAdvertisement(node ipld.Node) (*Advertisement, error) {
	return ns.UnwrapAdvertisement(node)
}

// Deprecated: Use github.com/ipni/go-libipni/ingest/schema.UnwrapEntryChunk instead
func UnwrapEntryChunk(node ipld.Node) (*EntryChunk, error) {
	return ns.UnwrapEntryChunk(node)
}
