package ingestion

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/schema"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

var linkproto = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.DagJson),
		MhType:   uint64(multicodec.Sha2_256),
		MhLength: 16,
	},
}

var mhCode = mh.Names["sha2-256"]

func cidsToString(cids []cid.Cid) []_String {
	out := make([]_String, len(cids))
	for i := range cids {
		out[i] = _String{x: cids[i].String()}
	}
	return out
}

// LinkContextKey used to propagate link info through the linkSystem context
type LinkContextKey string

// LinkContextValue used to propagate link info through the linkSystem context
type LinkContextValue bool

const (
	// IsIndexKey is a LinkContextValue that determines the schema type the
	// link belongs to. This is used to support different datastores for
	// the different type of schema types.
	IsIndexKey = LinkContextKey("isIndexLink")
)

func newIndex(lsys ipld.LinkSystem, lentries _List_Entry, previousIndex Link_Index) (Index, Link_Index, error) {
	// Create the index for this update from the entry list
	var index _Index
	// If genesis index
	if previousIndex == nil {
		index = _Index{
			Entries: lentries,
		}
	} else {
		index = _Index{
			Entries:  lentries,
			Previous: _Link_Index__Maybe{m: schema.Maybe_Value, v: *previousIndex},
		}
	}

	lnk, err := lsys.Store(ipld.LinkContext{
		Ctx: context.WithValue(context.Background(), IsIndexKey, LinkContextValue(true))},
		linkproto, &index)
	if err != nil {

		return nil, nil, err
	}
	return &index, &_Link_Index{lnk}, err
}

// NewSingleEntryIndex creates a new Index with a single entry
// from a list of CIDs to add or to remove.
func NewSingleEntryIndex(
	lsys ipld.LinkSystem, cids []cid.Cid,
	rmCids []cid.Cid, metadata []byte,
	previousIndex Link_Index) (Index, Link_Index, error) {

	// Generate the entry and entry list from CIDs
	entries := make([]_Entry, 1)
	entries[0] = _Entry{
		Cids:     _List_String__Maybe{m: schema.Maybe_Value, v: _List_String{cidsToString(cids)}},
		RmCids:   _List_String__Maybe{m: schema.Maybe_Value, v: _List_String{cidsToString(rmCids)}},
		Metadata: _Bytes__Maybe{m: schema.Maybe_Value, v: _Bytes{x: metadata}},
	}
	lentries := _List_Entry{x: entries}

	return newIndex(lsys, lentries, previousIndex)
}

// NewIndexFromEntries creates an index from a list of entries
// Providerse can choose how to generate their entries.
func NewIndexFromEntries(
	lsys ipld.LinkSystem, entries List_Entry,
	previousIndex Link_Index) (Index, Link_Index, error) {

	return newIndex(lsys, *entries, previousIndex)
}

// NewAdvertisement creates a new advertisement from an index link.
func NewAdvertisement(
	lsys ipld.LinkSystem,
	signKey crypto.PrivKey,
	previousAdvID []byte,
	indexID Link_Index,
	provider string, graphSupport bool) (Advertisement, Link_Advertisement, error) {

	advID, err := genAdvertisementID(indexID, provider, previousAdvID)
	if err != nil {
		return nil, nil, err
	}
	sig, err := signAdvertisement(signKey, advID)
	if err != nil {
		return nil, nil, err
	}

	adv := _Advertisement{
		ID:           _Bytes{x: advID},
		IndexID:      *indexID,
		Signature:    _Bytes{x: sig},
		PreviousID:   _Bytes{x: previousAdvID},
		Provider:     _String{x: provider},
		GraphSupport: _Bool{x: graphSupport},
	}
	lnk, err := lsys.Store(ipld.LinkContext{
		Ctx: context.WithValue(context.Background(), IsIndexKey, LinkContextValue(false))},
		linkproto, &adv)
	if err != nil {
		return nil, nil, err
	}

	return &adv, &_Link_Advertisement{lnk}, nil
}
