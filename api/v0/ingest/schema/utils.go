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

// Linkproto is the ipld.LinkProtocol used for the ingestion protocol.
// Refer to it if you have encoding questions.
var Linkproto = cidlink.LinkPrototype{
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

// LinkAdvFromCid creates a link advertisement from a CID
func LinkAdvFromCid(c cid.Cid) Link_Advertisement {
	return &_Link_Advertisement{x: cidlink.Link{Cid: c}}
}

// ToCid converts a link to CID
func (l Link_Advertisement) ToCid() cid.Cid {
	return l.x.(cidlink.Link).Cid
}

// LinkContext returns a linkContext for the type of link
func (l Advertisement) LinkContext(ctx context.Context) ipld.LinkContext {
	return ipld.LinkContext{
		Ctx: context.WithValue(ctx, IsIndexKey, LinkContextValue(false)),
	}
}

// LinkIndexFromCid creates a link index from a CID
func LinkIndexFromCid(c cid.Cid) Link_Index {
	return &_Link_Index{x: cidlink.Link{Cid: c}}
}

// ToCid converts a link to CID
func (l Link_Index) ToCid() cid.Cid {
	return l.x.(cidlink.Link).Cid
}

// LinkContext returns a linkContext for the type of link
func (l Index) LinkContext(ctx context.Context) ipld.LinkContext {
	return ipld.LinkContext{
		Ctx: context.WithValue(ctx, IsIndexKey, LinkContextValue(true)),
	}
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

func newIndex(lsys ipld.LinkSystem, cidEntries List_CidEntry, carEntries List_CarEntry,
	previousIndex Link_Index) (Index, Link_Index, error) {
	// Create the index for this update from the entry list
	var index _Index
	var cidEnt _List_CidEntry
	var carEnt _List_CarEntry

	if cidEntries != nil {
		cidEnt = *cidEntries
	}
	if carEntries != nil {
		carEnt = *carEntries
	}
	// If genesis index
	if previousIndex == nil {
		index = _Index{
			CidEntries: cidEnt,
			CarEntries: carEnt,
		}
	} else {
		index = _Index{
			CidEntries: cidEnt,
			CarEntries: carEnt,
			Previous:   _Link_Index__Maybe{m: schema.Maybe_Value, v: *previousIndex},
		}
	}

	lnk, err := lsys.Store((&index).LinkContext(context.Background()), Linkproto, &index)
	if err != nil {

		return nil, nil, err
	}
	return &index, &_Link_Index{lnk}, err
}

// NewIndexFromCids creates a new Index with a single entry
// from a list of CIDs to add or to remove.
func NewIndexFromCids(
	lsys ipld.LinkSystem, cids []cid.Cid,
	rmCids []cid.Cid, metadata []byte,
	previousIndex Link_Index) (Index, Link_Index, error) {

	// Generate the entry and entry list from CIDs
	entries := make([]_CidEntry, 1)
	entries[0] = _CidEntry{
		Put:      _List_String__Maybe{m: schema.Maybe_Value, v: _List_String{cidsToString(cids)}},
		Remove:   _List_String__Maybe{m: schema.Maybe_Value, v: _List_String{cidsToString(rmCids)}},
		Metadata: _Bytes__Maybe{m: schema.Maybe_Value, v: _Bytes{x: metadata}},
	}
	lentries := _List_CidEntry{x: entries}

	return newIndex(lsys, &lentries, nil, previousIndex)
}

// NewIndexFromCarID creates a new Index with a single entry
// from a local CarID
func NewIndexFromCarID(
	lsys ipld.LinkSystem, putCarID cid.Cid,
	rmCarID cid.Cid, metadata []byte,
	previousIndex Link_Index) (Index, Link_Index, error) {

	// NOTE: Depending on the selector and the linkingSystem
	// used to follow this links, traversals may fail if
	// any of these links point to cid.Undef. If this is the case
	// try making the link nil if one of the CIDs is cid.Undef
	// or build using its prototype.
	putLink := _Link{x: cidlink.Link{Cid: putCarID}}
	rmLink := _Link{x: cidlink.Link{Cid: rmCarID}}

	// Generate the entry and entry list from CIDs
	entries := make([]_CarEntry, 1)
	entries[0] = _CarEntry{
		Put:      _Link__Maybe{m: schema.Maybe_Value, v: putLink},
		Remove:   _Link__Maybe{m: schema.Maybe_Value, v: rmLink},
		Metadata: _Bytes__Maybe{m: schema.Maybe_Value, v: _Bytes{x: metadata}},
	}
	lentries := _List_CarEntry{x: entries}

	return newIndex(lsys, nil, &lentries, previousIndex)
}

// NewIndexFromEntries creates an index from a list of entries
// Providerse can choose how to generate their entries.
func NewIndexFromEntries(
	lsys ipld.LinkSystem, cidEntries List_CidEntry, carEntries List_CarEntry,
	previousIndex Link_Index) (Index, Link_Index, error) {

	return newIndex(lsys, cidEntries, carEntries, previousIndex)
}

// NewAdvertisement creates a new advertisement without link to
// let developerse choose the linking strategy they want to follow
func NewAdvertisement(
	signKey crypto.PrivKey,
	previousAdvID []byte,
	indexID Link_Index,
	provider string) (Advertisement, error) {

	// Create advertisement
	return newAdvertisement(signKey, previousAdvID, indexID, provider)

}

// NewAdvertisementWithLink creates a new advertisement from an index
// with its corresponsing link.
func NewAdvertisementWithLink(
	lsys ipld.LinkSystem,
	signKey crypto.PrivKey,
	previousAdvID []byte,
	indexID Link_Index,
	provider string) (Advertisement, Link_Advertisement, error) {

	// Create advertisement
	adv, err := newAdvertisement(signKey, previousAdvID, indexID, provider)
	if err != nil {
		return nil, nil, err
	}
	// Generate link
	lnk, err := AdvertisementLink(lsys, adv)
	if err != nil {
		return nil, nil, err
	}

	return adv, lnk, nil
}

// AdvertisementLink generates a new link from an advertisemenet using a specific
// linkSystem
func AdvertisementLink(lsys ipld.LinkSystem, adv Advertisement) (Link_Advertisement, error) {
	lnk, err := lsys.Store(adv.LinkContext(context.Background()), Linkproto, adv)
	if err != nil {
		return nil, err
	}

	return &_Link_Advertisement{lnk}, nil
}

// NewAdvertisementWithLink creates a new advertisement from an index
// with its corresponsing link.
func newAdvertisement(
	signKey crypto.PrivKey,
	previousAdvID []byte,
	indexID Link_Index,
	provider string) (Advertisement, error) {

	ad := &_Advertisement{
		IndexID:    *indexID,
		PreviousID: _Bytes{x: previousAdvID},
		Provider:   _String{x: provider},
	}

	// Sign advertisement
	sig, err := signAdvertisement(signKey, ad)
	if err != nil {
		return nil, err
	}

	// Add signature
	ad.Signature = _Bytes{x: sig}

	return ad, nil
}
