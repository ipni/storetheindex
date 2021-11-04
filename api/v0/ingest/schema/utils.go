//go:generate go run gen.go .
package schema

import (
	"context"

	v0 "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/schema"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
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

var mhCode = multihash.Names["sha2-256"]

// LinkContextKey used to propagate link info through the linkSystem context
type LinkContextKey string

// LinkContextValue used to propagate link info through the linkSystem context
type LinkContextValue bool

const (
	// IsAdKey is a LinkContextValue that determines the schema type the
	// link belongs to. This is used to understand what callback to trigger
	// in the linksystem when we come across a specific linkType.
	IsAdKey = LinkContextKey("isAdLink")
)

func mhsToBytes(mhs []multihash.Multihash) []_Bytes {
	out := make([]_Bytes, len(mhs))
	for i := range mhs {
		out[i] = _Bytes{x: mhs[i]}
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
		Ctx: context.WithValue(ctx, IsAdKey, LinkContextValue(true)),
	}
}

// NewListOfMhs is a convenient method to create a new list of bytes
// from a list of multihashes that may be consumed by a linksystem.
func NewListOfMhs(lsys ipld.LinkSystem, mhs []multihash.Multihash) (ipld.Link, error) {
	cStr := &_List_Bytes{x: mhsToBytes(mhs)}
	return lsys.Store(ipld.LinkContext{}, Linkproto, cStr)
}

// NewListBytesFromMhs converts multihashes to a list of bytes
func NewListBytesFromMhs(mhs []multihash.Multihash) List_Bytes {
	return &_List_Bytes{x: mhsToBytes(mhs)}
}

// NewLinkedListOfMhs creates a new element of a linked list that
// can be used to paginate large lists.
func NewLinkedListOfMhs(lsys ipld.LinkSystem, mhs []multihash.Multihash, next ipld.Link) (ipld.Link, EntryChunk, error) {
	cStr := &_EntryChunk{
		Entries: _List_Bytes{x: mhsToBytes(mhs)},
	}
	// If no next in the list.
	if next == nil {
		cStr.Next = _Link_EntryChunk__Maybe{m: schema.Maybe_Absent}
	} else {
		cStr.Next = _Link_EntryChunk__Maybe{m: schema.Maybe_Value, v: _Link_EntryChunk{x: next}}
	}

	lnk, err := lsys.Store(ipld.LinkContext{}, Linkproto, cStr.Representation())
	return lnk, cStr, err
}

// NewAdvertisement creates a new advertisement without link to
// let developerse choose the linking strategy they want to follow
func NewAdvertisement(
	signKey crypto.PrivKey,
	previousID Link_Advertisement,
	entries ipld.Link,
	contextID []byte,
	metadata v0.Metadata,
	isRm bool,
	provider string,
	addrs []string) (Advertisement, error) {

	// Create advertisement
	return newAdvertisement(signKey, previousID, entries, contextID, metadata, isRm, provider, addrs)

}

// NewAdvertisementWithLink creates a new advertisement from an index
// with its corresponsing link.
func NewAdvertisementWithLink(
	lsys ipld.LinkSystem,
	signKey crypto.PrivKey,
	previousID Link_Advertisement,
	entries ipld.Link,
	contextID []byte,
	metadata v0.Metadata,
	isRm bool,
	provider string,
	addrs []string) (Advertisement, Link_Advertisement, error) {

	// Create advertisement
	adv, err := newAdvertisement(signKey, previousID, entries, contextID, metadata, isRm, provider, addrs)
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
	lnk, err := lsys.Store(adv.LinkContext(context.Background()), Linkproto, adv.Representation())
	if err != nil {
		return nil, err
	}

	return &_Link_Advertisement{lnk}, nil
}

// NewAdvertisementWithFakeSig creates a new advertisement from an index
// with its corresponsing link.
func NewAdvertisementWithFakeSig(
	lsys ipld.LinkSystem,
	signKey crypto.PrivKey,
	previousID Link_Advertisement,
	entries ipld.Link,
	contextID []byte,
	metadata v0.Metadata,
	isRm bool,
	provider string,
	addrs []string) (Advertisement, Link_Advertisement, error) {

	encMetadata, err := metadata.MarshalBinary()
	if err != nil {
		return nil, nil, err
	}

	ad := &_Advertisement{
		Provider:  _String{x: provider},
		Addresses: GoToIpldStrings(addrs),
		Entries:   _Link{x: entries},
		ContextID: _Bytes{x: contextID},
		Metadata:  _Bytes{x: encMetadata},
		IsRm:      _Bool{x: isRm},
	}
	if previousID == nil {
		ad.PreviousID = _Link_Advertisement__Maybe{m: schema.Maybe_Absent}
	} else {
		ad.PreviousID = _Link_Advertisement__Maybe{m: schema.Maybe_Value, v: *previousID}
	}

	// Add signature
	ad.Signature = _Bytes{x: []byte("InvalidSignature")}

	// Generate link
	lnk, err := AdvertisementLink(lsys, ad)
	if err != nil {
		return nil, nil, err
	}
	return ad, lnk, nil
}

// NewAdvertisementWithoutSign creates a new advertisement from an index
// with random signature.
func newAdvertisement(
	signKey crypto.PrivKey,
	previousID Link_Advertisement,
	entries ipld.Link,
	contextID []byte,
	metadata v0.Metadata,
	isRm bool,
	provider string,
	addrs []string) (Advertisement, error) {

	encMetadata, err := metadata.MarshalBinary()
	if err != nil {
		return nil, err
	}

	ad := &_Advertisement{
		Provider:  _String{x: provider},
		Addresses: GoToIpldStrings(addrs),
		Entries:   _Link{x: entries},
		ContextID: _Bytes{x: contextID},
		Metadata:  _Bytes{x: encMetadata},
		IsRm:      _Bool{x: isRm},
	}
	if previousID == nil {
		ad.PreviousID = _Link_Advertisement__Maybe{m: schema.Maybe_Absent}
	} else {
		ad.PreviousID = _Link_Advertisement__Maybe{m: schema.Maybe_Value, v: *previousID}
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

func IpldToGoStrings(listString List_String) ([]string, error) {
	ipldStrs := listString.x
	if len(ipldStrs) == 0 {
		return nil, nil
	}

	strs := make([]string, len(ipldStrs))
	for i := range ipldStrs {
		s, err := ipldStrs[i].AsString()
		if err != nil {
			return nil, err
		}
		strs[i] = s
	}
	return strs, nil
}

func GoToIpldStrings(strs []string) _List_String {
	ipldStrs := make([]_String, len(strs))
	for i := range strs {
		ipldStrs[i] = _String{strs[i]}
	}
	return _List_String{
		x: ipldStrs,
	}
}
