package schema

import (
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

func NewListOfCids(lsys ipld.LinkSystem, cids []cid.Cid) (ipld.Link, error) {
	cStr := &_List_String{x: cidsToString(cids)}
	return lsys.Store(ipld.LinkContext{}, Linkproto, cStr)
}

// NewAdvertisement creates a new advertisement without link to
// let developerse choose the linking strategy they want to follow
func NewAdvertisement(
	signKey crypto.PrivKey,
	previousID Link_Advertisement,
	entries ipld.Link,
	metadata []byte,
	isRm bool,
	provider string) (Advertisement, error) {

	// Create advertisement
	return newAdvertisement(signKey, previousID, entries, metadata, isRm, provider)

}

// NewAdvertisementWithLink creates a new advertisement from an index
// with its corresponsing link.
func NewAdvertisementWithLink(
	lsys ipld.LinkSystem,
	signKey crypto.PrivKey,
	previousID Link_Advertisement,
	entries ipld.Link,
	metadata []byte,
	isRm bool,
	provider string) (Advertisement, Link_Advertisement, error) {

	// Create advertisement
	adv, err := newAdvertisement(signKey, previousID, entries, metadata, isRm, provider)
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
	lnk, err := lsys.Store(ipld.LinkContext{}, Linkproto, adv)
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
	metadata []byte,
	isRm bool,
	provider string) (Advertisement, Link_Advertisement, error) {

	var ad Advertisement
	if previousID != nil {
		ad = &_Advertisement{
			PreviousID: _Link_Advertisement__Maybe{m: schema.Maybe_Value, v: *previousID},
			Provider:   _String{x: provider},
			Entries:    _Link{x: entries},
			Metadata:   _Bytes{x: metadata},
			IsRm:       _Bool{x: isRm},
		}
	} else {
		ad = &_Advertisement{
			PreviousID: _Link_Advertisement__Maybe{m: schema.Maybe_Absent},
			Provider:   _String{x: provider},
			Entries:    _Link{x: entries},
			Metadata:   _Bytes{x: metadata},
			IsRm:       _Bool{x: isRm},
		}
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
	metadata []byte,
	isRm bool,
	provider string) (Advertisement, error) {

	var ad Advertisement
	if previousID != nil {
		ad = &_Advertisement{
			PreviousID: _Link_Advertisement__Maybe{m: schema.Maybe_Value, v: *previousID},
			Provider:   _String{x: provider},
			Entries:    _Link{x: entries},
			Metadata:   _Bytes{x: metadata},
			IsRm:       _Bool{x: isRm},
		}
	} else {
		ad = &_Advertisement{
			PreviousID: _Link_Advertisement__Maybe{m: schema.Maybe_Absent},
			Provider:   _String{x: provider},
			Entries:    _Link{x: entries},
			Metadata:   _Bytes{x: metadata},
			IsRm:       _Bool{x: isRm},
		}
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
