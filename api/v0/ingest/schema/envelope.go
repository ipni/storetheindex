package schema

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
	mh "github.com/multiformats/go-multihash"
)

const (
	adSignatureCodec  = "/indexer/ingest/adSignature"
	adSignatureDomain = "indexer"
)

type advSignatureRecord struct {
	domain *string
	codec  []byte
	advID  []byte
}

func (r *advSignatureRecord) Domain() string {
	if r.domain != nil {
		return *r.domain
	}
	return adSignatureDomain
}

func (r *advSignatureRecord) Codec() []byte {
	if r.codec != nil {
		return r.codec
	}
	return []byte(adSignatureCodec)
}

func (r *advSignatureRecord) MarshalRecord() ([]byte, error) {
	return r.advID, nil
}

func (r *advSignatureRecord) UnmarshalRecord(buf []byte) error {
	r.advID = buf
	return nil
}

// Generates the data payload used for signature.
func signaturePayload(previousID Link_Advertisement, provider string, addrs []string, entries Link, metadata []byte, isRm bool) ([]byte, error) {
	bindex := cid.Undef.Bytes()
	lindex, err := previousID.AsLink()
	if err != nil {
		return nil, err
	}
	if lindex != nil {
		bindex = lindex.(cidlink.Link).Cid.Bytes()
	}
	lent, err := entries.AsLink()
	if err != nil {
		return nil, err
	}
	ent := lent.(cidlink.Link).Cid.Bytes()

	var addrsLen int
	for _, addr := range addrs {
		addrsLen = len(addr)
	}

	// Signature data is previousID+entries+metadata+isRm
	var sigBuf bytes.Buffer
	sigBuf.Grow(len(bindex) + len(ent) + len(provider) + addrsLen + len(metadata) + 1)
	sigBuf.Write(bindex)
	sigBuf.Write(ent)
	sigBuf.WriteString(provider)
	for _, addr := range addrs {
		sigBuf.WriteString(addr)
	}
	sigBuf.Write(metadata)
	if isRm {
		sigBuf.WriteByte(1)
	} else {
		sigBuf.WriteByte(0)
	}

	return mh.Sum(sigBuf.Bytes(), mhCode, -1)
}

// Signs advertisements using libp2p envelope
func signAdvertisement(privkey crypto.PrivKey, ad Advertisement) ([]byte, error) {
	previousID := ad.FieldPreviousID().v
	provider := ad.FieldProvider().x
	addrs, err := IpldToGoStrings(ad.FieldAddresses())
	if err != nil {
		return nil, err
	}
	isRm := ad.FieldIsRm().x
	entries := ad.FieldEntries()
	metadata := ad.FieldMetadata().x

	advID, err := signaturePayload(&previousID, provider, addrs, entries, metadata, isRm)
	if err != nil {
		return nil, err
	}
	envelope, err := record.Seal(&advSignatureRecord{advID: advID}, privkey)
	if err != nil {
		return nil, err
	}
	return envelope.Marshal()
}

// VerifyAdvertisement verifies that the advertisement has been signed and
// generated correctly.  Returns the peer ID of the signer.
func VerifyAdvertisement(ad Advertisement) (peer.ID, error) {
	previousID := ad.FieldPreviousID().v
	provider := ad.FieldProvider().x
	addrs, err := IpldToGoStrings(ad.FieldAddresses())
	if err != nil {
		return peer.ID(""), err
	}
	isRm := ad.FieldIsRm().x
	entries := ad.FieldEntries()
	metadata := ad.FieldMetadata().x
	sig := ad.FieldSignature().x

	genID, err := signaturePayload(&previousID, provider, addrs, entries, metadata, isRm)
	if err != nil {
		return peer.ID(""), err
	}

	// Consume envelope
	rec := &advSignatureRecord{}
	envelope, err := record.ConsumeTypedEnvelope(sig, rec)
	if err != nil {
		return peer.ID(""), err
	}
	if !bytes.Equal(genID, rec.advID) {
		return peer.ID(""), errors.New("envelope signed with the wrong ID")
	}

	signerID, err := peer.IDFromPublicKey(envelope.PublicKey)
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot convert public key to peer ID: %s", err)
	}

	return signerID, nil
}
