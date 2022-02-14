package schema

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("indexer/schema")

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
func signaturePayload(previousID Link_Advertisement, provider string, addrs []string, entries Link, metadata []byte, isRm, oldFormat bool) ([]byte, error) {
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
		addrsLen += len(addr)
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

	// Generates the old (incorrect) data payload used for signature.  This is
	// only for compatability with existing advertisements that have the old
	// signatures, and should be removed when no longer needed.
	if oldFormat {
		return multihash.Encode(sigBuf.Bytes(), mhCode)
	}

	return multihash.Sum(sigBuf.Bytes(), mhCode, -1)
}

// SignAdvertisement signs an advertisement using the given private key.
func SignAdvertisement(privkey crypto.PrivKey, ad Advertisement) ([]byte, error) {
	previousID := ad.FieldPreviousID().v
	provider := ad.FieldProvider().x
	addrs, err := IpldToGoStrings(ad.FieldAddresses())
	if err != nil {
		return nil, err
	}
	isRm := ad.FieldIsRm().x
	entries := ad.FieldEntries()
	metadata := ad.FieldMetadata().x

	advID, err := signaturePayload(&previousID, provider, addrs, entries, metadata, isRm, false)
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
	// sigSize is the size of the current signature.  Any signature that is not
	// this size is the old signature format.
	const sigSize = 34

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

	// Consume envelope
	rec := &advSignatureRecord{}
	envelope, err := record.ConsumeTypedEnvelope(sig, rec)
	if err != nil {
		return peer.ID(""), err
	}

	oldFormat := len(rec.advID) != sigSize
	genID, err := signaturePayload(&previousID, provider, addrs, entries, metadata, isRm, oldFormat)
	if err != nil {
		return peer.ID(""), err
	}

	if !bytes.Equal(genID, rec.advID) {
		return peer.ID(""), errors.New("invalid signature")
	}

	signerID, err := peer.IDFromPublicKey(envelope.PublicKey)
	if err != nil {
		return peer.ID(""), fmt.Errorf("cannot convert public key to peer ID: %s", err)
	}

	if oldFormat {
		log.Warnw("advertisement has deprecated signature format", "signer", signerID)
	}

	return signerID, nil
}
