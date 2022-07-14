package schema

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/crypto"
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

// signaturePayload generates the data payload used to compute the Advertisement.Signature.
func signaturePayload(ad *Advertisement, oldFormat bool) ([]byte, error) {
	bindex := cid.Undef.Bytes()
	if ad.PreviousID != nil {
		bindex = ad.PreviousID.(cidlink.Link).Cid.Bytes()
	}
	ent := ad.Entries.(cidlink.Link).Cid.Bytes()

	var addrsLen int
	for _, addr := range ad.Addresses {
		addrsLen += len(addr)
	}

	// Signature data is previousID+entries+metadata+isRm
	var sigBuf bytes.Buffer
	sigBuf.Grow(len(bindex) + len(ent) + len(ad.Provider) + addrsLen + len(ad.Metadata) + 1)
	sigBuf.Write(bindex)
	sigBuf.Write(ent)
	sigBuf.WriteString(ad.Provider)
	for _, addr := range ad.Addresses {
		sigBuf.WriteString(addr)
	}
	sigBuf.Write(ad.Metadata)
	if ad.IsRm {
		sigBuf.WriteByte(1)
	} else {
		sigBuf.WriteByte(0)
	}

	// Generates the old (incorrect) data payload used for signature.  This is
	// only for compatibility with existing advertisements that have the old
	// signatures, and should be removed when no longer needed.
	if oldFormat {
		return multihash.Encode(sigBuf.Bytes(), multihash.SHA2_256)
	}

	return multihash.Sum(sigBuf.Bytes(), multihash.SHA2_256, -1)
}

// Sign signs an advertisement using the given private key.
func (ad *Advertisement) Sign(key crypto.PrivKey) error {
	advID, err := signaturePayload(ad, false)
	if err != nil {
		return err
	}
	envelope, err := record.Seal(&advSignatureRecord{advID: advID}, key)
	if err != nil {
		return err
	}

	sig, err := envelope.Marshal()
	if err != nil {
		return err
	}
	ad.Signature = sig
	return nil
}

// VerifySignature verifies that the advertisement has been signed and
// generated correctly.  Returns the peer ID of the signer.
//
// The signer may be different than the provider ID in the advertisement, so
// the caller will need to check if the signer is allowed to sign this
// advertisement.
func (ad *Advertisement) VerifySignature() (peer.ID, error) {
	// sigSize is the size of the current signature.  Any signature that is not
	// this size is the old signature format.
	const sigSize = 34

	// Consume envelope
	rec := &advSignatureRecord{}
	envelope, err := record.ConsumeTypedEnvelope(ad.Signature, rec)
	if err != nil {
		return "", err
	}

	// Calculate our own hash of the advertisement.
	oldFormat := len(rec.advID) != sigSize
	genID, err := signaturePayload(ad, oldFormat)
	if err != nil {
		return "", err
	}

	// Check that our own hash is equal to the hash from the signature.
	if !bytes.Equal(genID, rec.advID) {
		return "", errors.New("invalid signature")
	}

	// Get the peer ID that was used to sign the advertisement.  This may be
	// different than the provider ID, so caller will need to check if it was
	// allowed to sign this advertisement.
	signerID, err := peer.IDFromPublicKey(envelope.PublicKey)
	if err != nil {
		return "", fmt.Errorf("cannot convert public key to peer ID: %w", err)
	}

	if oldFormat {
		log.Warnw("advertisement has deprecated signature format", "signer", signerID)
	}

	return signerID, nil
}
