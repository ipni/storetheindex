package ingestion

import (
	"bytes"
	"errors"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
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
func signaturePayload(indexID Link_Index, provider string, previousAdvID []byte) ([]byte, error) {
	lindex, err := indexID.AsLink()
	if err != nil {
		return nil, err
	}
	bindex := lindex.(cidlink.Link).Cid.Bytes()
	// Signature data is indexID+provider+previousAdvID
	sigData := make([]byte, len(bindex)+len([]byte(provider))+len(previousAdvID))
	copy(sigData[:len(bindex)], bindex)
	copy(sigData[len(bindex):], []byte(provider))
	copy(sigData[len(bindex)+len(provider):], previousAdvID)

	return mh.Encode(sigData, mhCode)
}

// Signs advertisements using libp2p envelope
func signAdvertisement(privkey crypto.PrivKey, ad Advertisement) ([]byte, error) {
	index := ad.FieldIndexID()
	provider := ad.FieldProvider().x
	previous := ad.FieldPreviousID().x
	advID, err := signaturePayload(index, provider, previous)
	if err != nil {
		return nil, err
	}
	env, err := record.Seal(&advSignatureRecord{advID: advID}, privkey)
	if err != nil {
		return nil, err
	}
	return env.Marshal()
}

// VerifyAdvertisement verifies that the advertisement has been
// signed and generated correctly.
func VerifyAdvertisement(ad Advertisement) error {
	index := ad.FieldIndexID()
	provider := ad.FieldProvider().x
	previous := ad.FieldPreviousID().x
	sig := ad.FieldSignature().x

	genID, err := signaturePayload(index, provider, previous)
	if err != nil {
		return err
	}

	// Consume envelope
	rec := &advSignatureRecord{}
	_, err = record.ConsumeTypedEnvelope(sig, rec)
	if err != nil {
		return err
	}
	if !bytes.Equal(genID, rec.advID) {
		return errors.New("envelope signed with the wrong ID")
	}
	return nil
}
