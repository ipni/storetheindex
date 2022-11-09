package httpsync

import (
	"bytes"
	"errors"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	ic "github.com/libp2p/go-libp2p/core/crypto"
)

var typeSystem *schema.TypeSystem = createTypeSystem()

func createTypeSystem() *schema.TypeSystem {
	ts := schema.TypeSystem{}
	ts.Init()
	ts.Accumulate(schema.SpawnBytes("Bytes"))
	ts.Accumulate(schema.SpawnLink("Link"))
	ts.Accumulate(schema.SpawnStruct("SignedHead",
		[]schema.StructField{
			schema.SpawnStructField("head", "Link", false, false),
			schema.SpawnStructField("sig", "Bytes", false, false),
			schema.SpawnStructField("pubkey", "Bytes", false, false),
		},
		schema.SpawnStructRepresentationMap(nil),
	))

	return &ts
}

func SignedHeadSchema() schema.Type {
	return typeSystem.TypeByName("SignedHead")
}

// signedHead is the signed envelope of the head CID. It includes the
// public key of the signer so the receiver can verify it and convert it to a
// peer id. Note, the receiver is not required to use the provided public key.
type signedHead struct {
	Head   cidlink.Link
	Sig    []byte
	Pubkey []byte
}

// newEncodedSignedHead returns a new encoded SignedHead
func newEncodedSignedHead(cid cid.Cid, privKey ic.PrivKey) ([]byte, error) {
	sig, err := privKey.Sign(cid.Bytes())
	if err != nil {
		return nil, err
	}

	pubKeyBytes, err := ic.MarshalPublicKey(privKey.GetPublic())
	if err != nil {
		return nil, err
	}

	envelop := &signedHead{
		Head:   cidlink.Link{Cid: cid},
		Sig:    sig,
		Pubkey: pubKeyBytes,
	}
	node := bindnode.Wrap(envelop, SignedHeadSchema())
	var buf bytes.Buffer
	err = dagjson.Encode(node.Representation(), &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// openSignedHead returns the cid from the encoded signed head given a public key
func openSignedHead(pubKey ic.PubKey, SignedHead io.Reader) (cid.Cid, error) {
	envelop, err := decodeEnvelope(SignedHead)
	if err != nil {
		return cid.Undef, err
	}
	return openSignedHeadEnvelop(pubKey, *envelop)
}

// openSignedHeadWithIncludedPubKey verifies the signature with the
// included public key, then returns the public key and cid. The caller can
// use this public key to derive the signer's peer id.
func openSignedHeadWithIncludedPubKey(SignedHead io.Reader) (ic.PubKey, cid.Cid, error) {
	envelop, err := decodeEnvelope(SignedHead)
	if err != nil {
		return nil, cid.Undef, err
	}

	pubKey, err := ic.UnmarshalPublicKey(envelop.Pubkey)
	if err != nil {
		return nil, cid.Undef, err
	}

	cid, err := openSignedHeadEnvelop(pubKey, *envelop)
	return pubKey, cid, err
}

func decodeEnvelope(SignedHeadReader io.Reader) (*signedHead, error) {
	var envelop *signedHead
	proto := bindnode.Prototype((*signedHead)(nil), SignedHeadSchema())
	builder := proto.NewBuilder()
	err := dagjson.Decode(builder, SignedHeadReader)
	if err != nil {
		return envelop, err
	}
	envelop = bindnode.Unwrap(builder.Build()).(*signedHead)
	return envelop, nil
}

func openSignedHeadEnvelop(pubKey ic.PubKey, envelop signedHead) (cid.Cid, error) {
	ok, err := pubKey.Verify(envelop.Head.Bytes(), envelop.Sig)
	if err != nil {
		return cid.Undef, err
	}

	if !ok {
		return cid.Undef, errors.New("invalid signature")
	}

	return envelop.Head.Cid, err
}
