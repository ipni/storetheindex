package dhash

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"lukechampine.com/blake3"
)

const (
	// nonceLen defines length of the nonce to use for AESGCM encryption
	nonceLen = 12
	// blake3HashLength defines length of the blake 3 hash
	blake3HashLength = 10
)

type Keyer struct {
	hasher *blake3.Hasher
}

func NewKeyer() *Keyer {
	vk := &Keyer{
		hasher: blake3.New(blake3HashLength, nil),
	}
	return vk
}

// Hash calculates blake3 hash over the payload
func (k *Keyer) Hash(payload []byte) ([]byte, error) {
	k.hasher.Reset()
	if _, err := k.hasher.Write(payload); err != nil {
		return nil, err
	}
	return k.hasher.Sum(nil), nil
}

// SecondSHA returns SHA256 over the payload
func SHA256(payload, dest []byte) []byte {
	h := sha256.New()
	h.Write(payload)
	return h.Sum(dest)
}

// SecondMultihash calculates SHA256 over the multihash and wraps it into another multihash with DBL_SHA256 codec
func SecondMultihash(mh multihash.Multihash) (multihash.Multihash, error) {
	digest := SHA256(mh, nil)
	mh, err := multihash.Encode(digest, multihash.DBL_SHA2_256)
	if err != nil {
		return nil, err
	}
	return mh, nil
}

// deriveKey derives encryptioin key from the passphrase using SHA256
func deriveKey(passphrase []byte) []byte {
	return SHA256(append([]byte("AESGCM"), passphrase...), nil)
}

// DecryptAES decrypts AES payload using the nonce and the passphrase
func DecryptAES(nonce, payload, passphrase []byte) ([]byte, error) {
	key := deriveKey(passphrase)
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}

	return aesgcm.Open(nil, nonce, payload, nil)
}

// encryptAES uses AESGCM to encrypt the payload with the passphrase and a nonce derived from it.
// returns nonce and encrypted bytes.
func EncryptAES(payload, passphrase []byte) ([]byte, []byte, error) {
	// Derive the encryption key
	derivedKey := deriveKey([]byte(passphrase))

	// Create initialization vector (nonse) to be used during encryption
	// Nonce is derived from the mulithash (passpharase) so that encrypted payloads
	// for the same multihash can be compared to each other without having to decrypt
	nonce := SHA256(passphrase, nil)[:nonceLen]

	// Create cypher and seal the data
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	encrypted := aesgcm.Seal(nil, nonce, []byte(payload), nil)

	return nonce, encrypted, nil
}

// DecryptValueKey decrypts the value key using the passphrase
func DecryptValueKey(valKey, passphrase []byte) ([]byte, error) {
	return DecryptAES(valKey[:nonceLen], valKey[nonceLen:], passphrase)
}

// EncryptValueKey encrypts raw value key using the passpharse
func EncryptValueKey(valKey, passphrase []byte) ([]byte, error) {
	nonce, encValKey, err := EncryptAES(valKey, passphrase)
	if err != nil {
		return nil, err
	}

	return append(nonce, encValKey...), nil
}

// CreateValueKey creates value key from peer ID and context ID
func CreateValueKey(pid peer.ID, ctxID []byte) []byte {
	return append([]byte(pid), ctxID...)
}

// SplitValueKey splits value key into original components
func SplitValueKey(valKey []byte) (peer.ID, []byte, error) {
	pid, err := peer.IDFromBytes(valKey)
	if err != nil {
		return "", nil, err
	}
	return pid, valKey[pid.Size():], nil
}
