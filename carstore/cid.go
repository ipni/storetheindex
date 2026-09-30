package carstore

import (
	"fmt"

	"github.com/ipfs/go-cid"
)

// verifyCID reports whether data hashes to c using c's own prefix.
func verifyCID(c cid.Cid, data []byte) error {
	if !c.Defined() {
		return ErrUnusableUndefinedCID
	}
	got, err := c.Prefix().Sum(data)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnusableCIDHash, err)
	}
	if !got.Equals(c) {
		return fmt.Errorf("%w: cid does not match data (got %s want %s)", ErrUnusableCIDMismatch, got, c)
	}
	return nil
}
