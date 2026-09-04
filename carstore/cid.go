package carstore

import (
	"fmt"

	"github.com/ipfs/go-cid"
)

// verifyCID reports whether data hashes to c using c's own prefix.
func verifyCID(c cid.Cid, data []byte) error {
	if !c.Defined() {
		return fmt.Errorf("%w: undefined cid", ErrUnusable)
	}
	got, err := c.Prefix().Sum(data)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnusable, err)
	}
	if !got.Equals(c) {
		return fmt.Errorf("%w: cid does not match data (got %s want %s)", ErrUnusable, got, c)
	}
	return nil
}
