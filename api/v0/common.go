package v0

import (
	"github.com/ipni/go-libipni/apierror"
)

// Deprecated: Use github.com/ipni/go-libipni/apierror.ErrorMessage instead.
type ErrorMessage = apierror.ErrorMessage

// Deprecated: Use github.com/ipni/go-libipni/apierror.EncodeError instead.
func EncodeError(err error) []byte {
	return apierror.EncodeError(err)
}

// Deprecated: Use github.com/ipni/go-libipni/apierror.DecodeError instead.
func DecodeError(data []byte) error {
	return apierror.DecodeError(data)
}
