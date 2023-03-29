package v0

import (
	"github.com/ipni/go-libipni/apierror"
)

// Deprecated: Use github.com/ipni/go-libipni/apierror.Error instead.
type Error = apierror.Error

// Deprecated: Use github.com/ipni/go-libipni/apierror.New instead.
func NewError(err error, status int) *Error {
	return apierror.New(err, status)
}
