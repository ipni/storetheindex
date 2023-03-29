// Deprecated: The same functionality is provided by package
// github.com/ipni/go-libipni/find/model.
package model

import (
	"github.com/ipni/go-libipni/find/model"
)

// Deprecated: Use github.com/ipni/go-libipni/find/model.FindRequest instead
type FindRequest = model.FindRequest

// Deprecated: Use github.com/ipni/go-libipni/find/model.ProviderResult instead
type ProviderResult = model.ProviderResult

// Deprecated: Use github.com/ipni/go-libipni/find/model.MultihashResult instead
type MultihashResult = model.MultihashResult

// Deprecated: Use github.com/ipni/go-libipni/find/model.FindResponse instead
type FindResponse = model.FindResponse

// Deprecated: Use github.com/ipni/go-libipni/find/model.EncryptedMultihashResult instead
type EncryptedMultihashResult = model.EncryptedMultihashResult

// Deprecated: Use github.com/ipni/go-libipni/find/model.MarshalFindRequest instead
func MarshalFindRequest(r *FindRequest) ([]byte, error) {
	return model.MarshalFindRequest(r)
}

// Deprecated: Use github.com/ipni/go-libipni/find/model. instead
func UnmarshalFindRequest(b []byte) (*FindRequest, error) {
	return model.UnmarshalFindRequest(b)
}

// Deprecated: Use github.com/ipni/go-libipni/find/model. instead
func MarshalFindResponse(r *FindResponse) ([]byte, error) {
	return model.MarshalFindResponse(r)
}

// Deprecated: Use github.com/ipni/go-libipni/find/model. instead
func UnmarshalFindResponse(b []byte) (*FindResponse, error) {
	return model.UnmarshalFindResponse(b)
}
