package finderhttpclient

import (
	"github.com/ipni/go-libipni/find/client/http"
)

// Deprecated: Use github.com/ipni/go-libipni/find/client/http.DHashClient instead.
type DHashClient = httpclient.DHashClient

// Deprecated: Use github.com/ipni/go-libipni/find/client/http.NewDHashClient instead.
func NewDHashClient(dhstoreUrl string, stiUrl string, options ...httpclient.Option) (*DHashClient, error) {
	return httpclient.NewDHashClient(dhstoreUrl, stiUrl, options...)
}
