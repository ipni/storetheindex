// Deprecated: The same functionality is provided by package
// github.com/ipni/go-libipni/find/client/http.
package finderhttpclient

import (
	"github.com/ipni/go-libipni/find/client/http"
)

// Deprecated: Use github.com/ipni/go-libipni/find/client/http.Client instead.
type Client = httpclient.Client

// Deprecated: Use github.com/ipni/go-libipni/find/client/http.New instead.
func New(baseURL string, options ...httpclient.Option) (*Client, error) {
	return httpclient.New(baseURL, options...)
}
