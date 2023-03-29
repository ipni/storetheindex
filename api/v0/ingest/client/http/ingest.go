// Deprecated: The same functionality is provided by package
// github.com/ipni/go-libipni/ingest/client.
package ingesthttpclient

import (
	"github.com/ipni/go-libipni/ingest/client"
)

// Deprecated: Use github.com/ipni/go-libipni/ingest/client.Client instead
type Client = client.Client

// Deprecated: Use github.com/ipni/go-libipni/ingest/client.New instead
func New(baseURL string, options ...client.Option) (*Client, error) {
	return client.New(baseURL, options...)
}
