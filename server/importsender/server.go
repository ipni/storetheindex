// Package importsender runs the HTTP server of an indexer's import sender,
// which serves the indexer's datastore to an indexer that imports it.
package importsender

import (
	leveldbds "github.com/ipfs/go-ds-leveldb"
	"github.com/ipni/go-indexer-core/importer"
	"github.com/ipni/go-indexer-core/importer/importhttp"
	"github.com/ipni/storetheindex/internal/dsstream"
)

// New returns a server that listens on listen, a "host:port", and serves
// the kinds of dsstream.Kinds from ds with importhttp.NewSender: each
// request carries token, and each open part holds about partBytes of keys
// and values, the source's default when 0. Closing the server stops its
// requests and waits for them, so ds can be closed after it.
func New(listen string, ds *leveldbds.Datastore, token string, partBytes uint64) (*importhttp.Server, error) {
	sources := make(map[importer.Kind]importer.Source, len(dsstream.Kinds))
	src := dsstream.NewSource(ds)
	for _, kind := range dsstream.Kinds {
		sources[kind] = src
	}
	handler, err := importhttp.NewSender(sources, token, importhttp.WithPartBytes(partBytes))
	if err != nil {
		return nil, err
	}
	return importhttp.Listen(listen, handler)
}
