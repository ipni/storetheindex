module github.com/filecoin-project/storetheindex

go 1.16

// replace github.com/filecoin-project/go-legs => ../go-legs

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.0
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/go-dagaggregator-unixfs v0.2.0
	github.com/filecoin-project/go-indexer-core v0.2.7
	github.com/filecoin-project/go-legs v0.2.6-0.20220128203205-b176a8ca746d
	github.com/frankban/quicktest v1.14.0
	github.com/gogo/protobuf v1.3.2
	github.com/gorilla/mux v1.7.4
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-ipfs v0.11.0
	github.com/ipfs/go-log/v2 v2.5.0
	github.com/ipld/go-ipld-prime v0.14.4
	github.com/libp2p/go-libp2p v0.17.0
	github.com/libp2p/go-libp2p-core v0.13.0
	github.com/libp2p/go-msgio v0.1.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.5.0
	github.com/multiformats/go-multicodec v0.4.0
	github.com/multiformats/go-multihash v0.1.0
	github.com/multiformats/go-varint v0.0.6
	github.com/prometheus/client_golang v1.11.0
	github.com/stretchr/testify v1.7.0
	github.com/urfave/cli/v2 v2.3.0
	github.com/ybbus/jsonrpc/v2 v2.1.6
	go.opencensus.io v0.23.0
	go.uber.org/zap v1.19.1
)
