module github.com/filecoin-project/storetheindex

go 1.16

require (
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/go-dagaggregator-unixfs v0.2.0
	github.com/filecoin-project/go-indexer-core v0.0.0-20210816132949-bbccdebb905f
	github.com/filecoin-project/go-jsonrpc v0.1.4-0.20210217175800-45ea43ac2bec
	github.com/filecoin-project/lotus v1.9.1-0.20210607161144-fadc79a4875b
	github.com/gogo/protobuf v1.3.2
	github.com/gorilla/mux v1.7.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipld/go-ipld-prime v0.7.0
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/lib/pq v1.10.2
	github.com/libp2p/go-libp2p v0.14.4
	github.com/libp2p/go-libp2p-core v0.8.6
	github.com/libp2p/go-libp2p-swarm v0.5.3
	github.com/libp2p/go-msgio v0.0.6
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.3.3
	github.com/multiformats/go-multihash v0.0.15
	github.com/urfave/cli/v2 v2.3.0
)

replace github.com/filecoin-project/filecoin-ffi => github.com/hannahhoward/ffi-stub v0.1.1-0.20210611194822-18d26dc20744
