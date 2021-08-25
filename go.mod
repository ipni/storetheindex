module github.com/filecoin-project/storetheindex

go 1.16

require (
	github.com/filecoin-project/go-address v0.0.5
	github.com/filecoin-project/go-dagaggregator-unixfs v0.2.0
	github.com/filecoin-project/go-indexer-core v0.0.0-20210816132949-bbccdebb905f
	github.com/gogo/protobuf v1.3.2
	github.com/gorilla/mux v1.7.4
	github.com/im7mortal/kmutex v1.0.1
	github.com/ipfs/go-cid v0.1.0
	github.com/ipfs/go-datastore v0.4.6
	github.com/ipfs/go-ds-leveldb v0.4.2
	github.com/ipfs/go-graphsync v0.8.1-rc1
	github.com/ipfs/go-log/v2 v2.3.0
	github.com/ipld/go-ipld-prime v0.12.0
	github.com/libp2p/go-libp2p v0.15.0-rc.1
	github.com/libp2p/go-libp2p-core v0.9.0
	github.com/libp2p/go-msgio v0.0.6
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-multiaddr v0.4.0
	github.com/multiformats/go-multicodec v0.3.0
	github.com/multiformats/go-multihash v0.0.15
	github.com/urfave/cli/v2 v2.3.0
	github.com/willscott/go-legs v0.0.0-00010101000000-000000000000
	github.com/ybbus/jsonrpc/v2 v2.1.6
)

replace github.com/filecoin-project/filecoin-ffi => github.com/filecoin-project/ffi-stub v0.2.0

// TODO: Remove this when XXX merged
replace github.com/willscott/go-legs => /home/adlrocha/Desktop/main/work/ProtocolLabs/repos/datasystems/go-legs
