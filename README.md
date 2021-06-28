StoreTheIndex 🗂️
=======================

> The first place to go in order to find a CID stored in Filecoin


This library provides the implementation of an indexer node that can be used
to index data stored by different data providers (miners and eventually IPFS).


## Current status 🚧
This implementation is a work in progress. This section will list
the features that have implemented so far so I don't forget (and for
everyone to be able to understand what is implemented and yet to do in
a first glimpse).

- Simple primary storage based on https://github.com/gammazero/radixtree
- Run node (it starts the node's API, and intializes its storage).
```
./indexer-node daemon -e 127.0.0.1:3000
```
- Commands to read a list of CIDs from a manifest and a cid list.
```
./indexer-node import manifest --dir <manifest> --providerID <peer.ID> --pieceID <cid>
./indexer-node import cidlist --dir <manifest> --providerID <peer.ID> --pieceID <cid>

// Example
./indexer-node import cidlist --dir ./cid.out --providerID QmcJeseojbPW9hSejUM1sQ1a2QmbrryPK4Z8pWbRUPaYEn -e 127.0.0.1:3000
```
- Simple get command for single CID (for testing purposes).
```
./indexer-node get -e 127.0.0.1:3000 bafkreie4qmvnboqqgjp3tijhibgofvuqify2a2pl6ac4xyxd5rfmlvqsf4
```
- Synthetic harness to create test data to load in the indexer.
```
# Create plain cidlist
./indexer-node synthetic --dir <out_dir> -t cidlist -n <num_entries>
# [DagAggregator manifest](https://github.com/filecoin-project/go-dagaggregator-unixfs/blob/wip/aggregator.go#L29-L56).
./indexer-node synthetic --dir <out_dir> -t manifest -n <num_entries>
```

## License
[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)

