indexer-node
=======================

> The first place to go in order to find a CID stored in Filecoin


This library provides the implementation of an indexer node that can be used
to index data stored by different data providers (miners and eventually IPFS).


## License
[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)

## Current status 🚧
This implementation is a work in progress. This section will list
the features that have implemented so far so I don't forget (and for
everyone to be able to understand what is implemented and yet to do in
a first glimpse).

- Simple persistence storage based on Adaptive Radix Trees.
- Commands to read a list of CIDs from a manifest and a cid list.
```
./indexer-node import manifest --dir <manifest> --providerID <peer.ID> --pieceID <cid>
./indexer-node import cidlist --dir <manifest> --providerID <peer.ID> --pieceID <cid>
```
