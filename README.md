# StoreTheIndex 🗂️
[![Go Reference](https://pkg.go.dev/badge/github.com/ipni/storetheindex.svg)](https://pkg.go.dev/github.com/ipni/storetheindex)
[![Coverage Status](https://codecov.io/gh/ipni/storetheindex/branch/main/graph/badge.svg)](https://codecov.io/gh/ipni/storetheindex/branch/main)
> Find a CID stored by storage providers

This repo provides an indexer implementation that can be used to index data stored by a range of participating storage providers.

## Design
- [IPNI: InterPlanetary Network Indexer](https://github.com/ipni/specs/blob/main/IPNI.md#ipni-interplanetary-network-indexer)

## Current Status
Released for production: The current production release is running at https://cid.contact 

This project and is currently under active development 🚧.  

## Install
This assumes go is already installed.

Install storetheindex:
```sh
go install github.com/ipni/storetheindex@latest
```

Initialize the storetheindex repository and configuration:
```sh
storetheindex init
```

Optionally, edit the configuration
```sh
edit ~/.storetheindex/config 
```

## Running the Indexer Service
To run storetheindex as a service, run the `daemon` command. The service watches for providers to index, and exposes a query / content routing client interface.

The daemon is configured by the config file in the storetheindex repository. The config file and repo are created when storetheindex is initialized, using the `init` command. This repo is located in the local file system. By default, the repo is located at ~/.storetheindex. To change the repo location, set the `$STORETHEINDEX_PATH` environmental variable.

## Provider Removal Policy
After a configured amount of time without any updates from a provider (`PollInterval`), the indexer will poll the provider at its last know publisher address, for any index updates. If there is no response from the provider after at least one attempt to poll, then the provider is considered inactive and is not returned in any find results. The indexer will continue polling on an interval (`PollRetryAfter`) until a time limit (`PollStopAfter`) is reached. If there is still no response to the poll attempts after this time limit is reached, then the provider is removed from the indexer and its records are garbage-collected and will need to be refetched. 

The configuration that controls this is documented [here](https://pkg.go.dev/github.com/ipni/storetheindex/config#Discovery). A custom polling configuration may be applied for specific providers using the `PollOverrides` configuration value to specify per-provider [Polling configuration](https://pkg.go.dev/github.com/ipni/storetheindex/config#Polling).

## Indexer Administration CLI Commands
There are a number of administrative commands supported by storetheindex. These commands allow you to perform operations on a running indexer daemon. For a list of admin commands, see:

```
./storetheindex admin -help
```

## Help
To see a list of available commands, see `storetheindex --help`. For help with command usage, see `storetheindex <command> --help`.

## Configuration
The storetheindex config file [documentation](https://github.com/ipni/storetheindex/blob/main/doc/config.md#the-storetheindex-config-file)

## Assigner Service
The assigner service is responsible for assigning content advertisement publishers to indexers, and forwarding advertisement announcement messages to indexers. This allows the ingestion load to be 
divided among a pool of indexers, with a configurable replication factor.

Indexers that are under control of an assigner must be configured to use an assigner. If an assigner-controlled indexer runs out of storage space and enters frozen mode, then indexing is handed off to the next available indexer in the pool.

An assigner may also be used to forward announcement messages to indexers that are not in the assigner's indexer pool or to other assigners. For example, in the case of multiple redundant indexers it is not necessary to manage index publisher assignments, and forwarding announcements is all that is needed. This forwarding can be configured by specifying each recipient's ingest API base URL in the assigner's [`ForwardAnnounceURLs`](https://pkg.go.dev/github.com/ipni/storetheindex/assigner/config#Assignment) configuration.

### Assigner Install
Install storetheindex, which contains the assigner service, as described [above](#Install).

Initialize the assigner service:
```sh
storetheindex assigner init
```

Edit the assigner. Usually this includes configuring each [`Indexer`](https://pkg.go.dev/github.com/ipni/storetheindex/assigner/config#Indexer) in the assigner's [`IndexerPool`](https://pkg.go.dev/github.com/ipni/storetheindex/assigner/config#Assignment). 
```sh
edit ~/.assigner/config 
```
To configure an indexer to use the assigner, set `UseAssigner` to `true` in the indexer's [`Discovery`](https://pkg.go.dev/github.com/ipni/storetheindex/config#Discovery) configuration

### Assigner Configuration
The assigner service config file [documentation](https://pkg.go.dev/github.com/ipni/storetheindex/assigner/config)

## License
[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
