StoreTheIndex 🗂️
=======================

> The first place to go in order to find a CID stored in Filecoin


This repo provides an indexer implementation that can be used to index data stored by different data providers (miners and eventually IPFS).


## Current status 🚧
This implementation is currently under development and interfaces are no yet stable.


## Install
This assumes go is already installed.

Install storetheindex:
```sh
go get github.com/filecoin-project/storetheindex
```

Initialize the storetheindex repository and configuration:
```sh
storetheindex init
```


## Running Indexer Service
The run storetheindex as an indexer service, run the `daemon` command.  The service lets clients connect, and index content and find that content.

The daemon is configured by the config file in the storetheindex repository.  The config file and repo are created when storetheindex is initialized, using the `init` command. This repo is located in the local file system. By default, the repo is located at ~/.storetheindex.  To change the repo location, set the `$STORETHEINDEX_PATH` environment variable:

## Indexer Client Commands

There are a number of client commands included with storetheindex.  Their purpose is to perform simple indexing and lookup actions against a running daemon.  These can be helpful to test that an indexer is working.  These include the following commands:

- `find` Find value by multihash in indexer
- `import` Imports data to indexer from different sources
- `register` Register provider information with an indexer that trusts the provider
- `synthetic` Generate synthetic load to import in indexer
- `ingest` Admin commands to manage ingestion config of indexer

## Help

To see a list of available commands, see `storetheindex --help`.  For help with command usage, see `storetheindex <command> --help`.


## Configuration

The storetheindex config file is a JSON document located at `$STORETHEINDEX_PATH`/config.  It is read once, either for an offline command, or when starting the daemon.  For documentation of the items in the config file, see the [godoc documentation](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config) of the corresponding config data structures.


## License
[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
