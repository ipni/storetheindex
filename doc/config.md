# The storetheindex config file

The storetheindex config file is a JSON document located at
`$STORETHEINDEX_PATH/config` (default `~/.storetheindex/config`). It is read
when starting the daemon. Some portions may be re-read at runtime; see
[Runtime reloadable items](#runtime-reloadable-items).

The config file is created with `storetheindex init`. To upgrade an old config
file to the current version, or to add missing items to an existing config
file, run `storetheindex init --upgrade`. The current config version is `2`
(`config.Version` in [`config/config.go`](../config/config.go)).

Unset fields are filled with defaults on load (`populateUnset`). For
field-level documentation and defaults, refer to the
[online documentation](https://pkg.go.dev/github.com/ipni/storetheindex/config)
or the Go sources under [`config/`](../config).

## Top-level sections

These match the fields of `config.Config`:

| Section | Purpose |
| --- | --- |
| `Version` | Config file schema version. |
| `Identity` | Peer identity (`PeerID`, `PrivKey`). Private key may instead be loaded from `STORETHEINDEX_PRIV_KEY_PATH`. |
| `Addresses` | Listen addresses for Admin, Finder, Ingest, P2P, ReverseIndexer, and CarMirror servers. |
| `Bootstrap` | Bootstrap peers for gossip pubsub. |
| `Datastore` | Main and temporary datastore directories and types. |
| `Discovery` | Provider discovery policy, polling, and assigner-related settings. |
| `Finder` | Find HTTP API timeouts, connection limits, and homepage. |
| `Indexer` | Value store type and paths, freeze threshold, shutdown timeout, optional RelayX. |
| `ReverseIndexer` | Optional reverse index store. |
| `Ingest` | Ingestion workers, sync limits, HTTP sync tuning, and `AdvertisementMirror`. |
| `Logging` | Global and per-logger log levels. |
| `Peering` | Peers to keep persistent connections with. |

## Runtime reloadable items

The daemon can reload some portions of its config without a full restart. Edit
the config file, then use the admin subcommand `reload-config` or send the
daemon process a `SIGHUP` signal. When the daemon is run with `--watch-config`
(default true) or `STORETHEINDEX_WATCH_CONFIG=true`, it also reloads
automatically after the file changes (checked every
`Indexer.ConfigCheckInterval`, default 30s).

What is applied on reload is defined by `reloadConfig` / the daemon loop in
[`command/daemon.go`](../command/daemon.go):

- `Discovery.Policy`
- `Indexer.ConfigCheckInterval`
- `Indexer.ShutdownTimeout`
- `Ingest.IngestWorkerCount`
- `Ingest.Skip500EntriesError`
- `Logging`
- `Peering`

All other settings require a daemon restart. Config field comments that say
"This value is reloadable" should match this list.
