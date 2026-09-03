# storetheindex configuration

This document describes the JSON configuration file used by the storetheindex
daemon (from a binary, package, or container image).

## Where the file lives

By default the config is:

```text
~/.storetheindex/config
```

Override the directory with the `STORETHEINDEX_PATH` environment variable. The
file name inside that directory is always `config`.

Create an initial file:

```sh
storetheindex init
```

Upgrade an older file or fill in newly added settings with defaults:

```sh
storetheindex init --upgrade
```

The current config schema version is `2`.

## How values work

- The file is JSON. Nested sections are JSON objects; lists are JSON arrays.
- Time intervals and timeouts are duration strings such as `"30s"`, `"5m"`,
  `"2h"`.
- Byte sizes may be a number (bytes) or a string with a `Mi` / `Gi` suffix, for
  example `"1Gi"`.
- Omitted fields (and many empty/`0` values) are filled with the defaults listed
  below when the daemon starts.
- Only some settings can change while the daemon is running; see
  [Reloading configuration](#reloading-configuration). Everything else needs a
  restart.

Unusual formats for a given field (peer ID vs multiaddr, URL vs `host:port`,
allowed enum values, and so on) are noted in that field’s section.

## Top-level layout

| Section | What it configures |
| --- | --- |
| `Version` | Schema version of this file. |
| `Identity` | This indexer’s libp2p identity (peer ID and private key). |
| `Addresses` | Network listen addresses for the various HTTP / libp2p servers. |
| `Bootstrap` | Peers used to join gossip for announcements. |
| `Datastore` | On-disk databases for indexer metadata and temporary sync data. |
| `Discovery` | Which publishers are accepted, and how providers are polled. |
| `Finder` | The find (query) HTTP API. |
| `Indexer` | The multihash value store, disk freeze behavior, optional RelayX. |
| `ReverseIndexer` | Optional reverse index. |
| `Ingest` | How advertisements are ingested, plus optional CAR mirrors. |
| `Logging` | Log levels. |
| `Peering` | Peers to keep permanently connected. |

---

## `Identity`

Who this indexer is on the network.

| Field | Default | Meaning |
| --- | --- | --- |
| `PeerID` | Derived from the private key | Libp2p peer ID string (`"12D3KooW…"`), not a multiaddr. If set, it must match the private key. |
| `PrivKey` | — | Base64-encoded private key bytes (not PEM, not hex). If omitted, the key is read from the file pointed to by `STORETHEINDEX_PRIV_KEY_PATH`. |

---

## `Addresses`

Listen addresses for each server. Use the string `"none"` to disable a server.

Most fields are **libp2p multiaddrs**. `ReverseIndexer` is the exception: it
uses a plain **`host:port`** listen address.

| Field | Default | Meaning |
| --- | --- | --- |
| `Admin` | `/ip4/127.0.0.1/tcp/3002` | Admin API multiaddr (usually localhost only). |
| `Finder` | `/ip4/0.0.0.0/tcp/3000` | Find / query API multiaddr. |
| `Ingest` | `/ip4/0.0.0.0/tcp/3001` | Announce / ingest API multiaddr. |
| `P2PAddr` | `/ip4/0.0.0.0/tcp/3003` | Libp2p host multiaddr. |
| `ReverseIndexer` | `0.0.0.0:3004` | Reverse indexer HTTP listen address (`host:port`, not a multiaddr). |
| `NoResourceManager` | `false` | If `true`, disables the libp2p resource manager. |
| `CarMirror` | `/ip4/0.0.0.0/tcp/3005` | CAR mirror HTTP listen multiaddr. |

---

## `Bootstrap`

Peers used to discover the gossip network for ingest announcements. Those peers
must themselves participate in the ingest pubsub topic.

| Field | Default | Meaning |
| --- | --- | --- |
| `Peers` | Built-in public bootstrap peer list | Array of **multiaddrs**, each including `/p2p/<peerID>` (for example `"/dns4/…/tcp/41778/p2p/12D3KooW…"`). Not bare peer IDs. |
| `MinimumPeers` | `4` | If the node has fewer open connections than this, it dials bootstrap peers. Set `0` to disable bootstrapping. |

---

## `Datastore`

Local databases. Relative paths are under the indexer data directory
(`STORETHEINDEX_PATH`).

| Field | Default | Meaning |
| --- | --- | --- |
| `Dir` | `datastore` | Main datastore directory. |
| `Type` | `levelds` | Main datastore implementation. |
| `TmpDir` | `tmpstore` | Temporary datastore (in-progress sync data, etc.). |
| `TmpType` | `levelds` | Temporary datastore implementation. |
| `RemoveTmpAtStart` | `false` | If `true`, clear temporary datastore contents on startup. |

---

## `Discovery`

Controls which publishers may feed this indexer, and how known providers are
kept up to date when they go quiet.

| Field | Default | Meaning |
| --- | --- | --- |
| `FilterIPs` | `false` | If `true`, remove private, loopback, and unspecified IP addresses from provider and publisher address lists. |
| `IgnoreBadAdsTime` | `2h` | How long to ignore a publisher after it published an unusable advertisement chain. |
| `Policy` | see below | Allow / block rules for publishers and who may publish for whom. **Reloadable.** |
| `PollInterval` | `24h` | After this long with no updates from a provider, start polling for its latest advertisement. |
| `PollRetryAfter` | `5h` | Delay between poll attempts (and how often the poller looks for work). Should be smaller than `PollStopAfter` if you want more than one attempt. |
| `PollStopAfter` | `336h` (2 weeks) | Give up polling and remove that provider’s indexed data after this long with no successful update. |
| `DeactivateAfter` | `168h` (1 week) | Stop returning a provider in find results after it has been unseen this long. If this is less than `PollStopAfter`, it has no useful effect. |
| `PollOverrides` | `[]` | Optional per-provider poll settings (see below). |
| `UseAssigner` | `false` | If `true`, this indexer works with an assigner service. That also requires `Policy.Allow` to be `false` (so `Policy.Except` becomes the allow-list of assigned publishers). |
| `RemoveOldAssignments` | `false` | When using an assigner: if `true`, drop old assignment records; if `false`, migrate them. |
| `UnassignedPublishers` | `false` | When using an assigner: if `true`, leave unknown publishers unassigned until the assigner places them; if `false`, assign known publishers to this indexer immediately (safer for freeze handoff). |

### `Discovery.PollOverrides`

Each entry overrides poll timings for one provider:

| Field | Meaning |
| --- | --- |
| `ProviderID` | Provider **peer ID** (`"12D3KooW…"`), not a multiaddr. |
| `Interval` | Overrides `PollInterval`. |
| `RetryAfter` | Overrides `PollRetryAfter`. |
| `StopAfter` | Overrides `PollStopAfter`. |
| `DeactivateAfter` | Overrides `DeactivateAfter`. |

### `Discovery.Policy`

**Publishers** are peers that send advertisements to the indexer. **Providers**
are the peers listed inside advertisements (where clients retrieve content).

`Except`, `PublishExcept`, `PublishersForProvider[].Provider`, and
`PublishersForProvider[].Except` are arrays (or a single field) of **libp2p
peer ID strings**, for example `"12D3KooW…"`. They are not multiaddrs and not
IP addresses.

| Field | Default | Meaning |
| --- | --- | --- |
| `Allow` | `true` | Default rule: allow everyone (`true`) or allow no one (`false`). |
| `Except` | `[]` | Peer IDs that are exceptions to `Allow`. With `Allow: true` this is a deny-list; with `Allow: false` this is an allow-list. |
| `Publish` | `true` | Default rule for whether a peer may publish advertisements for a *different* provider ID. |
| `PublishExcept` | `[]` | Peer IDs that are exceptions to `Publish`. |
| `PublishersForProvider` | `[]` | Optional finer rules: which publishers may act for a specific provider (see below). |

#### `PublishersForProvider` entries

| Field | Meaning |
| --- | --- |
| `Provider` | Provider peer ID this policy applies to. |
| `Allow` | Default allow/block for publishing on behalf of that provider. |
| `Except` | Publisher peer IDs that are exceptions to `Allow`. |

---

## `Finder`

HTTP API used by clients to look up multihashes / CIDs.

| Field | Default | Meaning |
| --- | --- | --- |
| `ApiReadTimeout` | `30s` | Maximum time to read an incoming request. A negative value means no timeout. |
| `ApiWriteTimeout` | `30s` | Maximum time to write a response. A negative value means no timeout. |
| `MaxConnections` | `8000` | Maximum simultaneous HTTP connections. A negative value means no limit. |
| `Webpage` | `https://web-ipni.cid.contact/` | Page linked or shown when the finder root URL is opened in a browser. |

---

## `Indexer`

Where index mappings are stored, when the node freezes for lack of disk, and
optional RelayX forwarding.

| Field | Default | Meaning |
| --- | --- | --- |
| `CacheSize` | `0` | Maximum number of CIDs kept in an in-memory cache. `0` or a negative value disables the cache. |
| `ConfigCheckInterval` | `30s` | How often to check the config file for changes when config watching is enabled. **Reloadable.** |
| `ValueStoreType` | `pebble` | Backend for multihash → provider mappings. Allowed values are typically `pebble` or `dhstore`. If empty/`none` and `DHStoreURL` is set, `dhstore` is used; otherwise `pebble`. |
| `ValueStoreDir` | `valuestore` | Directory for a local value store (when the type needs one). Relative paths are under the indexer data directory. |
| `FreezeAtPercent` | `95.0` | Enter *frozen* mode when disk usage of the value store or datastore directories reaches this percent (for example `95.0`, not `0.95`). A negative value disables freezing. |
| `ShutdownTimeout` | none (`0`) | How long a graceful shutdown may take before the process exits anyway. `0` or unset means wait with no deadline (not “exit immediately”). **Reloadable.** |
| `UnfreezeOnStart` | `false` | If the indexer was frozen, undo freeze on startup (keeps only the latest known provider/publisher addresses). |
| `DHStoreURL` | — | HTTP(S) base URL of a DHStore service. Required when `ValueStoreType` is `dhstore`. |
| `DHStoreClusterURLs` | `[]` | Extra DHStore HTTP(S) URLs that also receive delete operations (in addition to `DHStoreURL`). |
| `DHStoreHttpClientTimeout` | `10s` | HTTP timeout for DHStore requests. |
| `DHBatchSize` | library default | Batch size when sending merges to DHStore. Values less than `1` use the built-in default. |
| `PebbleDisableWAL` | `false` | For `pebble` only: disable the write-ahead log. |
| `PebbleBlockCacheSize` | `1Gi` | For `pebble` only: block cache size. |
| `PebbleFormatMajorVersion` | `0` | For `pebble` only: on-disk format. `0` keeps the current format; `-1` upgrades to the latest supported format. |
| `RelayX` | omitted | Optional RelayX settings (see below). |

### `Indexer.RelayX`

| Field | Default | Meaning |
| --- | --- | --- |
| `ServerAddr` | — | HTTP(S) base URL of the RelayX server (for example `"http://relayx:8080/ipni/v0/relay"`). |

---

## `ReverseIndexer`

Optional reverse index of provider data.

| Field | Default | Meaning |
| --- | --- | --- |
| `Enabled` | `false` | Turn the reverse indexer on. |
| `StorePath` | `reverse_index` | Path where reverse index data is stored. |

---

## `Ingest`

How advertisement chains and entry data are fetched and indexed.

| Field | Default | Meaning |
| --- | --- | --- |
| `IngestWorkerCount` | `10` | How many advertisements can be processed concurrently (worker goroutines). **Reloadable.** |
| `MaxAsyncConcurrency` | `32` | How many announce-triggered syncs may run at once. `-1` means unlimited. Changing this requires a restart. |
| `AdvertisementDepthLimit` | `33554432` | Maximum number of advertisements to walk in a chain (across sync segments). `-1` means no limit. |
| `EntriesDepthLimit` | `65536` | Maximum number of entry chunks to walk. `-1` means no limit. |
| `FirstSyncDepth` | `0` | On first contact with a new provider, how deep to sync. `0` means unlimited; `1` means only the latest advertisement. |
| `SyncSegmentDepthLimit` | `2000` | How much of a chain is requested in one sync segment. `-1` syncs the whole chain in one go. |
| `SyncTimeout` | `2h` | Maximum time allowed for one advertisement-chain or entries sync. |
| `HttpSyncTimeout` | `10s` | Timeout for individual HTTP sync requests. |
| `HttpSyncRetryMax` | `0` | How many times to retry a failed HTTP sync. `0` means no retries. |
| `HttpSyncRetryWaitMin` | `1s` | Minimum wait before retrying. |
| `HttpSyncRetryWaitMax` | `30s` | Maximum wait before retrying. |
| `MinimumKeyLength` | `0` | Ignore multihashes whose digest is shorter than this many bytes. |
| `Skip500EntriesError` | `false` | If `true`, skip advertisements when the publisher returns HTTP 500 with message `failed to sync first entry`. **Reloadable.** |
| `PubSubTopic` | `/indexer/ingest/mainnet` | Gossip topic path for ingest announcements (a string like `"/indexer/ingest/mainnet"`, legacy / compatibility). |
| `ResendDirectAnnounce` | `false` | If `true`, re-broadcast announces received over HTTP onto gossip so other indexers see them. Always off when using an assigner. |
| `OverwriteMirrorOnResync` | `false` | When resyncing, overwrite existing mirrored CAR data. Rebuilds of Main from External or the publisher always overwrite so a broken Main CAR is replaced. |
| `AdvertisementMirror` | see below | Optional storage of advertisement CAR files for local reuse or sharing. |

### `Ingest.AdvertisementMirror`

Stores advertisement (and entry) data as CAR files so this indexer can read
them again later (especially on resync), or serve them to others, without
always fetching from the publisher.

Ingest **prefers Main**, then External. A CAR is indexed only if it contains
exactly the advertisement's entries chain in order, with CIDs matching the
block bytes. Incomplete, empty, reordered, extra, unrelated, or missing files
are not trusted: ingest tries the next source, and a writable Main is
overwritten from External or the publisher.

If Main is disabled and no External stores are configured (or their type is
unset / `"none"`), mirroring is not used.

| Field | Default | Meaning |
| --- | --- | --- |
| `MainMode` | `none` | How the **Main** store is used. Allowed values: `none` (or empty), `read`, `write`, or `readwrite`. Does not affect External stores. |
| `Main` | see store defaults below | The indexer’s own mirror store (read and/or write according to `MainMode`). |
| `External` | `[]` | Extra **read-only** mirror locations. After a Main miss or unusable Main CAR (or if Main is not readable), all External stores are queried in parallel; the first successful complete CAR wins. Missing files, incomplete files, and errors count as misses and ingest falls back to the publisher. |

Each of `Main` and every `External` entry is a **store** object:

| Field | Default | Meaning |
| --- | --- | --- |
| `Type` | unset | Backend. Allowed values: `local`, `s3`, `http`, or `none` / empty to disable. |
| `Compress` | `gzip` | CAR compression. Allowed values: `gzip` or `none`. Unset External entries that are configured also default to `gzip`. |
| `Local` | — | Settings when `Type` is `local`. |
| `S3` | — | Settings when `Type` is `s3`. |
| `HTTP` | — | Settings when `Type` is `http`. |

#### Local store (`Type`: `local`)

| Field | Default | Meaning |
| --- | --- | --- |
| `BasePath` | — | Directory on disk where CAR files are stored. |
| `DefaultPathSplit` | `[11, 2]` for the default Main store | Array of positive integers controlling subdirectory sharding of CAR file names (not a path string). |

#### S3 store (`Type`: `s3`)

| Field | Default | Meaning |
| --- | --- | --- |
| `BucketName` | — | S3 bucket name (required). |
| `Endpoint` | from environment | Optional custom S3 API URL (for example MinIO or LocalStack). |
| `Region` | from environment | Optional region override. |
| `AccessKey` | from environment | Optional access key override. |
| `SecretKey` | from environment | Optional secret key override. |

#### HTTP store (`Type`: `http`)

Read-only. Typically points at another indexer’s CarMirror URL.

| Field | Default | Meaning |
| --- | --- | --- |
| `BaseURL` | — | HTTP(S) base URL; CAR object paths are appended to this (for example `"http://other-indexer:3005/"`). |

Older config files may still use legacy mirror fields (`Read` / `Write`,
`Storage` / `Retrieval`, top-level `Compress`). On startup those are converted
to `MainMode` / `Main` / `External`. Do not mix legacy and new fields in the
same file.

---

## `Logging`

**Reloadable** as a whole.

Levels are case-insensitive strings. Allowed values: `fatal`, `panic`,
`dpanic`, `error`, `warn`, `info`, `debug`.

| Field | Default | Meaning |
| --- | --- | --- |
| `Level` | `info` | Default level for loggers not listed under `Loggers`. |
| `Loggers` | `{"bootstrap":"warn","p2p-config":"error"}` when no overall level was set | JSON **object** mapping logger name → level (not an array). |

---

## `Peering`

Keep long-lived connections to specific peers (stronger than bootstrap).
**Reloadable.**

| Field | Default | Meaning |
| --- | --- | --- |
| `Peers` | `[]` | Array of **multiaddrs** that include `/p2p/<peerID>` (same format as `Bootstrap.Peers`). A bare peer ID is not enough. |

---

## Reloading configuration

You can apply some changes without restarting:

1. Edit the config file.
2. Run `storetheindex admin reload-config`, **or** send `SIGHUP` to the daemon
   process.

If the daemon was started with `--watch-config` (default **on**) or
`STORETHEINDEX_WATCH_CONFIG=true`, it also picks up file changes automatically,
checking about every `Indexer.ConfigCheckInterval` (default 30 seconds).

### Settings that reload without restart

- `Discovery.Policy`
- `Indexer.ConfigCheckInterval`
- `Indexer.ShutdownTimeout`
- `Ingest.IngestWorkerCount`
- `Ingest.Skip500EntriesError`
- `Logging` (entire section)
- `Peering` (entire section)

All other settings require stopping and starting the daemon (or recreating the
container) to take effect.
