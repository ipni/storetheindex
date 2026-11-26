# Importing an indexer's data

`storetheindex migrate-in` fills a new, empty indexer with the data of an
existing one over HTTP: its providers, the advertisements it has processed
and where it is in each publisher's chain, and its index of multihashes. The
existing indexer keeps running and serving while it is read. When the
import is done, the new indexer starts and continues ingesting from where
the existing one was.

The import reads the data in many parts at once, so it is limited by the
network rather than by one connection, and it can stop and resume: a run
that stops for any reason continues where it stopped when it runs again.

## On the existing indexer

The existing indexer serves its data to the import from a separate port,
behind a token. Turn it on in its config (see
[`Import` in config.md](config.md#import)):

```json
"Import": {
  "ListenAddr": "/ip4/0.0.0.0/tcp/3006",
  "TokenFile": "/secrets/import-token"
}
```

When the indexer's value store is served by relayx, relayx serves its part
of the data on its own port, with the same token:

```sh
relayx serve --delegate pebble ... \
  --importListen 0.0.0.0:3007 --importTokenFile /secrets/import-token
```

The import reads everything from one URL, such as
`https://import.example.org`. Put both ports behind that host name, and
route by path prefix:

| Path prefix | Port |
| --- | --- |
| `/import/v1/kinds/ds/` | the indexer's `Import.ListenAddr` |
| `/import/v1/kinds/vs/` | relayx's `--importListen` |

Requests stream for as long as a part takes, so the route must have no
response timeout, and must not compress or buffer the responses. Keep the
port off the public internet and put TLS in front of it.

While the import runs, the existing indexer must not be frozen, and no
provider may be resynced by hand.

## On the new indexer

Stop every service that uses the new indexer's stores (the indexer, and
relayx when it serves the value store), then run:

```sh
storetheindex migrate-in \
  --source-url https://import.example.org \
  --token-file /secrets/import-token \
  --datastore-dir /data/datastore \
  --valuestore-dir /data/valuestore \
  --valuestore-options /config/pebble.options
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--source-url` | — | Base URL of the existing indexer to import from. |
| `--token-file` | — | File holding the token. |
| `--datastore-type` | `levelds` | Type of the new indexer's datastore. |
| `--datastore-dir` | — | Directory of the new indexer's datastore (its `Datastore.Dir`). |
| `--valuestore-type` | `pebble` | Type of the new indexer's value store. |
| `--valuestore-dir` | — | Directory of the new indexer's value store. |
| `--valuestore-options` | — | Options file that the value store is opened with, so that the import writes it the same way. For pebble, its options file. |
| `--workers` | `16` | Parts read at once. More use more of the network, and load the existing indexer more. |
| `--worker-stagger` | `300ms` | Time between the starts of new workers. |
| `--retry-window` | `30m` | How long a part may keep failing, for example while the existing indexer restarts, before the import stops. |

The command exits with an error when the import stops. Run it again with
the same flags to resume. It exits with no error once the import is done;
running it again then does nothing.

## Starting the new indexer

The new indexer's daemon refuses to start while an import into its
datastore has started and is not done, so that it never runs on part of the
data. Finish the import with `migrate-in` first.

`storetheindex daemon --force-incomplete-migration` starts it anyway, with a
warning. Its data is then incomplete; use it only to inspect the stores.
