# The storetheindex config file

The storetheindex config file is a JSON document located at `$STORETHEINDEX_PATH/config`. It
is read once at indexer instantiation, either for an offline command, or when
starting the daemon. Commands that execute on a running daemon do not read the
config file at runtime.

## Example Config
```json
{
  "Version": 2,
  "Identity": {
    "PeerID": "12D3KooWJZSPZN7cudwBJ6UdTm8V5FmgWhAd2oVa8qFagU7bZSm1",
    "PrivKey": "CAESQKP70L69Q7An97xPH6g3PgSypws6mHYcAZ77t9pC2a0/geY+Kh3gaSzfPqHpSyjy7Fd3hQPweU+BNbhhKHSHrJw="
  },
  "Addresses": {
    "Admin": "/ip4/127.0.0.1/tcp/3002",
    "Finder": "/ip4/0.0.0.0/tcp/3000",
    "Ingest": "/ip4/0.0.0.0/tcp/3001",
    "P2PAddr": "/ip4/0.0.0.0/tcp/3003",
    "NoResourceManager": false
  },
  "Bootstrap": {
    "Peers": [
      "/dns4/bootstrap-1.mainnet.filops.net/tcp/1347/p2p/12D3KooWCwevHg1yLCvktf2nvLu7L9894mcrJR4MsBCcm4syShVc",
      "/dns4/bootstrap-3.mainnet.filops.net/tcp/1347/p2p/12D3KooWKhgq8c7NQ9iGjbyK7v7phXvG6492HQfiDaGHLHLQjk7R",
      "/dns4/bootstrap-8.mainnet.filops.net/tcp/1347/p2p/12D3KooWScFR7385LTyR4zU1bYdzSiiAb5rnNABfVahPvVSzyTkR",
      "/dns4/bootstrap-0.ipfsmain.cn/tcp/34721/p2p/12D3KooWQnwEGNqcM2nAcPtRR9rAX8Hrg4k9kJLCHoTR5chJfz6d",
      "/dns4/bootstrap-0.mainnet.filops.net/tcp/1347/p2p/12D3KooWCVe8MmsEMes2FzgTpt9fXtmCY7wrq91GRiaC8PHSCCBj",
      "/dns4/bootstrap-2.mainnet.filops.net/tcp/1347/p2p/12D3KooWEWVwHGn2yR36gKLozmb4YjDJGerotAPGxmdWZx2nxMC4",
      "/dns4/bootstrap-5.mainnet.filops.net/tcp/1347/p2p/12D3KooWLFynvDQiUpXoHroV1YxKHhPJgysQGH2k3ZGwtWzR4dFH",
      "/dns4/bootstrap-6.mainnet.filops.net/tcp/1347/p2p/12D3KooWP5MwCiqdMETF9ub1P3MbCvQCcfconnYHbWg6sUJcDRQQ",
      "/dns4/bootstrap-1.starpool.in/tcp/12757/p2p/12D3KooWQZrGH1PxSNZPum99M1zNvjNFM33d1AAu5DcvdHptuU7u",
      "/dns4/lotus-bootstrap.ipfsforce.com/tcp/41778/p2p/12D3KooWGhufNmZHF3sv48aQeS13ng5XVJZ9E6qy2Ms4VzqeUsHk",
      "/dns4/bootstrap-0.starpool.in/tcp/12757/p2p/12D3KooWGHpBMeZbestVEWkfdnC9u7p6uFHXL1n7m1ZBqsEmiUzz",
      "/dns4/bootstrap-4.mainnet.filops.net/tcp/1347/p2p/12D3KooWL6PsFNPhYftrJzGgF5U18hFoaVhfGk7xwzD8yVrHJ3Uc",
      "/dns4/bootstrap-7.mainnet.filops.net/tcp/1347/p2p/12D3KooWRs3aY1p3juFjPy8gPN95PEQChm2QKGUCAdcDCC4EBMKf",
      "/dns4/node.glif.io/tcp/1235/p2p/12D3KooWBF8cpp65hp2u9LK5mh19x67ftAam84z9LsfaquTDSBpt",
      "/dns4/bootstrap-1.ipfsmain.cn/tcp/34723/p2p/12D3KooWMKxMkD5DMpSWsW7dBddKxKT7L2GgbNuckz9otxvkvByP"
    ],
    "MinimumPeers": 4
  },
  "Datastore": {
    "Dir": "datastore",
    "Type": "levelds"
  },
  "Discovery": {
    "LotusGateway": "https://api.chain.love",
    "Policy": {
      "Allow": true,
      "Except": ["12D3KooWEbhQxDZpDwvqBVPbxUXz8AquMziyUv2HT77YNKQYPiDx"],
      "Publish": true,
      "PublishExcept": null
    },
    "PollInterval": "24h0m0s",
    "PollRetryAfter": "5h0m0s",
    "PollStopAfter": "168h0m0s",
    "PollOverrides": [
      {
        "ProviderID": "12D3KooWRYLtcVBtDpBZDt5zkAVFceEHyozoQxr4giccF7fquHR2",
        "Interval": "12h0m0s",
        "RetryAfter": "45m0s",
        "StopAfter": "3h0m0s"
      }
    ],
    "RediscoverWait": "5m0s",
    "Timeout": "2m0s"
  },
  "Indexer": {
    "CacheSize": 300000,
    "GCInterval": "30m0s",
    "ValueStoreDir": "valuestore",
    "ValueStoreType": "sth"
  },
  "Ingest": {
    "AdvertisementDepthLimit": 33554432,
    "EntriesDepthLimit": 65536,
    "IngestWorkerCount": 10,
    "PubSubTopic": "/indexer/ingest/mainnet",
    "RateLimit": {
      "Apply": false,
      "Except": null,
      "BlocksPerSecond": 100,
      "BurstSize": 500
    },
    "StoreBatchSize": 4096,
    "SyncSegmentDepthLimit": 2000,
    "SyncTimeout": "2h0m0s",
    "HttpSyncRetryWaitMin": "1s",
    "HttpSyncRetryWaitMax": "30s",
    "HttpSyncRetryMax": 4,
    "HttpSyncTimeout": "10s"
  },
  "Logging": {
    "Level": "info",
    "Loggers": {
      "basichost": "warn",
      "bootstrap": "warn",
      "dt-impl": "warn",
      "dt_graphsync": "warn",
      "graphsync": "warn"
    }
  }
}
```

## `Version`
Description: Current version of configuration file. This is used to defermine if an existing configuration should be upgraded.

Default: No default. This is written during initialization.

## `Identity`
Description: [Identity](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Identity)

Default: There is no default value. This is generated during initialization.

## `Addresses`
Description: [Addresses](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Addresses)

Default:
```json
"Addresses": {
  "Admin": "/ip4/127.0.0.1/tcp/3002",
  "Finder": "/ip4/0.0.0.0/tcp/3000",
  "Ingest": "/ip4/0.0.0.0/tcp/3001",
  "P2PAddr": "/ip4/0.0.0.0/tcp/3003",
  "NoResourceManager": false
}
```

## `Bootstrap`
Description: [Bootstrap](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Bootstrap)

Default:
```json
"Bootstrap": {
  "Peers": [...],
  "MinimumPeers": 4
}
```
`Bootstrap.Peers` is an array of multiaddr strings. It defaults to the multiaddrs of the filecoin mainnet bootstrap nodes.

## `Datastore`
Description: [Datastore](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Datastore)

Default:
```json
"Datastore": {
  "Dir": "datastore",
  "Type": "levelds"
}
```

## `Discovery`
Description: [Discovery](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Discovery)

Default:
```json
"Discovery": {
  "LotusGateway": "https://api.chain.love",
  "Policy": {},
  "PollInterval": "24h0m0s",
  "PollRetryAfter": "5h0m0s",
  "PollStopAfter": "168h0m0s",
  "PollOverrides": null,
  "RediscoverWait": "5m0s",
  "Timeout": "2m0s"
}
```

### `Discovery.Policy`
Description: [Policy](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Policy)

Default:
```json
"Policy": {
  "Allow": true,
  "Except": null,
  "Publish": true,
  "PublishExcept": null
}
```

### `Discovery.PollOverrides` Element
Description: [Polling](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Polling)

Default: There are no default `Discovery.PollOverrides` elements. These are created manually.

See Example Config for example.

## `Indexer`
Description: [Indexer](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Indexer)

Default:
```json
"Indexer": {
  "CacheSize": 300000,
  "ConfigCheckInterval": "30s",
  "GCInterval": "30m0s",
  "ShutdownTimeout": "10s",
  "ValueStoreDir": "valuestore",
  "ValueStoreType": "sth"
}
```

## `Ingest`
Description: [Ingest](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Ingest)

Default:
```json
"Ingest": {
  "AdvertisementDepthLimit": 33554432,
  "EntriesDepthLimit": 65536,
  "IngestWorkerCount": 10,
  "PubSubTopic": "/indexer/ingest/mainnet",
  "RateLimit": {},
  "StoreBatchSize": 4096,
  "SyncTimeout": "2h0m0s",
  "HttpSyncRetryWaitMin": "1s",
  "HttpSyncRetryWaitMax": "30s",
  "HttpSyncRetryMax": 4,
  "HttpSyncTimeout": "10s"
}
```

### `Ingest.RateLimit`
Description: [RateLimit](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#RateLimit)

Default:
```json
"RateLimit": {
  "Apply": false,
  "Except": null,
  "BlocksPerSecond": 100,
  "BurstSize": 500
}
```

## `Logging`
Description: [Logging](https://pkg.go.dev/github.com/filecoin-project/storetheindex/config#Logging)

Default:
```json
"Logging": {
  "Level": "info",
  "Loggers": {
    "basichost": "warn",
    "bootstrap": "warn",
    "dt-impl": "warn",
    "dt_graphsync": "warn",
    "graphsync": "warn"
  }
}
```

## Runtime Reloadable Items
The storetheindex daemon can reload some portions of its config without restarting the entire daemon. This is done by editing the config file and then using the admin sub-command `reload-config` or sending the daemon process a `SIGHUP` signal. The daemon will automatically reload the edited config after 30 seconds when the daemon is run with the `--watch-config` flag or with the environ variable `STORETHEINDEX_WATCH_CONFIG=true`. The reloadable portions of the config files are:

- [`Discovery.Policy`](#discoverypolicy)
- [`Indexer.ConfigCheckInterval`](#indexer)
- [`Indexer.ShutdownTimeout`](#indexer)
- [`Ingest.IngestWorkerCount`](#ingest)
- [`Ingest.RateLimit`](#ingestratelimit)
- [`Ingest.StoreBatchSize`](#ingest)
- [`Logging`](#logging)
