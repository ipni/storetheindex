# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `STHBurstRate` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `binaryjson` | all                 | `8388608`      | `1s`              | [32e0eed4e3a0c4b3622965b89566454bb6406e78](https://github.com/filecoin-project/storetheindex/commit/32e0eed4e3a0c4b3622965b89566454bb6406e78) |
| `tara`   | 30           | 5             | `binaryjson` | all                 | `83886080`     | `6s`              | [32e0eed4e3a0c4b3622965b89566454bb6406e78](https://github.com/filecoin-project/storetheindex/commit/2d22525db807e5a823961bfc3b242fe016cc9a80) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `8388608`      | `1s`              | [2d22525db807e5a823961bfc3b242fe016cc9a80](https://github.com/filecoin-project/storetheindex/commit/32e0eed4e3a0c4b3622965b89566454bb6406e78) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `8388608`      | `3s`              | [32e0eed4e3a0c4b3622965b89566454bb6406e78](https://github.com/filecoin-project/storetheindex/commit/2d22525db807e5a823961bfc3b242fe016cc9a80) |
| `oden`   | N/A Pebble   | 5             | N/A Pebble   | `nft.storage` only  | N/A Pebble     | N/A Pebble        | [2d22525db807e5a823961bfc3b242fe016cc9a80](https://github.com/filecoin-project/storetheindex/commit/078d43ca27a0a57f4a568bc67f626ded2a44ecff) |
| `dido`   | N/A Pebble   | 5             | N/A Pebble   | all                 | N/A Pebble     | N/A Pebble        | [2d22525db807e5a823961bfc3b242fe016cc9a80](https://github.com/filecoin-project/storetheindex/commit/2d22525db807e5a823961bfc3b242fe016cc9a80) |
