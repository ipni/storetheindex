# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `STHBurstRate` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `binaryjson` | all                 | `8388608`      | `1s`              | [32e0eed4e3a0c4b3622965b89566454bb6406e78](https://github.com/filecoin-project/storetheindex/commit/32e0eed4e3a0c4b3622965b89566454bb6406e78) |
| `tara`   | 30           | 5             | `binaryjson` | all                 | `83886080`     | `6s`              | [c1b4f60ee5c24fac7b35685506fa34b2a332d923](https://github.com/filecoin-project/storetheindex/commit/c1b4f60ee5c24fac7b35685506fa34b2a332d923) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `8388608`      | `1s`              | [32e0eed4e3a0c4b3622965b89566454bb6406e78](https://github.com/filecoin-project/storetheindex/commit/32e0eed4e3a0c4b3622965b89566454bb6406e78) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `8388608`      | `3s`              | [3478e97a62a25dc15934565f860c13088ff2f602](https://github.com/filecoin-project/storetheindex/commit/3478e97a62a25dc15934565f860c13088ff2f602) |
| `oden`   | N/A Pebble   | 5             | N/A Pebble   | `nft.storage` only  | N/A Pebble     | N/A Pebble        | [3478e97a62a25dc15934565f860c13088ff2f602](https://github.com/filecoin-project/storetheindex/commit/3478e97a62a25dc15934565f860c13088ff2f602) |
| `dido`   | N/A Pebble   | 5             | N/A Pebble   | all                 | N/A Pebble     | N/A Pebble        | [3478e97a62a25dc15934565f860c13088ff2f602](https://github.com/filecoin-project/storetheindex/commit/3478e97a62a25dc15934565f860c13088ff2f602) |
