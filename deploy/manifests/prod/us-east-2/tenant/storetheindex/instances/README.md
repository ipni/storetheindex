# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `STHBurstRate` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `json`       | all                 | `8388608`      | `1s`              | [842ef52a78fb9158a8797f8cd5ac3db026fbf6c7](https://github.com/filecoin-project/storetheindex/commit/842ef52a78fb9158a8797f8cd5ac3db026fbf6c7) |
| `tara`   | 30           | 5             | `json`       | all                 | `83886080`     | `6s`              | [778339d270108841997806c86203ddd3a7341fcb](https://github.com/filecoin-project/storetheindex/commit/778339d270108841997806c86203ddd3a7341fcb) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `8388608`      | `1s`              | [842ef52a78fb9158a8797f8cd5ac3db026fbf6c7](https://github.com/filecoin-project/storetheindex/commit/842ef52a78fb9158a8797f8cd5ac3db026fbf6c7) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `8388608`      | `3s`              | [3478e97a62a25dc15934565f860c13088ff2f602](https://github.com/filecoin-project/storetheindex/commit/3478e97a62a25dc15934565f860c13088ff2f602) |
| `oden`   | N/A Pebble   | 5             | N/A Pebble   | `nft.storage` only  | N/A Pebble     | N/A Pebble        | [3478e97a62a25dc15934565f860c13088ff2f602](https://github.com/filecoin-project/storetheindex/commit/3478e97a62a25dc15934565f860c13088ff2f602) |
| `dido`   | N/A Pebble   | 5             | N/A Pebble   | all                 | N/A Pebble     | N/A Pebble        | [328150e710bf9a13b433d24048b867590d229257](https://github.com/filecoin-project/storetheindex/commit/328150e710bf9a13b433d24048b867590d229257) |
