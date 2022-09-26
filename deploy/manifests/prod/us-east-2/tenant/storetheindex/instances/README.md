# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `STHBurstRate` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `json`       | all                 | `8388608`      | `1s`              | [8936ca9485fcf7895e70e73c61c0fb4170d6e02c](https://github.com/filecoin-project/storetheindex/commit/8936ca9485fcf7895e70e73c61c0fb4170d6e02c) |
| `tara`   | 30           | 5             | `json`       | all                 | `83886080`     | `6s`              | [778339d270108841997806c86203ddd3a7341fcb](https://github.com/filecoin-project/storetheindex/commit/778339d270108841997806c86203ddd3a7341fcb) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `4194304`      | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `4194304`      | `3s`              | [778339d270108841997806c86203ddd3a7341fcb](https://github.com/filecoin-project/storetheindex/commit/778339d270108841997806c86203ddd3a7341fcb) |
| `oden`   | N/A Pebble   | 5             | N/A Pebble   | `nft.storage` only  | N/A Pebble     | N/A Pebble        | [8936ca9485fcf7895e70e73c61c0fb4170d6e02c](https://github.com/filecoin-project/storetheindex/commit/8936ca9485fcf7895e70e73c61c0fb4170d6e02c) |
| `dido`   | N/A Pebble   | 5             | N/A Pebble   | all                 | N/A Pebble     | N/A Pebble        | [8936ca9485fcf7895e70e73c61c0fb4170d6e02c](https://github.com/filecoin-project/storetheindex/commit/8936ca9485fcf7895e70e73c61c0fb4170d6e02c) |
