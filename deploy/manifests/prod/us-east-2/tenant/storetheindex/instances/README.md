# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `GCScanFree` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|--------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `json`       | all                 | `false`      | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `tara`   | 30           | 5             | `json`       | all                 | `true`       | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `true`       | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `false`      | `3s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
