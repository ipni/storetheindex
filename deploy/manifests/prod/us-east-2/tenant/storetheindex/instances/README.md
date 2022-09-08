# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `GCScanFree` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|--------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `json`       | all                 | `false`      | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `tara`   | 30           | 5             | `json`       | all                 | `true`       | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `true`       | `1s`              | [945940507682064093e846ecc8578a58a5f16535](https://github.com/filecoin-project/storetheindex/commit/945940507682064093e846ecc8578a58a5f16535) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `false`      | `3s`              | [1223d1070d8675441356c2fda92ce2c872f0f189](https://github.com/filecoin-project/storetheindex/commit/1223d1070d8675441356c2fda92ce2c872f0f189) |
| `oden`   | N/A Pebble         | 5             | N/A Pebble    | `nft.storage` only | N/A Pebble     | N/A Pebble         | [cf5098acf49b51915b610f3929643489db95f6e7](https://github.com/filecoin-project/storetheindex/commit/cf5098acf49b51915b610f3929643489db95f6e7) |
| `dido`   | N/A Pebble         | 5             | N/A Pebble    | all                | N/A Pebble     | N/A Pebble         | [6645a8260473dd9b1a15a77d310db5446f186846](https://github.com/filecoin-project/storetheindex/commit/6645a8260473dd9b1a15a77d310db5446f186846) |
