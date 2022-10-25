# Instances

List of individually configurable instances:

| Instance | sth bit-size | IOPS per GiB  | Value Codec  | Whitelist           | `STHBurstRate` | `STHSyncInterval` | Running                                                                                                                                       |
|----------|--------------|---------------|--------------|---------------------|----------------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| `romi`   | 30           | 5             | `binaryjson` | all                 | `8388608`      | `1s`              | [9df396fbbc40ca634872a47acae5a6b4008cf2e1](https://github.com/filecoin-project/storetheindex/commit/9df396fbbc40ca634872a47acae5a6b4008cf2e1) |
| `tara`   | 30           | 5             | `binaryjson` | all                 | `83886080`     | `6s`              | [a51f131e986b9ac3cbfd893e9ebc7669345a25d1](https://github.com/filecoin-project/storetheindex/commit/a51f131e986b9ac3cbfd893e9ebc7669345a25d1) |
| `xabi`   | 30           | 5             | `binary`     | all                 | `8388608`      | `1s`              | [9df396fbbc40ca634872a47acae5a6b4008cf2e1](https://github.com/filecoin-project/storetheindex/commit/9df396fbbc40ca634872a47acae5a6b4008cf2e1) |
| `vega`   | 30           | 5             | `binary`     | `nft.storage` only  | `8388608`      | `3s`              | [9df396fbbc40ca634872a47acae5a6b4008cf2e1](https://github.com/filecoin-project/storetheindex/commit/9df396fbbc40ca634872a47acae5a6b4008cf2e1) |
| `oden`   | N/A Pebble   | 5             | N/A Pebble   | `nft.storage` only  | N/A Pebble     | N/A Pebble        | [9df396fbbc40ca634872a47acae5a6b4008cf2e1](https://github.com/filecoin-project/storetheindex/commit/078d43ca27a0a57f4a568bc67f626ded2a44ecff) |
| `dido`   | N/A Pebble   | 5             | N/A Pebble   | all                 | N/A Pebble     | N/A Pebble        | [9df396fbbc40ca634872a47acae5a6b4008cf2e1](https://github.com/filecoin-project/storetheindex/commit/9df396fbbc40ca634872a47acae5a6b4008cf2e1) |
