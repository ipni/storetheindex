# The storetheindex config file

The storetheindex config file is a JSON document located at `$STORETHEINDEX_PATH/config`. It is read once when starting the daemon. Some portions may be re-read at runtime.

The config file is created using the `storetheindex init`. To upgrade an old config file to the current version, or to a add missing items to an existing config file, run `storetheindex init --upgrade`.

For documentation of config items, refer to the [online documentation](https://pkg.go.dev/github.com/ipni/storetheindex/config).

## Runtime Reloadable Items
The storetheindex daemon can reload some portions of its config without restarting the entire daemon. This is done by editing the config file and then using the admin sub-command `reload-config` or sending the daemon process a `SIGHUP` signal. The daemon will automatically reload the edited config after 30 seconds when the daemon is run with the `--watch-config` flag or with the environ variable `STORETHEINDEX_WATCH_CONFIG=true`. The reloadable portions of the config files are:

- `Discovery.Policy`
- `Indexer.ConfigCheckInterval`
- `Indexer.ShutdownTimeout`
- `Ingest.IngestWorkerCount`
- `Ingest.Skip500EntriesError`
- `Logging`
- `Peering`
