# cross-announce

Cross-announce propagates provider registrations from one IPNI indexer to another. It lists all providers on a source indexer, then sends an announce request for each to a target indexer. The tool only transfers advertisement pointers; it does not copy or sync index data. The target indexer fetches advertisement chains independently after receiving an announcement.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-source` | (empty) | Base URL of the source indexer. Must point to the find API (default port 3000). |
| `-target` | (empty) | Base URL of the target indexer. Must point to the ingest API (default port 3001). |
| `-pid` | (empty) | If set, only announce the provider with this peer ID. Omit to announce all providers. |
| `-help` | `false` | Print usage text and exit. |

## Port distinction

The `-source` URL must target the **find** interface (port 3000 by default) because the tool reads provider listings through the find API. The `-target` URL must target the **ingest** interface (port 3001 by default) because announcements are submitted through the ingest API. The client constructors only parse these URLs, so a wrong port is not rejected up front; it fails later, when the provider listing or the announce request is made.

## Container invocation

The binary is included in the published `storetheindex` image. Override the default entrypoint to run it:

```bash
docker run --rm \
  --entrypoint /usr/local/bin/cross-announce \
  ghcr.io/ipni/storetheindex@sha256:abcd1234... \
  -source http://source-indexer:3000 \
  -target http://target-indexer:3001
```

Pin the image by digest rather than tag to ensure a reproducible binary.

The usage text printed by `-help` shows a `go run ./scripts/cross_announce/main.go` invocation. That path does not exist inside the image; use the binary at `/usr/local/bin/cross-announce` as shown above.

## Verification

After a run, use `scripts/compare_providers` to verify that providers on the source and target are in sync:

```bash
go run ./scripts/compare_providers/main.go \
  -source http://source-indexer:3000 \
  -target http://target-indexer:3000
```

Note that compare_providers queries the find API (port 3000) on both indexers.

## Warning

This tool has previously triggered full advertisement chain resyncs on target indexers. Do not point it at a production indexer without first reading and understanding the current flag set and the state of both indexers. Use `-pid` to limit scope when testing.
