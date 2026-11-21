# cross-announce

Cross-announce propagates provider registrations from one IPNI indexer to another. It lists all providers on a source indexer, then sends an announce request for each to a target indexer. The tool only transfers advertisement pointers; it does not copy or sync index data. The target indexer fetches advertisement chains independently after receiving an announcement.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-source` | (empty) | Base URL of the source indexer. Must point to the find API (default port 3000). |
| `-target` | (empty) | Base URL of the target indexer. Must point to the ingest API (default port 3001). |
| `-target-find` | (empty) | Base URL of the target indexer's find API (default port 3000). Used to compare publisher heads against the target and run the safety guards. Required unless `-allow-no-target-find` is set. |
| `-pid` | (empty) | If set, only announce the publisher of the provider with this peer ID. Omit to announce all providers. |
| `-timeout` | `30m` | Overall run timeout; `0` disables. A timed-out run exits non-zero so the Kubernetes Job is marked Failed. |
| `-http-timeout` | `30s` | Per-request HTTP timeout. |
| `-max-failures` | `0` | Exit non-zero when the failure count exceeds this value. With `0`, a single failure fails the Job. |
| `-dry-run` | `false` | Log what would be announced without making any requests to the target. |
| `-skip-inactive` | `true` | Drop source publisher groups where every provider is `Inactive`. |
| `-skip-lagging` | `false` | Drop candidates whose target group has a non-zero `Lag` with a fresh `LastAdvertisementTime`. Off by default because a stale `Lag` would otherwise skip a provider on every run. |
| `-lag-fresh-within` | `1h` | A target `LastAdvertisementTime` within this window is fresh enough for `-skip-lagging` to act on a non-zero `Lag`. |
| `-allow-no-target-find` | `false` | Allow running without `-target-find`, which disables all safety guards and announces every candidate. |
| `-concurrency` | `4` | Number of candidates announced at once. Values below 1 run serially. |
| `-help` | `false` | Print usage text and exit. |

## Port distinction

The `-source` URL must target the **find** interface (port 3000 by default) because the tool reads provider listings through the find API. The `-target` URL must target the **ingest** interface (port 3001 by default) because announcements are submitted through the ingest API. The `-target-find` URL, when set, must target the target's **find** interface (port 3000 by default) so the tool can read the target's provider list. The client constructors only parse these URLs, so a wrong port is not rejected up front; it fails later, when the provider listing or the announce request is made.

## Publisher-keyed selection

Providers are grouped by publisher before anything is announced, because the target's own short-circuit is per publisher. Within a publisher group the provider with the newest parseable `LastAdvertisementTime` is the head; ties (and groups with no parseable time) break on the lowest provider ID, never on source order, because the source's `/providers` ordering is not stable. A head equal to the target's head for the same publisher is counted `upToDate` and dropped.

## Safety guards

A head that is not up to date passes through an ordered list of guards before it becomes a candidate. A publisher that is unknown to the target (absent, or registered but never ingested) short-circuits straight to the candidate list, since those are the providers that most need announcing. Otherwise, in order: a source group that is entirely `Inactive` is dropped (`inactive`) when `-skip-inactive` is set; a target group with a non-zero `Lag` and a fresh `LastAdvertisementTime` is dropped (`lagging`) when `-skip-lagging` is set; a target whose `LastAdvertisementTime` is at or after the source's is dropped (`targetAhead`); and a comparison that cannot be made because a timestamp is missing or unparseable is dropped (`unverifiable`).

`LastAdvertisementTime` is the time each indexer *received* the advertisement, not a property of the advertisement. The two receive times proxy for chain position only while both indexers are healthy and following the same publisher; during a backfill or a stuck sync on one side the inference inverts, so the guards are conservative. `Lag` is never reset on completion, so a non-zero `Lag` is usually a stale artefact; the freshness window in `-skip-lagging` is what stops a stale `Lag` from starving a provider on every run.

The run summary reports `total`, `announced`, `skipped`, `deduped`, `upToDate`, `inactive`, `lagging`, `targetAhead`, `unverifiable`, `notAllowed`, and `failed`; every source provider lands in exactly one of `skipped`, `deduped`, or a head, and each head lands in exactly one of the remaining counters.

## Concurrency

Candidates are announced by a bounded worker pool, `-concurrency` wide, so a scheduled run finishes in a predictable window. The number bounds **advertisement chain walks started on the target, not HTTP load**: the target answers `PUT /announce` with 204 immediately and syncs on a background context, so `-concurrency 4` means up to four concurrent syncs beginning on the target. That is why the default is low. Values below 1 are clamped up to 1 rather than rejected, so a bad cron argument degrades to a serial run instead of failing it.

Worker output interleaves, so the per-line prefix is a completed count rather than a candidate index. `-dry-run` starts no workers and prints in selection order. A cancelled run, from `-timeout` or SIGTERM, stops dispatching, prints the partial summary, and exits non-zero.

A panic inside a worker is recovered per candidate and recorded as a `failed` outcome; the worker keeps serving the rest of the run, so the pool keeps its full width and every candidate is still accounted for in the summary.

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
