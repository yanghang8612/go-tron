# gtron observability

gtron exposes metrics and runtime debugging on separate, opt-in listeners.
Neither listener provides authentication, so both should remain bound to
loopback and must not be proxied by the public API gateway.

## Metrics

Enable the Prometheus listener with:

```bash
gtron --metrics --metrics.addr 127.0.0.1 --metrics.port 6071
curl http://127.0.0.1:6071/metrics
```

`/debug/metrics/prometheus` is an Erigon-compatible alias for `/metrics`.
The metrics listener does not serve pprof. The repository's Prometheus scrape
configuration uses `6071` for mainnet and `6072` for Nile so the two instances
can run on the same host without colliding with the existing pprof ports.

The first operator-level gauges are:

| Prometheus name | Meaning |
| --- | --- |
| `chain_head_block` | canonical head height |
| `chain_solidified_block` | latest solidified block |
| `chain_solidified_lag` | head minus solidified height |
| `p2p_peers_connected` | transport-level peers |
| `p2p_peers_handshaked` | peers that completed TRON handshake |
| `txpool_pending` | transactions waiting in the pool |
| `sync_active` | 1 while a sync session is active |
| `sync_remaining_blocks` | estimated sync backlog |
| `state_commitment_rebuild_active` | number of full commitment branch rebuilds currently active |
| `state_commitment_rebuild_current_rows_scanned` | authoritative latest-domain rows scanned by the active/latest rebuild |
| `state_commitment_rebuild_current_bytes_scanned` | physical key and value bytes scanned by the active/latest rebuild |
| `state_commitment_rebuild_current_batches_folded` | commitment batches completed by the active/latest rebuild |
| `state_commitment_rebuild_last_progress_unix` | Unix timestamp of the latest rebuild phase/counter advance |
| `state_lifecycle_failure_active` | 1 while the snapshot/prune lifecycle has an unresolved pass failure |

The exporter also includes the existing RPC, Pebble/freezer and Go process
metrics registered through go-ethereum's metrics package.

Prometheus, alert-rule and Grafana provisioning examples live under
`deploy/observability/`.

## Rotating logs

The optional file sink supports terminal, JSON and logfmt output independently
of the console format, and creates the active file with `0600` permissions.
JSON remains the compatibility default. Defaults mirror the bounded Erigon
operating profile: rotate at 100 MiB, keep 3 backups for up to 28 days, and
compress rotated files.

```bash
gtron \
  --verbosity 3 \
  --log.file /data/gtron/main/gtron.log \
  --log.file.format terminal \
  --log.file.verbosity 4 \
  --log.file.max-size 100 \
  --log.file.max-backups 3 \
  --log.file.max-age 28 \
  --log.file.compress
```

`--log.file.format` accepts `terminal`, `json`, or `logfmt`, while
`--log.file.verbosity=-1` inherits `--verbosity`. Module overrides still apply
to both sinks. Startup arguments are logged for incident reconstruction after
secret-bearing flag values have been redacted.

## Sync progress logs

At info level, `Imported chain segment` is a compact operator status line. It
reports the current applied height (`head`), current throughput (`blocks/s`,
`txs/s`), transaction mix (`txTop`), the leading state mutation classes
(`stateMutTop`, `stateMutKVTop`), and peer/queue health (`peers`,
`activePeers`, `inflight`, `buffered`, `requested`, `retries`). The `blocks`,
`txs`, and `elapsed` values describe the latest reporting window, not the whole
session.

`Sync progress` is emitted separately on natural wall-clock boundaries during
active imports. Every `:00/:05/.../:55` it reports the preceding natural five
minutes; `:00/:30` also reports 30 minutes; the top of each hour also reports
one hour; and local midnight also reports the preceding calendar day. These
boundaries use the server's local timezone, not process uptime or sync-session
start time.

Each record identifies its bucket with `window`, `from`, and `to`, then reports
`coverage`, `windowBlocks`, `head`, `target`, `progress`, `remain`, `eta`, and
`avgBlocks/s`, `minBlocks/s`, `maxBlocks/s`. `coverage` is the percentage of
the natural bucket observed by this process, so the first record after startup
can be partial. The average uses the observed wall-clock duration; minimum and
maximum are the slowest and fastest real-time import intervals in the bucket,
with an observed idle gap making the minimum zero. Speed history survives sync
session changes but naturally resets when the process restarts.

Execution-phase, granular state-commit/mutation, peer-state, and staged-import
planner fields are emitted separately as `Imported chain segment details` at
debug level. Enable them only while collecting a sync diagnostic sample:

```bash
gtron ... --log.module net/sync=debug
```

The normal info configuration does not construct or emit the large detail
record. The Nile sampling script combines the latest real-time segment,
periodic progress, and (when enabled) debug detail records into one sample.

## Commitment rebuild and recovery logs

A commitment branch rebuild is a full scan of the authoritative
`account-latest`, `kv-generation`, and `kv-latest` tables. It rebuilds a
derived branch index; it does not redownload or re-execute the chain. Recovery
from an immutable branch-base root mismatch emits the marker tx/root and the
independently observed snapshot root in the start event.

The lifecycle is observable at normal info verbosity:

| Event | When | Important fields |
| --- | --- | --- |
| `Commitment branch rebuild started` | once | `reason`, `mode`, `trigger`, `snapshotTxNum`, roots, batch limits |
| `Commitment branch rebuild source started` | source-table transition | `source`, cumulative rows/bytes, `elapsed` |
| `Commitment branch rebuild progress` | every 30 seconds | `phase`, `source`, rows/bytes, folded batches, rates, `sinceLastProgress` |
| `Commitment branch rebuild completed` | once on success | final root, totals, `elapsed` |
| `Commitment branch rebuild failed` | once on failure | last phase/source, totals, `err` |

There is intentionally no percent or ETA: pre-counting the full keyspace would
perform another expensive scan and delay recovery. Progress is proven by
monotonically increasing `rowsScanned`, `bytesScanned`, or `batchesFolded`.
During a long fold, those counters can remain unchanged while `phase=fold`;
`sinceLastProgress` makes that interval explicit.

The sync watchdog does not re-kick or start a fetch session while
`state_commitment_rebuild_active` is nonzero. The rebuild progress record is
the authoritative explanation for a stationary chain head during that time.
Outside rebuilds, a stalled fetch emits one Info re-kick, suppresses identical
30-second attempts, emits a Warn summary every 10 minutes, and emits an Info
recovery transition when the head/session advances.

Snapshot/prune pass errors follow the same stateful policy: first failure,
one continuation summary per 10 minutes, then recovery. History coverage
failures expose `historyProgress`, `visibleCoverage`, and `coverageGap` as
numeric fields instead of requiring operators to parse the error string.

Suggested alerts:

- warning when a rebuild remains active for 30 minutes;
- critical when `state_commitment_rebuild_last_progress_unix` is unchanged for
  5 minutes while the active gauge is nonzero;
- critical on `Commitment branch rebuild failed` or a repeatedly active
  `state_lifecycle_failure_active` gauge.

For a concise live view:

```bash
tail -F /data/gtron/main/gtron.log | \
  rg 'Commitment branch rebuild|Sync fetch remains stalled|snapshot/prune pass'
```

## Runtime debugging

pprof remains separately controlled by `--pprof.addr` and `--pprof.port`.
Use a local request or an SSH tunnel to collect profiles; do not publish it
through Nginx:

```bash
go tool pprof http://127.0.0.1:6062/debug/pprof/profile?seconds=30
curl -o heap.pb.gz http://127.0.0.1:6062/debug/pprof/heap
curl http://127.0.0.1:6062/debug/goroutines
```

Block and mutex profiling add runtime overhead and are disabled until toggled:

```bash
curl 'http://127.0.0.1:6062/debug/block?rate=1000000'
curl 'http://127.0.0.1:6062/debug/mutex?fraction=10'
```

## Public gateway security check

The public Nginx server must reject both debug prefixes before a release is
considered deployed:

```nginx
location ^~ /debug/ {
    return 404;
}

location ^~ /debug-nile/ {
    return 404;
}
```

After `nginx -t` and reload, verify from outside the server:

```bash
curl --proxy socks5h://127.0.0.1:1088 -o /dev/null -w '%{http_code}\n' \
  http://3.12.206.71:6060/debug/pprof/
curl --proxy socks5h://127.0.0.1:1088 -o /dev/null -w '%{http_code}\n' \
  http://3.12.206.71:6060/debug/metrics
curl --proxy socks5h://127.0.0.1:1088 -o /dev/null -w '%{http_code}\n' \
  http://3.12.206.71:6060/debug-nile/pprof/
```

All three requests must return `404` (or an authenticated gateway response),
while direct loopback access on the host remains available to operators.
