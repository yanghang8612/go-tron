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
