# gtron observability design

## Context

gtron already has structured module logging, JSON debug metrics, pprof, state
hotspot inspection, RPC counters, Pebble/freezer metrics and VM tracing. The
missing production loop is a stable Prometheus surface, safe separation between
monitoring and invasive debugging, bounded log retention, and automatic
diagnostic artifacts for failures that are difficult to reproduce.

Erigon's useful operating model is the reference: structured console and file
logging with rotation, Prometheus/Grafana integration, pprof/runtime tracing,
and opt-in continuous or near-OOM profiling. gtron should adopt the operating
model without importing Erigon's legacy diagnostics event bus.

## Security boundary

The public Wallet, JSON-RPC and gRPC APIs are application surfaces. Metrics and
runtime debugging are operator surfaces and have separate listeners:

- Prometheus is opt-in, loopback-bound by default and serves metrics only.
- pprof/debug remains separately opt-in and loopback-bound by default.
- Nginx must not proxy either operator surface to the unauthenticated public
  gateway.

Metrics and pprof are deliberately not combined. Scraping metrics must never
make heap contents, goroutine stacks or CPU profiling remotely reachable.

## Metrics model

go-ethereum's process-wide metrics registry remains the single in-process
registry because gtron already uses it across RPC, Pebble and freezer code.
The Prometheus adapter exposes it at `/metrics`, with
`/debug/metrics/prometheus` retained as an Erigon-compatible alias.

The first stable node gauges cover:

- canonical and solidified height, plus their lag;
- transport-connected and TRON-handshaked peer counts;
- pending transaction count;
- active sync state and estimated remaining blocks;
- Go process CPU, memory, scheduler and disk counters already provided by the
  registry collector.

Metric names are slash-separated internally and underscore-normalized by the
Prometheus adapter. New labels must remain low-cardinality; peer IDs, hashes,
addresses and RPC request values must never become labels.

## Logging model

Console output keeps its selected terminal, JSON or logfmt format. The optional
file sink supports the same formats with an independent level; JSON remains the
compatibility default. File logs rotate at bounded size, retain a bounded
number/age of backups, compress archives, and use `0600` permissions.
Per-module level overrides apply to both sinks.

Startup arguments are useful incident evidence but secret-bearing flag values
must be redacted before logging. The witness private key, snapshot signing keys
and snapshot URLs are in the initial sensitive set.

## Runtime and performance diagnostics

pprof stays on the existing dedicated debug listener. Block and mutex profiles
remain opt-in because they add overhead. Later increments should add:

1. SIGUSR1-triggered goroutine and heap dumps to a bounded diagnostics folder;
2. an opt-in near-OOM heap monitor with cooldown/hysteresis;
3. pprof goroutine labels around sync, block execution and DB maintenance;
4. an opt-in continuous-profiling exporter (for example Pyroscope) without
   changing the local pprof workflow.

Debug JSON endpoints are kept for ad-hoc inspection but are not a substitute
for alertable Prometheus metrics.

## Rollout and compatibility

All new listeners and expensive diagnostics default off. Existing pprof and
logging flags retain their behavior. Metrics ports are explicitly assigned per
instance in deployment because mainnet and Nile share a host. A rollout must:

1. remove public Nginx debug routes;
2. deploy the new binary with loopback-only metrics ports;
3. validate scrape and alert rules;
4. verify public debug URLs reject access;
5. watch file descriptors, disk growth, scrape duration and node latency.
