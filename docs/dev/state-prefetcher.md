# State Prefetcher Runbook

Status: experimental, opt-in. Keep it disabled unless you are benchmarking,
soaking a non-production node, or investigating sync throughput.

## Configuration

CLI:

```bash
gtron --state.prefetch.enabled \
  --state.prefetch.workers 4 \
  --state.prefetch.lookahead 8
```

TOML:

```toml
[state.prefetch]
enabled = true
workers = 4       # 0 = GOMAXPROCS(0)/2, capped internally
lookahead = 8     # 0 = default
```

The default is `enabled = false`. Leaving it off exactly recovers the
pre-prefetch block execution path.

The 2026-07-31 default-on candidate was rejected before deployment. On the
Apple M1 Max heavy/hot-state benchmark, the five-sample baseline was roughly
2.59 ms/block while the 2-4 worker variants were roughly 5.35-5.51 ms/block;
allocations grew from roughly 16.4k to 47.6k per block. Cold-state latency
improved substantially, but production profiling showed a 99% Pebble block
cache hit rate, so the hot-state regression is the relevant rollout gate. The
session-scoped redesign must remove this worker/key-extraction overhead before
another default-on attempt.

## Benchmarking

Run the repeatable ProcessBlock benchmark harness before changing defaults:

```bash
scripts/dev/state_prefetch_benchmark.sh
```

For a quick smoke sample:

```bash
scripts/dev/state_prefetch_benchmark.sh --short --outdir /tmp/prefetch-smoke
```

The harness writes:

- `metadata.txt` — commit, branch, Go version, host info, and exact command
- `benchmark.txt` — raw `go test` benchmark output
- `benchstat.txt` — optional summary when `benchstat` is installed

After collecting a real multi-count sample, run the acceptance checker on the
raw benchmark output:

```bash
scripts/dev/state_prefetch_benchmark_acceptance.py \
  build/state-prefetch-bench/<run>/benchmark.txt \
  --min-samples 5 \
  --min-heavy-improvement 0.10 \
  --max-light-overhead 0.01 \
  --max-bytes-overhead 0.05 \
  --max-allocs-overhead 0.05
```

Without `--variant`, the checker selects the `prefetch=on_*` variant that meets
the gates and has the best heavy cold-state improvement. Use `--variant` when
validating proposed default worker/lookahead pairs; when repeated, every
explicit variant must pass the gates. Explicit variants must still be
`prefetch=on...` rows, so the `prefetch=off` baseline cannot pass as a
candidate when thresholds are loosened. The `--max-bytes-overhead` and
`--max-allocs-overhead` gates are optional but recommended for rollout
decisions; they fail any selected variant whose `B/op` or `allocs/op` median
grows beyond the configured ratio on any required benchmark case. The checker
requires at least five samples per required baseline and candidate case by
default, matching the harness `--count 5`; use `--min-samples 1` only for an
explicit smoke run. The checker is a benchmark gate only; keep the Nile/mainnet
replay soak as the final default-on gate.

The underlying focused benchmark command is:

```bash
go test ./core -run '^$' \
  -bench 'BenchmarkProcessBlock_(LightTRX|HeavyTRX)_(HeavyState|ColdState)' \
  -benchtime=10x -count=5 -benchmem
```

The benchmark variants are:

- `prefetch=off`
- `prefetch=on_workers=2_lookahead=8`
- `prefetch=on_workers=4_lookahead=8`
- `prefetch=on_workers=8_lookahead=8`

`LightTRX` is a one-transaction block, covering the skip path where enabling
prefetch config must not start workers. `HeavyTRX` is a 512-transaction transfer
block. `HeavyState` measures the normal in-memory test DB path. `ColdState`
wraps the same DB with a deterministic first-read latency and shared warm-key
map so the benchmark can expose prefetch overlap without requiring a slow
physical disk. Treat it as a tuning signal, not as a replacement for
Nile/mainnet replay.

## Sync Soak Evidence

When the sync service imports a segment, the summary and detail logs include
`statePrefetchEnqueued`, `statePrefetchDropped`, `statePrefetchProcessed`,
`statePrefetchHits`, `statePrefetchMisses`, and `statePrefetchErrors`. These
fields are diagnostic only; they do not affect consensus state.
`Enqueued` and `Processed` count unique accepted warmup keys for the prefetcher
lifetime; repeated keys are skipped without being counted as queue drops, so a
run with heavy Blackhole/reward-cursor reuse can show lower enqueue counts than
the raw number of extracted hints.

For Nile/mainnet soak runs, pass the gtron log to
`scripts/dev/nile_sync_sample.sh --sync-log-file <path>`. The sampler exports
the same counters as `syncLogStatePrefetch*` JSONL fields, so prefetch-on and
prefetch-off runs can be compared against throughput, memory, and stage-progress
evidence before changing defaults.
When those rows are collected with `--prometheus-output`, the sampler also emits
`gtron_nile_sync_log_state_prefetch_*` gauges for non-negative counter values.
Gate prefetch comparison runs with
`scripts/dev/nile_sync_acceptance.py --require-state-prefetch-evidence`; add
`--require-state-prefetch-activity` for prefetch-on runs and
`--max-state-prefetch-errors 0` for production soak evidence.

## Rollout Gate

Do not flip the default until all of these hold:

- `go test ./... -count=1 -timeout 300s` passes.
- `go test -race ./core ./core/state -run 'TestProcessBlockStatePrefetch|TestStatePrefetcher' -count=1` passes.
- Heavy-block benchmark improves meaningfully on cold-state and does not regress
  light/hot blocks beyond noise.
- A long Nile or mainnet replay soak with prefetch on/off produces identical
  state roots and transaction info, with stable memory and DB read amplification.

## Disable When

- The node is production-critical and prefetch has not been soaked on the same
  workload.
- CPU is saturated by Pebble compaction or VM execution; extra read workers can
  contend with the serial execution goroutine.
- Benchmark output shows worker counts above 2-4 increasing allocations or wall
  time on hot-state workloads.
