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

## Benchmarking

Run the focused ProcessBlock benchmark before changing defaults:

```bash
go test ./core -run '^$' \
  -bench 'BenchmarkProcessBlock_HeavyTRX_(HeavyState|ColdState)' \
  -benchtime=10x -count=5 -benchmem
```

The benchmark variants are:

- `prefetch=off`
- `prefetch=on_workers=2_lookahead=8`
- `prefetch=on_workers=4_lookahead=8`
- `prefetch=on_workers=8_lookahead=8`

`HeavyState` measures the normal in-memory test DB path. `ColdState` wraps the
same DB with a deterministic first-read latency and shared warm-key map so the
benchmark can expose prefetch overlap without requiring a slow physical disk.
Treat it as a tuning signal, not as a replacement for Nile/mainnet replay.

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
