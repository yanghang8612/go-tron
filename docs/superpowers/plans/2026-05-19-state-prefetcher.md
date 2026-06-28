# State prefetcher — plan

**Spec:** [2026-05-19-state-prefetcher-design.md](../specs/2026-05-19-state-prefetcher-design.md)

## Slice 1 — Audit + key types

- [x] Audit every actuator in [actuator/](../../../actuator) — first-pass
      envelope-derived account, contract-metadata, and delegation rows are
      listed in [state-prefetch-keys.md](../../dev/state-prefetch-keys.md)
- [x] Define `state.PrefetchKey` carrier and first safe raw latest-domain
      key kinds: account latest, account-KV latest, and contract storage.
      Code/TRC10/witness key kinds remain for the actuator audit slices.
- [x] Write the first-pass audit doc `docs/dev/state-prefetch-keys.md`,
      covering implemented contract families and explicitly listing gaps for
      future actuator/domain authors
- [x] Define a single dispatch function `actuator.PrefetchKeysFor(tx)
      []state.PrefetchKey` keyed on contract type

## Slice 2 — Prefetcher driver

- [x] `core/state/prefetcher.go` — `StatePrefetcher` struct + `Start /
      Stop / Enqueue`
- [x] Worker pool: `runtime.GOMAXPROCS(0)/2` capped at 8, configurable
- [x] Idle-safe: `Stop()` is idempotent and drains in-flight work
- [x] Tests:
  - [x] `prefetcher_test.go` — enqueue/start/stop, raw latest-domain
        hit/miss/error/drop statistics
  - [x] `prefetcher_race_test.go` — `go test -race -count=3` with
        concurrent main reads + mutations. The current driver avoids
        `StateDB` object-cache mutation from workers; direct cache warming
        must not land without this.

## Slice 3 — Wire into ProcessBlock

- [x] `core/state_processor.go::ProcessBlock` — instantiate prefetcher,
      enqueue keys for `lookahead` upcoming txs each iteration
- [x] Stop prefetcher on success + error paths (defer)
- [x] Gate behind `config.StatePrefetchEnabled` (currently opt-in; default
      remains false until benchmark and soak evidence justify flipping it)
- [x] Tests: existing block-apply tests stay green; one targeted test
      exercising a 100-tx block with prefetch on + off, asserting
      identical StateDB.Commit roots

## Slice 4 — Benchmarks + tuning

- [x] `core/state_processor_prefetch_bench_test.go`:
  - [x] `BenchmarkProcessBlock_LightTRX_HeavyState`
  - [x] `BenchmarkProcessBlock_LightTRX_ColdState` (prefetch skip-path
        overhead on a one-transaction block)
  - [x] `BenchmarkProcessBlock_HeavyTRX_HeavyState`
  - [x] `BenchmarkProcessBlock_HeavyTRX_ColdState` (deterministic first-read
        latency wrapper)
  - [x] Variants: `prefetch=off`, `prefetch=on,workers=2`, `=4`, `=8`
- [x] `scripts/dev/state_prefetch_benchmark.sh` repeatable sweep harness:
      records commit/env metadata, raw benchmark output, and optional
      `benchstat` summaries for comparing samples across machines and commits
- [ ] Pick default `workers / lookahead` from benchmark sweep, document
      in the audit doc
- [ ] Long-running soak: replay 100K Nile blocks with prefetch on/off,
      measure delta wall time and Pebble read amplification (Pebble
      metrics surface)

## Slice 5 — Production rollout

- [x] CLI flags `--state.prefetch.{enabled,workers,lookahead}`
- [x] `gtron.toml [state.prefetch]` section
- [x] Operator doc `docs/dev/state-prefetcher.md`: when to disable,
      benchmark workflow, known gotchas
- [ ] Default `true` after one full Nile soak with no regressions

## Acceptance criteria

- [ ] ≥ 10% ProcessBlock throughput on the heavy-TRX benchmark
- [ ] ≤ 1% overhead on lightblock benchmark
- [ ] Race detector clean across the full sweep
- [ ] No semantic regressions on existing tests
- [x] Leaving `state.prefetch.enabled=false` exactly recovers today's
      behaviour
