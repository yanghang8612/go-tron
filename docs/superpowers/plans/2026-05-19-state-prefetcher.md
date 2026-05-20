# State prefetcher — plan

**Spec:** [2026-05-19-state-prefetcher-design.md](../specs/2026-05-19-state-prefetcher-design.md)

## Slice 1 — Audit + key types

- [ ] Audit every actuator in [actuator/](../../../actuator) — list the
      deterministic state reads in Validate + Execute per contract type
- [ ] Define `state.PrefetchKey` enum: `AccountKey | StorageKey | CodeKey
      | TRC10Key | WitnessKey` + carrier struct
- [ ] Write the audit doc `docs/dev/state-prefetch-keys.md` (one section
      per contract type, copy-paste ready by future actuator authors)
- [ ] Define `actuator.Prefetcher` interface; implement on every actuator
      OR define a single dispatch function `actuator.PrefetchKeysFor(tx)
      []state.PrefetchKey` keyed on contract type

## Slice 2 — Prefetcher driver

- [ ] `core/state/prefetcher.go` — `StatePrefetcher` struct + `Start /
      Stop / Enqueue`
- [ ] Worker pool: `runtime.GOMAXPROCS(0)/2` capped at 8, configurable
- [ ] Idle-safe: `Stop()` is idempotent and drains in-flight work
- [ ] Tests:
  - [ ] `prefetcher_test.go` — basic enqueue/start/stop, cache
        population assertions
  - [ ] `prefetcher_race_test.go` — `go test -race -count=3` with
        concurrent main reads + mutations

## Slice 3 — Wire into ProcessBlock

- [ ] `core/state_processor.go::ProcessBlock` — instantiate prefetcher,
      enqueue keys for `lookahead` upcoming txs each iteration
- [ ] Stop prefetcher on success + error paths (defer)
- [ ] Gate behind `config.StatePrefetchEnabled` (default true)
- [ ] Tests: existing block-apply tests stay green; one targeted test
      exercising a 100-tx block with prefetch on + off, asserting
      identical StateDB.Commit roots

## Slice 4 — Benchmarks + tuning

- [ ] `core/state_processor_bench_test.go`:
  - [ ] `BenchmarkProcessBlock_HeavyTRX_HeavyState`
  - [ ] `BenchmarkProcessBlock_HeavyTRX_ColdState` (forces disk reads)
  - [ ] Variants: `prefetch=off`, `prefetch=on,workers=2`, `=4`, `=8`
- [ ] Pick default `workers / lookahead` from benchmark sweep, document
      in the audit doc
- [ ] Long-running soak: replay 100K Nile blocks with prefetch on/off,
      measure delta wall time and Pebble read amplification (Pebble
      metrics surface)

## Slice 5 — Production rollout

- [ ] CLI flags `--state.prefetch.{disable,workers,lookahead}`
- [ ] `gtron.toml [state.prefetch]` section
- [ ] Operator doc `docs/dev/state-prefetcher.md`: when to disable,
      benchmark results, known gotchas
- [ ] Default `true` after one full Nile soak with no regressions

## Acceptance criteria

- [ ] ≥ 10% ProcessBlock throughput on the heavy-TRX benchmark
- [ ] ≤ 1% overhead on lightblock benchmark
- [ ] Race detector clean across the full sweep
- [ ] No semantic regressions on existing tests
- [ ] Disable flag exactly recovers today's behaviour
