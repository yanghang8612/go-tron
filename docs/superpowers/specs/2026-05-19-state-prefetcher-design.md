# State prefetcher — design

**Status:** Partial implementation: raw latest-domain prefetch driver,
`actuator.PrefetchKeysFor(tx)` envelope-key extraction, opt-in
`ProcessBlock` wiring, focused benchmarks, and a repeatable benchmark harness
landed. Long replay soak and default-on rollout remain.
**Author:** yanghang8612
**Date:** 2026-05-19
**Inspiration:** [go-ethereum/core/state/trie_prefetcher.go](../../../../ethereum/go-ethereum/core/state/trie_prefetcher.go)
**Related plan:** [2026-05-19-state-prefetcher.md](../plans/2026-05-19-state-prefetcher.md)

## Background

Inside `core/state_processor.go::ProcessBlock`, transactions execute one at
a time:

```go
for _, tx := range block.Transactions() {
    actuator := CreateActuator(tx)
    if err := actuator.Validate(ctx); err != nil { ... }
    if err := actuator.Execute(ctx); err != nil { ... }
}
```

Each tx's `Validate` + `Execute` hits the state for the sender, receiver,
maybe a TRC10 issuer, possibly a witness record, and (for TVM contracts) a
chain of accounts touched through CALL / DELEGATECALL. Every one of those
reads is a **synchronous Pebble Get** that may miss memory caches and hit
disk. Sequential per-tx execution serializes all of these disk waits.

In go-ethereum the same problem is far worse (MPT traversal = multiple Gets
per state read), and they solve it with `core.state.triePrefetcher`:
during tx execution, a background goroutine **walks ahead** in the tx list
and prefetches every account the upcoming tx will touch. By the time the
main thread reaches that tx, the relevant trie nodes are already warm in
the cache.

gtron has a flat state (no MPT) so each "state read" is just one Pebble
Get. The same prefetch idea applies, but with an important current-state
constraint: `StateDB`'s account/storage maps are still single-writer data
structures, so the first safe implementation warms the underlying latest-domain
KV/blockbuffer reads rather than mutating `StateDB` object caches from worker
goroutines.

Initial benchmarks against geth's main-net replay show 10-20% block-apply
throughput uplift from this pattern. gtron should see similar gains on
heavy blocks (DEX trade clusters, dapp activity bursts).

## Goals

- Throughput improvement on Nile + mainnet block import (target ≥ 10% on
  blocks with ≥ 50 tx)
- No semantic change — same StateDB outcome, same per-tx commit order
- Zero overhead on light blocks (≤ 1 tx) — prefetcher skips work
- Composable with existing fixtures, fork-rewind, buffer routing — i.e.
  reads still go through `bc.buffer` so unflushed writes from the current
  block are visible

## Non-goals

- Do NOT change StateDB write semantics. Prefetches are read-only warmups.
- Do NOT prefetch from disk-key-set we don't know upfront (e.g. VM CALL
  targets only known at runtime). Prefetch the things we CAN derive from
  the tx envelope (sender, recipient, contract address, asset issuer).
- Do NOT introduce data races. Until `StateDB` object maps have explicit
  concurrent-read/write locking, worker goroutines must not call `GetAccount`,
  `GetState`, `GetCode`, or other methods that populate those maps. The landed
  driver reads raw latest-domain rows through `ethdb.KeyValueReader` only.

## Mental model

```
main goroutine:        run tx N → run tx N+1 → run tx N+2
prefetcher goroutine:  warm (tx N+1) → warm (tx N+2) → warm (tx N+3)
                       ^ runs in parallel; warms raw KV/blockbuffer caches
```

If the main goroutine catches up, prefetcher idles. If a tx execution is
slow (heavy TVM call), prefetcher gets further ahead.

## Architecture

### Prefetch surface

For each tx envelope type, determine which accounts/objects are
guaranteed-read by `Validate + Execute`:

| Tx type | Prefetch keys |
|---|---|
| `TransferContract` | sender, recipient |
| `TransferAssetContract` | sender, recipient, asset_issue (if first), zen_token if relevant |
| `VoteWitnessContract` | voter, each voted-witness |
| `WitnessCreateContract` | sender, dynamic-property keys for current witness creation cost |
| `FreezeBalanceV2Contract` | sender, delegated-balance index |
| `UnfreezeBalanceV2Contract` | sender, delegated-balance index, unfrozen list |
| `TriggerSmartContract` | sender, contract address, contract code, contract storage root, contract abi |
| `CreateSmartContract` | sender, blackhole (for fee) |
| `ShieldedTransferContract` | merkle current/last tree, nullifier set (cheap; usually cached) |
| (other ~20 types) | per-actuator audit |

The first implementation uses a single dispatcher rather than extending every
actuator:

```go
func actuator.PrefetchKeysFor(tx *types.Transaction) []state.PrefetchKey
```

The dispatcher emits best-effort hints and treats malformed payloads or invalid
addresses as "no prefetch" for that field. That keeps prefetching out of the
consensus validation path.

### Prefetcher driver

`core/state/prefetcher.go` currently provides the race-safe raw latest-domain
driver:

```go
type StatePrefetcher struct {
    db       ethdb.KeyValueReader
    workCh   chan PrefetchKey
    workers  int             // default GOMAXPROCS/2, capped at 8
    stats    StatePrefetcherStats
}

func (p *StatePrefetcher) Start()
func (p *StatePrefetcher) Stop()            // drains workers, idempotent
func (p *StatePrefetcher) Enqueue(keys []PrefetchKey) int
func (p *StatePrefetcher) Stats() StatePrefetcherStats
```

`state_processor.go::ProcessBlock` passes the same `ethdb.KeyValueReader`
surface used for block execution, normally `bc.buffer`, so prefetch reads see
already-buffered previous-block writes without touching `StateDB` caches. The
public test helpers keep prefetch disabled; production `BlockChain.applyBlock`
uses `params.ChainConfig.StatePrefetchEnabled`.
Accepted keys are deduplicated for the worker lifetime. This keeps repeated
hot hints from consuming queue capacity; only keys that could not fit in the
queue are counted as `Dropped`.

```go
prefetcher := state.NewStatePrefetcher(db, state.StatePrefetcherConfig{
    Workers: workers,
})
defer prefetcher.Stop()
prefetcher.Start()

nextPrefetchTx := 0
for i, tx := range txs {
    nextPrefetchTx = enqueueProcessBlockPrefetch(
        prefetcher, txs, i, nextPrefetchTx, lookahead,
    )
    runTx(i, tx)
}
```

The lookahead window is bounded — we don't want to enqueue 5000 keys for a
10K-tx block (memory pressure on the work channel and risk of cache
churn). 8 ahead is enough to keep workers fed without bloat.

### Worker behaviour

Each worker is a simple read loop over raw latest-domain rows:

```go
for key := range workCh {
    switch key.Kind {
    case PrefetchAccountLatest:
        rawdb.ReadStateAccountLatest(db, key.Owner)
    case PrefetchAccountKVLatest:
        generation := key.Generation
        if !key.HasGeneration {
            generation, _, _ = rawdb.ReadStateKVGeneration(db, key.Owner)
        }
        rawdb.ReadStateKVLatest(db, key.Owner, generation, key.Domain, key.Key)
    case PrefetchContractStorage:
        generation := key.Generation
        if !key.HasGeneration {
            generation, _, _ = rawdb.ReadStateKVGeneration(db, key.Owner)
        }
        metadata, _, _ := rawdb.ReadStateKVLatest(db, key.Owner, generation, ContractMetadata, []byte("meta"))
        meta := decodeContractMetadata(metadata)
        rowKey := javaStorageRowKey(key.Owner, key.Slot, meta)
        rawdb.ReadStateKVLatest(db, key.Owner, generation, ContractStorage, rowKey.Bytes())
    }
    // stats record hits, misses, drops, and errors; no consensus state changes.
}
```

Future work may warm `StateDB` object caches directly, but only after those
maps have an explicit concurrency model and race tests.

## Race / correctness

The landed driver shares only the underlying `ethdb.KeyValueReader` with the
main goroutine. Pebble and `blockbuffer.Buffer` already support concurrent
reads; no `StateDB` map is touched by worker goroutines.

Edge case: prefetcher fetches account X; main thread mutates X mid-flight
(during current tx). Mutation goes through the same locked path, so:

1. Prefetcher raw latest-domain read warms the DB/blockbuffer read path.
2. Main Execute remains the only path mutating `StateDB` object caches.
3. Next tx's `StateDB` read observes the same serial state as today.

No staleness is introduced because prefetched values are not stored in a side
cache used for consensus reads.

## Stop semantics

When `ProcessBlock` returns (success or error), prefetcher.Stop():

- Closes `workCh` so workers exit
- Waits for in-flight work via the worker `WaitGroup`
- `Enqueue` is non-blocking and returns the number of accepted keys.
- Work that cannot fit in the queue is counted in `Dropped`.

This is bounded: per-block prefetch fan-out should stay at most
`lookahead × deterministic-keys-per-tx`, all returning O(1) latest-domain
reads each.

## Configuration

`gtron.toml`:

```toml
[state.prefetch]
enabled    = true     # currently opt-in; default is false until soak evidence
workers    = 0        # 0 = GOMAXPROCS/2, capped at 8
lookahead  = 8
```

CLI: `--state.prefetch.enabled`, `--state.prefetch.workers=4`, and
`--state.prefetch.lookahead=8`. Leaving `enabled=false` recovers the prefetch-off
behaviour exactly.

## Per-actuator audit format

Slice 2 audit lives in `docs/dev/state-prefetch-keys.md` with one section
per actuator listing:

- The Validate + Execute hot path's `state.X` reads
- Which reads are deterministic from the tx envelope (eligible for prefetch)
- Which reads depend on intermediate state (NOT prefetchable)

## Acceptance criteria

- `go test -race ./core ./core/state -run 'TestProcessBlockStatePrefetch|TestStatePrefetcher' -count=1` clean
- Focused benchmarks in `core/state_processor_prefetch_bench_test.go`:
  - `BenchmarkProcessBlock_HeavyTRX_HeavyState`
  - `BenchmarkProcessBlock_HeavyTRX_ColdState`
  - variants: `prefetch=off`, `prefetch=on_workers=2_lookahead=8`,
    `prefetch=on_workers=4_lookahead=8`, `prefetch=on_workers=8_lookahead=8`
- Repeatable benchmark collection through
  `scripts/dev/state_prefetch_benchmark.sh`, preserving raw output plus commit
  and host metadata for later sweep comparisons
- Default-on threshold:
  - on cold-state/heavy-block replay: ≥ 10% throughput improvement
  - on hot/light blocks: ≤ 1% overhead vs baseline
- Long-running Nile import (24h): no regressions in correctness, +5-15%
  in steady-state block import rate
- Disable flag flips back to today's behaviour exactly

## Risks

- Prefetcher reads on Pebble compete with hot path. If we over-parallelize
  we starve the main goroutine's reads. Cap workers at 8 by default; the
  benchmark will tune this.
- Memory bloat: the landed driver does not populate `StateDB` object caches.
  Queue size is bounded and values are discarded after the raw read returns.
- Wrong prefetch keys (over-prefetch) waste work but never affect
  correctness. Under-prefetch just leaves perf on the table.

## Future follow-ups

- **VM-aware prefetch** — once inside TVM execution, dynamically prefetch
  CALL targets one frame ahead. Requires interpreter hook (see
  [tracing-hooks](./2026-05-19-vm-tracing-hooks-design.md) work first).
- **Cross-block prefetch** — when a block lands, kick off prefetch of the
  next block's tx targets before it's fully validated. Marginal win;
  defer.
