# State prefetcher — design

**Status:** Proposed
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
Get. But the same prefetch idea applies: while tx N runs on the main
thread, kick off async `state.GetAccount(senderOfTxN+1)` and friends in
parallel goroutines so when tx N+1 starts, the in-memory `state.StateDB`
cache is warm.

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
- Do NOT introduce data races. Prefetcher results must be visible to the
  main goroutine via `state.StateDB`'s existing locked access patterns —
  not a side cache.

## Mental model

```
main goroutine:        run tx N → run tx N+1 → run tx N+2
prefetcher goroutine:  warm (tx N+1) → warm (tx N+2) → warm (tx N+3)
                       ^ runs in parallel; populates state.StateDB cache
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

Each `actuator.Actuator` interface gains a new optional method:

```go
type Prefetcher interface {
    PrefetchKeys(tx *types.Transaction) []state.PrefetchKey
}
```

Actuators that don't implement it default to "no prefetch" (no harm; just
no speedup for that contract type).

### Prefetcher driver

`core/state/prefetcher.go`:

```go
type StatePrefetcher struct {
    statedb  *StateDB
    workCh   chan PrefetchKey
    workers  int             // default GOMAXPROCS/2, capped at 8
    done     chan struct{}
}

func (p *StatePrefetcher) Start()
func (p *StatePrefetcher) Stop()           // drains workers, idempotent
func (p *StatePrefetcher) Enqueue(keys []PrefetchKey)
```

`state_processor.go::ProcessBlock`:

```go
prefetcher := state.NewPrefetcher(statedb, runtime.GOMAXPROCS(0)/2)
defer prefetcher.Stop()
prefetcher.Start()

const lookahead = 8        // tunable
for i, tx := range txs {
    // Enqueue prefetch work for txs [i+1, i+lookahead]
    for j := i + 1; j <= i+lookahead && j < len(txs); j++ {
        if pfk, ok := actuatorPrefetchKeys(txs[j]); ok {
            prefetcher.Enqueue(pfk)
        }
    }
    // Execute current tx (synchronous, as today)
    runTx(i, tx)
}
```

The lookahead window is bounded — we don't want to enqueue 5000 keys for a
10K-tx block (memory pressure on the work channel and risk of cache
churn). 8 ahead is enough to keep workers fed without bloat.

### Worker behaviour

Each worker is a simple read loop:

```go
for key := range workCh {
    switch key.Kind {
    case AccountKey:
        statedb.GetAccount(key.Addr)            // populates cache
    case StorageKey:
        statedb.GetState(key.Addr, key.Slot)
    case CodeKey:
        statedb.GetCode(key.Addr)
    case TRC10Key:
        statedb.GetTRC10Balance(key.Addr, key.AssetID)
    case WitnessKey:
        statedb.GetWitness(key.Addr)
    }
    // errors silently ignored — this is just a warmup
}
```

`statedb.GetAccount` etc. are already thread-safe (the StateDB has an
RWMutex over its object cache). The prefetcher's reads only populate the
cache; no writes; concurrent main-thread reads see the populated entry
instead of going to disk.

## Race / correctness

The only shared mutable state is `StateDB`'s object cache. Today's reads
already lock the cache, so prefetcher reads are race-free by construction.
Verify with `go test -race -count=3 ./core/...` after landing.

Edge case: prefetcher fetches account X; main thread mutates X mid-flight
(during current tx). Mutation goes through the same locked path, so:

1. Prefetcher Get → cache populated with disk value V
2. Main Execute → cache write (V → V')
3. Next tx's GetAccount → cache returns V' (correct)

No staleness possible: cache writes invalidate any prefetched read.

## Stop semantics

When `ProcessBlock` returns (success or error), prefetcher.Stop():

- Closes `workCh` so workers exit
- Waits for in-flight work via `done` channel
- Any work that hadn't started gets dropped silently

This is bounded: per-block prefetch fan-out is at most `lookahead × workers`
keys, all returning O(1) Pebble Gets each.

## Configuration

`gtron.toml`:

```toml
[state.prefetch]
enabled    = true     # default
workers    = 0        # 0 = GOMAXPROCS/2, capped at 8
lookahead  = 8
```

CLI: `--state.prefetch.disable`, `--state.prefetch.workers=4`, etc. The
disable flag is the operator escape hatch if we discover a regression in
production.

## Per-actuator audit format

Slice 2 audit lives in `docs/dev/state-prefetch-keys.md` with one section
per actuator listing:

- The Validate + Execute hot path's `state.X` reads
- Which reads are deterministic from the tx envelope (eligible for prefetch)
- Which reads depend on intermediate state (NOT prefetchable)

## Acceptance criteria

- `go test -race ./core/state/... ./core/state_processor/...` clean
- A new benchmark `BenchmarkProcessBlock_HeavyTRX/prefetch=on`:
  - on `prefetch=off`: baseline
  - on `prefetch=on`: ≥ 10% throughput improvement
  - on `prefetch=on` lightblock: ≤ 1% overhead vs baseline
- Long-running Nile import (24h): no regressions in correctness, +5-15%
  in steady-state block import rate
- Disable flag flips back to today's behaviour exactly

## Risks

- Prefetcher reads on Pebble compete with hot path. If we over-parallelize
  we starve the main goroutine's reads. Cap workers at 8 by default; the
  benchmark will tune this.
- Memory bloat: prefetcher cache lives on `StateDB`, which is per-block.
  Worst case 50 KB extra per block (8 cached accounts × 6 KB account
  proto). Negligible.
- Wrong prefetch keys (over-prefetch) waste work but never affect
  correctness. Under-prefetch just leaves perf on the table.

## Future follow-ups

- **VM-aware prefetch** — once inside TVM execution, dynamically prefetch
  CALL targets one frame ahead. Requires interpreter hook (see
  [tracing-hooks](./2026-05-19-vm-tracing-hooks-design.md) work first).
- **Cross-block prefetch** — when a block lands, kick off prefetch of the
  next block's tx targets before it's fully validated. Marginal win;
  defer.
