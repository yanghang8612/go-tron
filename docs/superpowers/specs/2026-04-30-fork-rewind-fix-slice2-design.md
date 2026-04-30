# Fork-rewind correctness — buffered post-block writes (slice 2)

**Status:** In progress
**Author:** core team
**Date:** 2026-04-30
**Slice 1 spec:** [2026-04-30-fork-rewind-fix-design.md](2026-04-30-fork-rewind-fix-design.md)
**Source-of-truth:** [docs/dev/fork-rewind-rawdb-writes.md](../../dev/fork-rewind-rawdb-writes.md)

## Background

Slice 1 introduced `core/blockbuffer.Buffer` and retrofitted exactly one
post-`applyBlock` writer (`consensus/dpos.ApplyBlockStatistics`). The
remaining rawdb-direct writes from `applyBlock` continued to land on disk
inside `applyBlock`, so they still leak across `switchFork`. Slice 1 also
deliberately deferred any flush-to-disk policy: buffered layers stay in
memory for the lifetime of the process. That makes process restart lose
state, and bounds the buffer growth only by hand.

Slice 2 closes both gaps:

1. Retrofit the remaining 5 enumerated writers onto the buffer so that
   `switchFork`'s `Buffer.DiscardBlock(hash)` rolls back **every**
   post-applyBlock rawdb-direct mutation.
2. Add a stable-flush policy keyed on the solidified-block boundary.
   Layers below solidified flush oldest-first to disk; layers at or above
   solidified stay in memory (so reorgs above solidified are rewindable).
   This matches java-tron's `Manager.eraseBlock` invariant (it never
   pops past solidified — `revokingStore.maxFlushCount` controls how many
   blocks may be queued before being flushed, capped against the solidified
   horizon).

## Scope — the 5 retrofitted writers

| # | Writer | Location | Buffer routing |
|---|--------|----------|----------------|
| 1 | `DynamicProperties.Flush` (dirty DP keys, including `block_filled_slots`, `total_create_witness_cost`, `burn_trx_amount`, `latest_solidified_block_num`) | `core/state/dynamic_properties.go::Flush` | `dp.Flush` already accepts `ethdb.KeyValueWriter`; pass `bc.buffer` instead of `bc.db`. |
| 2 | `applyRewardMaintenance` cycle brokerage / vote / VI writes | `core/reward.go::applyRewardMaintenance` | Widen `db` parameter from `ethdb.KeyValueStore` to a local `KVReadWriter` interface (`ethdb.KeyValueReader + ethdb.KeyValueWriter`). Reads (`ReadCycleReward`, `ReadWitnessVI`, `ReadWitnessBrokerage`) and writes (`WriteCycleBrokerage`, `WriteCycleVote`, `WriteWitnessVI`) all already use narrow signatures. Pass `bc.buffer` from `applyBlock`. |
| 3 | `WriteTotalTransactionCount` | `core/blockchain.go::applyBlock` | Routed through `bc.buffer`. The matching `ReadTotalTransactionCount(bc.db)` becomes `ReadTotalTransactionCount(bc.buffer)` so accumulator reads see in-flight increments. |
| 4 | `burnFee` cumulative `burn_trx_amount` (lands via DP) | `actuator/fees.go::burnFee` | Covered transitively by retrofit (1): `burnFee` mutates `ctx.DynProps` in-memory only, and the DP `Flush` lands on `bc.buffer`. No actuator-side change needed. |
| 5 | `updateSolidifiedBlock` — `WriteWitnessLatestBlock` and `latest_solidified_block_num` (DP) | `core/blockchain.go::updateSolidifiedBlock` | `WriteWitnessLatestBlock` and the per-witness `ReadWitnessLatestBlock` both go through `bc.buffer`. The DP set lands via (1). |

The brief's earlier name "Solidified-block update — `WriteLatestSolidifiedBlock`"
maps to the DP key `latest_solidified_block_num` (no separate `WriteLatestSolidifiedBlock`
accessor exists in go-tron's rawdb — slice 1 source-of-truth doc was approximate).

### Java-tron parity citations

For each writer, the java-tron reference path that would re-execute the same
mutation under the same `eraseBlock`/`switchFork` rules is:

- **DP.Flush** → `org.tron.core.db.DynamicPropertiesStore` writes via
  `revokingStore`-wrapped `ChainBaseManager` putters. `Manager.eraseBlock`
  calls `revokingStore.fastPop()` which pops the last session;
  the layered DP writes go away.
- **applyRewardMaintenance** → `MaintenanceManager.doMaintenance` and
  `MortgageService.payBlockReward` write through `DelegationStore`, also
  `revokingStore`-wrapped. (Note: per-block `AddCycleReward` from
  `payBlockReward` is **not** in slice 2 — see "Out of slice 2" below.)
- **WriteTotalTransactionCount** → `Manager.processBlock` calls
  `chainBaseManager.getDynamicPropertiesStore().addTotalTransactionCost(...)`
  which is again `revokingStore`-wrapped.
- **burn_trx_amount** → `Manager` and the fee-charging actuators in java-tron
  use `dynamicPropertiesStore.burnTrx(amount)`, also session-tracked.
- **updateSolidifiedBlock / WriteWitnessLatestBlock** →
  `Manager.updateSolidifiedBlock` writes the per-witness latest block via
  `WitnessStore.put` + DP `latestSolidifiedBlockNum`, both session-tracked.

Java-tron's invariant is that `revokingStore.fastPop()` rewinds **every**
session-tracked write at once; go-tron's slice-2 buffer achieves the same by
collecting them into a single per-block layer.

## Out of slice 2 (known follow-up)

The following per-block rawdb-direct writes are **not** retrofitted in slice 2
and remain on the disk-direct path. They will leak across `switchFork` if and
only if the gating condition fires during the orphan branch:

- `payBlockReward → AddCycleReward(db, ...)` from `core/reward.go`. Gated on
  `ChangeDelegation()` (proposal #82, off on mainnet at the time of writing).
  Retrofitting would widen `ProcessBlock` and `ApplyTransaction`'s `db
  ethdb.KeyValueStore` parameter, which is also the actuator dispatch
  surface (`actuator.Context.DB`). That cross-cuts M11.5 slice 2a's
  territory and is deferred to a later slice.
- Actuator-side rawdb-direct writes: `WriteAssetIssue`, `WriteExchange`,
  `WriteProposal`, `WriteContractCode`, etc. These are mutated inside
  `act.Execute(ctx)` and reverted today only by `statedb.RevertToSnapshot`
  on per-tx error — not on per-block reorg. Same M11.5 ownership.
- **Graceful shutdown flush.** A `kill -9` or unhandled panic loses the
  buffered layers above solidified (~19 blocks on mainnet). `BlockChain`
  does not yet expose a `Shutdown` lifecycle hook that calls
  `bc.buffer.Flush(bc.db)` for the safe (i.e. linear-extension, no
  in-flight reorg) shutdown case. On next start, `NewBlockChain` reloads
  from `rawdb.ReadHeadBlockHash`, so the on-disk image is internally
  consistent — but post-applyBlock counters (witness statistics, total
  tx count, etc.) for the lost range are never recovered without a
  re-sync. Slice 3 follow-up.

Both are documented as gaps in the source-of-truth doc, and slice 2's
flush-at-solidified policy makes them easier to retrofit in a future slice
because the buffer mechanism is already proven.

## Buffer API extension

Slice 2 adds one new method to `core/blockbuffer.Buffer`:

```go
// FlushUpTo flushes every committed layer whose block number is <= cutoff
// to w, oldest-first, then drops those layers. Layers above the cutoff
// stay in the layered slice and remain rewindable. The active layer is
// untouched.
//
// numberOf maps a block hash to its block number; if a hash is unknown
// (returns 0, false), that layer is conservatively kept (not flushed).
// In practice the caller (BlockChain) supplies a closure backed by
// rawdb.ReadBlockNumber on the disk store, so once a layer's block has
// been written to disk (which applyBlock does inside the same tick) the
// number lookup succeeds.
func (b *Buffer) FlushUpTo(
    cutoff uint64,
    numberOf func(common.Hash) (uint64, bool),
    w ethdb.KeyValueWriter,
) error
```

Why a closure rather than tracking the number inside the layer? Two reasons:

1. Backwards compatibility — slice-1 callers of `BeginBlock(hash)` keep that
   signature. No churn in `consensus/dpos/statistic.go` or anywhere else.
2. The block number is already determinable from the hash via
   `rawdb.ReadBlockNumber`. Threading it through `BeginBlock` would be
   redundant state.

Layer ordering inside the buffer remains "newest at end of slice", and
`FlushUpTo` walks oldest-first, stopping when it hits a layer whose number
is `> cutoff` *or* whose number is unknown.

The existing `Flush(w)` is left in place (drains everything) for any caller
that wants the slice-1 nuclear semantics; slice 2 itself does not call it.

## Flush policy — flush at solidified-block boundary

### Trigger

The flush happens at the end of `applyBlock`, immediately after
`updateSolidifiedBlock` has set the new `latest_solidified_block_num` and
`dp.Flush(bc.buffer)` has buffered the value:

```
applyBlock(block):
  ... process ...
  bc.updateSolidifiedBlock(...)       // updates DP in-memory
  dp.Flush(bc.buffer)                 // DP changes go into the active layer
  ... write block + tx infos ...
  bc.buffer.CommitBlock()             // promote active layer
  bc.flushBufferUpToSolidified(dp)    // <-- NEW
  return nil
```

`flushBufferUpToSolidified` reads the just-committed `dp.LatestSolidifiedBlockNum()`
and calls `bc.buffer.FlushUpTo(solidified, lookup, bc.db)` where `lookup` is
`func(h) (uint64, bool) { p := rawdb.ReadBlockNumber(bc.db, h); return *p, p != nil }`.

### Why solidified, not "head − N"

- Java-tron's invariant: `Manager.eraseBlock` fails (refuses to pop) past the
  solidified block. The solidified line is the natural "this can never reorg"
  horizon for both implementations.
- `latest_solidified_block_num` is already maintained per-block by
  `updateSolidifiedBlock` using java-tron's `floor(N * 0.3)` rule. No new
  knob to tune.
- A static "head − N" horizon would either be larger than necessary (wastes
  memory and risks a deep reorg appearing post-flush) or smaller than the
  actual safe horizon for slow-finality chains.

### Idempotency / safety

- `FlushUpTo` removes flushed layers. A second call with the same cutoff
  is a no-op (zero matching layers).
- After flush, on-disk state for blocks `<= solidified` matches exactly what
  slice 1's direct writes would have produced for the same canonical
  sequence. (Property tested by the restart test below.)
- Reorgs above solidified are still rewindable: `switchFork`'s
  `DiscardBlock(hash)` only operates on layers in memory; the LCA is by
  construction `> solidified` for any orphan, so the orphan's layer is
  guaranteed to still be in memory.
- Process crash between `bc.buffer.CommitBlock()` and the `FlushUpTo` call
  loses at most the layers above the previous solidified line. On next
  start, `BlockChain` reloads from `rawdb.ReadHeadBlockHash(bc.db)`. Pebble
  never sees the unflushed mutations, so the on-disk image is internally
  consistent (just stale by up to `head - solidified` blocks of post-block
  counters). Recovery: re-fetch the missing range from peers and re-apply.

### Reorg interaction matrix

| Pre-flush state | Reorg crosses solidified? | Outcome |
|-----------------|---------------------------|---------|
| Layer L at height N is in memory | N > solidified, reorg LCA at M < N, M ≥ solidified | `DiscardBlock(L.hash)` works; canonical re-apply produces a fresh layer at N. |
| Layer L at height N already flushed (N ≤ solidified) | LCA M ≥ N | Impossible — by construction, java-tron's `eraseBlock` cannot reorg below solidified, and KhaosDB's mini-store horizon is `solidified - 8` (`MaxFutureBlockSize` constant). The reorg is rejected at the KhaosDB level before reaching `switchFork`. |
| Layer L at height N already flushed | LCA M < N (deep reorg crossing solidified) | This would be a consensus-level violation in TRON. Not a recovery target. |

## Required code-level reads via the buffer

The retrofit also requires updating reads to consult the buffer wherever
the read participates in the same per-block accumulator that the buffer
writes. Otherwise, a buffered increment is overwritten by a buffered re-read
of the disk's stale value.

| Read site | Old | New |
|-----------|-----|-----|
| `ReadTotalTransactionCount` in `applyBlock` (just before its `WriteTotalTransactionCount`) | `bc.db` | `bc.buffer` |
| `ReadWitnessLatestBlock` in `updateSolidifiedBlock` (the N-way read for the solidified compute) | `bc.db` | `bc.buffer` |
| `LoadDynamicProperties` at the top of `applyBlock` | `bc.db` | **leave on `bc.db`** — DP is reset and rebuilt from scratch each block from the `dirty` set; reading the still-buffered values would be a no-op and risks reading uncommitted state from a discarded layer. |

The DP `LoadDynamicProperties` distinction is important: `dp.Flush` writes only
`dirty` keys; it does not snapshot the entire DP map. So an `applyBlock` that
opens a fresh `dp` from the buffer would see (a) on-disk values for keys not
recently dirtied + (b) the most recent active-layer / committed-layer write for
keys that were dirty in any pending block. That second category includes
post-applyBlock writes from blocks that have not yet been flushed — which is
exactly what we want (so e.g. `current_cycle_number` written by an unflushed
maintenance boundary is visible to the next block). The right resolution for
slice 2: load DP from `bc.buffer`, not `bc.db`, so all cross-block DP reads
go through the buffer.

| Read site | Old | New |
|-----------|-----|-----|
| `state.LoadDynamicProperties` in `applyBlock` | `bc.db` | `bc.buffer` |
| `state.LoadDynamicProperties` in `BlockChain.DynProps()` (RPC accessor) | `bc.db` | `bc.buffer` |
| `state.LoadDynamicProperties` in `BlockChain.NextMaintenanceTime()` | `bc.db` | `bc.buffer` |

External callers (RPC, txpool) read DP via `bc.DynProps()`, so routing that
through `bc.buffer` automatically gives them the fresh post-applyBlock values
without leaking partial state (the buffer is already gated by `bc.chainmu`
since slice 1).

## Read-side migration outside `applyBlock`

The buffer is single-writer. Reads from outside the chainmu (RPC handlers,
metrics, txpool) **must** still go through it: it's an `ethdb.KeyValueReader`
and the underlying maps are only mutated under chainmu, so a concurrent read
sees a consistent snapshot of either the new or old layer state. No locking
needed.

`bc.BufferedDB()` is already exposed; slice 2 promotes it from a test helper
to the canonical read path for any callsite that previously used `bc.DB()`
to read DP / witness statistics / cycle reward keys / total tx count /
witness latest block.

Slice 2 does NOT migrate every existing read site — that's a separate cleanup
pass. The minimum required is the three reads listed above (inside applyBlock /
DynProps accessors) so that the buffer remains internally consistent.

## DP Flush ordering

`dp.Flush(bc.buffer)` writes 8-byte int64 / string-typed keys into the active
layer. Two ordering subtleties:

1. **Within `applyBlock`**: `ApplyBlockStatistics` mutates `dp.ApplyBlockToFilledSlots(...)`,
   which calls `dp.SetString("block_filled_slots", ...)` (marks the key
   dirty in-memory). `dp.Flush(bc.buffer)` later in `applyBlock` writes those
   accumulated string keys. Slice 1 already lands the per-witness counters
   on the buffer but the BLOCK_FILLED_SLOTS ring lands on disk via
   `dp.Flush(bc.db)`. Slice 2 closes that — both go through the buffer.
2. **Between `applyBlock` and `commitBlock`**: the active layer holds DP
   writes alongside witness statistics. `CommitBlock` promotes the whole
   layer atomically; there is no window where DP has been flushed and
   the layer hasn't been committed.

## On-disk consistency model

After `FlushUpTo(N, lookup, bc.db)`, on-disk state for every key written
during `applyBlock(block_k)` for `k ≤ N` is byte-identical to what slice-1's
direct-disk path would have produced for the same canonical sequence
(modulo write ordering of independent keys, which Pebble's atomic batch is
not used for in either path — both write key-by-key).

Concretely:
- `rawdb.ReadWitness(bc.db, addr).TotalProduced()` after a long enough
  canonical extension equals exactly the number of canonical blocks
  produced by `addr`.
- `rawdb.ReadTotalTransactionCount(bc.db)` equals the sum of `len(tx)`
  across canonical blocks `1..N`.
- `state.LoadDynamicProperties(bc.db).BurnTrxAmount()` equals exactly
  the sum of fees burned on canonical blocks `1..N` (subject to fork
  gating).
- `rawdb.ReadCycleBrokerage / ReadCycleVote / ReadWitnessVI(bc.db, ...)`
  reflect only the maintenance boundaries on the canonical chain.

The restart test below is the property test for this claim: build a chain,
let solidified advance, drop the in-memory `BlockChain`, rebuild from
`bc.db` only, verify counters match a side-channel oracle.

## Test plan

### Buffer unit test additions (`core/blockbuffer/buffer_test.go`)

- [ ] `FlushUpTo(cutoff)` flushes only layers ≤ cutoff and keeps higher
      layers rewindable.
- [ ] `FlushUpTo(cutoff)` with an unknown hash in `numberOf` keeps the
      layer (does not flush).
- [ ] `FlushUpTo(cutoff)` is idempotent — second call drops zero layers.
- [ ] `FlushUpTo` then `DiscardBlock(higherHash)` still works (the higher
      layer was kept in memory).

### Reorg correctness extension (`core/blockchain_insert_test.go`)

- [ ] Extend `TestForkSwitch_WitnessCountersNoDoubleCount` to also assert,
      after the switch, via `bc.BufferedDB()`:
  - `rawdb.ReadTotalTransactionCount` reflects only canonical blocks.
  - DP `burn_trx_amount` reflects only canonical blocks (set up burn-eligible
    fees on both branches with `AllowBlackholeOptimization` flipped).
  - DP `latest_solidified_block_num` reflects only the canonical chain's
    advance.
  - DP `block_filled_slots` ring index advanced by exactly `len(canonical chain)`.
- [ ] New `TestForkSwitch_RewardMaintenance_NoDoubleCount`: reorg across a
      maintenance boundary with `change_delegation = true`. After the switch,
      `ReadCycleBrokerage(bc.BufferedDB(), nextCycle, addr)` reflects only
      the canonical maintenance application — not the orphan's.

### Restart-safety test (new)

- [ ] `TestFlushAtSolidified_SurvivesRestart`: build N=10 canonical blocks
      on a single-SR chain (so `floor(1*0.3)=0` → solidified == head). After
      every block, `flushBufferUpToSolidified` should drain that block's
      layer to disk. Drop the `BlockChain`, rebuild on the same `diskdb`,
      then read counters via `rawdb.ReadX(diskdb, ...)` (NOT buffered) and
      assert they match. This is the on-disk consistency property.

### Existing tests must remain green

- `TestBlockChain_ForkSwitch_10Block` (state-root parity) — unchanged.
- `TestLinearExtension_WitnessCountersThroughBuffer` — unchanged in shape;
  may now also be readable via disk after solidified flush.
- `TestForkSwitch_WitnessCountersNoDoubleCount` (slice 1 portion) —
  unchanged assertions.

## Constraints recap

- DO NOT touch `consensus/dpos/statistic.go`'s slice-1 retrofit except for
  whatever the new `FlushUpTo` API requires (it doesn't — slice-1 callers
  still use `BeginBlock(hash)`).
- DO NOT touch `actuator/account.go`, `actuator/witness.go`,
  `core/types/account.go`, `core/state/statedb.go` (M11.5 slice 2a territory).
- DO NOT touch `net/pbft_producer.go` (M6b territory).
- Buffer API additions are purely additive; existing slice-1 methods retain
  their signatures and semantics.
- No on-disk schema change; existing chaindata works.
