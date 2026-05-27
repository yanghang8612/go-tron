# Fork-rewind correctness — buffered post-block writes (slice 1)

**Status:** In progress (slice 1 lands buffer mechanism + witness statistics)
**Author:** core team
**Date:** 2026-04-30
**Source-of-truth follow-up:** [docs/dev/fork-rewind-rawdb-writes.md](../../dev/fork-rewind-rawdb-writes.md)
**State assumption:** matches behavior implied by `docs/dev/fork-rewind-rawdb-writes.md` analysis;
pending M0″ Phase 2 cross-impl reorg verification.

## Background

`core/blockchain.go::applyBlock` writes several post-block facts directly to the
persistent Pebble store, bypassing `state.StateDB`. The full enumeration is in
[docs/dev/fork-rewind-rawdb-writes.md](../../dev/fork-rewind-rawdb-writes.md);
the categories are:

1. `dynProps.Flush(bc.db)` (DynamicProperties — including `BLOCK_FILLED_SLOTS`,
   `total_create_witness_cost`, `burn_trx_amount`).
2. Cycle brokerage / vote / `WitnessVI` writes from `applyRewardMaintenance`.
3. Per-witness statistics writes from `dpos.ApplyBlockStatistics`
   (`TotalProduced`, `TotalMissed`, `LatestBlockNum`, `LatestSlotNum`).
4. `total_transaction_count` increment.
5. `burn_trx` (via DP Flush — see (1)).
6. Solidified-block update (`updateSolidifiedBlock` writes
   `WriteWitnessLatestBlock` plus a DP set; the DP set lands via Flush).

`switchFork` rewinds canonical state by re-running `applyBlock` across the new
branch on top of the LCA. It does **not** roll back the orphaned branch's prior
direct rawdb writes, which were committed inside earlier `InsertBlock` calls.
After a switchFork the orphan-side mutations remain on disk and the
canonical-side mutations are **layered on top**, so witness counters / fee
totals / ring contents are double-counted.

## Approach

**Buffer + commit (approach 1 in the doc).**

Rationale (verbatim from the slice-1 task brief):

> Cleaner mechanism. Matches the existing `state.StateDB` model (in-memory journal until Commit). switchFork integration is trivially "drop the buffer for orphaned applyBlock calls" rather than synthesizing inverse mutations for tricky writers (e.g. BLOCK_FILLED_SLOTS ring rotation requires remembering pre-write index + bit, which is bug-prone).

### Why per-applyBlock-flush does not fix the bug

A naive reading is "create a buffer in `applyBlock`, commit on success, discard
on error". That only protects against **mid-applyBlock failures**. Orphan-branch
`applyBlock` calls returned successfully in earlier `InsertBlock` invocations,
so by the time `switchFork` runs the orphan writes are already on disk —
nothing to discard.

The bug fix therefore requires **buffering across `applyBlock` boundaries** and
deferring the disk flush. The buffer must remember writes per-block so
`switchFork` can drop just the orphan-block layers and keep canonical layers.

## Buffer abstraction

New package: `core/blockbuffer`. Single type `Buffer`.

### Shape

```go
type Buffer struct {
    base   ethdb.KeyValueReader // disk
    layers []*layer             // newest at end; one per applyBlock
    active *layer               // current open layer; nil between blocks
}

type layer struct {
    blockHash common.Hash
    writes    map[string][]byte
    deletes   map[string]struct{} // tombstones
}
```

Internally the layers slice is append-only during normal operation; orphan
discard removes specific layers.

### API

| Method | Behavior |
|--------|----------|
| `New(base ethdb.KeyValueReader) *Buffer` | constructor |
| `BeginBlock(hash common.Hash)` | open a fresh top layer; panics if a layer is already active |
| `CommitBlock()` | promote `active` to the layered slice; panics if no active layer |
| `DiscardActive()` | drop `active` without promoting (used on applyBlock failure) |
| `DiscardBlock(hash common.Hash)` | remove the layer with this block hash from `layers`; no-op if absent |
| `Discard()` | drop all layers and any active layer (used on whole-buffer reset) |
| `Get(k []byte) ([]byte, error)` | search top→bottom: active layer, layered slice newest-first, then `base` |
| `Has(k []byte) (bool, error)` | as `Get` but boolean; tombstones return false |
| `Put(k, v []byte) error` | write into active layer (panics if no active layer) |
| `Delete(k []byte) error` | tombstone in active layer (panics if no active layer) |
| `Flush(w ethdb.KeyValueWriter) error` | drain all layered writes to `w` (oldest layer first), then clear layers. Used by future slice-2 stable-flush policy; not invoked in slice 1. |

`Buffer` satisfies both `ethdb.KeyValueReader` and `ethdb.KeyValueWriter`.

### Read-through ordering

When reading a key, the buffer searches:

1. The active layer (if any)
2. Each layered entry from newest to oldest
3. The `base` reader (disk)

A tombstone in any layer short-circuits the search and returns
`ethdb-style not-found` (e.g. `ErrKeyNotFound` from leveldb).

### Concurrency

Single-writer (`bc.chainmu` already serializes `applyBlock`/`switchFork`). No
internal locking; callers must hold `bc.chainmu`.

## Slice 1 scope

Only the **witness-statistics writer** is migrated to the buffer.
Specifically `dpos.ApplyBlockStatistics` accepts a narrower interface
`{ ethdb.KeyValueReader; ethdb.KeyValueWriter }` instead of
`ethdb.KeyValueStore`, so callers can pass either the disk store or the
buffer. `BlockChain.applyBlock` passes `bc.buffer` for the layer it just opened.

Affected fields: `TotalProduced`, `TotalMissed`, `LatestBlockNum`,
`LatestSlotNum` — i.e. the data persisted via `rawdb.WriteWitness` inside
`ApplyBlockStatistics`.

### Out of slice 1

The following five writers explicitly remain on the disk-direct path until
slice 2:

> **Superseded (2026-05-27) — rooted-state refactor.** The DynamicProperties
> items below (`BLOCK_FILLED_SLOTS`, `total_create_witness_cost`,
> `burn_trx_amount`, and the `LatestSolidifiedBlockNum` DP set) were *not*
> retrofitted onto `bc.buffer`. Instead, the rooted-state refactor stages every
> consensus DP key into the rooted `SystemDynamicProperty` KV via
> `DynamicProperties.FlushRooted` *before* state `Commit`, so they enter the
> internal full-state root and rewind with it on reorgs — a cleaner mechanism
> than the per-writer buffer pattern. `DynamicProperties.Flush` now only mirrors
> the four derived runtime keys to the flat `dp-` store (diagnostic; rebuilt
> from the rooted KV on load). The non-DP writers (reward-maintenance state,
> total-tx-count) flow through the rooted state commit / `bc.buffer`.

- [ ] `state.DynamicProperties.Flush` — including `BLOCK_FILLED_SLOTS` (set via
      `dp.ApplyBlockToFilledSlots` in `ApplyBlockStatistics`),
      `total_create_witness_cost`, `burn_trx_amount`. Slice 1 does **not** fix
      `BLOCK_FILLED_SLOTS` ring double-counting — it piggybacks on the DP flush.
- [ ] `core/reward.go::applyRewardMaintenance` — cycle brokerage / vote /
      WitnessVI writes.
- [ ] `core/blockchain.go::InsertBlock` total-tx-count increment
      (`rawdb.WriteTotalTransactionCount`).
- [ ] `actuator/fees.go::burnFee` (lands via DP Flush).
- [ ] `core/blockchain.go::updateSolidifiedBlock` — both
      `rawdb.WriteWitnessLatestBlock` and the `LatestSolidifiedBlockNum` DP set
      (DP set lands via Flush).

Each is a separate retrofit in slice 2 with the same pattern: pass `bc.buffer`
in place of `bc.db` to the writer, and ensure the writer uses only
`KeyValueReader+Writer` operations on that argument.

### State on restart

Slice 1 does not flush the buffer to disk at any point. On process restart the
buffered witness counters are lost. **This is acceptable for slice 1** because:

- No mainnet/testnet deployment is gated on this slice.
- All existing tests that cross a process boundary use the full DB and don't
  check witness counters.
- M0″ Phase 2 acceptance / production rollout depends on slice 2 completing the
  remaining writers and adding a stable-flush policy (e.g. flush at the
  solidified-block horizon, or N-block lag below current head).

A `Flush` method is exposed on the buffer so slice 2 can wire a flush trigger
without further API changes.

### StateDB witness-cache interaction

`applyBlock` seeds the in-memory `statedb.witnesses` cache from
`rawdb.ReadWitness(bc.db, addr)` for each witness in the index. That read uses
the **disk store** (not the buffer) and consumes the `URL` / `VoteCount`
fields. The buffer in slice 1 only writes counters
(`TotalProduced`/`Missed`/`LatestBlockNum`/`LatestSlotNum`), which the
in-memory cache does not consume. The two caches therefore do not race or
diverge.

## switchFork integration contract

```
switchFork(newHead):
  newBranch, oldBranch := khaosDB.GetBranch(newHead, currentHead)
  for each kb in oldBranch:
      bc.buffer.DiscardBlock(kb.block.Hash())   // <-- new
  rewind currentBlock to LCA
  for each kb in newBranch (reverse to LCA→tip order):
      applyBlock(kb.block)   // BeginBlock / CommitBlock per call
```

`oldBranch` from `khaosDB.GetBranch` contains exactly the orphan-only blocks
above the LCA — the LCA itself and any deeper shared prefix are not included,
so `DiscardBlock` is safe to call for every entry. Blocks not in pending
buffers are no-ops.

`applyBlock` integration:

```
applyBlock(block):
  bc.buffer.BeginBlock(block.Hash())
  defer { if err: bc.buffer.DiscardActive() }
  ... open statedb / process / ApplyBlockStatistics(buffer, ...) ...
  bc.buffer.CommitBlock()  // promote layer to pending
  return nil
```

## Test strategy

### Buffer unit tests (`core/blockbuffer/buffer_test.go`)

- Writes accumulate; `Get` after `Put` returns the buffered value.
- Reads fall through to base when key is not buffered.
- `Delete` tombstones — a deleted key reports not-found even if base has it.
- Multiple layers stack: newer layer's `Put` overrides older.
- `DiscardActive` drops in-progress writes; subsequent reads see prior-layer
  state.
- `DiscardBlock(hash)` removes only that layer.
- `CommitBlock` without `BeginBlock` panics.
- Double `BeginBlock` panics.
- `Flush` writes oldest-first; after flush, layers are empty.

### Reorg correctness test (`core/blockchain_insert_test.go` — `TestForkSwitch_WitnessCountersNoDoubleCount`)

- 3-block chain A produced by witness W (counters via buffer). Verify pending
  read sees `TotalProduced = 3`.
- 4-block chain B produced by W on a different branch (timestamps/parent
  diverge). The 4th block triggers `switchFork`, which:
  - Discards layers for A1..A3.
  - Re-applies B1..B4 → fresh layers.
- After the switch, `bc.buffer` reads must report `TotalProduced = 4` (not 7,
  which is what direct-disk writes would produce due to double-counting).
- Comment in the test must explicitly note: the assertion reads via
  `bc.buffer`, not via `rawdb.ReadWitness(diskdb, ...)`, because slice 1
  doesn't flush to disk — disk reads would return zero / stale.

### Linear-extension regression test

- Apply a 3-block linear chain; the buffer should not affect canonical
  counters. Reads via the buffer match what a direct-disk implementation
  would produce.

### Existing tests must remain green

`TestBlockChain_ForkSwitch_10Block` checks state-root parity (StateDB-backed,
not buffer-backed). State-root behavior is unchanged in slice 1 because the
witness-counter writer never participated in the state root.

## Constraints (recap)

- `consensus/dpos/` — only `statistic.go` is touched (signature narrowing).
- `actuator/`, `core/state/dynamic_properties.go` — untouched in slice 1.
- `BlockChain.AddBlockHook` / `BlockChain.ProcessBlock` / public surface — no
  renames, additive only.
- The buffer is composable with `rawdb.KeyValueStore`; existing code paths that
  read from disk continue to work unchanged.
- No on-disk schema change.
