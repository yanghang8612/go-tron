# Plan: fork-rewind correctness — slice 2

**Spec:** [2026-04-30-fork-rewind-fix-slice2-design.md](../specs/2026-04-30-fork-rewind-fix-slice2-design.md)
**Source-of-truth follow-up:** [docs/dev/fork-rewind-rawdb-writes.md](../../dev/fork-rewind-rawdb-writes.md)

## Slice 2 goals

1. Retrofit the 5 remaining post-applyBlock rawdb-direct writers onto
   `core/blockbuffer.Buffer`.
2. Add `Buffer.FlushUpTo(cutoff, numberOf, w)` for selective flushing.
3. Wire a flush-at-solidified-boundary policy into `applyBlock`.
4. Cover with unit tests + an extended reorg correctness test + a new
   restart-safety test.

## Tasks

### 1. Buffer API extension

In `core/blockbuffer/buffer.go`:

- [ ] Add `FlushUpTo(cutoff uint64, numberOf func(common.Hash) (uint64, bool), w ethdb.KeyValueWriter) error`.
      Walk `b.layers` oldest-first, stop when `numberOf(layer.blockHash)`
      returns `(_, false)` or returns `n > cutoff`. Drain matching layers
      to `w`, then drop them from the slice.
- [ ] Document it in package doc above `Flush`.

### 2. Buffer unit tests

In `core/blockbuffer/buffer_test.go`:

- [ ] `TestBuffer_FlushUpTo_FlushesOnlyMatchingLayers`.
- [ ] `TestBuffer_FlushUpTo_UnknownHashKeepsLayer`.
- [ ] `TestBuffer_FlushUpTo_Idempotent`.
- [ ] `TestBuffer_FlushUpTo_KeepsHigherLayersRewindable` — flush below N,
      then `DiscardBlock(higherHash)` still works.

### 3. Retrofit DP Flush

In `core/blockchain.go::applyBlock`:

- [ ] Replace `dynProps.Flush(bc.db)` with `dynProps.Flush(bc.buffer)`.
- [ ] Replace `state.LoadDynamicProperties(bc.db)` with `state.LoadDynamicProperties(bc.buffer)`.

In `core/blockchain.go::DynProps()` and `NextMaintenanceTime()`:

- [ ] Same swap to `bc.buffer`.

DP `Flush` already accepts `ethdb.KeyValueWriter` — no signature change.

### 4. Retrofit applyRewardMaintenance

In `core/reward.go`:

- [ ] Define a local `kvReadWriter` interface (mirroring slice-1's `KVReadWriter`):
      `ethdb.KeyValueReader + ethdb.KeyValueWriter`.
- [ ] Change `applyRewardMaintenance(db ethdb.KeyValueStore, ...)` to
      `applyRewardMaintenance(db kvReadWriter, ...)`. The body uses only
      `Read*` and `Write*` accessors which already take narrower interfaces.
- [ ] Change `accumulateWitnessVi(db ethdb.KeyValueStore, ...)` similarly.
- [ ] Leave `payBlockReward` and `payStandbyWitness` on `ethdb.KeyValueStore`
      (out of slice 2; documented).
- [ ] Verify `core/reward_maintenance_test.go` still compiles — it passes
      `rawdb.NewMemoryDatabase()` which satisfies the narrower interface.

In `core/blockchain.go::applyBlock`:

- [ ] Replace `applyRewardMaintenance(bc.db, ...)` with `applyRewardMaintenance(bc.buffer, ...)`.

`core/block_builder.go` keeps `applyRewardMaintenance(bc.db, ...)` since the
producer path is not in slice 2 (M6b territory).

### 5. Retrofit total transaction count

In `core/blockchain.go::applyBlock`:

- [ ] Replace `count := rawdb.ReadTotalTransactionCount(bc.db)` with
      `bc.buffer`. Replace `rawdb.WriteTotalTransactionCount(bc.db, ...)`
      with `bc.buffer`.

### 6. Retrofit updateSolidifiedBlock

In `core/blockchain.go::updateSolidifiedBlock`:

- [ ] Add a `db kvReadWriter` parameter (or use `bc.buffer` directly inside
      via the receiver — chosen approach: receiver-local `bc.buffer` access,
      since the function is already a method on `*BlockChain`).
- [ ] Replace `rawdb.WriteWitnessLatestBlock(bc.db, ...)` with `bc.buffer`.
- [ ] Replace `rawdb.ReadWitnessLatestBlock(bc.db, addr)` (the loop) with
      `bc.buffer`.

### 7. Flush-at-solidified policy

In `core/blockchain.go`:

- [ ] Add helper `flushBufferUpToSolidified(solidified int64)`:
      - if solidified ≤ 0: return.
      - build closure `numberOf := func(h common.Hash) (uint64, bool) { p := rawdb.ReadBlockNumber(bc.db, h); if p == nil { return 0, false }; return *p, true }`.
      - call `bc.buffer.FlushUpTo(uint64(solidified), numberOf, bc.db)`.
- [ ] Call it from `applyBlock` at the very end, after `bc.buffer.CommitBlock()`,
      using `dynProps.LatestSolidifiedBlockNum()` (which was just set by
      `updateSolidifiedBlock`).

### 8. Reorg correctness extension

In `core/blockchain_insert_test.go::TestForkSwitch_WitnessCountersNoDoubleCount`:

- [ ] After `switchFork`, additionally assert via `bc.BufferedDB()`:
  - `rawdb.ReadTotalTransactionCount` matches the sum of canonical txs
    (zero in this test — no txs).
  - DP `latest_solidified_block_num` matches the canonical solidified num.

In a new test `TestForkSwitch_BurnTrxNoDoubleCount`:

- [ ] Configure `AllowBlackholeOptimization = true` in DP defaults.
- [ ] Build a chain A and a longer chain B, each with txs whose `burnFee`
      hits the burn path. After the switch, `dp.BurnTrxAmount()` via the
      buffer reflects only canonical burns.

This will require constructing a transaction whose multi-sign or memo
fee triggers `burnFee`. If that's too expensive, fall back to a synthetic
test that calls `DynProps.AddBurnTrx` from a hook — but the brief asks
for end-to-end so prefer the real path. **Decision: use a hook-based
sanity test if the full burn-fee path is gated on actuator setup that
slice 2 cannot reach without touching M11.5 territory.**

In a new test `TestForkSwitch_RewardMaintenance_NoDoubleCount`:

- [ ] Configure `change_delegation = true`, short maintenance interval so a
      maintenance boundary lands inside the fork. Build chain A (3 blocks
      crossing maintenance) and chain B (4 blocks crossing maintenance,
      different cycle vote count). After the switch,
      `rawdb.ReadCycleBrokerage(bc.BufferedDB(), nextCycle, addr)` and
      `ReadCycleVote(bc.BufferedDB(), nextCycle, addr)` match canonical.

### 9. Restart-safety test

In a new test `TestFlushAtSolidified_SurvivesRestart`:

- [ ] Single-SR chain (so solidified == head every block). Build 5 blocks.
- [ ] After every block, the buffer should be empty (everything flushed).
      Assert `len(bc.buffer.PendingBlocks()) == 0`.
- [ ] Drop the `BlockChain`, build a fresh one on the same `diskdb`.
- [ ] Read via `rawdb.ReadWitness(diskdb, addr)` directly — NOT buffered.
      Assert `TotalProduced == 5`, `LatestBlockNum == 5`.
- [ ] Read DP via `state.LoadDynamicProperties(diskdb)`. Assert
      `LatestSolidifiedBlockNum() == 5`.

### 10. `make test` green

- [ ] All packages pass.
- [ ] Especially: `core/blockbuffer`, `core`, `consensus/dpos`,
      `core/state`, `actuator`, `core/rawdb`.

### 11. Commit

- [ ] GPG-signed (`E3673E008F6D506E`).
- [ ] 1-2 commits. Subject:
      `feat(core,state,reward): switchFork rewind retrofit + flush policy (slice 2)`.
- [ ] Body ≤3 lines.

## Out of slice 2 (logged, not implemented)

- `payBlockReward → AddCycleReward` per-block writes (gated on
  `change_delegation`, off on mainnet).
- Actuator-side rawdb-direct writes (`WriteAssetIssue` /
  `WriteExchange` / `WriteProposal` / `WriteContractCode`).
- Producer path (`core/block_builder.go`) — uses the same
  `applyRewardMaintenance` but is M6b territory and out of scope here.
- Any read-side migration outside the three specifically listed in the
  spec (callers of `bc.DB()`).
