# Plan: fork-rewind correctness — slice 1

**Spec:** [2026-04-30-fork-rewind-fix-design.md](../specs/2026-04-30-fork-rewind-fix-design.md)
**Source-of-truth follow-up:** [docs/dev/fork-rewind-rawdb-writes.md](../../dev/fork-rewind-rawdb-writes.md)

## Slice 1 goals

1. Land the buffer abstraction (`core/blockbuffer`).
2. Retrofit one writer category (witness statistics) as proof-of-pattern.
3. Wire the buffer into `applyBlock` + `switchFork`.
4. Cover with unit tests + a reorg correctness test.

## Tasks

### 1. Create `core/blockbuffer/buffer.go`

- [ ] `type Buffer` with `base ethdb.KeyValueReader`, `layers []*layer`,
      `active *layer` fields.
- [ ] `New(base) *Buffer`.
- [ ] `BeginBlock(hash) / CommitBlock() / DiscardActive()`.
- [ ] `DiscardBlock(hash)`, `Discard()`.
- [ ] `Get / Has / Put / Delete` satisfying `ethdb.KeyValueReader+Writer`.
- [ ] `Flush(w ethdb.KeyValueWriter) error` — drains layered writes oldest-first.

### 2. Buffer unit tests (`core/blockbuffer/buffer_test.go`)

- [ ] read-through to base
- [ ] write-then-read in active layer
- [ ] tombstone semantics (Delete then Get)
- [ ] layer stacking (newer overrides older)
- [ ] `DiscardActive` drops in-progress writes
- [ ] `DiscardBlock(hash)` removes that layer only
- [ ] `BeginBlock` while active panics
- [ ] `CommitBlock` without active panics
- [ ] `Flush` drains oldest-first

### 3. Narrow `dpos.ApplyBlockStatistics` signature

In `consensus/dpos/statistic.go`:

- [ ] Replace `db ethdb.KeyValueStore` with a local interface
      `kvReadWriter { ethdb.KeyValueReader; ethdb.KeyValueWriter }`.
- [ ] Verify existing `consensus/dpos/statistic_test.go` still compiles (passes
      `rawdb.NewMemoryDatabase()` which satisfies the narrower interface).

### 4. Wire into `core/blockchain.go`

- [ ] Add `buffer *blockbuffer.Buffer` field to `BlockChain`. Initialize in
      `NewBlockChain` with `blockbuffer.New(db)`.
- [ ] In `applyBlock`:
  - call `bc.buffer.BeginBlock(block.Hash())` at start
  - on any return-with-error path, call `bc.buffer.DiscardActive()`
  - pass `bc.buffer` (instead of `bc.db`) to `dpos.ApplyBlockStatistics`
  - on success path, call `bc.buffer.CommitBlock()` before final return
- [ ] In `switchFork`:
  - after `khaosDB.GetBranch` succeeds, iterate `oldBranch` and call
    `bc.buffer.DiscardBlock(kb.block.Hash())` for each.
- [ ] Add helper `bc.BufferedDB() ethdb.KeyValueReader` returning the buffer
      (used by tests).

### 5. Reorg correctness test

In `core/blockchain_insert_test.go`:

- [ ] `TestForkSwitch_WitnessCountersNoDoubleCount`:
  - 3 blocks chain A, 4 blocks chain B, switchFork on block 4B.
  - Read via `bc.BufferedDB()` to assert `TotalProduced == 4` after switch.
  - Comment explaining why disk read won't work in slice 1.

- [ ] `TestLinearExtension_WitnessCountersThroughBuffer`:
  - 3 linear blocks, no fork.
  - Assert `TotalProduced == 3` via buffer.

### 6. `make test` green

- [ ] All packages pass.
- [ ] Existing fork-switch state-root test still passes.

### 7. Commit

- [ ] GPG-signed (`E3673E008F6D506E`).
- [ ] Subject: `fix(core): switchFork rolls back rawdb-direct writes (slice 1)`.
- [ ] 1 commit (or 2 if buffer pkg + integration land separately).

## Out of slice 1 — slice 2 backlog

Each requires the same retrofit pattern: change writer to take
`KeyValueReader+Writer`, and pass `bc.buffer` in `applyBlock`.

- [ ] `core/state/dynamic_properties.go::Flush` (DP changes — including
      `BLOCK_FILLED_SLOTS`, `total_create_witness_cost`, `burn_trx_amount`,
      `latest_solidified_block_num`).
- [ ] `core/reward.go::applyRewardMaintenance` (cycle brokerage, vote, VI).
- [ ] `core/blockchain.go::InsertBlock` total-tx-count increment.
- [ ] `actuator/fees.go::burnFee` (already lands via DP Flush; re-verify after
      DP Flush retrofit).
- [ ] `core/blockchain.go::updateSolidifiedBlock` (`WriteWitnessLatestBlock`
      direct write).

Slice 2 also needs:

- [ ] Stable-flush policy (e.g. flush layers below `head - reorg_horizon`, or
      flush at solidified-block boundary).
- [ ] Restart safety: persist any unflushed buffer at shutdown OR document
      that the buffer is bounded by `reorg_horizon`.
- [ ] M0″ Phase 2 acceptance check that includes a real-mainnet reorg range.
