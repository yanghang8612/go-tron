# Rawdb-direct writes vs switchFork rewind — known follow-up

**Status:** Open follow-up (not blocking M11 / current work)
**First documented:** 2026-04-29 (during M11.1 review)
**Owner:** TBD

## Problem

`BlockChain.applyBlock` writes several pieces of post-block state directly to
`rawdb.KeyValueStore` (the persistent Pebble store), not into the
`StateDB` MPT or any in-memory buffer:

| Write | Path | Introduced by |
|-------|------|---------------|
| `dp.Flush(bc.db)` (DynamicProperties) | `core/state/dynamic_properties.go:Flush` | M1.1 |
| `rawdb.WriteCycleBrokerage` / `WriteCycleVote` / `WriteWitnessVI` | `core/reward.go:applyRewardMaintenance` | M1.5 |
| `rawdb.WriteWitness` (TotalProduced / TotalMissed / LatestBlockNum / LatestSlotNum) | `consensus/dpos/statistic.go:ApplyBlockStatistics` | M11.1 |
| `rawdb.WriteTotalTransactionCount` | `core/blockchain.go:InsertBlock` | M10 |
| `dp.AddBurnTrx` (via Flush) | `actuator/fees.go:burnFee` | M10 |
| Solidified-block update (`rawdb.WriteLatestSolidifiedBlock`) | M9.5 |

`BlockChain.switchFork` rewinds `currentBlock` to LCA and re-runs `applyBlock`
across the new branch, but the rawdb writes from the orphaned branch's prior
`applyBlock` calls are **not** rolled back. After a switchFork:

- Witness `TotalProduced` and `TotalMissed` will have been incremented twice
  for slots present on both branches (orphaned + canonical).
- `BLOCK_FILLED_SLOTS` ring will have moved further than the canonical chain
  warrants.
- `total_transaction_count` will overshoot.
- `burn_trx_amount` may overshoot if the orphaned branch had multi-sign /
  memo / blackhole-routed fees.
- Cycle brokerage / VI for any maintenance boundary that fell on the
  orphaned branch persists, then a second copy is written for the canonical
  branch (overwrite-or-merge semantics depend on the specific writer).

## Why it does not currently fail tests

- `core/blockchain_insert_test.go::TestInsertBlock_*Fork*` and the M3.1
  switchFork tests validate state-root correctness on the canonical tip
  after recovery. They do not assert witness counters, fee totals, or
  ring contents.
- M0″ Phase 1 synthetic ranges are linear (no reorgs).

The bug surfaces when:

1. Real-mainnet single- or multi-block reorgs are exercised (M3.1 stress
   tests cover up to 10-block depth, but only state-root parity).
2. M0″ Phase 2 captures a real mainnet range that includes a reorg —
   captured snapshots reflect java-tron's correct rewind semantics; go-tron's
   rawdb writes will diverge.

java-tron's `Manager.eraseBlock` reads the orphaned block's transaction
results and explicitly reverses any DP / witness mutations before
re-applying the canonical chain.

## Resolution sketch (not implemented here)

Two viable approaches:

1. **Buffer + commit**: collect all rawdb-direct mutations into an in-memory
   buffer during `applyBlock`. Commit to disk only after `applyBlock`
   returns successfully. `switchFork` discards the buffer for any
   orphaned-branch apply. Cost: every DP / witness / counter writer
   currently using `rawdb.Write*` switches to a buffer abstraction.

2. **Per-block journal**: persist a per-block "undo log" alongside the block
   itself. `switchFork` reads each orphaned block's undo log and applies
   the inverse mutations in reverse order before re-applying the canonical
   branch. Cost: every writer must produce a paired inverse op.

Approach 1 is cleaner and matches go-ethereum's `state.StateDB` model
(in-memory journal until Commit). Approach 2 is closer to java-tron's
`Manager.eraseBlock` model.

Either should land before:
- M0″ Phase 2 acceptance, OR
- Any production deployment that could face natural reorgs.

## Affected commits (not exhaustive)

- M1.1: `dp.Flush` introduction
- M1.5: cycle brokerage / VI writes
- M9.5: solidified-block update
- M10: burn_trx_amount, total-tx-count
- M11.1: witness statistics
- M11.4: total_create_witness_cost (via DP)
