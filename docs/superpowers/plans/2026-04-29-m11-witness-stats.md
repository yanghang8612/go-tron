# M11 Witness Statistics — Plan

**Spec:** [2026-04-29-m11-witness-stats-design.md](../specs/2026-04-29-m11-witness-stats-design.md)
**Slice:** 1 of N (witness counters + filled-slots ring)

## Slice 1 — Witness counters + BLOCK_FILLED_SLOTS ring

- [ ] **DP layer**
    - [ ] Add `BlockFilledSlotsNumber = 128` constant to `core/state/dynamic_properties.go`.
    - [ ] Add `"block_filled_slots"` (128 zero bytes) to `defaultStringProps`.
    - [ ] Add `BlockFilledSlots() []byte`.
    - [ ] Add `SetBlockFilledSlots(v []byte)` — panic on length mismatch.
    - [ ] Add `ApplyBlockToFilledSlots(filled bool)` — set bit at current index, advance index mod 128.
    - [ ] Add `CalculateFilledSlotsCount() int64` — `100 * sum / 128`.
    - [ ] Tests in `core/state/dynamic_properties_test.go`:
        - [ ] `TestBlockFilledSlots_RingRotation` — apply 130 filled blocks, verify index wraps.
        - [ ] `TestBlockFilledSlots_FillRate` — 64 filled + 64 empty → 50%.
        - [ ] `TestBlockFilledSlots_Persistence` — Flush + reload, ring round-trips.

- [ ] **Witness proto accessors**
    - [ ] Add `LatestBlockNum() / SetLatestBlockNum(int64)` to `core/types/witness.go`.
    - [ ] Add `LatestSlotNum() / SetLatestSlotNum(int64)` to same file.

- [ ] **StatisticManager port**
    - [ ] Create `consensus/dpos/statistic.go`:
        - [ ] `ApplyBlockStatistics(db, dp, block, prevHeadTimestamp, activeWitnesses, genesisTimestamp, isMaintenance)`.
        - [ ] Helper `loadOrInitWitness(db, addr) *types.Witness`.
    - [ ] Create `consensus/dpos/statistic_test.go`:
        - [ ] `TestApplyBlockStatistics_ProducerCounters`.
        - [ ] `TestApplyBlockStatistics_NoMissed`.
        - [ ] `TestApplyBlockStatistics_OneMissed`.
        - [ ] `TestApplyBlockStatistics_Block1Skip`.
        - [ ] `TestApplyBlockStatistics_LoadOrInit`.

- [ ] **Wire into BlockChain.applyBlock**
    - [ ] Capture `previousHeadTimestamp = current.Timestamp()` at top of `applyBlock` (before ProcessBlock).
    - [ ] Compute `isMaintenance` from previousHeadTimestamp.
    - [ ] Call `dpos.ApplyBlockStatistics(...)` after ProcessBlock returns, before maintenance check at line 265.

- [ ] **Verification**
    - [ ] `make test` green across all 28+ packages.
    - [ ] If any reward / maintenance / engine tests break: triage — these are the only paths that touch witnesses, so a green run is strong evidence the rawdb-only writeback path is non-conflicting.
    - [ ] Commit with message `feat(consensus,state): witness statistics + BLOCK_FILLED_SLOTS ring (M11.1)`.

## Out of slice 1 (deferred)

- Conformance digest extension (DigestB/C) to include Witness proto fields and DP string-typed values — separate slice; without it M0″ Phase 2 will not catch witness state divergence.
- Producer-side `LOW_PARTICIPATION` gate using `CalculateFilledSlotsCount()`.
- `WitnessProductBlockService` cheat-witness detection.
- AVAILABLE_CONTRACT_TYPE / ACTIVE_DEFAULT_OPERATIONS bitmaps (M11 slice 2 candidate).
- Freeze-V2 per-account window size + lock/unlock split (separate milestone).
