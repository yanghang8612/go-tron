# M11 Witness Statistics + BLOCK_FILLED_SLOTS — Design Spec

**Date:** 2026-04-29
**Status:** Active
**Milestone:** M11 — Witness statistics consensus-state gap closure
**Slice:** Slice 1 (witness counters + filled-slots ring)

---

## 1. Problem

`core/types/witness.go` exposes `TotalProduced / TotalMissed / LatestSlotNum` (proto fields 5/6/8) and proto field 7 `LatestBlockNum`, but **nothing in the go-tron block-insert path ever updates them**. Every block produced by go-tron leaves the witness state in its post-genesis (zero) form. Mainnet replay would diverge on every block where any witness has produced or missed a slot.

`BLOCK_FILLED_SLOTS` (java-tron `int[128]` rolling array of 0/1 per slot, persisted under `BLOCK_FILLED_SLOTS` DP key) is similarly absent. java-tron's `BLOCK_FILLED_SLOTS_INDEX` exists in go-tron's defaultProps but the array itself does not, so `applyBlock(filled)` and `calculateFilledSlotsCount()` cannot be implemented.

Combined effect:
- Witness state diverges per block (TotalProduced/TotalMissed counters).
- `LatestBlockNum / LatestSlotNum` per witness diverge — these gate cheat-witness detection in java-tron `WitnessProductBlockService`.
- Producer-side `StateManager.getState()` cannot return `LOW_PARTICIPATION` because participation rate is unimplemented.

## 2. Java-tron Reference

### `BLOCK_FILLED_SLOTS` storage (`DynamicPropertiesStore.java`)

```java
public static final int BLOCK_FILLED_SLOTS_NUMBER = 128;     // ChainConstant.java

public void saveBlockFilledSlots(int[] blockFilledSlots) {
    this.put(BLOCK_FILLED_SLOTS,
        new BytesCapsule(ByteArray.fromString(intArrayToString(blockFilledSlots))));
}

public int[] getBlockFilledSlots() { ... }            // load+parse comma-separated string
public int getBlockFilledSlotsNumber() { return 128; }

public void applyBlock(boolean fillBlock) {
    int[] blockFilledSlots = getBlockFilledSlots();
    int idx = getBlockFilledSlotsIndex();
    blockFilledSlots[idx] = fillBlock ? 1 : 0;
    saveBlockFilledSlotsIndex((idx + 1) % getBlockFilledSlotsNumber());
    saveBlockFilledSlots(blockFilledSlots);
}

public int calculateFilledSlotsCount() {
    int[] blockFilledSlots = getBlockFilledSlots();
    return 100 * IntStream.of(blockFilledSlots).sum() / getBlockFilledSlotsNumber();
}
```

### `StatisticManager.applyBlock` (`consensus/dpos/StatisticManager.java`)

```java
public void applyBlock(BlockCapsule blockCapsule) {
  long blockNum = blockCapsule.getNum();
  long blockTime = blockCapsule.getTimeStamp();
  byte[] blockWitness = blockCapsule.getWitnessAddress().toByteArray();

  WitnessCapsule wc = consensusDelegate.getWitness(blockWitness);
  wc.setTotalProduced(wc.getTotalProduced() + 1);
  wc.setLatestBlockNum(blockNum);
  wc.setLatestSlotNum(dposSlot.getAbSlot(blockTime));
  consensusDelegate.saveWitness(wc);

  long slot = 1;
  if (blockNum != 1) {
    slot = dposSlot.getSlot(blockTime);
  }
  for (int i = 1; i < slot; ++i) {
    byte[] witness = dposSlot.getScheduledWitness(i).toByteArray();
    wc = consensusDelegate.getWitness(witness);
    wc.setTotalMissed(wc.getTotalMissed() + 1);
    consensusDelegate.saveWitness(wc);
    consensusDelegate.applyBlock(false);
  }
  consensusDelegate.applyBlock(true);
}
```

`dposSlot.getSlot(blockTime)` is `SlotForTime(blockTime, headTimestamp, genesisTime, isMaintenance, maintenanceSkipSlots)` — the **head before this block was inserted**, not the new block's parent in the canonical-chain sense (they're the same when the chain is linear; on fork switch the canonical head jumps but this hook only runs on linear-extension `applyBlock`). `dposSlot.getAbSlot(blockTime)` is `AbsoluteSlot(blockTime, genesisTime)`.

### Where it's consumed

| Consumer | Reads |
|---|---|
| `consensus/dpos/StateManager.getState()` | `calculateFilledSlotsCount()` — gates producer-side `LOW_PARTICIPATION` |
| `framework/services/WitnessProductBlockService` | witness `latestBlockNum` — cheat-witness detection |
| `core/conformance` (M0″) | per-witness state (currently NOT digested — pre-existing gap noted in §6) |

## 3. Design

### 3.1 DP layer (`core/state/dynamic_properties.go`)

Add to `defaultStringProps`:

```go
"block_filled_slots": string(make([]byte, BlockFilledSlotsNumber)),
```

Constant in same file:

```go
const BlockFilledSlotsNumber = 128
```

New methods:

```go
func (dp *DynamicProperties) BlockFilledSlots() []byte
func (dp *DynamicProperties) SetBlockFilledSlots(v []byte)        // panics if len != 128
func (dp *DynamicProperties) ApplyBlockToFilledSlots(filled bool) // sets cur idx, advances mod 128
func (dp *DynamicProperties) CalculateFilledSlotsCount() int64    // 100 * sum / 128
```

Storage: 128 raw bytes via `stringProps`. go-tron's DB layout already diverges from java-tron's at the key string level (`next_proposal_id` vs `LATEST_PROPOSAL_NUM` etc.), so we don't need wire-compat at the value byte level either. Each byte is 0 or 1.

### 3.2 Witness proto accessors (`core/types/witness.go`)

```go
func (w *Witness) LatestBlockNum() int64       { return w.pb.LatestBlockNum }
func (w *Witness) SetLatestBlockNum(v int64)   { w.pb.LatestBlockNum = v }
func (w *Witness) LatestSlotNum() int64        { return w.pb.LatestSlotNum }
func (w *Witness) SetLatestSlotNum(v int64)    { w.pb.LatestSlotNum = v }
```

(`pb.LatestBlockNum` and `pb.LatestSlotNum` already exist on `corepb.Witness`.)

### 3.3 StatisticManager port (`consensus/dpos/statistic.go` — new file)

```go
package dpos

func ApplyBlockStatistics(
    db ethdb.KeyValueStore,
    dp *state.DynamicProperties,
    block *types.Block,
    previousHeadTimestamp int64,
    activeWitnesses []common.Address,
    genesisTimestamp int64,
    isMaintenance bool,
) {
    blockNum := int64(block.Number())
    blockTime := block.Timestamp()
    producer := block.WitnessAddress()

    // 1. Producing witness counters.
    wc := loadOrInitWitness(db, producer)
    wc.SetTotalProduced(wc.TotalProduced() + 1)
    wc.SetLatestBlockNum(blockNum)
    wc.SetLatestSlotNum(AbsoluteSlot(blockTime, genesisTimestamp))
    rawdb.WriteWitness(db, producer, wc)

    // 2. Slot offset from old head (block 1 short-circuits, mirrors java-tron).
    var slot int64 = 1
    if blockNum != 1 {
        slot = SlotForTime(blockTime, previousHeadTimestamp, genesisTimestamp,
            isMaintenance, params.MaintenanceSkipSlots)
    }

    // 3. Missed slots [1..slot).
    for i := int64(1); i < slot; i++ {
        missed := GetScheduledWitness(i, previousHeadTimestamp, genesisTimestamp,
            activeWitnesses, isMaintenance, params.MaintenanceSkipSlots)
        m := loadOrInitWitness(db, missed)
        m.SetTotalMissed(m.TotalMissed() + 1)
        rawdb.WriteWitness(db, missed, m)
        dp.ApplyBlockToFilledSlots(false)
    }

    // 4. Mark current slot filled.
    dp.ApplyBlockToFilledSlots(true)
}
```

`loadOrInitWitness` reads via `rawdb.ReadWitness`; if absent, returns `types.NewWitness(addr, "")` so the counter increment still persists.

### 3.4 Wire-up (`core/blockchain.go applyBlock`)

Insert after `ProcessBlock`, before maintenance check. Capture `previousHeadTimestamp = current.Timestamp()` BEFORE calling ProcessBlock; `current` is `bc.CurrentBlock()` at the top of `applyBlock`.

```go
previousHeadTimestamp := current.Timestamp()
isMaintenance := dynProps.NextMaintenanceTime() > 0 &&
    previousHeadTimestamp >= dynProps.NextMaintenanceTime()
dpos.ApplyBlockStatistics(bc.db, dynProps, block, previousHeadTimestamp,
    bc.ActiveWitnesses(), bc.GenesisTimestamp(), isMaintenance)
```

`isMaintenance` mirrors `DPoS.IsInMaintenance(previousHeadTimestamp)` — it gates the slot calculator the same way java-tron's `dposSlot.getSlot()` does (uses the OLD head's maintenance status, since the new block's slot is being computed against the old head).

## 4. Persistence Path Decision

Witnesses are written via `rawdb.WriteWitness` directly, NOT via `statedb.PutWitness`:

- `StateDB.witnesses` is an in-memory cache only — `Commit()` does not write witnesses anywhere.
- The existing flow already loads witnesses into statedb at the top of `applyBlock` (lines 247-256) but for read access (URL + VoteCount) only.
- Maintenance / reward distribution mutations (`AddAllowance`, `WriteCycleBrokerage`, `WriteWitnessVI`) never touch `TotalProduced/TotalMissed/LatestSlotNum/LatestBlockNum`, so there is no double-writer hazard with rawdb.
- After `applyBlock` returns, the next block reloads witnesses from rawdb → updated counters surface naturally.

## 5. Exit Gate

| Test | Assertion |
|------|-----------|
| `TestBlockFilledSlots_RingRotation` | apply 130 blocks alternating filled/empty → index wraps, ring contains expected pattern |
| `TestBlockFilledSlots_FillRate` | apply 64 filled + 64 empty → CalculateFilledSlotsCount == 50 |
| `TestBlockFilledSlots_Persistence` | flush + reload → array round-trips |
| `TestApplyBlockStatistics_ProducerCounters` | single block → producer.TotalProduced=1, LatestBlockNum=N, LatestSlotNum=AbsoluteSlot(blockTime) |
| `TestApplyBlockStatistics_NoMissed` | blockTime = headTime + interval (slot=1) → TotalMissed unchanged for everyone |
| `TestApplyBlockStatistics_OneMissed` | blockTime = headTime + 2×interval (slot=2) → scheduled-at-1 witness gets TotalMissed+1, ring[idx]=0 then ring[idx+1]=1 |
| `TestApplyBlockStatistics_Block1Skip` | block.Number()==1 → no missed loop runs even if blockTime is far from genesis |
| `TestApplyBlockStatistics_LoadOrInit` | producer not previously in witness store → counter still persists with TotalProduced=1 |
| `make test` | Full suite (28+ packages) green |

## 6. Known Gaps Not Addressed Here

1. **Conformance digest does not include Witness proto** — `core/conformance/digest.go` DigestB/DigestC iterate touched-address Account state and DP int64 keys only. Witness proto fields (TotalProduced/Missed/LatestBlockNum/SlotNum) and DP string-typed values are silently ignored. Pre-existing limitation — fixing requires extending DigestB/C to fold in `rawdb.ReadWitness(addr).pb` for witness addresses, and the DP string map. Defer to a separate slice; without it, M0″ Phase 2 will not catch divergence in the values M11 introduces.
2. **Missed-slot loop crossing maintenance boundary** — if the missed-slot range spans a maintenance, java-tron's `getScheduledWitness` returns witnesses from the active set as captured at call time (which is post-maintenance for blocks AFTER the boundary). go-tron's `bc.ActiveWitnesses()` similarly returns the current set. Edge case is rare (only when no block was produced for a full maintenance interval) and the divergence direction is not yet characterized. Note as TODO; if M0″ Phase 2 hits it, allowlist or fix then.
3. **`StateManager.getState()` LOW_PARTICIPATION gate** — M11 makes `CalculateFilledSlotsCount()` available but does not wire it into producer-side block production gating. That's a separate slice for the producer (`cmd/gtron` / producer service).
4. **Cheat-witness detection (`WitnessProductBlockService`)** — same: `LatestBlockNum` per witness is now updated correctly, but no consumer in go-tron yet flags duplicate-witness production. Out of M11 scope.

## 7. Implementation Order

1. DP layer (`block_filled_slots` + helpers) — independent, testable in isolation.
2. Witness proto accessors — trivial, unblocks (3).
3. `consensus/dpos/statistic.go` + tests — heaviest piece, exercises the rawdb writeback.
4. Wire into `core/blockchain.go` — minimal diff, integration-tested via existing `make test` block-insert paths.
5. `make test`, fix breakage, commit.
