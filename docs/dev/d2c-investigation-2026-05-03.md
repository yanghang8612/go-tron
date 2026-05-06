# D-2.c Investigation — SR allowance drift (2026-05-03)

## Observation

At H=96498, after 8758 blocks since the H=87740 byte-equal baseline:

| node | allowance |
|---|---|
| gtron | 927,410,688,000 |
| java-tron | 927,400,491,089 |
| Δ | **10,196,911 (gtron HIGHER)** |

~1166 sun/block average drift. gtron is sync-only (java-tron produces).

## What was ruled out

- **Per-block brokerage math**: `int64(0.2 × 32_000_000) = 6_400_000`,
  `int64(0.2 × 16_000_000) = 3_200_000` — bit-identical IEEE 754 on both sides.
  Float simulation confirms no rounding drift for vote_count=100 or any
  single-SR scenario.
- **GR vote removal**: `getRemoveThePowerOfTheGr=0` (default) in the genesis
  fixture. Neither side removes genesis votes.
- **Float precision in standby pay**: with a single SR taking all votes,
  `int64(V × totalPay / V) == totalPay` exactly for any reasonable V.
- **`payTransactionFeeReward` absence**: java-tron distributes tx fee pool
  per-block via this path. If absent from gtron it would make java HIGHER,
  not lower. Direction mismatch — not D-2.c.
- **`allowOldRewardOpt` / `RewardViCalService` path**: `getAllowNewReward=1`
  from genesis sets `NewRewardAlgorithmEffectiveCycle=0`, meaning ALL cycles
  use the VI-based algorithm. `oldRewardSum` is never called.
- **Maintenance ordering**: go-tron `ProcessBlock` (payBlockReward +
  payStandbyWitness) runs before `applyRewardMaintenance`, same as
  java-tron's `payReward` running before `consensus.applyBlock → doMaintenance`.
  No ordering divergence.

## Leading hypothesis

**Witness VoteCount is never persisted from statedb to rawdb after a
VoteWitness transaction. This causes `accumulateWitnessVi` to use a stale
(lower) vote count in gtron, producing a larger per-vote VI delta, and
therefore a larger voter reward when `withdrawReward` is eventually called.**

### Mechanism

1. `core/state/statedb.go::Commit()` (lines 1010-1045) iterates only
   `s.stateObjects` (accounts). The `s.witnesses` map is NOT committed.
   Witness changes (VoteCount, URL) made via `AddWitnessVoteCount` /
   `PutWitness` during a block's execution are in-memory only.

2. `consensus/dpos/statistic.go::ApplyBlockStatistics` calls
   `rawdb.ReadWitness → update TotalProduced/Missed/LatestBlock/LatestSlot
   → rawdb.WriteWitness`. VoteCount is read from rawdb (genesis value = 100)
   and written back unchanged. The in-memory statedb vote delta from
   VoteWitness is never persisted.

3. `VoteWitnessActuator.Execute` calls `ctx.State.AddWitnessVoteCount` —
   updates statedb only. At start of the next `applyBlock`, witnesses are
   reloaded from rawdb (still 100). The effect is **transient** — valid only
   within the block where VoteWitness ran.

4. In java-tron, `VoteWitnessActuator.countVoteAccount` writes to
   `VotesStore` only (not WitnessStore). At the next maintenance,
   `MaintenanceManager.countVote` reads VotesStore, applies the delta, and
   persists the new VoteCount to WitnessStore. The VoteCount persists.

### Per-maintenance divergence

After Flow 5 (VoteWitness, vote_count=1) runs in a prior cross-flows session:

| node | rawdb witness VoteCount | accumulateWitnessVi uses |
|---|---|---|
| gtron | 100 (genesis, never updated) | 100 |
| java-tron | 101 (100 genesis + 1 VoteWitness applied at maintenance) | 101 |

`accumulateWitnessVi(cycle, voteCount)` computes:
```
delta_VI = cycleReward × 10^18 / voteCount
```

Per cycle (2160 blocks, witness brokerage 20%):
```
cycleReward ≈ (25.6M + 12.8M) × 2160 = 82,944,000,000 sun
delta_VI_gtron  = 82,944,000,000 × 10^18 / 100
delta_VI_java   = 82,944,000,000 × 10^18 / 101
```

When SR (1 vote) calls `withdrawReward` for N cycles under this divergence:
```
voter_reward_gtron = N × cycleReward / 100
voter_reward_java  = N × cycleReward / 101
Δ = N × cycleReward × (1/100 − 1/101) = N × 82,944,000,000 / 10100 ≈ N × 8.2M
```

For N=1 cycle: Δ ≈ 8.2M. Two `withdrawReward` calls across the 4 maintenance
cycles in the 8758-block window produce ~10–16M differential — consistent
with the observed 10.2M.

### Direction check

gtron uses a smaller divisor (100 < 101), so delta_VI is larger, and the
voter reward for 1 vote is larger. gtron HIGHER. ✓

## Side finding (real bug, wrong direction for D-2.c)

`WitnessUpdateActuator.Execute` (actuator/witness_update.go:51) calls
`ctx.State.PutWitness(ownerAddr, string(c.UpdateUrl))`, which calls
`types.NewWitness(addr, url)` — a fresh witness with **VoteCount=0**.

This resets the in-memory VoteCount to 0 for the duration of the block. If
a WitnessUpdate tx lands on a maintenance block, `applyRewardMaintenance`
reads `statedb.GetWitness(a).VoteCount() == 0` and skips `accumulateWitnessVi`
(the `voteCount == 0` branch). That cycle's VI would not accumulate in gtron
but would in java-tron → makes java HIGHER (opposite direction from D-2.c).
Not the cause of the current drift but a real latent bug for any WitnessUpdate
that coincides with a maintenance block.

## Files and lines

| location | issue |
|---|---|
| `core/state/statedb.go:1010–1045` | `Commit()` loops `s.stateObjects` only; `s.witnesses` excluded |
| `core/reward.go:188–200` | `applyRewardMaintenance` reads statedb VoteCount which reflects transient in-block changes |
| `actuator/witness_update.go:51` | `PutWitness` creates fresh witness with VoteCount=0 |
| `consensus/dpos/statistic.go:48–52` | `ApplyBlockStatistics` rewrites rawdb witness but never persists statedb VoteCount |

## Recommended fix

Implement a persistent VoteCount update path:

1. Add `WriteCycleVoteCount` (or extend `WriteWitness`) so that vote
   count deltas from VoteWitness are applied to rawdb atomically as part of
   the block commit (similar to how java-tron's VotesStore defers to
   maintenance). The simplest approach:
   - Accumulate vote-count deltas on a pending-vote store (key: block buffer
     layer) during VoteWitness execution.
   - At the end of `applyBlock` (before `CommitBlock`), apply the deltas to
     the rawdb witness records via `WriteWitness`.
   - This matches java-tron's VotesStore semantic (deferred application), but
     in go-tron's block-scoped buffer model.

2. Fix `WitnessUpdateActuator` to use a `SetWitnessURL` helper that reads the
   existing witness, mutates only the URL, and writes back — preserving VoteCount.

## Status

Unresolved. Fix requires a VotesStore-equivalent refactor (pending-vote
accumulation deferred to end-of-block or maintenance). Not a 1-line change.
