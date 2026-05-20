# V2 energy-window divergence (cross-impl audit 2026-05-19, backlog item 7)

**Status:** CONFIRMED real consensus divergence — **FIXED 2026-05-20** (energy
settle/limit path; bandwidth + delegation windows remain follow-ups).
**Date:** 2026-05-20

## Verdict

go-tron and java-tron diverge on the **per-account energy recovery window**, stored
in the committed account proto as `Account.AccountResource.energy_window_size`
(field 9) + `energy_window_optimized` (field 12).

- java-tron maintains this window **per account** and recovers energy usage
  against it on every consumption.
- go-tron's energy-bill settle path recovers against the **global**
  `params.WindowSizeSlots` (28800 slots) and **never reads or writes** the
  per-account field. go-tron's StateDB has no setter for it at all.

The divergence is **not** in the post-settle `energy_usage` *value* (that matches —
see proof below). It is in (a) the committed `energy_window_size` / `optimized`
fields and (b) the window used to recover usage on the *next* transaction.

This is gated by `supportUnfreezeDelay()` (Stake 2.0), which is **live on mainnet
and Nile** — not a dormant fork path. go-tron's own `calcAccountEnergyLimit`
already takes the V2 branch on `UnfreezeDelayDays() > 0`.

## The two sides, exactly

### java-tron (settle path)
`TransactionTrace.pay()` → `ReceiptCapsule.payEnergyBill` → `EnergyProcessor.useEnergy`
→ `ResourceProcessor.increase`/`increaseV2`:

- `EnergyProcessor.useEnergy` (V2 branch, ResourceProcessor.java:127) calls
  `increase(account, ENERGY, energyUsage, energy, latestConsumeTime, now)`.
- `increase`/`increaseV2` read `account.getWindowSize(ENERGY)` and **write it back**
  via `setNewWindowSize` / `setNewWindowSizeV2` (ResourceProcessor.java:118-130 /
  133-188). `setNewWindowSizeV2` also sets `energy_window_optimized = true`.
- The window is read from / written to `AccountResource.energy_window_size`
  (AccountCapsule.java:1372-1420).

### go-tron (settle path)
`PayEnergyBill` → `billCallerSide` → `useEnergyForBill` (actuator/energy_bill.go:153):

```go
recovered := recoverEnergyUsageForDP(GetEnergyUsage, GetLatestConsumeTimeForEnergy, now, dp)
SetEnergyUsage(addr, recovered+usage)
SetLatestConsumeTimeForEnergy(addr, now)
```

`recoverEnergyUsageForDP` → `recoverEnergyUsageWithHarden` (energy_bill.go:334) and
`core/resource.go::recoverUsageWithHarden` both hardcode
`windowSize := int64(params.WindowSizeSlots)`. No account argument; no window write.

## Corrected mental model (vs the task's framing)

The task framed the renormalization as living in the settle:
`increase(R + preCharge, actualUsage, now, now)`. The actual trace is subtler:

1. **Pre-charge** (VMActuator.java:573-591) recovers usage via `updateUsage`, sets
   `latestConsumeTimeForEnergy = now`, captures pre-merge `{R, W_R}` into the
   receipt, then pre-charges `min(leftFrozen, feeLimit/sun)` and commits to the
   VM repository. This is where the window can **shrink** — `updateUsage`'s
   `increase` runs with `lastTime != now`, so the decay branch fires and the
   stored window becomes `oldWindowSizeV2 - delta·WINDOW_SIZE_PRECISION`.
2. **`resetAccountUsage`** (TransactionTrace.java:290-325) restores the account to
   the **pre-merge** `{energy_usage = R, window = W_R}`, undoing the pre-charge
   (`newArea = R·W_R`, `newUsage = R`). It does **not** restore
   `latestConsumeTimeForEnergy`, which stays at `now`.
3. **Settle** `useEnergy` then calls `increase(account, ENERGY, R, actualUsage, now, now)`.
   Because `lastTime == now`, the decay branch is skipped — the settle mostly
   *preserves* the window the recovery already produced; it does not itself shrink it.

So: the **recovery** (`updateUsage`, `lastTime != now`) is what makes the window
non-default; the settle (`lastTime == now`) carries it forward. go-tron does
neither — it always uses 28800 and stores nothing.

## Why the `energy_usage` value still matches

With the window at the default `W = 28800` on both sides, java's
`increase(R, s, now, now)` collapses to exactly `R + s`:

```
newUsage = floor( (ceil(R·P/W)·W + ceil(s·P/W)·W) / P )
```
Each `ceil(x·P/W)·W/P ∈ [x, x + W/P)`. Summed overshoot `< 2·W/P = 2·28800/1_000_000
= 0.0576 < 1`, so the floor lands exactly on `R + s` = go-tron's `recovered + usage`.
The pre-charge nets out via `resetAccountUsage`. **The value is not the divergence;
the window is.** (This supersedes the original
`project_v2_pre_charge_followup` concern about a double-counted pre-charge.)

## Three consequences

1. **Committed-state byte divergence.** After a V2-staked contract charge, java's
   account has `energy_window_size = <renormalized>`, `optimized = true`; go-tron's
   stays `0`/`false` (or the stale ingested value). If `AllowAccountStateRoot` is
   active, the per-account state root diverges → block-hash fork (immediate).
   Otherwise latent (see below).
2. **Feed-forward recovery divergence.** The *next* tx recovers usage against the
   per-account window in java vs the global window in go-tron. Different decay →
   different recovered usage → different available stake-energy → can flip an
   `OUT_OF_ENERGY` boundary → different committed receipt (`energy_usage` /
   `energy_fee`) → fork. **This bites even without state-root.**
3. **In-contract observability + internal inconsistency.** go-tron's own staking
   query precompile (`vm/precompile_tron.go::resourceUsageBalanceAndRestoreSeconds`
   → `stakingWindowSizeSlots` + `recoverStakingUsage`) **already reads the
   per-account window faithfully**. So go-tron is self-contradictory: the settle
   path ignores the field that the precompile honors. A contract querying resource
   balance/restore-time after a go-tron energy charge sees a stale window.

## Magnitude (concrete, from the tests)

Account with `energy_usage = 1_000_000`, 7200 slots (6h) elapsed, window stored as
`14_400_000` (V2, optimized → 14400 slots, half the default):

| recovered via | window | result |
|---|---|---|
| go-tron settle (`recoverEnergyUsageForDP`) | 28800 (global) | **750_000** |
| java-tron / go-tron precompile (`recoverStakingUsage`) | 14400 (per-account) | **500_000** |

A 250_000-energy gap in "available energy from freeze" on the next tx. Identical for
the legacy and hardened formulas.

## Tests (landed, passing — characterize current divergent behavior)

- `vm/energy_window_divergence_test.go`
  - `TestEnergyWindow_PrecompileReadsPerAccountWindow` — precompile returns 14400.
  - `TestEnergyWindow_RecoveryDivergesOnWindow` — 500_000 vs 750_000 (legacy + hardened).
- `actuator/energy_window_divergence_test.go`
  - `TestEnergyWindow_RecoverHelperCannotSeePerAccountWindow` — settle helper is
    structurally global (returns 750_000, can't produce 500_000).
  - `TestEnergyWindow_UseEnergyForBillLeavesWindowStale` — full settle entry point:
    `energy_usage` becomes 800_000 (global) not java's 550_000, and
    `energy_window_size` is left stale at 14_400_000 (never renormalized).

When the fix lands, the latter two flip to assert java-matching behavior.

## Relationship to known gaps

- **Expands TODO.md §1.5.** That entry scopes the per-account-window gap to
  "reshuffled via `getNewWindowSize` *during undelegation*." In reality the window
  mutates on **every energy consumption** (`updateUsage` recovery + settle), not
  just undelegation. The gap is broader than §1.5 documents.
- Supersedes the pre-charge double-count worry in
  `project_v2_pre_charge_followup` (nets out via `resetAccountUsage`).

## Implemented fix (2026-05-20)

Ported java-tron's per-account energy window into go-tron's settle/limit path,
gated on `SupportUnfreezeDelay()`. Energy-scoped; bandwidth + delegation windows
left as follow-ups.

- `params/protocol_params.go` — `WindowSizePrecision = 1000`.
- `core/types/account.go` — per-account energy window accessors mirroring
  AccountCapsule: `RawEnergyWindowSize`, `EnergyWindowOptimized`,
  `EnergyWindowSize` (V1 view), `EnergyWindowSizeV2` (scaled view),
  `SetNewEnergyWindowSize` / `SetNewEnergyWindowSizeV2` / `SetEnergyWindow`.
- `core/state/statedb.go` — `SetEnergyWindow(addr, raw, optimized)` (journals +
  markDirty, mirroring `SetEnergyUsage`).
- `actuator/energy_window.go` — pure `computeEnergyIncrease` porting java
  `ResourceProcessor.increase` (V1) / `increaseV2` (V2), incl. the harden
  (BigInteger) and non-harden branches, window renormalization, and the
  `getWindowSize`/`getWindowSizeV2` views.
- `actuator/energy_bill.go` — `useEnergyForBill(…, success bool)` settles the
  per-account window in the V2 regime, with a **success/failure gate** (Codex
  review, confirmed against java):
  - **success** → two-step (recover-with-window-shrink, then settle at
    `lastTime==now`), matching java's pre-charge + `resetAccountUsage` net effect.
  - **REVERT/exception/OOE** → single-step `increase(oldUsage, billed, oldTime,
    now)` over the ORIGINAL state, because java discards the pre-charge on failure
    (VMActuator.java:234-250 never commits `rootRepository`) and skips
    `resetAccountUsage`. The two shapes differ by ±1 energy_usage / a few window
    units for some inputs (java-verified at delta=7155), so the gate is
    consensus-relevant. `contractSucceeded(result)` (== `ContractRet` SUCCESS)
    drives it at all three call sites.
  - `availableAccountEnergyForBill` recovers via the same scaled per-account path
    (`recoveredEnergyUsage`, read-only) so limit-time and settle-time recovery
    agree. Pre-Stake-2.0 behavior is byte-unchanged.

The "skip the pre-charge" model is justified because java's pre-charge +
`resetAccountUsage` net out to `(R, W_R)` on the success path (advisor-verified).

### Golden values (java-tron verified, corretto-17, 2026-05-20)

- V1 / V2 settle: `EnergyProcessorTest.testUseEnergyInWindowSizeV2` (CI-enforced)
  — usage 72368521; V1 window 300; V2 window 1224/1224919, optimized.
- Path B success (pre-charge→reset→settle, 7200-slot decay): usage 550000, window
  9163637 (V2), optimized.
- Success vs failure divergence (delta=7155): success 553124 / window 9193479;
  failure (single-step) 553125 / window 9193462 — confirmed by driving java
  EnergyProcessor through both sequences.

Pinned in `actuator/energy_window_divergence_test.go` and
`vm/energy_window_divergence_test.go`.

### Known remaining (out of scope; follow-ups)

- **Bandwidth** `net_window_size` has the identical structural gap (recovery uses
  the global window, never written). Same fix shape; deferred.
- **Delegation/undelegation** window reshuffle (TODO §1.5 / java
  `unDelegateIncrease`) still uses the global window.
- **Non-harden vs harden recovery formula:** go-tron's *legacy* (`recoverUsage`,
  pre-Stake-2.0, and the vm staking-query precompile `recoverStakingUsage`) uses
  the simple `usage*remaining/window` form, which diverges from java's scaled
  `increase` for non-harden inputs. The V2 settle/limit path now uses the scaled
  form (java-correct); the precompile + pre-fork paths still use the simple form.
  Pre-existing, non-harden-only; flagged separately.
- The rare `resetAccountUsage` suicide-area-merge branch (`mergedSize != currentSize`).

### Revert / suicide parity

Revert/exception/OOE is now handled by the success/failure gate (single-step on
failure), validated against java goldens. The rare `resetAccountUsage`
suicide-area-merge branch (`mergedSize != currentSize`) is still not modeled and
should get a cross-impl case before mainnet replay (M0″ Phase 2).

## To verify before/while fixing

- `AllowAccountStateRoot` activation on the chains in scope (immediate state-root
  fork vs latent feed-forward only).
- Whether the cross-impl stress harness currently exercises V2 contract callers
  (the 2026-05-18 dailyBuild green run skipped ~39 V2/freezeV2 tests — likely why
  this hasn't surfaced).
- The pre-charge / `resetAccountUsage` reconciliation must be ported faithfully so
  the window write-back doesn't perturb the (currently-correct) `energy_usage` value.
