# Plan: port per-account energy window to go-tron settle/limit path

Closes the confirmed divergence in `docs/dev/energy-window-divergence-2026-05-20.md`.

## Goal

Make go-tron's energy recovery read AND write the per-account window
(`AccountResource.energy_window_size` / `energy_window_optimized`), byte-matching
java-tron, gated on `SupportUnfreezeDelay()`.

## Scope

- **In:** ENERGY recovery at limit-time (`availableAccountEnergyForBill`, read-only)
  and settle-time (`useEnergyForBill`, read+write).
- **Out (separate follow-ups):** bandwidth `net_window_size`; delegation/undelegation
  window (TODO §1.5); the rare suicide-area-merge branch of `resetAccountUsage`.

## Live surface (confirmed)

All in `actuator/energy_bill.go`. Core `RecoverEnergy` / `availableAccountEnergy`
have no live callers. The vm precompile (`stakingWindowSizeSlots` /
`recoverStakingUsage`) already reads the per-account window correctly.

## java reference (exact)

- gate OFF (`!supportUnfreezeDelay`): `useEnergy` uses the STATIC `increase`
  (global window, no per-account read/write). **= go-tron's current code.** Keep.
- gate ON: `useEnergy` →
  - recovery: `recovery(account,…)` = static increase with `oldWindowSize = getWindowSize(ENERGY)` (read-only).
  - settle: `increase(account, ENERGY, energyUsage, energy, latestConsumeTime, now)`
    (account-aware; writes window via `setNewWindowSize` / `setNewWindowSizeV2`).
- `increase` (V1, ResourceProcessor.java:86-131) vs `increaseV2`
  (133-188, when `supportAllowCancelAllUnfreezeV2`).
- Window units: V1 stores slots; V2 stores `slots * WINDOW_SIZE_PRECISION` and sets
  `energy_window_optimized = true`. `getWindowSize` returns the V1 (slots) view;
  default when unset = `WINDOW_SIZE_MS / BLOCK_PRODUCED_INTERVAL = 28800`.

## go-tron model: two-step, skip the pre-charge

java's pre-charge (VMActuator) + `resetAccountUsage` net out to: the account ends at
`updateUsage`'s `(R, W_R)` then the settle's `increase(R, energy, now, now)`. So
go-tron's `useEnergyForBill` reproduces the COMMITTED state without modeling the
pre-charge:

```
old = (GetEnergyUsage, GetLatestConsumeTimeForEnergy); now = ResourceTime()
if !SupportUnfreezeDelay():           # current behavior, unchanged
    SetEnergyUsage(recoverGlobal(old.usage, old.time, now) + usage); SetLatestConsumeTimeForEnergy(now); return
# gate ON:
R   = increaseEnergyWindow(acct, old.usage, 0, old.time, now)   # recovery: decay + shrink window -> W_R (mutates acct window)
fin = increaseEnergyWindow(acct, R, usage, now, now)            # settle: lastTime==now, blend window
SetEnergyUsage(fin); SetLatestConsumeTimeForEnergy(now)
```

`availableAccountEnergyForBill` (limit-time, read-only): recover with the
per-account window instead of the global one (mirrors `getAccountLeftEnergyFromFreeze`
→ `recovery`). Same formula, per-account window fed in.

## Files

1. `params/protocol_params.go` — add `WindowSizePrecision = 1000`.
2. `core/types/account.go` — window accessors mirroring AccountCapsule:
   `EnergyWindowSize()` (V1 slots view, default 28800), `EnergyWindowSizeV2()`
   (scaled view), `SetNewEnergyWindowSize(slots)`, `SetNewEnergyWindowSizeV2(scaled)`
   (sets optimized=true), `EnergyWindowOptimized()`.
3. `actuator/energy_bill.go` — port `increaseEnergyWindow` (V1 + V2, harden + non),
   `getNewEnergyWindowSize`; rewrite `useEnergyForBill` (two-step, gated) and
   `availableAccountEnergyForBill` recovery (per-account window).

## TDD

1. Golden values for `(energy_usage, energy_window_size, optimized)` after 1 and 2
   contract charges, V2 + V1 regimes, harden on/off — **java-verified if feasible**,
   else hand-derived from the formula and flagged for pre-merge java check.
2. RED: rewrite the two characterization tests to assert java-matching behavior;
   add window-write + 2-call feed-forward tests. Watch fail.
3. GREEN: implement. 4. Verify full `actuator` + `vm` packages stay green.

## Open questions for advisor

- Worth running java-tron (gradle) for golden window values, or hand-derive?
- Two-step (skip pre-charge) equivalence — any gate combo where it breaks?
