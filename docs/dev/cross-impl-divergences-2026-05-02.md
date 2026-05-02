# Cross-impl divergences — 2026-05-02

Findings from `make system-test-cross-flows` against the live java-tron
private chain at `/Users/asuka/Works/Tests/TVM/run/config.conf` (single-SR
Zion @ `TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC`, networkId=0, chain_id=9999).
Both nodes at `totalProduced=79956` when baseline assertions ran.

Source: `scripts/system_test_cross_flows.sh` commit `c884699`. See
`docs/dev/p2p-interop-status.md` for the underlying interop record.

## Repro

```bash
# java-tron must already be running with the config.conf genesis
# (see docs/dev/java-tron-local.md Option 1, with the private-chain
# adjustments documented in p2p-interop-status.md).
JAVA_TRON_HTTP=127.0.0.1:8090 make system-test-cross-flows
```

The script syncs gtron from genesis, then runs 7 P0 contract-type flows
and asserts post-tx state byte-equal on both sides.

## D-1 SR balance — **P0 consensus**

| node | balance (sun) |
|---|---|
| gtron | 98_999_998_972_389_000 |
| java-tron | 98_999_998_955_474_000 |
| Δ | **16,915,000 (gtron HIGHER)** |

Observed at baseline (pre-flow), so the divergence is accumulated during
historical replay of blocks 0..79956 — not from any test tx. gtron is
under-debiting or over-crediting on some path.

The chain is single-SR with mostly-empty blocks; only a small number of
blocks contain test txs from prior interop sessions. The 16.915M /
100,000 = 169.15 ratio doesn't cleanly match a per-tx 100k fee (no clean
integer factor). Most likely cause: a per-block or per-maintenance
small-amount accumulation that gtron skips.

Hooks to investigate:
- `core/state_processor.go::ApplyTransaction` — bandwidth fee, multisign
  fee, memo fee. Compare against java-tron `Manager.processTransaction`.
- `actuator/fees.go` — bandwidth/burn helpers.
- `core/reward.go::payBlockReward` — does brokerage split deduct from SR
  balance, or only from a non-balance reward pool?
- java-tron: `framework/.../db/Manager.java::processTransaction`,
  `MortgageService`, `BandwidthProcessor.consume*`.

Recommended probe: query both nodes' tx-info on a representative
non-empty block (e.g. earliest block with a Transfer); compare the
SR-side fee delta byte-for-byte.

## D-2 SR allowance — **P0 consensus**

| node | allowance (sun) |
|---|---|
| gtron | 6_820_992_000_000 |
| java-tron | 767_577_600_000 |
| ratio | **8.886× (gtron HIGHER)** |

Maintenance cycles elapsed: 79956 × 3s ≈ 66.6h ≈ 11 cycles
(`maintenance_time_interval = 21_600_000ms`). java-tron credit per
maintenance ≈ 7e10 sun; gtron ≈ 6.2e11.

The chain runs without `change_delegation` proposal active (default).
java-tron's no-change-delegation path uses a different formula from
the post-`change_delegation` brokerage split.

Hooks:
- `core/reward.go` — `payBlockReward`, `payStandbyWitness`,
  `applyRewardMaintenance`. Verify which fires when
  `change_delegation` is OFF.
- java-tron: `consensus/dpos/MaintenanceManager.payReward`,
  `MortgageService.payBlockReward`, `MortgageService.payStandbyWitness`.
- Witness pay constant: `dynProps.WitnessPayPerBlock()` — default mainnet
  16e6 sun/block; verify on this chain.

8.886× is suggestive of compound (e.g., paying both witness brokerage
AND standby pool to same allowance), or of paying per-block instead of
per-maintenance with wrong magnitude.

## D-3 proposal_id off-by-one — **P0 consensus**

First proposal created on each chain:

| node | proposal_id |
|---|---|
| gtron | 0 |
| java-tron | 1 |

java-tron `ProposalCreateActuator.execute` (master):
```java
long id = dynamicStore.getLatestProposalNum() + 1;
dynamicStore.saveLatestProposalNum(id);
proposal.setID(id);
```

go-tron `actuator/proposal_create.go` likely post-increments or starts
the counter at 0 instead of using the +1 offset. Trivial 1-line fix
once located. Watch for whether the issue is in the increment OR in
the default value of the `latest_proposal_num` DP key.

## Wire-format `listproposals.parameters` — **P1 SDK compat**

Same proposal, same chain:

| node | output |
|---|---|
| gtron | `"parameters": {"19": 259200000}` |
| java-tron | `"parameters": [{"key":19,"value":259200000}]` |

This is OUTPUT-side; M9.4 fixed INPUT parsing for the same array form on
`/wallet/proposalcreate`. The fix lives in
`internal/tronapi/`, wherever Proposal proto serialization happens.

Affected endpoints (likely all four):
- HTTP `/wallet/listproposals`
- HTTP `/wallet/getproposalbyid`
- gRPC `wallet.Wallet/ListProposals`
- gRPC `wallet.Wallet/GetProposalById`

## Status (2026-05-02 — closed)

| # | status | fix commit |
|---|---|---|
| D-1 SR balance | **closed** — energy fee path landed in two slices: (1) `952a3b3` introduced `actuator/PayEnergyBill` mirroring `ReceiptCapsule.payEnergyBill`, closing 99.985% of the 16,915,000-sun gap; (2) `de4cb47` fixed the residual by removing gtron's spurious `EnergyVeryLow=3` base on MLOAD/MSTORE/MSTORE8/CODECOPY/CALLDATACOPY/RETURNDATACOPY (java-tron charges memDelta+copy only, with a `SPECIAL_TIER=1` only after proposal #65). Final verification at H=87740: SR balance byte-equal at 98,999,998,950,874,000 sun. | `952a3b3` + `de4cb47` |
| D-2 SR allowance | **closed** — fixture missed the `committee` block from java-tron's config.conf, so gtron started with `change_delegation=0` while java-tron had it on. Adding all committee flags to the fixture made per-block allowance accumulator byte-equal. Allowance verified at 842,304,000,000 sun on both nodes at H=87740. | `52a78ad` |
| D-2.b extra maintenance fires | **closed (false positive)** — regression test in `9bf4a7f` proves trigger fires exactly once per boundary. Original "37 fires" was an attribution error: under CD=OFF on a 1-SR chain, allowance also accrues from `payBlockReward` (per-block), inflating the inferred fire count. Trigger code itself matches java-tron `MaintenanceManager.doMaintenance` byte-for-byte. | `9bf4a7f` (test only) |
| D-3 proposal_id off-by-one | **closed** — `next_proposal_id` DP key default was 0; java-tron's pre-increment from latest=0 yields 1 for first id. Bumped default to 1. Verified `latest_proposal_id` byte-equal on both nodes across multiple re-tests. | `42c597f` |
| listproposals.parameters wire format | **closed** — switched HTTP-side `ProposalInfo.Parameters` from `map[string]int64` to `[]ProposalParameterEntry` (sorted by key). gRPC unaffected (returns `corepb.Proposal` directly). | `7b202d4` |
| D-4 V1 freeze gate vs V2-open chains | **closed** — script's `flow_freeze` defaulted to V1 on chains where `unfreeze_delay_days > 0` makes V1 closed (`FreezeBalanceActuator` rejects with "freeze v2 is open, old freeze is closed"). Probe now consults both `getUnfreezeDelayDays` and `getAllowNewResourceModel`; V2 BANDWIDTH is the right path on this chain (V2 open, ALLOW_NEW_RM=0). | `4557886` |
| D-5 frozenV2 list wire format | **closed** — gtron's `/wallet/getaccount` returned only the actually-frozen entries; java-tron's `Wallet.sortFrozenV2List` always emits one placeholder entry per ResourceCode (BANDWIDTH/ENERGY/TRON_POWER) in enum order with 0-amount stubs. `wireSortFrozenV2` clones the proto and rewrites the list to match. State on disk is unchanged. | `4557886` |
| D-6 V2 freeze missing total weight update | **closed** — `FreezeBalanceV2Actuator.Execute` and `UnfreezeBalanceV2Actuator.Execute` never called `addTotal*Weight`. Result: gtron's `availableAccountNet` returned 0 even after a V2 BANDWIDTH freeze, so subsequent txs from the staker fell to free-net or to `consumeFeeForCreateNewAccount`, silently debiting 100k sun per create-new-account that java covered with stake (200k drift accumulated after two cross-impl runs at H≈89k). Fix mirrors java's `(newFrozenWithDelegated/TRX - oldFrozenWithDelegated/TRX)` formula in both actuators; new tests cover BANDWIDTH/ENERGY/TRON_POWER and the integer-TRX boundary case. Cross-impl `system-test-cross-flows` now byte-equal across all 21 assertions. | `540a467` |

## Closed: D-1.b 2,400-sun balance residual

Root cause: gtron's `vm/instructions.go` charged `EnergyVeryLow=3` as a
base tier on MLOAD/MSTORE/MSTORE8/CODECOPY/CALLDATACOPY/RETURNDATACOPY;
java-tron charges only memDelta + copy on these ops, with a
`SPECIAL_TIER=1` added to MLOAD/MSTORE/MSTORE8 only after proposal #65
(`allow_higher_limit_for_max_cpu_time_of_one_tx`). Probe walked all 87,032
blocks, found 6 historical TVM txs (all CreateSmartContract from Zion),
each with init code containing MSTORE+CODECOPY → +6 energy/tx × 4 larger
txs × 100 sun/energy = exactly 2,400 sun over-charge.

Fix: `de4cb47`. Removed the base from the 6 ops; added `SPECIAL_TIER=1`
behind proposal #65; new test `vm/memory_ops_energy_test.go`.

H1 (origin-stake split) was correctly diagnosed as a real follow-up but
NOT the source of the residual on this chain — all 6 historical TVM
txs have caller==origin. Implementing it remains a real future task
for chains that exercise TRC-20-style triggers; flagged below.

## Genuinely open follow-ups (not exercised on this chain)

These are real cross-impl divergences for code paths the cross-impl
chain doesn't currently exercise. Will resurface on richer chains.

### ~~`consume_user_resource_percent` origin-stake split (caller ≠ origin)~~ — closed `8748568`

Implemented the 3-arg overload in `splitOriginCallerUsage` /
`billCallerSide` / `PayEnergyBill`. Origin absorbs `percent%` of
EnergyUsageTotal capped by min(stake-left, origin_energy_limit);
caller covers the remainder. Modern `getOriginUsage` formula only
(allowTvmFreeze / supportUnfreezeDelay branch) — pre-4.0 historical
replay would need a fork gate. 5 new unit tests cover happy path,
cap-by-limit, cap-by-stake, percent=0, caller==origin.

Cross-impl baseline (H=88062) still byte-equal after the change —
the chain's 6 historical TVM txs are all CreateSmartContract with
caller==origin so the split path is unexercised here; the fix lands
without disturbing chain replay.

### EXTCODECOPY pre-existing under-charge

D-1.b agent flagged: gtron charges `EnergyCopy*words + memDelta`; java
charges `EXT_CODE_COPY=20 + memDelta + 3*words`. Not exercised by the
cross-impl chain.

### Dynamic-energy penalty on memory ops

D-1.b agent flagged: gtron's interpreter applies the dynamic-energy
factor only to `operation.energyCost` (static field). Memory ops have
static cost 0 and charge dynamically inside the op function, so the
factor never multiplies the memory portion. java applies the factor to
the whole `op.getEnergyCost(program)` return. Pre-existing divergence;
not exercised because no high-usage contract on this chain.

### Producer-side `payBlockReward` double-write

D-2.b agent flagged: `core/block_builder.go:87,100` calls
`payBlockReward(bc.db, ...)` and `applyRewardMaintenance(bc.db, ...)`
directly on `bc.db`; the subsequent `InsertBlock → applyBlock` re-runs
the same writes through `bc.buffer`. When `change_delegation=1` and the
local node is producing, `cycleReward[N][witness]` is written twice
per locally-produced block. Does NOT affect the cross-impl follower
test (gtron is sync-only there) and does NOT affect M0″ Phase 2
conformance replay (no BuildBlock invocation). Affects local witness
production only. File separately.

### V1 freeze with empty receiver_address

Cross-impl test Flow 4 fails: java-tron rejects V1 FreezeBalance with
empty `receiver_address` as "receiver account does not exist" when
`allow_delegate_resource=1`. Likely a script bug (need to omit or
explicitly set receiver=owner) rather than a cross-impl divergence.

### (Old D-1.b briefing follows for reference; root cause now identified above)
## Follow-up D-1.b — 2,400-sun balance residual

Symptom (re-test at H=85582):
- gtron:    `98_999_998_952_071_600` sun
- java-tron: `98_999_998_952_074_000` sun
- Δ = **2,400 sun** (gtron HIGHER), stable across new flows.

Hypotheses, ranked:
1. **`consume_user_resource_percent` origin-stake split** — agent's note
   on `actuator/energy_bill.go` says: only the caller-pays branch is
   wired; the three-arg `ReceiptCapsule.payEnergyBill(caller, origin,
   percent)` overload (java-tron `ReceiptCapsule.java:201-239`) is
   unimplemented. If this private chain ever ran a TRC-20-style call
   (caller != origin contract owner), the origin's stake share leaked.
2. **OUT_OF_TIME / REVERT branch difference** — java-tron routes the
   bill differently when `result == OUT_OF_TIME`; gtron may not
   distinguish. Same file `ReceiptCapsule.java:283-308`.
3. **`energy_fee` proposal-update timing** — java-tron updates the
   per-energy SUN price when proposal #28 (or its successor) activates
   mid-chain; gtron may update at a different boundary.

Probes (cheap, do these first):
- List historical TVM txs on the chain: walk java-tron's HTTP
  `gettransactioninfobyblocknum` for blocks 1..head (or the few
  non-empty ones), filter `receipt.energy_fee > 0`, count and sum
  `(caller, origin, energy_fee, result)` tuples. If sum ≈ 2,400 sun,
  the residual is fully energy-fee-attributed; if not, look elsewhere.
- Diff gtron vs java-tron on `getTransactionInfoById` for each TVM tx.
  First mismatch on `receipt.energy_fee`, `receipt.result`, or
  `receipt.origin_energy_usage` reveals the leaking branch.

Files:
- gtron: `actuator/energy_bill.go`, `actuator/vm_actuator.go`
- java-tron: `chainbase/src/main/java/org/tron/core/capsule/ReceiptCapsule.java` (master)
- Cross-check: `framework/src/main/java/org/tron/core/db/TransactionTrace.java::pay`

## Follow-up D-2.b — extra `distributeLegacyStandby` fires

Original symptom (under CD=OFF, before fixture fix):
- Expected per chain age (66.6h, 11 cycles × 21,600,000 ms): 11 fires
- Observed gtron allowance accumulation: 37 fires
- Excess: 26 × 115,200,000,000 sun (`witness_standby_allowance`)

Now masked: CD=ON suppresses `distributeLegacyStandby` so allowance
no longer leaks. But the underlying maintenance-trigger bug may affect
other cycle-bound state — VI accumulation (rawdb prefix `rvi-`),
brokerage cycle snapshots, `total_*` cycle counters. Needs inspection
before declaring cross-impl byte-equal across all reads.

Probe ideas:
- After a fresh run, scan gtron's rawdb under `rvi-` prefix; count
  cycle slots with non-zero accumulators. Compare to expected 11.
- Read gtron `core/blockchain.go::applyBlock` — find the maintenance
  trigger (`if NextMaintenanceTime() > 0 && blockTime >= ...`). Step
  through one cycle: does it fire exactly once, or does the trigger
  re-fire because `next_maintenance_time` isn't advanced atomically?
- Read `consensus/dpos/maintenance.go::DoMaintenance` — confirm it
  calls `calcNextMaintenanceTime` exactly once per fire and persists
  the new value before returning.
- java-tron reference: `framework/.../db/Manager.java::processBlock`
  near "maintenanceManager.applyBlock"; `MaintenanceManager.java::doMaintenance`.

Files:
- gtron: `core/blockchain.go`, `consensus/dpos/maintenance.go`,
  `cmd/gtron/genesis_file.go::makeGenesis` (next_maintenance_time init)
- java-tron: `framework/.../consensus/dpos/MaintenanceManager.java`

## Other open items (out of scope for these follow-ups)

- **V1 freeze cross-impl test failures**: with allowDelegateResource=1
  active on this chain, V1 freeze with empty receiver_address fails on
  java-tron with "receiver account does not exist". Likely a test
  script issue (need to omit or set receiver explicitly) rather than a
  cross-impl divergence.

## Re-run command

```bash
make gtron && JAVA_TRON_HTTP=127.0.0.1:8090 make system-test-cross-flows
```
