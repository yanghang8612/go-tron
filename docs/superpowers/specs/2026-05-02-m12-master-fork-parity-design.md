# M12 master fork-version-gated parity — design

**Status:** active.
**Plan:** [2026-05-02-m12-master-fork-parity.md](../plans/2026-05-02-m12-master-fork-parity.md).

## Problem

A 2026-05-02 audit of go-tron against `java-tron` master HEAD
`GreatVoyage-v4.8.1-1-g55781c0283` surfaced two block-version-fork-gated
behaviors that go-tron does not implement. Both are **P0 active**:
go-tron's `params.BlockVersion = 35` (VERSION_4_8_2), so a single-SR or
high-rate dev / private chain crosses the version-rate threshold for
v33 (VERSION_4_8_0_1, 70%) and v34 (VERSION_4_8_1, 80%) immediately,
and any chain (cross-impl mainnet, Nile, the just-shipped private-chain
sync) will diverge from java-tron the moment a relevant transaction is
included.

## Out of scope

- TIP-7823 / Osaka proposal #78 — currently `develop`-only, defer until
  released to master.
- JSON-RPC `blockTimestamp` on logs/receipts — develop-only.
- The two **P1** master gaps from the same audit (JSON-RPC hardening,
  ABI semantic validator) — they are separately scoped as M12.3 and
  M12.4 and not part of this slice. PLAN.md lists them.

## Slice 1 — `ExchangeTransactionContract` rejection

### Reference behavior (java-tron master, PR #6507, commit 45e3bf88ca)

Two hooks in `framework/.../db/Manager.java`:

1. `pushTransaction` (mempool inbound, **unconditional**):
   ```java
   if (isExchangeTransaction(trx.getInstance())) {
     throw new ContractValidateException("ExchangeTransactionContract is rejected");
   }
   ```
2. `processBlock` (per-tx loop, **fork-gated**):
   ```java
   for (TransactionCapsule t : block.getTransactions()) {
     rejectExchangeTransaction(t.getInstance());        // throws iff fork active
     ...
   }
   ```
   where `rejectExchangeTransaction` throws iff
   `forkController.pass(VERSION_4_8_0_1)`.

The asymmetry is intentional — pre-fork blocks may legitimately contain
exchange txs and must replay successfully; new mempool submissions are
rejected outright once the patch ships, regardless of fork state.

### go-tron mapping

| java-tron site | go-tron site | Behavior |
|---|---|---|
| `Manager.pushTransaction` | `core/txpool.(*TxPool).Add` | unconditional reject |
| `Manager.processBlock` per-tx | `core/state_processor.ApplyTransaction` (top of function, before `act.Validate`) | reject iff `forks.PassVersion(db, 33, blockTime, maintenanceMs)` |

`ApplyTransaction` is the right hook for the block path because it runs
in both `ProcessBlock` (replay) and `BuildBlock` (production), matching
java-tron's coverage where exchange txs must be excluded from
production AND rejected on receipt.

### Helper to add

```go
// core/forks/version_pass.go
func PassVersion(db ethdb.KeyValueReader, version int32, latestBlockTime, maintenanceIntervalMs int64) bool
```

Stateless duplicate of `ForkController.Pass`'s read path. The actuator
context carries `ctx.DB` (a `BufferedKVStore` which is also a
`ethdb.KeyValueReader`) and `ctx.DynProps.MaintenanceTimeInterval()`,
so this signature is callable from every layer that already holds a
Context. ForkController.Pass / passLocked stay (used by ForkController
clients that already hold the controller).

### Tests

- `txpool.TestAdd_RejectsExchangeTransaction`: build an
  `ExchangeTransactionContract` tx, expect `Add` to return error.
  Locks down behavior independent of fork state.
- `state_processor.TestApplyTransaction_ExchangeRejectedAfterFork`:
  apply an exchange tx with v33 stats >= 70% upgrade votes → returns
  error. With insufficient votes → succeeds (replay-safety).
- One end-to-end block test in `core/blockchain_test.go` (or wherever
  block-level exchange tests already live): block containing exchange
  tx with v33 active → `applyBlock` fails.

## Slice 2 — AssetIssue `FrozenSupply` expire-time overflow gate

### Reference behavior (java-tron master, v4.8.1 release `44a4bc8263`)

In `AssetIssueActuator.validate()`'s `for (FrozenSupply next : ...)`
loop, after the existing per-supply checks:

```java
if (chainBaseManager.getForkController().pass(VERSION_4_8_1)) {
  long frozenPeriod = next.getFrozenDays() * FROZEN_PERIOD;
  try {
    StrictMathWrapper.addExact(assetIssueContract.getStartTime(), frozenPeriod);
  } catch (ArithmeticException e) {
    throw new ContractValidateException(
        "Start time and frozen days would cause expire time overflow");
  }
}
```

Notes:
- `frozenDays * FROZEN_PERIOD` itself uses Java's silent-overflow `long *
  long` (no `StrictMath.multiplyExact`). Mirror that — do not improve
  it. Parity over correctness.
- `Math.addExact` throws on signed overflow only. Mirror with an
  explicit overflow check on the addition.
- `FROZEN_PERIOD = 86_400_000`; go-tron has it at `params.FrozenPeriod`.

### go-tron mapping

`actuator/asset_issue.go::(*AssetIssueActuator).Validate`, after the
existing
```go
frozenTotal += f.FrozenAmount
```
line, add:
```go
if forks.PassVersion(ctx.DB, 34, ctx.BlockTime, ctx.DynProps.MaintenanceTimeInterval()) {
  frozenPeriod := f.FrozenDays * params.FrozenPeriod  // silent overflow, mirror Java
  sum := c.StartTime + frozenPeriod
  if (frozenPeriod > 0 && sum < c.StartTime) ||
     (frozenPeriod < 0 && sum > c.StartTime) {
    return errors.New("Start time and frozen days would cause expire time overflow")
  }
}
```

### Tests

- `asset_issue_test.TestValidate_FrozenSupplyOverflow_GatedOff`: with v34
  vote stats absent / below threshold, an extreme `(StartTime,
  FrozenDays)` combo passes validation (pre-fork replay safety).
- `asset_issue_test.TestValidate_FrozenSupplyOverflow_PostFork`: with v34
  vote stats at quorum, the same combo returns the
  "expire time overflow" error.
- `asset_issue_test.TestValidate_FrozenSupplyOverflow_BoundaryCases`:
  values just at `MaxInt64`, `0+MaxInt64`, `MaxInt64-1+1`, etc. Cross-
  reference java-tron's
  `framework/.../actuator/AssetIssueActuatorTest.java` overflow tests.

## Acceptance

- `make test` green.
- `forks.PassVersion` symmetry verified via existing
  `core/forks/controller_test.go` vectors (run them through the new
  helper as a parallel assertion).
- `scripts/system_test.sh` 2-node dev chain still passes (no
  regression on non-exchange / non-asset-issue paths).
- PLAN.md progress table records both slices with commit hashes.

## Risk

Low. Both gaps are gated on `forks.PassVersion` reading a per-version
bitmap that ForkController already maintains; the helper duplicates the
existing read-path logic verbatim. The mempool-side exchange reject is
unconditional but only blocks a contract type that's been deprecated
on java-tron mainnet for >4 months.
