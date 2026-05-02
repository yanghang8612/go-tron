# M12 master fork-version-gated parity — plan

**Spec:** [2026-05-02-m12-master-fork-parity-design.md](../specs/2026-05-02-m12-master-fork-parity-design.md).

## Slice 1 — `ExchangeTransactionContract` rejection

### 1.1 `forks.PassVersion` stateless helper

- [ ] In a new file `core/forks/version_pass.go`, add
      `PassVersion(db ethdb.KeyValueReader, version int32, latestBlockTime,
      maintenanceIntervalMs int64) bool`. Behavior identical to
      `ForkController.Pass` minus the mutex (uses `rawdb.ReadForkStats`
      directly).
- [ ] Refactor: extract a private `passFromStats(stats []byte, vp
      VersionParam, latestBlockTime, maintenanceIntervalMs int64) bool`
      and have `ForkController.Pass`, `ForkController.passLocked`, and
      `PassVersion` all call it. Removes existing duplication.
- [ ] Test `core/forks/version_pass_test.go`: cover the same cases as
      `controller_test.go`'s `Pass*` tests (time gate, rate gate, at
      threshold, unknown version, empty stats, legacy-version
      strict-all-slots) directly through `PassVersion`.

### 1.2 Mempool reject (txpool.Add)

- [ ] In `core/txpool/pool.go::(*TxPool).Add`, after the `tx.Contract()
      == nil` early-return, reject if
      `tx.ContractType() == corepb.Transaction_Contract_ExchangeTransactionContract`
      with `ErrExchangeRejected`.
- [ ] Add `var ErrExchangeRejected = errors.New("ExchangeTransactionContract is rejected")`
      next to the other `Err*` declarations. Match java-tron's error
      string ("ExchangeTransactionContract is rejected") so any
      ecosystem code grepping the wire is consistent.
- [ ] Test `core/txpool/pool_test.go::TestAdd_RejectsExchangeTransaction`:
      build a tx with `ExchangeTransactionContract`, expect
      `ErrExchangeRejected`.

### 1.3 Block-apply reject (state_processor.ApplyTransaction)

- [ ] In `core/state_processor.go::ApplyTransaction`, at the top of the
      function (after `actuator.CreateActuator` returns its actuator),
      if `tx.ContractType() == ...ExchangeTransactionContract` and
      `forks.PassVersion(db, 33, blockTime, dynProps.MaintenanceTimeInterval())`,
      return a wrapped error
      `fmt.Errorf("ExchangeTransactionContract is rejected")`.
- [ ] Use the **same** error string as the mempool path so log-grep
      consumers don't have to handle two variants.
- [ ] No fork gate for legacy versions <= Version4_0 — exchange
      contract was added long after; `PassVersion(..., 33, ...)` returns
      false until the bitmap quorum is met, which is the correct
      replay-safety behavior.

### 1.4 Tests

- [ ] `core/state_processor_test.go::TestApplyTransaction_ExchangeRejectedAfterFork`:
      seed `rawdb.WriteForkStats(db, 33, [...]byte{0x01,0x01,0x01})`
      (3-witness, all upgrade-voted) so `PassVersion` returns true →
      ApplyTransaction rejects.
- [ ] `core/state_processor_test.go::TestApplyTransaction_ExchangePassesPreFork`:
      empty bitmap → `PassVersion` false → ApplyTransaction proceeds
      (the actuator can fail later for unrelated reasons, which is
      fine; the test only asserts the early reject doesn't fire).
- [ ] `core/blockchain_test.go::TestInsertBlock_RejectsExchangeAfterFork`:
      end-to-end block insert with a single exchange tx + active v33
      bitmap → `applyBlock` returns error.

### 1.5 Compile + test

- [ ] All packages compile.
- [ ] `make test` green.
- [ ] `scripts/system_test.sh` still PASS (no exchange txs in the dev
      flow, so the mempool reject doesn't fire).

### 1.6 Commit

- [ ] Subject: `fix(core,txpool): reject ExchangeTransactionContract per master VERSION_4_8_0_1`.
- [ ] Body: 2-3 lines. Reference java-tron `45e3bf88ca` and the spec.
- [ ] GPG-signed (`E3673E008F6D506E`).

## Slice 2 — AssetIssue `FrozenSupply` expire-time overflow

### 2.1 Validate hook

- [ ] In `actuator/asset_issue.go::(*AssetIssueActuator).Validate`,
      inside the `for _, f := range c.FrozenSupply` loop, after the
      `frozenTotal += f.FrozenAmount` line, add the v34 fork-gated
      overflow check exactly as specified in the design doc.
- [ ] Imports: `github.com/tronprotocol/go-tron/core/forks`,
      `github.com/tronprotocol/go-tron/params` (already imported elsewhere
      in the package; check `actuator/asset_issue.go` first).

### 2.2 Tests

- [ ] `actuator/asset_issue_test.go::TestValidate_FrozenSupplyOverflow_GatedOff`:
      empty / sub-quorum v34 stats; `c.StartTime = math.MaxInt64 - 1`
      and `f.FrozenDays = 365` → overflow on addition; gate off →
      Validate succeeds (boundary-test only the early-fork branch).
- [ ] `actuator/asset_issue_test.go::TestValidate_FrozenSupplyOverflow_PostFork`:
      seed v34 stats at quorum; same overflow combo → Validate returns
      "expire time overflow" error.
- [ ] `actuator/asset_issue_test.go::TestValidate_FrozenSupplyOverflow_NoOverflow`:
      gate active, normal `(StartTime=now, FrozenDays=30)` → Validate
      succeeds.
- [ ] Boundary suite: `MaxInt64 - frozenPeriod`, `MaxInt64`,
      `MaxInt64 - 1`. Mirror java-tron `AssetIssueActuatorTest`'s
      overflow vectors where they exist.

### 2.3 Compile + test

- [ ] `make test` green.
- [ ] No regression in existing `actuator/asset_issue_test.go` tests.

### 2.4 Commit

- [ ] Subject: `fix(actuator): AssetIssue FrozenSupply expire-time overflow gate per master VERSION_4_8_1`.
- [ ] Body: 2-3 lines. Reference java-tron `44a4bc8263` and the spec.
- [ ] GPG-signed (`E3673E008F6D506E`).

## Slice 3 — PLAN.md progress table

- [ ] Add two rows to PLAN.md:
  - `M12.1 ExchangeTransactionContract VERSION_4_8_0_1 reject | 完成 | 2026-05-02 | <hash> | mempool 无条件 + applyBlock fork-gated；mirror java-tron 45e3bf88ca`
  - `M12.2 AssetIssue FrozenSupply VERSION_4_8_1 overflow gate | 完成 | 2026-05-02 | <hash> | actuator/asset_issue.go::Validate；mirror java-tron 44a4bc8263 (v4.8.1)`
- [ ] Mention M12.3 (JSON-RPC hardening) and M12.4 (AbiValidator) under
      "PLAN.md 已知未关项" — they are P1, separately scoped.

## Out of scope

- TIP-7823 / Osaka (`develop`-only, defer).
- JSON-RPC `blockTimestamp` (`develop`-only).
- M12.3 and M12.4 (P1, separate slices).
- Refactoring `ForkController.Update` to feed the rate gate from a
  cached witness count — already correct; not relevant here.
