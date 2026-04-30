# M11.5 Default Active Permission Operations on New Account — Plan

**Spec:** [2026-04-30-m11-5-default-active-operations-design.md](../specs/2026-04-30-m11-5-default-active-operations-design.md)

- [ ] **Helpers** (`core/types/account.go`)
    - [ ] `MakeDefaultOwnerPermission(addr) *corepb.Permission` — type=Owner, id=0, name="owner", threshold=1, single key (addr, weight=1), no operations.
    - [ ] `MakeDefaultActivePermission(addr, activeDefaultOps []byte) *corepb.Permission` — type=Active, id=2, name="active", threshold=1, single key (addr, weight=1), operations is a defensive copy of `activeDefaultOps`.
    - [ ] Unit tests in `core/types/account_test.go`.

- [ ] **StateDB wire-up** (`core/state/statedb.go`)
    - [ ] `(*StateDB).ApplyDefaultAccountPermissions(addr, dp *DynamicProperties)` — journals prior permissions, sets Owner + Active[0]. No-op if account does not exist.
    - [ ] Unit test in `core/state/statedb_test.go` covering both populates-both and no-op-on-missing.

- [ ] **Actuator wire-up** (4 sites)
    - [ ] `actuator/account.go` — gate + helper after `CreateAccount(newAddr, ac.Type)`.
    - [ ] `actuator/transfer.go` — same after the toAddr CreateAccount.
    - [ ] `actuator/transfer_asset.go` — same after the to CreateAccount.
    - [ ] `actuator/shielded_transfer.go` — same after the transparent-receiver CreateAccount.

- [ ] **Tests in `actuator/`**
    - [ ] `TestTransferExecute_PreFork_NoDefaultPermissions` (gate negative).
    - [ ] `TestTransferExecute_PostFork_LoadsDefaultPermissions`.
    - [ ] `TestCreateAccountExecute_PostFork_LoadsDefaultPermissions`.
    - [ ] `TestTransferAssetExecute_PostFork_LoadsDefaultPermissions`.
    - [ ] `TestShieldedTransferExecute_PostFork_LoadsDefaultPermissions`.
    - [ ] `TestDefaultPermissions_ProposalFlipAffectsOnlyLaterAccounts` — flip a bit in `active_default_operations` between two transfer-creates; assert different bitmaps.

- [ ] **Verification**
    - [ ] `make test` green across all 28+ packages.
    - [ ] Commit (1-2 commits, GPG-signed): `feat(state,actuator): default active permission ops from DP (M11.5)`.

## Out of slice 1 (deferred)

- VM internal transfer creating normal accounts (`vm/tvm.go` CALL-with-value to non-existent addr). Java-tron decorates via `RepositoryImpl.createNormalAccount`; go-tron currently goes through `state.GetOrCreateAccount` without decoration. Task scope excludes `vm/`. Track in next M11.x slice.
- `WitnessCreateActuator` back-filling Owner/Active on existing account upgraded to witness (`AccountCapsule.setDefaultWitnessPermission`, java-tron `WitnessCreateActuator.java:137`). Different semantic; deferred.
- `Account.create_time` parity (java-tron passes `latestBlockHeaderTimestamp`; go-tron's `StateDB.CreateAccount` does not set it). Separate parity bug, separate slice.
- Retroactive migration of existing accounts. Out of scope per task spec.

## Assumption

The shape of the new permissions matches java-tron source reading
(`AccountCapsule.createDefaultOwnerPermission` / `createDefaultActivePermission`).
**Byte-for-byte parity is NOT yet claimed**; that depends on M0″ Phase 2
fixture replay against captured java-tron account protos.
