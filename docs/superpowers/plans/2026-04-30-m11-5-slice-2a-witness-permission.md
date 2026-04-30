# M11.5 Slice 2a — WitnessCreate setDefaultWitnessPermission Backfill — Plan

**Spec:** [2026-04-30-m11-5-slice-2a-witness-permission-design.md](../specs/2026-04-30-m11-5-slice-2a-witness-permission-design.md)

- [ ] **Helper** (`core/types/account.go`)
    - [ ] `MakeDefaultWitnessPermission(addr) *corepb.Permission` — type=Witness, id=1, name="witness", threshold=1, parent_id=0, single key (addr, weight=1), no operations.
    - [ ] Test `TestMakeDefaultWitnessPermission` in `core/types/account_test.go`.

- [ ] **StateDB method** (`core/state/statedb.go`)
    - [ ] `(*StateDB).ApplyWitnessPermissions(addr, dp)` — journal once, always set Witness, conditionally set Owner if nil, conditionally append Active[0] if list empty. No-op on missing account.
    - [ ] Tests in `core/state/statedb_test.go`:
        - [ ] `TestApplyWitnessPermissions_NoOpIfMissing`
        - [ ] `TestApplyWitnessPermissions_PopulatesAllOnEmptyAccount`
        - [ ] `TestApplyWitnessPermissions_PreservesCustomOwner`
        - [ ] `TestApplyWitnessPermissions_PreservesCustomActives`

- [ ] **Actuator wire-up** (`actuator/witness.go`)
    - [ ] After `ctx.State.SetIsWitness(ownerAddr, true)`, gate on `ctx.DynProps.AllowMultiSign()` and call `ApplyWitnessPermissions`.

- [ ] **Tests in `actuator/witness_test.go`**
    - [ ] `TestWitnessCreateExecute_PreFork_NoPermissionChange` — `AllowMultiSign==false`: account.OwnerPermission()==nil, ActivePermission empty, WitnessPermission()==nil after Execute.
    - [ ] `TestWitnessCreateExecute_PostFork_InstallsDefaultPermissions` — `AllowMultiSign==true`: Owner default, Active[0] default with `dp.ActiveDefaultOperations()`, Witness default; balance reduced by fee; witness object exists.
    - [ ] `TestWitnessCreateExecute_PostFork_PreservesCustomOwner` — pre-installed custom Owner (e.g., 2-of-3 multisig with mixed key weights) is preserved; Active[0] populated (was empty); Witness default.

- [ ] **Verification**
    - [ ] `make test` green across all 28+ packages.
    - [ ] 1 commit, GPG-signed (key `E3673E008F6D506E`): `feat(state,actuator,types): witness permission setup on WitnessCreate (M11.5 slice 2a)`.

## Out of slice 2a (deferred)

- M6b block-producer signing path consuming `witness_permission`. Out of scope here.
- Retroactive migration of pre-existing witnesses. Requires snapshot-boundary sweep; defer to M0″ or a dedicated slice.
- `WitnessUpdate` permissions (java-tron doesn't touch them either).

## Assumption

Permission shapes match java-tron source reading
(`AccountCapsule.createDefaultWitnessPermission` and the conditional
guards in `setDefaultWitnessPermission`). **Byte-for-byte parity is NOT
yet claimed**; that depends on M0″ Phase 2 fixture replay.
