# M11.5 Slice 2a — WitnessCreate setDefaultWitnessPermission Backfill

**Date:** 2026-04-30
**Status:** Active
**Milestone:** M11 — Witness statistics consensus-state gap closure
**Slice:** 5 / 2a (witness-permission backfill on `WitnessCreateActuator`)
**Predecessor:** [M11.5 slice 1](2026-04-30-m11-5-default-active-operations-design.md) (default Owner+Active on new-account paths)

---

## 1. Problem

Slice 1 wired `ApplyDefaultAccountPermissions` into the four NEW-account paths
(CreateAccount / Transfer / TransferAsset / ShieldedTransfer), gated by
`AllowMultiSign`. Slice 1 explicitly deferred the EXISTING-account upgrade
path: when an account is upgraded to a witness via `WitnessCreateActuator`,
java-tron rebuilds the permission set on that account (java-tron
`AccountCapsule.java:304-316`, called from `WitnessCreateActuator.java:137`).

Concrete effect: a witness-bound account on go-tron currently has no
`witness_permission` proto field, even when `AllowMultiSign` is on. That
field is the one signing block-producer signatures in java-tron's signing
pipeline (see `getWitnessPermissionAddress`). Without it, any future
multi-sig-aware signing path on go-tron disagrees with java-tron about
which key is authoritative for block production.

This slice closes the witness-creation half of the gap. The block-signing
side (M6b) consumes `witness_permission`, but is out of scope here; only the
DP-driven account-state install matters at this layer.

## 2. Java-tron Reference

### 2.1 `AccountCapsule.setDefaultWitnessPermission` (chainbase, line 304)

```java
public void setDefaultWitnessPermission(DynamicPropertiesStore dynamicPropertiesStore) {
  Builder builder = this.account.toBuilder();
  Permission witness = createDefaultWitnessPermission(this.getAddress());
  if (!this.account.hasOwnerPermission()) {
    Permission owner = createDefaultOwnerPermission(this.getAddress());
    builder.setOwnerPermission(owner);
  }
  if (this.account.getActivePermissionCount() == 0) {
    Permission active = createDefaultActivePermission(this.getAddress(), dynamicPropertiesStore);
    builder.addActivePermission(active);
  }
  this.account = builder.setWitnessPermission(witness).build();
}
```

Three things to notice:

1. **Witness is unconditional.** Always set/overwritten.
2. **Owner is conditional** on `!hasOwnerPermission()`. If the account
   previously called `AccountPermissionUpdate` and installed a custom owner
   (e.g., a 2-of-3 multisig), that custom owner is **preserved**.
3. **Active is conditional** on `getActivePermissionCount() == 0`. Same
   preservation logic for any custom active permissions.

This differs from slice 1's helper (`ApplyDefaultAccountPermissions` which
overwrites Owner+Active unconditionally), and that difference matters for
wire compatibility. We mirror java-tron exactly.

### 2.2 `createDefaultWitnessPermission` (chainbase, line 228)

```java
public static Permission createDefaultWitnessPermission(ByteString address) {
  Key.Builder key = Key.newBuilder();
  key.setAddress(address); key.setWeight(1);

  Permission.Builder p = Permission.newBuilder();
  p.setType(PermissionType.Witness)
   .setId(1)
   .setPermissionName("witness")
   .setThreshold(1)
   .setParentId(0)
   .addKeys(key);
  return p.build();
}
```

Field-by-field shape:
- `type`: `PermissionType.Witness` (= 1 in the proto enum).
- `id`: 1.
- `permission_name`: "witness".
- `threshold`: 1.
- `parent_id`: 0.
- `keys`: single `Key{address: addr, weight: 1}`.
- `operations`: NOT set (witness-permission has no operations bitmap;
  java-tron's `Permission` proto leaves it as the empty default).

### 2.3 Call site `WitnessCreateActuator.java:137`

```java
private void createWitness(final WitnessCreateContract witnessCreateContract) ... {
  // ... build WitnessCapsule, witnessStore.put, fetch accountCapsule ...
  accountCapsule.setIsWitness(true);
  if (dynamicStore.getAllowMultiSign() == 1) {
    accountCapsule.setDefaultWitnessPermission(dynamicStore);
  }
  accountStore.put(accountCapsule.createDbKey(), accountCapsule);
  // ... fee adjust ...
}
```

The gate is `AllowMultiSign == 1`. It runs AFTER `setIsWitness(true)` and
BEFORE the account is written back. In go-tron we run it after the existing
`SetIsWitness` / `PutWitness` calls (order-equivalent because the StateDB
mutation is journaled and committed atomically per actuator).

## 3. Design

### 3.1 Helper `core/types.MakeDefaultWitnessPermission` (additive)

```go
// MakeDefaultWitnessPermission builds the default Witness permission for addr:
// type=Witness, id=1, name="witness", threshold=1, parent_id=0, single key
// (addr, weight=1), no operations bitmap. Mirrors java-tron
// AccountCapsule.createDefaultWitnessPermission.
func MakeDefaultWitnessPermission(addr common.Address) *corepb.Permission
```

Symmetric to the slice-1 `MakeDefaultOwnerPermission` /
`MakeDefaultActivePermission` helpers, lives in the same file.

### 3.2 StateDB method `ApplyWitnessPermissions` (additive)

```go
// ApplyWitnessPermissions installs the witness permission on addr and
// back-fills default Owner / Active[0] if they are missing — mirrors
// java-tron AccountCapsule.setDefaultWitnessPermission. Caller is
// responsible for the AllowMultiSign gate. No-op if the account does not
// exist.
//
// Conditional semantics (java-tron parity):
//   - Witness is ALWAYS set/overwritten.
//   - Owner is only set if account.OwnerPermission() == nil.
//   - Active[0] is only appended if len(account.ActivePermission()) == 0.
//
// This preserves any custom Owner/Active permissions an account installed
// via AccountPermissionUpdate before becoming a witness.
func (s *StateDB) ApplyWitnessPermissions(addr tcommon.Address, dp *DynamicProperties)
```

Implementation: journal once (so revert restores the full prior account
proto), build the Witness permission, set it; then check Owner/Active
existence and conditionally fill.

A new method (rather than extending `ApplyDefaultAccountPermissions`) because
the semantics differ (preserve-existing vs overwrite) and the slice-1
helper's docstring promises overwrite for the "freshly-minted account" use
case; mixing semantics in one helper would either break that promise or
require a flag, both worse than two clearly-named helpers.

### 3.3 Actuator wire-up `actuator/witness.go`

After the existing `SetIsWitness(true)` call, gated by
`ctx.DynProps.AllowMultiSign()`:

```go
ctx.State.SetIsWitness(ownerAddr, true)
if ctx.DynProps.AllowMultiSign() {
    ctx.State.ApplyWitnessPermissions(ownerAddr, ctx.DynProps)
}
```

`ctx.DynProps.AllowMultiSign()` mirrors slice 1's gate idiom and matches
java-tron's `dynamicStore.getAllowMultiSign() == 1`.

### 3.4 Account proto bytes impact

Pre-fork (`AllowMultiSign == 0`): the witness-bound account proto is
unchanged from current go-tron behavior — only `is_witness=true` is set,
no permissions.

Post-fork (`AllowMultiSign == 1`):
- If the account had no permissions (typical first-time witness): account
  gains `owner_permission`, `active_permission[0]` (operations =
  `dp.ActiveDefaultOperations()`), and `witness_permission`.
- If the account already had a custom Owner: `owner_permission` is
  preserved; `witness_permission` is added.
- If the account already had custom Active permissions: those are
  preserved; `witness_permission` is added.
- In all post-fork cases: `witness_permission` is the default-shape one
  (single signer = the witness address).

This changes serialized proto bytes for upgraded-to-witness accounts. **We
do not claim byte-for-byte parity in this slice** — that depends on M0″
Phase 2 fixture replay against captured java-tron account protos. The
shape (field presence, sub-fields, semantics) matches java-tron source
reading; byte-level verification is pending.

## 4. Out of slice 2a

| Item | Reason | When |
|---|---|---|
| Block-producer signing reading `witness_permission` | M6b territory; signing pipeline is a separate concern | M6b |
| `AccountPermissionUpdateActuator` — what creates a custom Owner/Active in the first place | Already implemented (`actuator/account_permission.go`). Only its post-condition matters here, via the conditional preserve. | n/a |
| Retroactive backfill for existing witnesses created before this lands | Out of scope: requires sweeping every account marked `is_witness=true` and rewriting permissions on a snapshot boundary. Handled (or not) by M0″ migration. | Future slice or M0″ |
| `WitnessUpdate` (URL changes) | Doesn't touch permissions in java-tron either. | n/a |

## 5. Exit Gate

| Test | Assertion |
|------|-----------|
| `TestMakeDefaultWitnessPermission` | type=Witness, id=1, name="witness", threshold=1, parent_id=0, single key (addr, weight=1), no operations |
| `TestApplyWitnessPermissions_NoOpIfMissing` | Helper called on non-existent addr is no-op (no panic, no account created) |
| `TestApplyWitnessPermissions_PopulatesAllOnEmptyAccount` | After helper call on bare account: Owner=default, Active[0]=default with `dp.ActiveDefaultOperations()`, Witness=default |
| `TestApplyWitnessPermissions_PreservesCustomOwner` | Pre-existing custom Owner (e.g., 2-of-3) is preserved; Active[0] still installed (was empty); Witness=default |
| `TestApplyWitnessPermissions_PreservesCustomActives` | Pre-existing custom Active list is preserved (length, contents); Owner installed (was missing); Witness=default |
| `TestWitnessCreateExecute_PreFork_NoPermissionChange` | With `AllowMultiSign==false`, WitnessCreate leaves account permissions untouched (preserves current behavior; `is_witness=true`, witness object exists) |
| `TestWitnessCreateExecute_PostFork_InstallsDefaultPermissions` | With `AllowMultiSign==true` and a bare account, WitnessCreate installs Owner+Active[0]+Witness with the expected shapes; Active[0].Operations == `dp.ActiveDefaultOperations()` at execute-time; balance is reduced by fee, witness object exists |
| `TestWitnessCreateExecute_PostFork_PreservesCustomOwner` | With `AllowMultiSign==true` and an account with a pre-installed custom Owner, WitnessCreate adds Witness, preserves custom Owner, fills Active[0] |
| `make test` | All 28+ packages green |

## 6. Implementation Order

1. `MakeDefaultWitnessPermission` helper in `core/types/account.go` + test
   in `core/types/account_test.go`.
2. `(*StateDB).ApplyWitnessPermissions` in `core/state/statedb.go` + tests
   in `core/state/statedb_test.go` (no-op-if-missing,
   populates-all-on-empty-account, preserves-custom-owner,
   preserves-custom-active).
3. Wire into `actuator/witness.go` Execute (after `SetIsWitness(true)`,
   gated on `AllowMultiSign`).
4. Per-actuator tests in `actuator/witness_test.go`
   (pre-fork-no-change, post-fork-installs-defaults,
   post-fork-preserves-custom-owner).
5. `make test` green; commit.

## 7. Assumption

The shape of the new permissions matches java-tron source reading
(`AccountCapsule.createDefaultWitnessPermission` and the conditional
guards in `setDefaultWitnessPermission`). **Byte-for-byte parity is NOT
yet claimed**; that depends on M0″ Phase 2 fixture replay against captured
java-tron account protos.
