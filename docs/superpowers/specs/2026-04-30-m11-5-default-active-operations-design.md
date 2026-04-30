# M11.5 Default Active Permission Operations on New Account — Design Spec

**Date:** 2026-04-30
**Status:** Active
**Milestone:** M11 — Witness statistics consensus-state gap closure
**Slice:** 5 of N (default active permission `operations` loaded from DP at account creation)

---

## 1. Problem

M11.3 (commit `677129b`) introduced the 32-byte `active_default_operations`
bitmap DP and the proposal side effects that mutate it. The bitmap value is
maintained correctly, but no go-tron code reads it when a NEW account is
created. As a result every newly-created account on go-tron has an empty
`ActivePermission[0].operations` (and a missing `OwnerPermission`), which
diverges from java-tron's behavior whenever `AllowMultiSign` is active.

Concrete failure mode: after `AccountPermissionUpdate` becomes available
(also gated on `AllowMultiSign`), java-tron-style accounts can sign for the
contract types whose bits are set in `active_default_operations`, but
go-tron accounts cannot — their default `operations` is empty so every
`active.Operations` bit-check fails until the user issues an explicit
`AccountPermissionUpdate` to populate the bitmap.

This slice closes that gap for accounts created AFTER the fork. Existing
on-disk accounts are NOT touched (retroactive migration is a separate slice).

## 2. Java-tron Reference

### 2.1 Default permission constructors (`AccountCapsule.java`)

```java
// AccountCapsule.java:99-122 (AccountCreateContract path)
public AccountCapsule(final AccountCreateContract contract, long createTime,
    boolean withDefaultPermission, DynamicPropertiesStore dynamicPropertiesStore) {
  if (withDefaultPermission) {
    Permission owner = createDefaultOwnerPermission(contract.getAccountAddress());
    Permission active = createDefaultActivePermission(contract.getAccountAddress(),
        dynamicPropertiesStore);

    this.account = Account.newBuilder()
        .setType(contract.getType())
        .setAddress(contract.getAccountAddress())
        .setTypeValue(contract.getTypeValue())
        .setCreateTime(createTime)
        .setOwnerPermission(owner)
        .addActivePermission(active)
        .build();
  } else { /* no permissions, no createTime field set differently */ }
}

// AccountCapsule.java:158-180 (Transfer / TransferAsset / ShieldedTransfer path)
public AccountCapsule(ByteString address, AccountType accountType, long createTime,
    boolean withDefaultPermission, DynamicPropertiesStore dynamicPropertiesStore) {
  if (withDefaultPermission) {
    Permission owner = createDefaultOwnerPermission(address);
    Permission active = createDefaultActivePermission(address, dynamicPropertiesStore);
    this.account = Account.newBuilder()
        .setType(accountType).setAddress(address).setCreateTime(createTime)
        .setOwnerPermission(owner).addActivePermission(active).build();
  } else { /* fallback: type+addr+createTime only */ }
}

// AccountCapsule.java:194-208 — Owner default
public static Permission createDefaultOwnerPermission(ByteString address) {
  Key.Builder key = Key.newBuilder();
  key.setAddress(address); key.setWeight(1);
  Permission.Builder owner = Permission.newBuilder();
  owner.setType(PermissionType.Owner).setId(0).setPermissionName("owner")
       .setThreshold(1).setParentId(0).addKeys(key);
  return owner.build();
}

// AccountCapsule.java:210-226 — Active default (loads ACTIVE_DEFAULT_OPERATIONS)
public static Permission createDefaultActivePermission(ByteString address,
    DynamicPropertiesStore dynamicPropertiesStore) {
  Key.Builder key = Key.newBuilder();
  key.setAddress(address); key.setWeight(1);
  Permission.Builder active = Permission.newBuilder();
  active.setType(PermissionType.Active).setId(2).setPermissionName("active")
        .setThreshold(1).setParentId(0)
        .setOperations(ByteString.copyFrom(dynamicPropertiesStore.getActiveDefaultOperations()))
        .addKeys(key);
  return active.build();
}
```

### 2.2 The 4 actuator call sites (`withDefaultPermission = AllowMultiSign == 1`)

| Java file:line | Path | Construct |
|---|---|---|
| `actuator/.../CreateAccountActuator.java:42-45` | new account from `AccountCreateContract` | `new AccountCapsule(accountCreateContract, latestBlockHeaderTimestamp, withDefaultPermission, dynamicStore)` |
| `actuator/.../TransferActuator.java:51-54` | TRX transfer to non-existent addr | `new AccountCapsule(addr, Normal, latestBlockHeaderTimestamp, withDefaultPermission, dynamicStore)` |
| `actuator/.../TransferAssetActuator.java:64-67` | TRC10 transfer to non-existent addr | same as Transfer |
| `actuator/.../ShieldedTransferActuator.java:140-143` | shielded → transparent receiver | same as Transfer |

In every case the gate is identical:
```java
boolean withDefaultPermission = dynamicStore.getAllowMultiSign() == 1;
```

### 2.3 Out-of-slice java-tron sites

- `RepositoryImpl.java:1104` — `createNormalAccount(byte[])` is invoked when
  the VM transfers to a non-existent address (CALL with value, MUtil.transfer,
  selfdestruct beneficiary). go-tron's equivalent goes through
  `vm/tvm.go:165 AddBalance → state.GetOrCreateAccount` and currently does
  not decorate. The task scope explicitly excludes `vm/`.
- `WitnessCreateActuator.java:137` — `setDefaultWitnessPermission` is called
  on an EXISTING account that's being upgraded to witness. It also back-fills
  Owner+Active if missing. This is an account-MUTATION path, not a new-account
  path; deferred.

## 3. Design

### 3.1 Helper (`core/types/account.go`, additive)

```go
// MakeDefaultOwnerPermission builds the default owner permission for addr:
// type=Owner, id=0, name="owner", threshold=1, single key (addr, weight=1),
// no operations bitmap. Mirrors java-tron AccountCapsule.createDefaultOwnerPermission.
func MakeDefaultOwnerPermission(addr common.Address) *corepb.Permission

// MakeDefaultActivePermission builds the default active permission for addr,
// loading the operations bitmap from `activeDefaultOps` (which the caller
// passes from DynamicProperties.ActiveDefaultOperations()). Mirrors java-tron
// AccountCapsule.createDefaultActivePermission. Threshold=1, single key (addr, weight=1).
// activeDefaultOps must be exactly 32 bytes (caller's contract); otherwise the
// returned permission's operations is set to a copy of the slice as-is — DP
// invariant elsewhere ensures the length.
func MakeDefaultActivePermission(addr common.Address, activeDefaultOps []byte) *corepb.Permission
```

These live next to existing `Permission` accessors in `core/types/account.go`
to keep the proto-construction code beside the proto-reading code.

### 3.2 StateDB wire-up (`core/state/statedb.go`, additive)

Do NOT change `CreateAccount`'s signature — `core/conformance/seed.go` and
`snapshot.go` call it for fixture-replay (which doesn't need defaults). Add a
new helper:

```go
// ApplyDefaultAccountPermissions installs default Owner + Active[0] permissions
// on addr, reading the active operations bitmap from dp. Mirrors java-tron's
// `withDefaultPermission=true` constructor behavior. Caller is responsible for
// the AllowMultiSign gate. No-op if the account does not exist.
func (s *StateDB) ApplyDefaultAccountPermissions(addr tcommon.Address, dp *DynamicProperties)
```

The helper journals the prior permissions so revert works, then writes the
two permissions via the existing `SetPermissions` plumbing.

### 3.3 Actuator call sites (4 actuators, pattern identical)

After `ctx.State.CreateAccount(addr, ...)`:

```go
if ctx.DynProps.AllowMultiSign() {
    ctx.State.ApplyDefaultAccountPermissions(addr, ctx.DynProps)
}
```

The gate uses `ctx.DynProps.AllowMultiSign()` directly to mirror java-tron's
`dynamicStore.getAllowMultiSign() == 1` literal — same idiom already present
in `actuator/fees.go:56`. (Note: `forks.IsActive(forks.AllowMultiSign, ...)`
ends up reading the same DP bit, so the two are functionally equivalent. We
prefer the direct DP read at all 4 sites for one-line audit-grep parity.)

The 4 sites:
- `actuator/account.go:46` — after `CreateAccount(newAddr, ac.Type)`
- `actuator/transfer.go:66` — after `CreateAccount(toAddr, Normal)`
- `actuator/transfer_asset.go:73` — after `CreateAccount(to, Normal)`
- `actuator/shielded_transfer.go:132` — after `CreateAccount(to, Normal)`

### 3.4 Default bitmap source: read at account-creation time

`active_default_operations` is mutated by governance proposals (M11.3,
proposals 26→48, 30→49, 44→52,53, 70→54-58, 77→59) at maintenance-cycle
boundaries. By reading the DP value at account-creation time (not e.g.
snapshotting it at fork activation), every account picks up the bitmap as it
existed when the block containing the create-tx was processed. Accounts
created before a proposal flips do NOT retroactively gain the new bit;
accounts created after do.

### 3.5 Account proto bytes impact

Pre-fork (or with `AllowMultiSign==0`): the account proto for newly-created
addresses is identical to current go-tron behavior — no Owner/Active
permission populated, no operations bitmap.

Post-fork (`AllowMultiSign==1`): the account proto for newly-created
addresses now contains:
- `owner_permission`: type=Owner, id=0, name="owner", threshold=1,
  parent_id=0, keys=[(addr, weight=1)], no operations.
- `active_permission[0]`: type=Active, id=2, name="active", threshold=1,
  parent_id=0, keys=[(addr, weight=1)], operations = current
  `dp.ActiveDefaultOperations()` (32 bytes).

This changes the serialized proto bytes for those accounts. **We do not
claim byte-for-byte parity with java-tron in this slice**: that requires the
M0″ Phase-2 fixture replay to verify against a captured java-tron account
proto. The shape (field presence, sub-fields, semantics) matches
java-tron's source reading; byte-level verification is pending.

## 4. Out of slice 1

| Item | Reason | When |
|---|---|---|
| VM internal transfer creating a normal account (`vm/tvm.go` Call/CallToken with value to non-existent addr → `RepositoryImpl.createNormalAccount` equivalent) | Task scope excludes `vm/`; needs separate review of VM call paths | Future M11.x slice |
| `WitnessCreateActuator` back-filling Owner/Active on existing account being made witness (`AccountCapsule.setDefaultWitnessPermission`) | Different semantic (mutate-existing, not create-new); witness-permission also added | Future slice |
| Retroactive migration of existing accounts to populate operations | Out-of-scope per task spec; would also require migrating across snapshot heights | Separate slice, requires conformance work |
| `Account.create_time` field on new accounts | Java-tron sets `latestBlockHeaderTimestamp`; go-tron's `StateDB.CreateAccount` doesn't set it. Independent parity bug; touching it expands proto-bytes diff | Separate slice |
| `proposal == 0` deactivation un-setting the operations default | Java-tron doesn't clear bits on proposal reversal either | N/A |

## 5. Exit Gate

| Test | Assertion |
|------|-----------|
| `TestMakeDefaultOwnerPermission` | type=Owner, id=0, name="owner", threshold=1, single key with weight=1 and addr, no operations |
| `TestMakeDefaultActivePermission` | type=Active, id=2, name="active", threshold=1, single key (addr, weight=1), operations==32-byte copy of input |
| `TestApplyDefaultAccountPermissions_PopulatesBoth` | After helper call, account has expected Owner+Active[0] permissions and operations matches dp |
| `TestApplyDefaultAccountPermissions_NoOpIfMissing` | Helper called on non-existent addr is no-op (no panic, no journal entry that breaks revert) |
| `TestTransferExecute_PreFork_NoDefaultPermissions` | With `dp.AllowMultiSign==false`, transfer creating recipient leaves it with empty Owner & Active permissions (preserves current behavior) |
| `TestTransferExecute_PostFork_LoadsDefaultPermissions` | With `dp.AllowMultiSign==true`, transfer creating recipient yields recipient with the expected Owner shape AND Active[0].Operations equals `dp.ActiveDefaultOperations()` byte-for-byte |
| `TestCreateAccountExecute_PostFork_LoadsDefaultPermissions` | Same assertion via `AccountCreateContract` path |
| `TestTransferAssetExecute_PostFork_LoadsDefaultPermissions` | Same assertion via TRC10 transfer-create path |
| `TestShieldedTransferExecute_PostFork_LoadsDefaultPermissions` | Same assertion via shielded → transparent path |
| `TestDefaultPermissions_ProposalFlipAffectsOnlyLaterAccounts` | Create acct A, flip a bit in `active_default_operations` via DP mutate, create acct B; A's operations is the pre-flip bitmap, B's is post-flip |
| `make test` | All 28+ packages green |

## 6. Implementation Order

1. Helpers in `core/types/account.go` (MakeDefaultOwnerPermission, MakeDefaultActivePermission) + tests in `core/types/account_test.go`.
2. `(*StateDB).ApplyDefaultAccountPermissions` in `core/state/statedb.go` + test in `core/state/statedb_test.go`.
3. Wire into 4 actuators (transfer, transfer_asset, account, shielded_transfer).
4. Per-actuator gate + post-fork tests in the matching `*_test.go`.
5. Cross-cutting "proposal flip affects only later accounts" test in actuator/transfer_test.go (transfer is the simplest carrier).
6. `make test` green; commit.
