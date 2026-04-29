# M11.3 AVAILABLE_CONTRACT_TYPE + ACTIVE_DEFAULT_OPERATIONS — Design Spec

**Date:** 2026-04-29
**Status:** Active
**Milestone:** M11 — Witness statistics consensus-state gap closure
**Slice:** 3 of N (contract-type bitmap DPs + permission gating + proposal side effects)

---

## 1. Problem

`AVAILABLE_CONTRACT_TYPE` and `ACTIVE_DEFAULT_OPERATIONS` are two 32-byte
bitmap DPs in java-tron's `DynamicPropertiesStore` that gate which contract
types can be referenced from a custom permission's `operations` bitmap and
which contract types appear in the default active permission's operations
bitmap. Both are mutated by `addSystemContractAndSetPermission(int id)`,
which is called at six places in `ProposalService` (proposals 26, 30, 44, 70,
77).

go-tron currently:
- Has no bitmap DP for either.
- `AccountPermissionUpdateActuator.Validate` only checks
  `len(active.Operations) == 32` — it does NOT verify that bits set in
  `operations` correspond to actually-available contract types.
- Has no proposal side-effect that mutates these bitmaps.

Effect:
1. Permission validation gap: a malicious tx can construct an active
   permission referencing arbitrary "operations" bits including those that
   should never be allowed (e.g. system contracts that haven't been enabled
   yet). java-tron rejects; go-tron accepts.
2. Bitmap value divergence: even if go-tron starts with java-tron's defaults,
   without proposal side effects the value will not evolve identically.

## 2. Java-tron Reference

### Defaults (`DynamicPropertiesStore.init()`)

```java
String contractType = "7fff1fc0037e0000000000000000000000000000000000000000000000000000";
saveAvailableContractType(ByteArray.fromHexString(contractType));
saveActiveDefaultOperations(ByteArray.fromHexString(
    "7fff1fc0033e0000000000000000000000000000000000000000000000000000"));
```

Each is exactly 32 bytes. Bit `i` (within `byte[i/8] & (1<<(i%8))`) corresponds
to `ContractType` value `i`.

### Mutator (`DynamicPropertiesStore.addSystemContractAndSetPermission`)

```java
public void addSystemContractAndSetPermission(int id) {
  byte[] availableContractType = getAvailableContractType();
  availableContractType[id / 8] |= (1 << id % 8);
  saveAvailableContractType(availableContractType);

  byte[] activeDefaultOperations = getActiveDefaultOperations();
  activeDefaultOperations[id / 8] |= (1 << id % 8);
  saveActiveDefaultOperations(activeDefaultOperations);
}
```

Idempotent OR-in.

### Consumer (`AccountPermissionUpdateActuator.validate`)

```java
byte[] types1 = dynamicStore.getAvailableContractType();
for (int i = 0; i < 256; i++) {
  boolean b = (operations.byteAt(i / 8) & (1 << (i % 8))) != 0;
  boolean t = ((types1[(i / 8)] & 0xff) & (1 << (i % 8))) != 0;
  if (b && !t) {
    throw new ContractValidateException(i + " isn't a validate ContractType");
  }
}
```

### Proposal call sites (`framework/.../ProposalService.java`)

| Proposal | go-tron paramID | DP key | addSystemContractAndSetPermission call |
|---|---|---|---|
| ALLOW_TVM_CONSTANTINOPLE | 26 | allow_tvm_constantinople | (48) ClearABIContract |
| ALLOW_CHANGE_DELEGATION | 30 | change_delegation | (49) UpdateBrokerageContract |
| ALLOW_MARKET_TRANSACTION | 44 | allow_market_transaction | (52) MarketSellAsset, (53) MarketCancelOrder |
| UNFREEZE_DELAY_DAYS | 70 | unfreeze_delay_days | (54) FreezeBalanceV2, (55) UnfreezeBalanceV2, (56) WithdrawExpireUnfreeze, (57) DelegateResource, (58) UnDelegateResource |
| ALLOW_CANCEL_ALL_UNFREEZE_V2 | 77 | allow_cancel_all_unfreeze_v2 | (59) CancelAllUnfreezeV2 |

## 3. Design

### 3.1 DP layer (`core/state/dynamic_properties.go`)

Add to `defaultStringProps`:
```go
"available_contract_type":   string(decodeHex("7fff1fc0037e0000000000000000000000000000000000000000000000000000")),
"active_default_operations": string(decodeHex("7fff1fc0033e0000000000000000000000000000000000000000000000000000")),
```

(Implemented inline as `var ... = []byte{0x7f, 0xff, 0x1f, ...}` to avoid pulling in encoding/hex at package-init time.)

New methods:
```go
const ContractTypeBitmapBytes = 32

func (dp *DynamicProperties) AvailableContractType() []byte
func (dp *DynamicProperties) SetAvailableContractType(v []byte)        // panic on len mismatch
func (dp *DynamicProperties) ActiveDefaultOperations() []byte
func (dp *DynamicProperties) SetActiveDefaultOperations(v []byte)      // panic on len mismatch
func (dp *DynamicProperties) IsContractTypeAvailable(id int) bool
func (dp *DynamicProperties) AddSystemContractAndSetPermission(id int) // OR-in to both
```

### 3.2 Validation gate (`actuator/account_permission.go`)

In `Validate`, after the existing `len(active.Operations) != 32` check, add
the per-bit check. Reject the tx if any bit set in `operations` does not have
a corresponding bit set in `available_contract_type`.

```go
for _, active := range c.Actives {
    // ... existing checks ...
    if len(active.Operations) == 32 {
        avail := ctx.DynProps.AvailableContractType()
        for i := 0; i < 256; i++ {
            opBit := (active.Operations[i/8] & (1 << (i % 8))) != 0
            availBit := (avail[i/8] & (1 << (i % 8))) != 0
            if opBit && !availBit {
                return fmt.Errorf("%d isn't a validate ContractType", i)
            }
        }
    }
}
```

### 3.3 Proposal side effects (`core/proposal.go`)

Extend `applyProposalSideEffects` switch with the 5 new cases (one
proposal handler may invoke `AddSystemContractAndSetPermission` multiple
times). Use the same numeric paramID convention as existing cases.

```go
case 26: // ALLOW_TVM_CONSTANTINOPLE
    if value != 0 { dynProps.AddSystemContractAndSetPermission(48) }
case 30: // ALLOW_CHANGE_DELEGATION
    if value != 0 { dynProps.AddSystemContractAndSetPermission(49) }
case 44: // ALLOW_MARKET_TRANSACTION
    if value != 0 {
        dynProps.AddSystemContractAndSetPermission(52)
        dynProps.AddSystemContractAndSetPermission(53)
    }
case 70: // UNFREEZE_DELAY_DAYS
    if value != 0 {
        dynProps.AddSystemContractAndSetPermission(54)
        dynProps.AddSystemContractAndSetPermission(55)
        dynProps.AddSystemContractAndSetPermission(56)
        dynProps.AddSystemContractAndSetPermission(57)
        dynProps.AddSystemContractAndSetPermission(58)
    }
case 77: // ALLOW_CANCEL_ALL_UNFREEZE_V2
    if value != 0 { dynProps.AddSystemContractAndSetPermission(59) }
```

The `value != 0` guard mirrors java-tron's `if (... == 0) {... save+addSystemContract; }` pattern for the proposals where the side effect is one-shot on first activation. (For ALLOW_TVM_CONSTANTINOPLE and ALLOW_CHANGE_DELEGATION java-tron does it unconditionally; we use `value != 0` as the closer-match — these proposals only flip 0→1 in practice and never go back.)

## 4. What is NOT in this slice

- **Default active permission for newly-created accounts** — java-tron's
  `AccountCapsule` constructor sets up a default active permission with
  `getActiveDefaultOperations()` as its operations bitmap. go-tron's
  `StateDB.CreateAccount` doesn't set up any permissions. Wiring this is a
  behavioral change with broader test surface (every account-creation test
  would need updating). Defer to a separate slice / milestone.

  Even without the wire-up, this slice still maintains
  `active_default_operations` correctly (proposal mutations apply), so when
  the wire-up happens the value is already in sync.

- **proposal == 0 deactivation paths** — java-tron's existing handlers don't
  un-set bits if a proposal is reversed; we mirror that.

## 5. Exit Gate

| Test | Assertion |
|------|-----------|
| `TestAvailableContractType_Default` | First 4 bytes match java-tron 7f ff 1f c0 |
| `TestActiveDefaultOperations_Default` | First 4 bytes match java-tron 7f ff 1f c0 |
| `TestIsContractTypeAvailable_DefaultSet` | bits 0..6 are available; bit 7 is not (7-th bit of 0x7f is 0) |
| `TestAddSystemContractAndSetPermission_Idempotent` | Adding 48 twice = adding once |
| `TestAddSystemContractAndSetPermission_BothBitmaps` | Adding 49 sets bit in both available_contract_type and active_default_operations |
| `TestAccountPermissionUpdate_RejectsUnavailableType` | Permission with bit set for an unavailable type returns ContractValidateError |
| `TestAccountPermissionUpdate_AcceptsAvailableType` | Permission with bits set only for available types passes |
| `TestApplyProposalSideEffects_Constantinople` | Approve proposal 26 → bit 48 set in both bitmaps |
| `TestApplyProposalSideEffects_UnfreezeDelay` | Approve proposal 70 → bits 54-58 set in both bitmaps |
| `make test` | Full suite green |

## 6. Implementation Order

1. DP layer — add defaults, getters/setters, IsContractTypeAvailable, AddSystemContractAndSetPermission.
2. Validation gate in AccountPermissionUpdate.
3. Proposal side-effect cases (5 cases, ~15 lines).
4. Tests + `make test`.
