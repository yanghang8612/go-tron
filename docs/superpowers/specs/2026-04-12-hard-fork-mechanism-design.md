# Hard Fork Mechanism — Design Spec

**Date:** 2026-04-12
**Status:** ⚠️ **SUPERSEDED — do not consult for planning**

This spec was the original single-shot plan for the fork mechanism.
The work was re-scoped and delivered as three independent milestones:

- DP backfill → `2026-04-15-m1-1-dp-backfill-design.md` + `…plan` (M1.1)
- Freeze V1 legacy support → `2026-04-15-m1-2-freeze-v1-design.md` + `…plan` (M1.2)
- Version-bit voting + audit → `2026-04-15-m1-3-version-bits-fork-audit-design.md` + `…plan` (M1.3)

This file contains several references to AllowFlags that do not exist
in java-tron (`AllowTvmBigInteger` proposal-ID 78, `AllowTvmSolidity058`,
`AllowTvmShieldedToken` / `AllowStakingV2` as standalone proposals).
Those were either removed (phantom flags) or aliased to their real
java-tron counterparts during M1.3 — see `docs/dev/fork-gates.md` for
the current authoritative table.

**Approach:** Option C — Hybrid proposal flags + block-height fallback

---

## 1. Goals

Implement java-tron's hard fork mechanism in go-tron:
- Add 24 `AllowXxx` feature flags to `DynamicProperties` (proposal-based activation)
- Create a thin `core/forks` package with block-height fallbacks for genesis-time flags
- Gate all affected actuators on the relevant flag in `Validate()`
- Make the VM fork-aware via a `TVMConfig` struct computed per-transaction
- Wire proposal approval → flag activation in `ProposalApproveActuator`

---

## 2. New Files

| File | Purpose |
|---|---|
| `core/forks/forks.go` | `AllowFlag` enum, `Config` struct with block heights, `IsActive()` function |
| `vm/tvm_config.go` | `TVMConfig` struct + `From()` constructor |

---

## 3. Modified Files

| File | Change |
|---|---|
| `core/state/dynamic_properties.go` | Add 24 `AllowXxx()` getter/setter pairs |
| `actuator/proposal_approve.go` | Add `applyProposal()` mapping proposal IDs → DynProps keys |
| `actuator/asset_issue.go` | Gate on `AllowSameTokenName` |
| `actuator/participate_asset_issue.go` | Gate on `AllowSameTokenName` |
| `actuator/transfer_asset.go` | Gate on `AllowSameTokenName` |
| `actuator/account_permission.go` | Gate on `AllowMultiSign` |
| `actuator/delegate_resource.go` | Gate on `AllowDelegateResource` |
| `actuator/undelegate_resource.go` | Gate on `AllowDelegateResource` |
| `actuator/freeze_v2.go` | Gate on `AllowStakingV2` |
| `actuator/unfreeze_v2.go` | Gate on `AllowStakingV2` |
| `actuator/withdraw_expire_unfreeze.go` | Gate on `AllowStakingV2` |
| `actuator/cancel_unfreeze.go` | Gate on `AllowStakingV2` |
| `actuator/market_sell_asset.go` | Gate on `AllowMarketTransaction` |
| `actuator/market_cancel_order.go` | Gate on `AllowMarketTransaction` |
| `actuator/update_brokerage.go` | Gate on `AllowChangeDelegation` |
| `actuator/vm_actuator.go` | Build `TVMConfig`, pass to VM |
| `vm/jump_table.go` | Add `enabledFn` per opcode; check `TVMConfig` in lookup |
| `vm/interpreter.go` | Accept `TVMConfig`; validate opcode via `enabledFn` |
| `vm/evm.go` | Accept `TVMConfig`; pass to interpreter |

---

## 4. `AllowXxx` Flags — Full Table

Each flag corresponds to a java-tron proposal ID and a `DynamicProperties` string key.

| Go Constant | DynProps Key | java-tron Proposal ID | Affects |
|---|---|---|---|
| `AllowSameTokenName` | `allow_same_token_name` | 3 | AssetIssue, ParticipateAssetIssue, TransferAsset |
| `AllowDelegateResource` | `allow_delegate_resource` | 4 | DelegateResource, UnDelegateResource |
| `AllowAdaptiveTotalEnergyLimit` | `allow_adaptive_energy_limit` | 6 | VM energy cap |
| `AllowMultiSign` | `allow_multi_sign` | 1 | AccountPermissionUpdate |
| `AllowChangeDelegation` | `allow_change_delegation` | 16 | UpdateBrokerage |
| `AllowTvmTransferTrc10` | `allow_tvm_transfer_trc10` | 15 | VM: TRC10 transfers |
| `AllowTvmConstantinople` | `allow_tvm_constantinople` | 30 | VM: CREATE2, EXTCODEHASH, SHL/SHR/SAR |
| `AllowTvmSolidity059` | `allow_tvm_solidity059` | 32 | VM: compiler compat fixes |
| `AllowTvmIstanbul` | `allow_tvm_istanbul` | 41 | VM: CHAINID, SELFBALANCE |
| `AllowMarketTransaction` | `allow_market_transaction` | 45 | MarketSellAsset, MarketCancelOrder |
| `AllowTvmFreeze` | `allow_tvm_freeze` | 33 | VM: FREEZE/UNFREEZE/VOTEWITNESS/WITHDRAWREWARD precompiles |
| `AllowTvmShieldedToken` | `allow_tvm_shielded_token` | 35 | VM: shielded token precompiles |
| `AllowTvmVote` | `allow_tvm_vote` | 57 | VM: VOTE opcode |
| `AllowAccountHistory` | `allow_account_history` | 52 | (query only, no actuator change) |
| `AllowPbft` | `allow_pbft` | 40 | (consensus, no actuator change) |
| `AllowStakingV2` | `allow_staking_v2` | 74 | FreezeV2, UnfreezeV2, DelegateResource v2, UnDelegateResource v2, CancelAllUnfreezeV2, WithdrawExpireUnfreeze |
| `AllowTvmLondon` | `allow_tvm_london` | 65 | VM: BASEFEE |
| `AllowTvmCompatibility` | `allow_tvm_compatibility` | 48 | VM: backwards compat |
| `AllowDynamicEnergy` | `allow_dynamic_energy` | 70 | VM: dynamic energy pricing |
| `AllowTvmBigInteger` | `allow_tvm_big_integer` | 78 | VM: big-integer precompile |
| `AllowTvmBlob` | `allow_tvm_blob` | 83 | VM: blob opcodes; Nile-only EIP-4844 precompile at 0x02000a |
| `AllowNewResourceModel` | `allow_new_resource_model` | 18 | (already exists) |
| `AllowEnergyAdjustment` | `allow_energy_adjustment` | 66 | VM energy fee adjustment |
| `AllowTvmCancun` | `allow_tvm_cancun` | 82 | VM: Cancun-era opcodes (TLOAD, TSTORE, MCOPY) |

---

## 5. `core/forks/forks.go`

```go
package forks

import "github.com/tronprotocol/go-tron/core/state"

type AllowFlag int

const (
    AllowSameTokenName AllowFlag = iota
    AllowDelegateResource
    AllowMultiSign
    AllowChangeDelegation
    AllowTvmTransferTrc10
    AllowTvmConstantinople
    AllowTvmSolidity059
    AllowTvmIstanbul
    AllowMarketTransaction
    AllowTvmFreeze
    AllowTvmShieldedToken
    AllowTvmVote
    AllowStakingV2
    AllowTvmLondon
    AllowTvmCompatibility
    AllowDynamicEnergy
    AllowAdaptiveTotalEnergyLimit
    AllowNewResourceModel
    AllowEnergyAdjustment
    AllowTvmBigInteger
    AllowTvmBlob
    AllowTvmCancun
    AllowPbft
    AllowAccountHistory
)

// dynKey maps each flag to its DynamicProperties key.
var dynKey = map[AllowFlag]string{
    AllowSameTokenName:            "allow_same_token_name",
    AllowDelegateResource:         "allow_delegate_resource",
    AllowMultiSign:                "allow_multi_sign",
    AllowChangeDelegation:         "allow_change_delegation",
    AllowTvmTransferTrc10:         "allow_tvm_transfer_trc10",
    AllowTvmConstantinople:        "allow_tvm_constantinople",
    AllowTvmSolidity059:           "allow_tvm_solidity059",
    AllowTvmIstanbul:              "allow_tvm_istanbul",
    AllowMarketTransaction:        "allow_market_transaction",
    AllowTvmFreeze:                "allow_tvm_freeze",
    AllowTvmShieldedToken:         "allow_tvm_shielded_token",
    AllowTvmVote:                  "allow_tvm_vote",
    AllowStakingV2:                "allow_staking_v2",
    AllowTvmLondon:                "allow_tvm_london",
    AllowTvmCompatibility:         "allow_tvm_compatibility",
    AllowDynamicEnergy:            "allow_dynamic_energy",
    AllowAdaptiveTotalEnergyLimit: "allow_adaptive_energy_limit",
    AllowNewResourceModel:         "allow_new_resource_model",
    AllowEnergyAdjustment:         "allow_energy_adjustment",
    AllowTvmBigInteger:            "allow_tvm_big_integer",
    AllowTvmBlob:                  "allow_tvm_blob",
    AllowTvmCancun:                "allow_tvm_cancun",
    AllowPbft:                     "allow_pbft",
    AllowAccountHistory:           "allow_account_history",
}

// IsActive returns true if the flag is activated either by DynProps
// or (for genesis-always-on flags) by block number.
func IsActive(flag AllowFlag, blockNum uint64, dp *state.DynamicProperties) bool {
    if dp != nil {
        if dp.GetInt64(dynKey[flag]) != 0 {
            return true
        }
    }
    return false
}
```

---

## 6. `TVMConfig`

Computed once in `VMActuator.Execute()` before calling the VM:

```go
type TVMConfig struct {
    TransferTrc10     bool  // allow_tvm_transfer_trc10
    Constantinople    bool  // allow_tvm_constantinople
    Solidity059       bool  // allow_tvm_solidity059
    Istanbul          bool  // allow_tvm_istanbul
    Freeze            bool  // allow_tvm_freeze
    ShieldedToken     bool  // allow_tvm_shielded_token
    Vote              bool  // allow_tvm_vote
    London            bool  // allow_tvm_london
    Compatibility     bool  // allow_tvm_compatibility
    DynamicEnergy     bool  // allow_dynamic_energy
    BigInteger        bool  // allow_tvm_big_integer
    Blob              bool  // allow_tvm_blob
    Cancun            bool  // allow_tvm_cancun
}
```

---

## 7. VM Jump Table Changes

Each opcode entry in the jump table gains an optional `enabledFn func(TVMConfig) bool` field. If `nil`, the opcode is always enabled. Before executing any opcode, the interpreter checks:

```go
if op.enabledFn != nil && !op.enabledFn(in.tvmConfig) {
    return ErrInvalidOpCode
}
```

Opcode groups by fork:
- **Constantinople**: CREATE2, EXTCODEHASH, SHL, SHR, SAR
- **Solidity059**: (bytecode compat, no new opcodes — handled via TVMConfig flag in precompiles)
- **Istanbul**: CHAINID, SELFBALANCE
- **London**: BASEFEE
- **TRON Freeze**: FREEZE, UNFREEZE, VOTEWITNESS, WITHDRAWREWARD (precompile addresses, not opcodes — gated in `instructions_call.go`)
- **TRON Vote**: VOTE opcode (if it exists in opcodes.go)
- **Cancun**: TLOAD, TSTORE, MCOPY

---

## 8. Proposal Approval Wiring

`applyProposal(ctx *Context, proposalID int64) error` in `proposal_approve.go`:

| Proposal ID | Action |
|---|---|
| 1 | `ctx.DynProps.Set("allow_multi_sign", 1)` |
| 3 | `ctx.DynProps.Set("allow_same_token_name", 1)` |
| 4 | `ctx.DynProps.Set("allow_delegate_resource", 1)` |
| 15 | `ctx.DynProps.Set("allow_tvm_transfer_trc10", 1)` |
| 16 | `ctx.DynProps.Set("allow_change_delegation", 1)` |
| 18 | `ctx.DynProps.Set("allow_new_resource_model", 1)` |
| 30 | `ctx.DynProps.Set("allow_tvm_constantinople", 1)` |
| 32 | `ctx.DynProps.Set("allow_tvm_solidity059", 1)` |
| 33 | `ctx.DynProps.Set("allow_tvm_freeze", 1)` |
| 35 | `ctx.DynProps.Set("allow_tvm_shielded_token", 1)` |
| 40 | `ctx.DynProps.Set("allow_pbft", 1)` |
| 41 | `ctx.DynProps.Set("allow_tvm_istanbul", 1)` |
| 45 | `ctx.DynProps.Set("allow_market_transaction", 1)` |
| 48 | `ctx.DynProps.Set("allow_tvm_compatibility", 1)` |
| 52 | `ctx.DynProps.Set("allow_account_history", 1)` |
| 57 | `ctx.DynProps.Set("allow_tvm_vote", 1)` |
| 65 | `ctx.DynProps.Set("allow_tvm_london", 1)` |
| 66 | `ctx.DynProps.Set("allow_energy_adjustment", 1)` |
| 70 | `ctx.DynProps.Set("allow_dynamic_energy", 1)` |
| 74 | `ctx.DynProps.Set("allow_staking_v2", 1)` |
| 78 | `ctx.DynProps.Set("allow_tvm_big_integer", 1)` |
| 82 | `ctx.DynProps.Set("allow_tvm_cancun", 1)` |
| 83 | `ctx.DynProps.Set("allow_tvm_blob", 1)` |

---

## 9. Error Messages

All fork gate errors follow the pattern:
```
"<feature> is not yet enabled"
```
e.g., `"market transactions not yet enabled"`, `"staking v2 not yet enabled"`, `"multi-sign not yet enabled"`

---

## 10. Testing

- `core/forks/forks_test.go` — unit tests: `IsActive` returns false when dp flag is 0, true when 1
- `actuator/*_test.go` additions — each gated actuator gets a test that submits a tx with the flag off (expect error) and on (expect success)
- `vm/interpreter_test.go` additions — Istanbul/London opcodes return `ErrInvalidOpCode` without the flag; work with it

---

## 11. What Is NOT Changed

- `transfer.go`, `freeze_balance.go`, `unfreeze_balance.go`, `witness.go`, `vote.go`, `withdraw.go`, `proposal_create.go`, `proposal_delete.go`, `set_account_id.go`, `account_update.go`, `account.go`, `update_setting.go`, `update_energy_limit.go`, `update_asset.go`, `unfreeze_asset.go`, `vote_asset.go` — these are always-active contract types, no fork gate needed
- `clear_abi.go` — always active
- Consensus code — PBFT flag stored but consensus module is separate

---

## 12. Implementation Order

1. `core/state/dynamic_properties.go` — add all AllowXxx getters/setters + `GetInt64(key)`  
2. `core/forks/forks.go` — AllowFlag enum, dynKey map, `IsActive()`  
3. `vm/tvm_config.go` — TVMConfig + `From()`  
4. `vm/jump_table.go` + `vm/interpreter.go` + `vm/evm.go` — opcode gating  
5. `actuator/proposal_approve.go` — `applyProposal()` wiring  
6. All 14 gated actuators — add fork check at top of `Validate()`  
7. `actuator/vm_actuator.go` — build TVMConfig, pass to VM  
8. Tests  
