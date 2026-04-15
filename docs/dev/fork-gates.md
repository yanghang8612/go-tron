# Fork Gate Reference

Every governance `AllowFlag` in go-tron, the DP key it reads, the proposal ID
that toggles it, and any SR-version quorum that must pass before the feature
can be considered active.

Maintenance: edit this table whenever you add, alias, or retire a flag.
The machinery behind each column:

- **AllowFlag**: `core/forks/forks.go`
- **DP key**: `core/state/dynamic_properties.go` defaults + getters
- **Proposal ID**: `core/forks/forks.go::proposalParamKey` (java source:
  `actuator/src/main/java/org/tron/core/utils/ProposalUtil.java`
  `ProposalType` enum)
- **Required version**: `core/forks/required_versions.go`
- **Audit snapshot**: `docs/dev/fork-audit-<date>.md`

## Active gates

| AllowFlag | DP key | Proposal ID | Required version | Notes |
|---|---|---|---|---|
| `AllowSameTokenName` | `allow_same_token_name` | 15 | — | |
| `AllowDelegateResource` | `allow_delegate_resource` | 16 | — | |
| `AllowAdaptiveEnergy` | `allow_adaptive_energy` | 21 | `VERSION_3_6_5` (9) | Seeded in `required_versions.go` |
| `AllowMultiSign` | `allow_multi_sign` | 20 | — | |
| `AllowChangeDelegation` | `change_delegation` | 30 | — | |
| `AllowTvmTransferTrc10` | `allow_tvm_transfer_trc10` | 18 | — | |
| `AllowTvmConstantinople` | `allow_tvm_constantinople` | 26 | `VERSION_3_6_5` (9) | Seeded in `required_versions.go` |
| `AllowTvmSolidity059` | `allow_tvm_solidity059` | 32 | — | |
| `AllowTvmIstanbul` | `allow_tvm_istanbul` | 41 | — | |
| `AllowMarketTransaction` | `allow_market_transaction` | 44 | — | |
| `AllowTvmFreeze` | `allow_tvm_freeze` | 52 | — | |
| `AllowTvmVote` | `allow_tvm_vote` | 59 | — | |
| `AllowPbft` | `allow_pbft` | 40 | — | |
| `AllowTvmLondon` | `allow_tvm_london` | 63 | — | |
| `AllowTvmCompatibleEvm` | `allow_tvm_compatible_evm` | 60 | — | |
| `AllowDynamicEnergy` | `allow_dynamic_energy` | 72 | — | |
| `AllowNewResourceModel` | `allow_new_resource_model` | 51 | — | Freeze V2 + V1 rejection gate |
| `AllowEnergyAdjustment` | `allow_energy_adjustment` | 81 | — | |
| `AllowTvmBlob` | `allow_tvm_blob` | 89 | — | |
| `AllowTvmCancun` | `allow_tvm_cancun` | 83 | — | |

## Aliases — one proposal flips multiple flags

| Alias AllowFlag | Canonical DP key | Reason |
|---|---|---|
| `AllowStakingV2` → `AllowNewResourceModel` | `allow_new_resource_model` | Proposal #51 gates both state-layer V2 and VM V2 precompiles in java-tron; go-tron historically had two separate flags. Fixed in M1.3 Task 5. |
| `AllowTvmShieldedToken` → `AllowShieldedTrc20Transaction` | `allow_shielded_trc20_transaction` | Proposal #39 gates shielded-TRC20 precompiles in java-tron; go-tron's historical naming was VM-centric. |

## go-tron specific (no java-tron proposal)

| AllowFlag | DP key | Disposition | Notes |
|---|---|---|---|
| `AllowTvmBigInteger` | `allow_tvm_big_integer` | **Keep private.** | Consumed by `vm/tvm_config.go:44` (TVM BigInt opcode). java-tron has no matching `ProposalType`; no proposal can activate it. Revisit in M7 TVM alignment when deciding whether to remove or map to a version-bit gate. |
| `AllowTvmSolidity058` | `allow_tvm_solidity058` | **Dead leaf.** | Loaded into `vm/tvm_config.go` but interpreter doesn't actually branch on it. No java counterpart. Candidate for deletion in M7 after VM cleanup sweep. |

## Retired

(none yet)

## Audit workflow

When adding a new flag, run:

```bash
scripts/dev/fork_audit.sh
```

and diff the new `docs/dev/fork-audit-<date>.md` against the previous snapshot
to confirm no existing gate moved, broke, or orphaned. The script does not run
in CI — freeze and commit the snapshot whenever the fork gate surface changes.
