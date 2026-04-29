# M11.3 Contract-type Bitmaps — Plan

**Spec:** [2026-04-29-m11-3-contract-type-bitmaps-design.md](../specs/2026-04-29-m11-3-contract-type-bitmaps-design.md)

- [ ] **DP layer** (`core/state/dynamic_properties.go`)
    - [ ] Const `ContractTypeBitmapBytes = 32`.
    - [ ] Add `available_contract_type` + `active_default_operations` to `defaultStringProps` with java-tron defaults (`7fff1fc0037e...` and `7fff1fc0033e...`).
    - [ ] `AvailableContractType() / SetAvailableContractType([]byte)` (panic on length mismatch).
    - [ ] `ActiveDefaultOperations() / SetActiveDefaultOperations([]byte)` (panic on length mismatch).
    - [ ] `IsContractTypeAvailable(id int) bool`.
    - [ ] `AddSystemContractAndSetPermission(id int)` — OR-in to both bitmaps.
    - [ ] Tests: defaults, idempotent add, both-bitmaps mutation.

- [ ] **AccountPermissionUpdate validation** (`actuator/account_permission.go`)
    - [ ] Inside the `c.Actives` loop, after the `len(active.Operations) != 32` check, iterate i=0..255 and reject if any bit set in operations does not have a corresponding bit in available_contract_type.
    - [ ] Tests: reject (bit references unavailable type) + accept (only available types).

- [ ] **Proposal side effects** (`core/proposal.go`)
    - [ ] Add 5 cases to switch in `applyProposalSideEffects`: 26, 30, 44, 70, 77.
    - [ ] Each calls `dynProps.AddSystemContractAndSetPermission(...)` for the appropriate contract type IDs.
    - [ ] Tests: approve proposal 26 → bit 48 set; approve proposal 70 → bits 54-58 set.

- [ ] **Verification**
    - [ ] `make test` green across all 28+ packages.
    - [ ] Commit: `feat(state,actuator,core): contract-type bitmap DPs + permission gating + proposal side effects (M11.3)`.
