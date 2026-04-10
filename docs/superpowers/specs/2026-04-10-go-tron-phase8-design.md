# Phase 8: Governance, Delegation & Account Management

## 1. Overview

Phase 8 expands the transaction execution layer from 9 contract types to 20 by adding governance (proposals), resource delegation, and account/witness management actuators. These are the most impactful missing contract types that don't require new storage subsystems (unlike TRC-10 tokens or market orders).

After this phase, the node can:
- Create, vote on, and finalize governance proposals that modify chain parameters
- Delegate bandwidth/energy resources between accounts
- Cancel pending V2 unfreezes
- Update account names, IDs, and multi-sig permissions
- Update witness URLs and reward brokerage ratios

## 2. Scope

### In scope
- 11 new actuators (3 governance + 3 delegation + 5 account/witness management)
- Proposal storage in rawdb (per-proposal and index)
- Delegation record storage in rawdb
- Witness brokerage storage in rawdb
- Proposal finalization logic in maintenance cycle
- 4 new HTTP API endpoints (proposal create/approve/delete, list proposals)
- Unit tests for all actuators
- System test extensions

### Out of scope
- Permission enforcement during transaction signature validation (store only)
- TRC-10 asset system
- gRPC API
- Shielded transactions
- Market orders

## 3. Contract Types

### 3.A Governance

#### ProposalCreateContract (type 16)

Creates a proposal to change one or more chain dynamic properties.

**Proto:** `protocol.ProposalCreateContract`
- `owner_address` — must be an active SR
- `parameters` — map<int64, int64> of parameter ID → proposed value

**Validate:**
- Owner account exists
- Owner is in the active witness set
- `parameters` is non-empty
- Each parameter key is a valid chain parameter ID
- Each parameter value is within the allowed range for that parameter

**Execute:**
- Allocate next proposal ID from `DynamicProperties.next_proposal_id` (increment)
- Create Proposal record: id, proposer=owner, parameters, create_time=block_time, expiration_time=block_time + 3 days (259200000 ms), state=PENDING, approvals=[]
- Store via `rawdb.WriteProposal(id, proposal)`
- Append ID to proposal index via `rawdb.WriteProposalIndex`
- Fee: bandwidth only (no special fee)
- ContractRet: SUCCESS (1)

#### ProposalApproveContract (type 17)

An active SR votes to approve a pending proposal.

**Proto:** `protocol.ProposalApproveContract`
- `owner_address` — must be an active SR
- `proposal_id` — ID of the proposal to approve
- `is_add_approval` — true to approve, false to revoke approval

**Validate:**
- Owner account exists
- Owner is in the active witness set
- Proposal exists with the given ID
- Proposal state is PENDING
- Proposal has not expired (expiration_time > current block time)
- If `is_add_approval=true`: owner has not already approved
- If `is_add_approval=false`: owner has already approved (to revoke)

**Execute:**
- If `is_add_approval=true`: add owner address to proposal approvals list
- If `is_add_approval=false`: remove owner address from proposal approvals list
- Update proposal in rawdb
- ContractRet: SUCCESS (1)

#### ProposalDeleteContract (type 18)

The proposer cancels their own pending proposal.

**Proto:** `protocol.ProposalDeleteContract`
- `owner_address` — must be the proposal creator
- `proposal_id` — ID of the proposal

**Validate:**
- Owner account exists
- Proposal exists with the given ID
- Proposal state is PENDING
- Owner is the proposal's proposer

**Execute:**
- Set proposal state to CANCELED
- Update proposal in rawdb
- ContractRet: SUCCESS (1)

#### Proposal Finalization (Maintenance)

During each maintenance cycle (already triggered in `block_builder.go` via `dpos.DoMaintenance`):

1. Read all proposal IDs from the proposal index
2. For each PENDING proposal:
   - If expired (expiration_time <= maintenance_time):
     - Count approvals vs active SR count
     - If approvals >= 70% of active SRs (approvals * 10 >= activeCount * 7):
       - Apply each parameter: `dynProps.Set(paramKey, paramValue)`
       - Set proposal state to APPROVED
     - Else:
       - Set proposal state to CANCELED
     - Update proposal in rawdb

### 3.B Resource Delegation

#### DelegateResourceContract (type 57)

Delegate frozen V2 bandwidth or energy to another account.

**Proto:** `protocol.DelegateResourceContract`
- `owner_address` — delegator
- `resource_type` — BANDWIDTH (0) or ENERGY (1)
- `balance` — amount of frozen TRX to delegate (in sun)
- `receiver_address` — recipient of the delegated resource
- `lock` — if true, delegation is locked (cannot be undone immediately)
- `lock_period` — lock duration in ms (0 = no lock, max 3 days)

**Validate:**
- Owner account exists
- Receiver account exists
- Owner != receiver
- Balance > 0
- Owner has enough frozen V2 balance of the specified resource type (after subtracting existing delegations)
- Resource type is valid (0 or 1)

**Execute:**
- Subtract `balance` from owner's frozen V2 balance for the resource type
- Read or create DelegatedResource record for (owner → receiver)
- Add `balance` to the delegation record's frozen amount for the resource type
- If `lock=true`: set expire_time = block_time + lock_period
- Increase receiver's delegated resource (so their usable bandwidth/energy increases)
- Store delegation record via `rawdb.WriteDelegatedResource`
- Update delegation index via `rawdb.WriteDelegationIndex`
- ContractRet: SUCCESS (1)

#### UnDelegateResourceContract (type 58)

Revoke a previous resource delegation.

**Proto:** `protocol.UnDelegateResourceContract`
- `owner_address` — original delegator
- `resource_type` — BANDWIDTH (0) or ENERGY (1)
- `balance` — amount to un-delegate (in sun)
- `receiver_address` — the account that was receiving the delegation

**Validate:**
- Owner account exists
- Delegation record exists for (owner → receiver)
- Delegation record has enough frozen balance of the resource type
- If delegation has a lock: lock period has expired (expire_time <= block_time)
- Balance > 0

**Execute:**
- Subtract `balance` from delegation record's frozen amount for the resource type
- Add `balance` back to owner's frozen V2 balance for the resource type
- Decrease receiver's delegated resource
- If delegation record is now empty (both bandwidth and energy = 0): remove record
- Update rawdb
- ContractRet: SUCCESS (1)

#### CancelAllUnfreezeV2Contract (type 59)

Cancel all pending V2 unfreeze operations and re-freeze the balance.

**Proto:** `protocol.CancelAllUnfreezeV2Contract`
- `owner_address`

**Validate:**
- Owner account exists
- Owner has at least one pending unfreeze V2 entry

**Execute:**
- Sum all pending unfreeze entries by resource type
- Add sums back to owner's frozen V2 balance (bandwidth and/or energy)
- Clear the unfreeze queue
- ContractRet: SUCCESS (1)

### 3.C Account & Witness Management

#### AccountUpdateContract (type 10)

Set the account name.

**Proto:** `protocol.AccountUpdateContract`
- `owner_address`
- `account_name` — desired name (bytes)

**Validate:**
- Owner account exists
- `account_name` is non-empty and ≤ 32 bytes
- Account doesn't already have a name set (one-time operation in TRON)

**Execute:**
- Set `account_name` on the account
- ContractRet: SUCCESS (1)

#### SetAccountIdContract (type 19)

Set the account ID.

**Proto:** `protocol.SetAccountIdContract`
- `owner_address`
- `account_id` — desired ID (bytes)

**Validate:**
- Owner account exists
- `account_id` is non-empty and ≤ 32 bytes
- Account doesn't already have an ID set (one-time operation)

**Execute:**
- Set `account_id` on the account
- ContractRet: SUCCESS (1)

#### WitnessUpdateContract (type 8)

Update a witness's URL.

**Proto:** `protocol.WitnessUpdateContract`
- `owner_address`
- `update_url` — new URL (bytes)

**Validate:**
- Owner account exists
- Owner is a registered witness (exists in witness index)
- `update_url` is non-empty and ≤ 256 bytes

**Execute:**
- Update witness URL in rawdb
- ContractRet: SUCCESS (1)

#### UpdateBrokerageContract (type 49)

Set the witness brokerage ratio (percentage of block reward the witness keeps; the rest goes to voters).

**Proto:** `protocol.UpdateBrokerageContract`
- `owner_address`
- `brokerage` — integer 0-100

**Validate:**
- Owner account exists
- Owner is a registered witness
- Brokerage is in range [0, 100]

**Execute:**
- Store brokerage via `rawdb.WriteWitnessBrokerage(owner, brokerage)`
- ContractRet: SUCCESS (1)

#### AccountPermissionUpdateContract (type 46)

Configure multi-sig permissions on the account.

**Proto:** `protocol.AccountPermissionUpdateContract`
- `owner_address`
- `owner` — Permission proto (the owner permission)
- `witness` — Permission proto (witness permission, only if account is a witness)
- `actives` — list of Permission protos (active permissions for contract calls)

Each Permission has: type, id, permission_name, threshold, operations (bytes bitmap), keys (list of Key{address, weight}).

**Validate:**
- Owner account exists
- `owner` permission is provided and has at least 1 key
- `owner` permission threshold > 0 and ≤ sum of key weights
- Each active permission has threshold > 0 and ≤ sum of key weights
- If `witness` permission is provided: owner must be a registered witness
- Total number of keys across all permissions ≤ 5 (to limit complexity)

**Execute:**
- Set `owner_permission`, `witness_permission`, `active_permission` fields on the account proto
- ContractRet: SUCCESS (1)

Note: This phase only **stores** permissions. Enforcing multi-sig during transaction signature validation is a future phase.

## 4. New Storage

### 4.A Proposal Storage (rawdb)

**Schema keys:**
- `prop-<id>` — proposal record (protobuf-encoded)
- `propi` — proposal index (list of all proposal IDs, protobuf-encoded)

**Types:**
```
Proposal struct:
  ID            int64
  Proposer      Address
  Parameters    map[int64]int64
  CreateTime    int64
  ExpirationTime int64
  Approvals     []Address
  State         int32  // 0=PENDING, 1=APPROVED, 2=CANCELED
```

Use a simple Go struct with protobuf serialization (or JSON encoding). Not a proto message — internal storage only.

**Accessors:**
- `WriteProposal(db, id int64, proposal *Proposal) error`
- `ReadProposal(db, id int64) *Proposal`
- `WriteProposalIndex(db, ids []int64) error`
- `ReadProposalIndex(db) []int64`

### 4.B Delegation Storage (rawdb)

**Schema keys:**
- `dr-<from><to>` — delegated resource record (42 bytes key: prefix + 21-byte from + 21-byte to, minus prefix)
- `dri-<from>` — delegation index for an account (list of receiver addresses)

**Types:**
```
DelegatedResource struct:
  From                      Address
  To                        Address
  FrozenBalanceForBandwidth int64
  FrozenBalanceForEnergy    int64
  ExpireTimeForBandwidth    int64
  ExpireTimeForEnergy       int64
```

**Accessors:**
- `WriteDelegatedResource(db, from, to Address, dr *DelegatedResource) error`
- `ReadDelegatedResource(db, from, to Address) *DelegatedResource`
- `DeleteDelegatedResource(db, from, to Address) error`
- `WriteDelegationIndex(db, from Address, receivers []Address) error`
- `ReadDelegationIndex(db, from Address) []Address`

### 4.C Witness Brokerage Storage (rawdb)

**Schema keys:**
- `wb-<addr>` — brokerage ratio (int64, 8 bytes)

**Accessors:**
- `WriteWitnessBrokerage(db, addr Address, brokerage int64) error`
- `ReadWitnessBrokerage(db, addr Address) int64` (default 20 if not set)

### 4.D DynamicProperties Extensions

Add to `core/state/dynamic_properties.go`:
- `next_proposal_id` — auto-incrementing proposal ID counter (default 0)

### 4.E StateDB Extensions

Add to `core/state/statedb.go`:
- `SetAccountName(addr, name)` / `GetAccountName(addr)`
- `SetAccountId(addr, id)` / `GetAccountId(addr)`
- `SetPermissions(addr, owner, witness, actives)` / `GetPermissions(addr)`
- `GetDelegatedFrozenBalance(addr, resourceType)` — total frozen balance delegated TO this account
- `AddDelegatedFrozenBalance(addr, resourceType, amount)`
- `SubDelegatedFrozenBalance(addr, resourceType, amount)`

## 5. Maintenance Integration

Extend `dpos.DoMaintenance()` or add a new `ProcessProposals()` function called from the maintenance path in `block_builder.go`.

The maintenance cycle already runs in `BuildBlock()` when `timestamp >= dynProps.NextMaintenanceTime()`. Add proposal processing to this path:

```
if maintenance triggered:
    1. existing: witness election, reward distribution
    2. new: process pending proposals (approve if 70%+ votes, cancel if expired)
```

## 6. API Endpoints

### New HTTP Endpoints (4)

| Endpoint | Method | Body | Description |
|---|---|---|---|
| `/wallet/proposalcreate` | POST | `{owner_address, parameters: {key: value, ...}}` | Build ProposalCreate transaction |
| `/wallet/proposalapprove` | POST | `{owner_address, proposal_id, is_add_approval}` | Build ProposalApprove transaction |
| `/wallet/proposaldelete` | POST | `{owner_address, proposal_id}` | Build ProposalDelete transaction |
| `/wallet/listproposals` | POST | `{}` | List all proposals with state |

### Backend Interface Extensions

Add to `tronapi.Backend`:
- `BuildProposalCreateTransaction(owner Address, params map[int64]int64) (*corepb.Transaction, error)`
- `BuildProposalApproveTransaction(owner Address, proposalID int64, approve bool) (*corepb.Transaction, error)`
- `BuildProposalDeleteTransaction(owner Address, proposalID int64) (*corepb.Transaction, error)`
- `ListProposals() ([]*ProposalInfo, error)`

```
ProposalInfo struct:
  ProposalID     int64
  ProposerAddress string
  Parameters     map[string]int64
  ExpirationTime int64
  CreateTime     int64
  Approvals      []string
  State          string  // "PENDING", "APPROVED", "CANCELED"
```

## 7. Testing

### Unit Tests

One test file per actuator in `actuator/`:
- `proposal_create_test.go` — valid creation, non-SR rejected, invalid params rejected
- `proposal_approve_test.go` — valid approval, double-approve rejected, expired rejected, revoke approval
- `proposal_delete_test.go` — valid deletion, non-creator rejected
- `delegate_resource_test.go` — valid delegation for bandwidth/energy, insufficient balance, self-delegation rejected
- `undelegate_resource_test.go` — valid undelegation, lock period check, insufficient delegation
- `cancel_unfreeze_test.go` — valid cancel, no pending unfreezes rejected
- `account_update_test.go` — valid update, empty name, already set
- `set_account_id_test.go` — valid set, already set
- `witness_update_test.go` — valid update, non-witness rejected
- `update_brokerage_test.go` — valid update, out of range rejected
- `account_permission_test.go` — valid permission set, invalid threshold, too many keys

### rawdb Tests

- `accessors_proposal_test.go` — proposal write/read/index
- `accessors_delegation_test.go` — delegation record write/read/delete/index
- `accessors_brokerage_test.go` — brokerage write/read/default

### Maintenance Test

- Test in `consensus/dpos/` or `core/`: create proposals, simulate maintenance, verify parameter changes applied

### System Test Extension

Add to `scripts/system_test.sh`:
- **Test Group 8: Governance** — create proposal to change `witness_pay_per_block`, approve it, trigger maintenance, verify parameter changed via `getchainparameters`
- **Test Group 9: Account Management** — update account name, query account, verify name field present

## 8. File Plan

### New Files
- `actuator/proposal_create.go`
- `actuator/proposal_approve.go`
- `actuator/proposal_delete.go`
- `actuator/delegate_resource.go`
- `actuator/undelegate_resource.go`
- `actuator/cancel_unfreeze.go`
- `actuator/account_update.go`
- `actuator/set_account_id.go`
- `actuator/witness_update.go`
- `actuator/update_brokerage.go`
- `actuator/account_permission.go`
- `core/rawdb/accessors_proposal.go`
- `core/rawdb/accessors_delegation.go`
- `core/rawdb/accessors_brokerage.go`
- Test files for each of the above

### Modified Files
- `actuator/actuator.go` — register 11 new types in `CreateActuator`
- `core/rawdb/schema.go` — add key prefixes for proposals, delegations, brokerage
- `core/state/dynamic_properties.go` — add `next_proposal_id`
- `core/state/statedb.go` — add account name/id/permission accessors, delegation balance tracking
- `core/block_builder.go` — call proposal processing during maintenance
- `internal/tronapi/backend.go` — add 4 new Backend methods
- `internal/tronapi/api.go` — register 4 new routes
- `core/tron_backend.go` — implement 4 new Backend methods
- `scripts/system_test.sh` — add test groups 8 and 9
