# Phase 9: Smart Contract Completion

## 1. Overview

Phase 9 fixes two known gaps flagged in the system test and adds three smart contract management actuators, completing the smart contract subsystem.

**Known gaps:**
- Contract bytecode and metadata (`contractMeta`) are stored only in memory and lost on restart. `rawdb.WriteCode` and `rawdb.WriteContract` exist but are never called.
- When a transaction is submitted via the HTTP API (`/wallet/broadcasttransaction`), it is added to the local pool but never announced to P2P peers — other nodes never see it.

**New actuators:** `UpdateSettingContract` (type 33), `UpdateEnergyLimitContract` (type 45), `ClearABIContract` (type 48) — all modify a deployed smart contract's metadata.

After Phase 9:
- Deployed contracts survive node restarts (code + ABI persisted)
- Transactions submitted to any node propagate to all peers
- Contract owners can update consume percent, energy limit, and clear ABI
- System test no longer has "not yet implemented" skips for contract storage or TX propagation

## 2. Scope

### In scope
- Fix contract persistence: `statedb.Commit()` writes code and contractMeta to rawdb; `GetCode`/`GetContract` lazy-load from rawdb on cache miss
- Fix TX pool → P2P propagation: `TronBackend.BroadcastTransaction` announces tx to P2P peers after adding to pool
- 3 new actuators: UpdateSetting (33), UpdateEnergyLimit (45), ClearABI (48)
- Register 3 new contract types in `CreateActuator`
- Unit tests for all 3 actuators
- System test: remove "not yet implemented" skips, add contract-persist test

### Out of scope
- Legacy V1 FreezeBalance / UnfreezeBalance (types 11, 12) — superseded by V2
- TRC-10 asset system — separate major subsystem
- gRPC API
- Exchange contracts
- Shielded transactions

## 3. Contract Persistence Fix

### 3.A Problem

`statedb.Commit()` iterates dirty objects and serializes the account proto to the MPT trie. But:
- Contract bytecode (`obj.code`) is tracked with `codeDirty` but never written to rawdb
- Contract metadata (`obj.contractMeta`) has no dirty flag and is never written to rawdb
- `GetCode` only checks the in-memory cache; if the cache is cold, returns empty
- `GetContract` only checks the in-memory cache; if the cache is cold, returns nil

`rawdb.WriteCode(db, addr, code)` and `rawdb.WriteContract(db, addr, bytes)` already exist. They just need to be called.

### 3.B Fix: Commit

In `statedb.Commit()`, after writing account data to the trie for each dirty object:
- If `obj.codeDirty`: call `rawdb.WriteCode(s.db.DiskDB(), addr, obj.code)`, then clear `obj.codeDirty`
- If `obj.contractMeta != nil`: marshal and call `rawdb.WriteContract(s.db.DiskDB(), addr, bytes)`

The diskdb is accessible via `s.db.DiskDB()` which returns `ethdb.Database` (satisfies `ethdb.KeyValueWriter`).

### 3.C Fix: Lazy-Load on Cache Miss

In `GetCode(addr)`:
- If `obj.code` is empty: try `rawdb.ReadCode(s.db.DiskDB(), addr)`, populate cache if found

In `GetContract(addr)`:
- If `obj.contractMeta` is nil: try `rawdb.ReadContract(s.db.DiskDB(), addr)`, unmarshal to `*contractpb.SmartContract`, populate cache if found

### 3.D contractMeta Dirty Tracking

`contractMeta` currently has no dirty flag. Options:
1. Add `contractMetaDirty bool` to `stateObject` — clear tracking
2. Always write contractMeta if non-nil in Commit (idempotent, slightly wasteful)

**Decision:** Add `contractMetaDirty bool` to `stateObject`. Set it in `SetContract()`. This avoids rewriting unchanged contract metadata on every commit.

## 4. TX Pool → P2P Propagation Fix

### 4.A Problem

`TronBackend.BroadcastTransaction` calls `b.pool.Add(tx)` but never announces the tx to peers. The `BroadcastService` is created in `main.go` but not wired into `TronBackend`.

`net` imports `core`, so `core` cannot import `net` (circular). Dependency is:
```
main → core, net
net → core
core → (no net)
```

### 4.B Fix: TxBroadcaster Interface

Define in `core/tron_backend.go`:

```go
// TxBroadcaster is the interface for announcing new transactions to P2P peers.
type TxBroadcaster interface {
    BroadcastTx(tx *types.Transaction)
}
```

Add to `TronBackend`:
```go
type TronBackend struct {
    chain       *BlockChain
    pool        *txpool.TxPool
    txBroadcast TxBroadcaster // nil until wired in main
}

func (b *TronBackend) SetTxBroadcaster(bc TxBroadcaster) {
    b.txBroadcast = bc
}

func (b *TronBackend) BroadcastTransaction(tx *types.Transaction) error {
    if err := b.pool.Add(tx); err != nil {
        return err
    }
    if b.txBroadcast != nil {
        b.txBroadcast.BroadcastTx(tx)
    }
    return nil
}
```

`net.BroadcastService.BroadcastTx(*types.Transaction)` already exists and satisfies the interface.

Wire in `cmd/gtron/main.go` after both are constructed:
```go
backend.SetTxBroadcaster(broadcaster)
```

## 5. New Actuators

### 5.A UpdateSettingContract (type 33)

**Proto:** `contractpb.UpdateSettingContract`
- `owner_address`
- `contract_address`
- `consume_user_resource_percent` — new value (0–100)

**Validate:**
- Owner exists
- Contract exists at `contract_address` (statedb.GetContract)
- Owner is the contract's `origin_address`
- `consume_user_resource_percent` in [0, 100]

**Execute:**
- Load SmartContract meta
- Set `meta.ConsumeUserResourcePercent = c.ConsumeUserResourcePercent`
- Call `statedb.SetContract(contractAddr, meta)` (marks contractMetaDirty)
- ContractRet: SUCCESS (1)

### 5.B UpdateEnergyLimitContract (type 45)

**Proto:** `contractpb.UpdateEnergyLimitContract`
- `owner_address`
- `contract_address`
- `origin_energy_limit` — new value (> 0)

**Validate:**
- Owner exists
- Contract exists
- Owner is the contract's `origin_address`
- `origin_energy_limit` > 0

**Execute:**
- Load SmartContract meta
- Set `meta.OriginEnergyLimit = c.OriginEnergyLimit`
- Call `statedb.SetContract(contractAddr, meta)`
- ContractRet: SUCCESS (1)

### 5.C ClearABIContract (type 48)

**Proto:** `contractpb.ClearABIContract`
- `owner_address`
- `contract_address`

**Validate:**
- Owner exists
- Contract exists
- Owner is the contract's `origin_address`

**Execute:**
- Load SmartContract meta
- Set `meta.Abi = nil`
- Call `statedb.SetContract(contractAddr, meta)`
- ContractRet: SUCCESS (1)

### 5.D Common Pattern

All three actuators need to retrieve the SmartContract metadata. The StateDB `GetContract` method becomes the gateway — it must lazy-load from rawdb (per section 3.C). After the persistence fix, `GetContract` returns non-nil for any previously deployed contract even after restart.

## 6. StateDB Changes

### 6.A `state_object.go`

Add:
```go
contractMetaDirty bool
```

In `SetContract` (called by `statedb.SetContract`):
```go
obj.contractMeta = contract
obj.contractMetaDirty = true
obj.markDirty()
```

### 6.B `statedb.go` — Commit

In the dirty-object loop, after writing account data to trie:
```go
if obj.codeDirty {
    rawdb.WriteCode(s.db.DiskDB(), addr, obj.code)
    obj.codeDirty = false
}
if obj.contractMetaDirty && obj.contractMeta != nil {
    data, err := proto.Marshal(obj.contractMeta)
    if err == nil {
        rawdb.WriteContract(s.db.DiskDB(), addr, data)
        obj.contractMetaDirty = false
    }
}
```

Imports needed: `"google.golang.org/protobuf/proto"` and `"github.com/tronprotocol/go-tron/core/rawdb"`.

### 6.C `statedb.go` — GetCode lazy-load

```go
func (s *StateDB) GetCode(addr tcommon.Address) []byte {
    obj := s.getStateObject(addr)
    if obj == nil {
        return nil
    }
    if len(obj.code) == 0 {
        code := rawdb.ReadCode(s.db.DiskDB(), addr)
        if len(code) > 0 {
            obj.code = code
            obj.codeHash = tcommon.Sha256(code)
        }
    }
    return obj.code
}
```

### 6.D `statedb.go` — GetContract lazy-load

```go
func (s *StateDB) GetContract(addr tcommon.Address) *contractpb.SmartContract {
    obj := s.getStateObject(addr)
    if obj == nil {
        return nil
    }
    if obj.contractMeta == nil {
        data := rawdb.ReadContract(s.db.DiskDB(), addr)
        if len(data) > 0 {
            var sc contractpb.SmartContract
            if err := proto.Unmarshal(data, &sc); err == nil {
                obj.contractMeta = &sc
            }
        }
    }
    return obj.contractMeta
}
```

## 7. Testing

### Unit Tests — Actuators

**`actuator/update_setting_test.go`:**
- Valid update by contract owner
- Rejected if not contract owner
- Rejected if contract doesn't exist
- Rejected if consume_percent out of range [0, 100]

**`actuator/update_energy_limit_test.go`:**
- Valid update by contract owner
- Rejected if not contract owner
- Rejected if origin_energy_limit == 0

**`actuator/clear_abi_test.go`:**
- Valid clear by contract owner
- Rejected if not contract owner

### Unit Tests — Contract Persistence

**`core/state/contract_persist_test.go`** (or extend existing statedb test):
- Deploy contract (SetCode + SetContract), Commit, create new StateDB from same root+db, GetCode → returns code, GetContract → returns meta

### System Test Updates

In `scripts/system_test.sh`:
- Remove `skip "getcontract returns bytecode" "SmartContract proto storage not yet implemented"` — let the check run and verify it passes
- Remove `skip "node getcontract" "SmartContract proto storage not yet implemented"` — same
- Remove comment `# Tx pool propagation across P2P is not yet implemented` and actually broadcast from node to SR via P2P path

## 8. File Plan

### New Files
- `actuator/update_setting.go`
- `actuator/update_setting_test.go`
- `actuator/update_energy_limit.go`
- `actuator/update_energy_limit_test.go`
- `actuator/clear_abi.go`
- `actuator/clear_abi_test.go`

### Modified Files
- `core/state/state_object.go` — add `contractMetaDirty` field
- `core/state/statedb.go` — fix Commit (WriteCode/WriteContract), fix GetCode/GetContract (lazy-load)
- `core/tron_backend.go` — add `TxBroadcaster` interface, `txBroadcast` field, `SetTxBroadcaster`, update `BroadcastTransaction`
- `cmd/gtron/main.go` — call `backend.SetTxBroadcaster(broadcaster)`
- `actuator/actuator.go` — register types 33, 45, 48 in `CreateActuator`
- `scripts/system_test.sh` — remove "not yet implemented" skips for contract storage
