# Phase 9: Smart Contract Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix two known system-test gaps (contract persistence lost on restart, tx pool not propagated to P2P) and add 3 smart contract management actuators (UpdateSetting, UpdateEnergyLimit, ClearABI).

**Architecture:** Contract bytecode and metadata are committed to rawdb alongside the MPT trie; cold-cache loads lazy-pull from rawdb. TX propagation uses a `TxBroadcaster` interface injected into `TronBackend` from `main.go`, keeping the import direction `net→core` intact. The three management actuators follow the existing actuator pattern and update `contractMeta` via `statedb.SetContract`.

**Tech Stack:** Go 1.21+, `google.golang.org/protobuf/proto`, existing `core/rawdb` write/read helpers, `net.BroadcastService`.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `core/state/state_object.go` | Modify | Add `contractMetaDirty bool` field |
| `core/state/statedb.go` | Modify | Fix `SetContract`/`Commit`/`GetCode`/`GetContract`/`Copy` |
| `core/state/contract_persist_test.go` | Create | Persistence round-trip tests |
| `core/tron_backend.go` | Modify | `TxBroadcaster` interface + `SetTxBroadcaster` + updated `BroadcastTransaction` |
| `cmd/gtron/main.go` | Modify | Wire `backend.SetTxBroadcaster(broadcaster)` |
| `actuator/update_setting.go` | Create | `UpdateSettingActuator` (type 33) |
| `actuator/update_setting_test.go` | Create | Tests for type 33 |
| `actuator/update_energy_limit.go` | Create | `UpdateEnergyLimitActuator` (type 45) |
| `actuator/update_energy_limit_test.go` | Create | Tests for type 45 |
| `actuator/clear_abi.go` | Create | `ClearABIActuator` (type 48) |
| `actuator/clear_abi_test.go` | Create | Tests for type 48 |
| `actuator/actuator.go` | Modify | Register types 33, 45, 48 in `CreateActuator` |

---

### Task 1: Contract Persistence Fix

**Files:**
- Modify: `core/state/state_object.go`
- Modify: `core/state/statedb.go`
- Create: `core/state/contract_persist_test.go`

**Background:** `statedb.Commit()` writes account data to the MPT trie but never calls `rawdb.WriteCode` or `rawdb.WriteContract`, so both are lost on restart. `GetCode` and `GetContract` read only from the in-memory cache. This is why the system test skips `getcontract` checks.

- [ ] **Step 1: Write the failing persistence test**

Create `core/state/contract_persist_test.go`:

```go
package state

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestContractCodePersistence(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	contractAddr := testAddr(10)
	code := []byte{0x60, 0x00, 0x60, 0x00, 0xf3}

	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.SetCode(contractAddr, code)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Open a fresh StateDB — empty in-memory cache, same disk storage
	db2 := NewDatabase(diskdb)
	sdb2, err := New(root, db2)
	if err != nil {
		t.Fatal(err)
	}

	got := sdb2.GetCode(contractAddr)
	if !bytes.Equal(got, code) {
		t.Fatalf("code not persisted after restart: got %x", got)
	}
}

func TestContractMetaPersistence(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	owner := testAddr(11)
	contractAddr := testAddr(12)
	meta := &contractpb.SmartContract{
		OriginAddress:              owner[:],
		Name:                       "PersistTest",
		ConsumeUserResourcePercent: 50,
	}

	sdb.CreateAccount(contractAddr, corepb.AccountType_Contract)
	sdb.SetContract(contractAddr, meta)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	db2 := NewDatabase(diskdb)
	sdb2, err := New(root, db2)
	if err != nil {
		t.Fatal(err)
	}

	gotMeta := sdb2.GetContract(contractAddr)
	if gotMeta == nil {
		t.Fatal("contract metadata not persisted after restart")
	}
	if gotMeta.Name != "PersistTest" {
		t.Fatalf("wrong name: %s", gotMeta.Name)
	}
	if gotMeta.ConsumeUserResourcePercent != 50 {
		t.Fatalf("wrong consume percent: %d", gotMeta.ConsumeUserResourcePercent)
	}
}
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
go test ./core/state/ -run TestContractCodePersistence -v
go test ./core/state/ -run TestContractMetaPersistence -v
```

Expected: both FAIL — `code not persisted after restart` and `contract metadata not persisted`.

- [ ] **Step 3: Add `contractMetaDirty` to `state_object.go`**

In `core/state/state_object.go`, replace the `stateObject` struct:

```go
// stateObject represents an in-memory account with dirty tracking.
type stateObject struct {
	address tcommon.Address
	account *types.Account
	dirty   bool
	deleted bool

	// Contract fields
	code              []byte                       // contract bytecode
	codeHash          tcommon.Hash                 // SHA256 hash of the code
	codeDirty         bool                         // true if code was modified
	contractMeta      *contractpb.SmartContract    // contract metadata
	contractMetaDirty bool                         // true if contractMeta was modified
	storage           map[tcommon.Hash]tcommon.Hash // dirty contract storage
	selfDestructed    bool
}
```

- [ ] **Step 4: Fix `SetContract` in `statedb.go` to set `contractMetaDirty`**

In `core/state/statedb.go`, replace the `SetContract` function:

```go
// SetContract stores contract metadata at addr.
func (s *StateDB) SetContract(addr tcommon.Address, contract *contractpb.SmartContract) {
	obj := s.GetOrCreateAccount(addr)
	obj.contractMeta = contract
	obj.contractMetaDirty = true
	obj.markDirty()
}
```

- [ ] **Step 5: Update `Copy()` in `statedb.go` to copy `contractMetaDirty`**

In the `Copy()` function (around line 606), find the struct literal that creates `newObj` and add the new field. Replace the entire `newObj := &stateObject{...}` literal with:

```go
newObj := &stateObject{
    address:           addr,
    dirty:             obj.dirty,
    deleted:           obj.deleted,
    code:              append([]byte{}, obj.code...),
    codeHash:          obj.codeHash,
    codeDirty:         obj.codeDirty,
    contractMeta:      obj.contractMeta,
    contractMetaDirty: obj.contractMetaDirty,
    storage:           make(map[tcommon.Hash]tcommon.Hash),
    selfDestructed:    obj.selfDestructed,
}
```

- [ ] **Step 6: Add new imports to `statedb.go`**

In `core/state/statedb.go`, update the import block to:

```go
import (
	"errors"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)
```

- [ ] **Step 7: Fix `Commit()` to write code and contract metadata to rawdb**

In `core/state/statedb.go`, replace the dirty-object loop in `Commit()`. The loop currently ends with `obj.dirty = false`. Replace the entire loop body for the non-deleted case:

```go
// Commit writes all dirty accounts to the MPT and returns the new root hash.
func (s *StateDB) Commit() (tcommon.Hash, error) {
	for addr, obj := range s.stateObjects {
		if !obj.dirty {
			continue
		}
		if obj.deleted {
			if err := s.trie.Delete(trieKey(addr)); err != nil {
				return tcommon.Hash{}, err
			}
			obj.dirty = false // Issue 2: clear dirty flag for deleted objects
			continue
		}
		data, err := obj.account.Marshal()
		if err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.trie.Update(trieKey(addr), data); err != nil {
			return tcommon.Hash{}, err
		}
		if obj.codeDirty {
			rawdb.WriteCode(s.db.DiskDB(), addr, obj.code)
			obj.codeDirty = false
		}
		if obj.contractMetaDirty && obj.contractMeta != nil {
			metaBytes, err := proto.Marshal(obj.contractMeta)
			if err == nil {
				rawdb.WriteContract(s.db.DiskDB(), addr, metaBytes)
				obj.contractMetaDirty = false
			}
		}
		obj.dirty = false
	}

	root, nodes := s.trie.Commit(false)
	if nodes != nil {
		// Issue 3: pass s.originRoot as parent so the hashdb reference graph is correct.
		if err := s.db.TrieDB().Update(root, s.originRoot, 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}

	// Issue 1: reopen the trie from the new root so StateDB remains usable.
	newTrie, err := s.db.OpenTrie(root)
	if err != nil {
		return tcommon.Hash{}, err
	}
	s.trie = newTrie

	// Issue 3: advance originRoot for the next commit.
	s.originRoot = root

	// Issue 4: clear journal and snapshots after a successful commit.
	s.journal = newJournal()
	s.snapshots = s.snapshots[:0]

	return tcommon.Hash(root), nil
}
```

- [ ] **Step 8: Fix `GetCode` to lazy-load from rawdb on cache miss**

In `core/state/statedb.go`, replace `GetCode`:

```go
// GetCode returns the contract bytecode at addr.
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

- [ ] **Step 9: Fix `GetContract` to lazy-load from rawdb on cache miss**

In `core/state/statedb.go`, replace `GetContract`:

```go
// GetContract returns the contract metadata at addr.
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

- [ ] **Step 10: Run tests to verify persistence tests pass**

```bash
go test ./core/state/ -run TestContractCodePersistence -v
go test ./core/state/ -run TestContractMetaPersistence -v
```

Expected: both PASS.

- [ ] **Step 11: Run full state package tests**

```bash
go test ./core/state/ -v
```

Expected: all PASS.

- [ ] **Step 12: Commit**

```bash
git add core/state/state_object.go core/state/statedb.go core/state/contract_persist_test.go
git commit -m "fix(state): persist contract code and metadata to rawdb across restarts"
```

---

### Task 2: TX Pool → P2P Propagation

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `cmd/gtron/main.go`

**Background:** `TronBackend.BroadcastTransaction` adds to the local pool but never announces the tx to peers. `net.BroadcastService` has `BroadcastTx` but lives in package `net` which imports `core` — so `core` can't import `net`. Solution: define a narrow `TxBroadcaster` interface in `core`, inject `BroadcastService` via `SetTxBroadcaster` in `main.go`.

- [ ] **Step 1: Add `TxBroadcaster` interface, field, and wiring to `tron_backend.go`**

In `core/tron_backend.go`, make these changes:

1. Add the interface and update the struct after the existing imports section:

```go
// TxBroadcaster announces new transactions to P2P peers.
// Implemented by net.BroadcastService; defined here to avoid an import cycle.
type TxBroadcaster interface {
	BroadcastTx(tx *types.Transaction)
}
```

2. Update `TronBackend` struct to add the field (after `pool`):

```go
// TronBackend implements tronapi.Backend.
type TronBackend struct {
	chain       *BlockChain
	pool        *txpool.TxPool
	txBroadcast TxBroadcaster // nil until wired from main
}
```

3. Add the setter method:

```go
// SetTxBroadcaster wires in the P2P broadcaster so BroadcastTransaction
// announces the tx to peers after adding it to the local pool.
func (b *TronBackend) SetTxBroadcaster(bc TxBroadcaster) {
	b.txBroadcast = bc
}
```

4. Replace `BroadcastTransaction`:

```go
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

- [ ] **Step 2: Wire broadcaster into backend in `main.go`**

In `cmd/gtron/main.go`, add one line right after both `backend` and `broadcaster` are created (after line `broadcaster.SetPeersFunc(handler.HandshakedPeers)`):

```go
backend.SetTxBroadcaster(broadcaster)
```

The section should look like:

```go
broadcaster := tnet.NewBroadcastService(nil)
handler := tnet.NewTronHandler(bc, pool, broadcaster)
syncService := tnet.NewSyncService(bc, handler)
handler.SetSyncService(syncService)

p2pServer := p2p.NewServer(p2p.ServerConfig{
    ListenAddr: fmt.Sprintf(":%d", cfg.P2PPort),
    MaxPeers:   cfg.MaxPeers,
    SeedNodes:  cfg.SeedNodes,
}, handler)
handler.SetServer(p2pServer)
handler.StartKeepAlive()
broadcaster.SetPeersFunc(handler.HandshakedPeers)
backend.SetTxBroadcaster(broadcaster)  // ← add this line
```

- [ ] **Step 3: Build to verify no compile errors**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add core/tron_backend.go cmd/gtron/main.go
git commit -m "fix(net): propagate submitted transactions to P2P peers via TxBroadcaster interface"
```

---

### Task 3: UpdateSetting Actuator (type 33)

**Files:**
- Create: `actuator/update_setting.go`
- Create: `actuator/update_setting_test.go`
- Modify: `actuator/actuator.go`

**Background:** `UpdateSettingContract` (type 33) allows the contract's original deployer to change how much of the energy fee burden falls on the caller (0–100 percent). It modifies `SmartContract.ConsumeUserResourcePercent`.

- [ ] **Step 1: Write the failing test**

Create `actuator/update_setting_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func makeContractState(ctx *Context, owner, contractAddr tcommon.Address, consumePct int64) {
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:              owner[:],
		ConsumeUserResourcePercent: consumePct,
	})
}

func TestUpdateSettingValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               owner[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 75,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	act := &UpdateSettingActuator{}

	// Owner doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	// Contract doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent contract")
	}

	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	// Valid
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUpdateSettingNonOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	other := tcommon.Address{0x41, 0x03}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               other[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 50,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	ctx.State.CreateAccount(other, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &UpdateSettingActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: sender is not contract origin")
	}
}

func TestUpdateSettingOutOfRange(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               owner[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 101,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	makeContractState(ctx, owner, contractAddr, 30)

	act := &UpdateSettingActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: consume_percent > 100")
	}
}

func TestUpdateSettingExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	contractAddr := tcommon.Address{0x41, 0x02}
	c := &contractpb.UpdateSettingContract{
		OwnerAddress:               owner[:],
		ContractAddress:            contractAddr[:],
		ConsumeUserResourcePercent: 80,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateSettingContract, c, 0)
	makeContractState(ctx, owner, contractAddr, 20)

	act := &UpdateSettingActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	got := ctx.State.GetContract(contractAddr)
	if got == nil || got.ConsumeUserResourcePercent != 80 {
		t.Fatalf("consume percent not updated: got %v", got)
	}
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
go test ./actuator/ -run TestUpdateSetting -v
```

Expected: FAIL — `UpdateSettingActuator undefined`.

- [ ] **Step 3: Implement `actuator/update_setting.go`**

```go
package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UpdateSettingActuator struct{}

func (a *UpdateSettingActuator) getContract(ctx *Context) (*contractpb.UpdateSettingContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateSettingContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateSettingContract")
	}
	return c, nil
}

func (a *UpdateSettingActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)

	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return errors.New("contract does not exist")
	}
	originAddr := tcommon.BytesToAddress(meta.OriginAddress)
	if originAddr != ownerAddr {
		return errors.New("sender is not the contract origin")
	}
	if c.ConsumeUserResourcePercent < 0 || c.ConsumeUserResourcePercent > 100 {
		return errors.New("consume_user_resource_percent must be in [0, 100]")
	}
	return nil
}

func (a *UpdateSettingActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return nil, errors.New("contract not found")
	}
	meta.ConsumeUserResourcePercent = c.ConsumeUserResourcePercent
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Register type 33 in `actuator/actuator.go`**

In the `CreateActuator` switch, add after `WitnessUpdateContract`:

```go
case corepb.Transaction_Contract_UpdateSettingContract:
    return &UpdateSettingActuator{}, nil
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./actuator/ -run TestUpdateSetting -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/update_setting.go actuator/update_setting_test.go actuator/actuator.go
git commit -m "feat(actuator): add UpdateSettingActuator (type 33)"
```

---

### Task 4: UpdateEnergyLimit Actuator (type 45)

**Files:**
- Create: `actuator/update_energy_limit.go`
- Create: `actuator/update_energy_limit_test.go`
- Modify: `actuator/actuator.go`

**Background:** `UpdateEnergyLimitContract` (type 45) allows the contract deployer to set the maximum energy the origin account will pay per call.

- [ ] **Step 1: Write the failing test**

Create `actuator/update_energy_limit_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUpdateEnergyLimitValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      owner[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	act := &UpdateEnergyLimitActuator{}

	// Owner doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	// Contract doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent contract")
	}

	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:     owner[:],
		OriginEnergyLimit: 1_000_000,
	})

	// Valid
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUpdateEnergyLimitNonOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	other := tcommon.Address{0x41, 0x13}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      other[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 5_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	ctx.State.CreateAccount(other, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &UpdateEnergyLimitActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: sender is not contract origin")
	}
}

func TestUpdateEnergyLimitZeroRejected(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      owner[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 0,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &UpdateEnergyLimitActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: origin_energy_limit must be > 0")
	}
}

func TestUpdateEnergyLimitExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x11}
	contractAddr := tcommon.Address{0x41, 0x12}
	c := &contractpb.UpdateEnergyLimitContract{
		OwnerAddress:      owner[:],
		ContractAddress:   contractAddr[:],
		OriginEnergyLimit: 8_000_000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateEnergyLimitContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:     owner[:],
		OriginEnergyLimit: 1_000_000,
	})

	act := &UpdateEnergyLimitActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	got := ctx.State.GetContract(contractAddr)
	if got == nil || got.OriginEnergyLimit != 8_000_000 {
		t.Fatalf("origin_energy_limit not updated: got %v", got)
	}
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
go test ./actuator/ -run TestUpdateEnergyLimit -v
```

Expected: FAIL — `UpdateEnergyLimitActuator undefined`.

- [ ] **Step 3: Implement `actuator/update_energy_limit.go`**

```go
package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UpdateEnergyLimitActuator struct{}

func (a *UpdateEnergyLimitActuator) getContract(ctx *Context) (*contractpb.UpdateEnergyLimitContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateEnergyLimitContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateEnergyLimitContract")
	}
	return c, nil
}

func (a *UpdateEnergyLimitActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)

	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return errors.New("contract does not exist")
	}
	originAddr := tcommon.BytesToAddress(meta.OriginAddress)
	if originAddr != ownerAddr {
		return errors.New("sender is not the contract origin")
	}
	if c.OriginEnergyLimit <= 0 {
		return errors.New("origin_energy_limit must be positive")
	}
	return nil
}

func (a *UpdateEnergyLimitActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return nil, errors.New("contract not found")
	}
	meta.OriginEnergyLimit = c.OriginEnergyLimit
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Register type 45 in `actuator/actuator.go`**

In the `CreateActuator` switch, add after `AccountPermissionUpdateContract`:

```go
case corepb.Transaction_Contract_UpdateEnergyLimitContract:
    return &UpdateEnergyLimitActuator{}, nil
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./actuator/ -run TestUpdateEnergyLimit -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/update_energy_limit.go actuator/update_energy_limit_test.go actuator/actuator.go
git commit -m "feat(actuator): add UpdateEnergyLimitActuator (type 45)"
```

---

### Task 5: ClearABI Actuator (type 48)

**Files:**
- Create: `actuator/clear_abi.go`
- Create: `actuator/clear_abi_test.go`
- Modify: `actuator/actuator.go`

**Background:** `ClearABIContract` (type 48) lets the contract deployer remove the ABI stored on-chain, reducing storage and preventing ABI-based interface discovery. Sets `SmartContract.Abi = nil`.

- [ ] **Step 1: Write the failing test**

Create `actuator/clear_abi_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestClearABIValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x21}
	contractAddr := tcommon.Address{0x41, 0x22}
	c := &contractpb.ClearABIContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ClearABIContract, c, 0)
	act := &ClearABIActuator{}

	// Owner doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	// Contract doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent contract")
	}

	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
		Abi:           &contractpb.SmartContract_ABI{},
	})

	// Valid
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestClearABINonOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x21}
	other := tcommon.Address{0x41, 0x23}
	contractAddr := tcommon.Address{0x41, 0x22}
	c := &contractpb.ClearABIContract{
		OwnerAddress:    other[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ClearABIContract, c, 0)
	ctx.State.CreateAccount(other, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
	})

	act := &ClearABIActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: sender is not contract origin")
	}
}

func TestClearABIExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x21}
	contractAddr := tcommon.Address{0x41, 0x22}
	c := &contractpb.ClearABIContract{
		OwnerAddress:    owner[:],
		ContractAddress: contractAddr[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ClearABIContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner[:],
		Abi:           &contractpb.SmartContract_ABI{},
	})

	act := &ClearABIActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	got := ctx.State.GetContract(contractAddr)
	if got == nil {
		t.Fatal("contract deleted unexpectedly")
	}
	if got.Abi != nil {
		t.Fatal("ABI not cleared")
	}
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
go test ./actuator/ -run TestClearABI -v
```

Expected: FAIL — `ClearABIActuator undefined`.

- [ ] **Step 3: Implement `actuator/clear_abi.go`**

```go
package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type ClearABIActuator struct{}

func (a *ClearABIActuator) getContract(ctx *Context) (*contractpb.ClearABIContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ClearABIContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ClearABIContract")
	}
	return c, nil
}

func (a *ClearABIActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)

	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return errors.New("contract does not exist")
	}
	originAddr := tcommon.BytesToAddress(meta.OriginAddress)
	if originAddr != ownerAddr {
		return errors.New("sender is not the contract origin")
	}
	return nil
}

func (a *ClearABIActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	contractAddr := tcommon.BytesToAddress(c.ContractAddress)
	meta := ctx.State.GetContract(contractAddr)
	if meta == nil {
		return nil, errors.New("contract not found")
	}
	meta.Abi = nil
	ctx.State.SetContract(contractAddr, meta)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Register type 48 in `actuator/actuator.go`**

In the `CreateActuator` switch, add after `UpdateBrokerageContract`:

```go
case corepb.Transaction_Contract_ClearABIContract:
    return &ClearABIActuator{}, nil
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./actuator/ -run TestClearABI -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/clear_abi.go actuator/clear_abi_test.go actuator/actuator.go
git commit -m "feat(actuator): add ClearABIActuator (type 48)"
```

---

### Task 6: Full Build & Test Verification

**Files:** None (verification only)

- [ ] **Step 1: Full build**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 2: All unit tests**

```bash
go test -count=1 ./...
```

Expected: all 18+ packages pass.

- [ ] **Step 3: Fix any failures and commit**

If any test fails, diagnose, fix, and commit.

```bash
git add <changed files>
git commit -m "fix: resolve test failures from Phase 9 integration"
```

---

## Self-Review

### 1. Spec coverage

| Spec requirement | Task |
|---|---|
| Fix contract code persistence (WriteCode in Commit) | Task 1 |
| Fix contractMeta persistence (WriteContract in Commit) | Task 1 |
| Lazy-load GetCode from rawdb | Task 1 |
| Lazy-load GetContract from rawdb | Task 1 |
| contractMetaDirty field on stateObject | Task 1 |
| SetContract sets contractMetaDirty | Task 1 |
| Copy() preserves contractMetaDirty | Task 1 |
| TxBroadcaster interface | Task 2 |
| TronBackend.SetTxBroadcaster + txBroadcast field | Task 2 |
| BroadcastTransaction propagates to P2P | Task 2 |
| Wire broadcaster in main.go | Task 2 |
| UpdateSettingActuator (type 33) + tests | Task 3 |
| UpdateEnergyLimitActuator (type 45) + tests | Task 4 |
| ClearABIActuator (type 48) + tests | Task 5 |
| Full build + test | Task 6 |

All spec requirements are covered.

### 2. Placeholder scan

No TBDs, TODOs, or incomplete code blocks.

### 3. Type consistency

- `TxBroadcaster.BroadcastTx(tx *types.Transaction)` matches `net.BroadcastService.BroadcastTx` signature ✓
- `ctx.State.GetContract(contractAddr)` returns `*contractpb.SmartContract` — all actuator tasks use this consistently ✓
- `makeContractState` helper in Task 3 is not used in Task 4/5 — each test is self-contained ✓
- `rawdb.WriteCode(s.db.DiskDB(), addr, ...)` — `DiskDB()` returns `ethdb.Database` which satisfies `ethdb.KeyValueWriter` ✓
