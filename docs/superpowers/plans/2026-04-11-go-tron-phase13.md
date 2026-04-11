# Phase 13: Stake 1.0 + Market Orders — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement 4 remaining practical TRON contract types — FreezeBalance v1 (11), UnfreezeBalance v1 (12), MarketSellAsset (52), MarketCancelOrder (53) — completing all practical contract types in go-tron.

**Architecture:** Two independent subsystems. Stake 1.0 adds V1 frozen balance accessors to Account and StateDB, then two actuators mirroring the existing V2 pattern. Market Orders adds rawdb storage (4 prefixes), a price-time-priority matching engine, and two actuators plus 3 HTTP query endpoints.

**Tech Stack:** Go standard library only (`math/big`, `encoding/binary`, `bytes`, `strconv`, `sort`). No new external dependencies.

---

### Task 1: V1 Frozen Balance Account Accessors + StateDB Methods

**Files:**
- Modify: `core/types/account.go` (after V2 delegation block ~line 219, before `ClearUnfrozenV2`)
- Modify: `core/state/statedb.go` (after `AddFreezeV2` block ~line 168)
- Create: `core/state/statedb_v1_test.go`

- [ ] **Step 1: Write failing tests for V1 state methods**

Create `core/state/statedb_v1_test.go`:

```go
package state

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestFreezeV1Bandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Bandwidth(addr, 500, 3_000_000)

	obj := sdb.getStateObject(addr)
	if obj == nil {
		t.Fatal("account not found")
	}
	if got := obj.account.TotalFrozenBandwidth(); got != 500 {
		t.Fatalf("frozen bandwidth: want 500, got %d", got)
	}
	list := obj.account.FrozenBandwidthList()
	if len(list) != 1 {
		t.Fatalf("frozen list length: want 1, got %d", len(list))
	}
	if list[0].FrozenBalance != 500 || list[0].ExpireTime != 3_000_000 {
		t.Fatalf("frozen entry: want {500, 3000000}, got {%d, %d}", list[0].FrozenBalance, list[0].ExpireTime)
	}
}

func TestUnfreezeV1Bandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(2)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Bandwidth(addr, 300, 2_000_000) // expires at 2M
	sdb.FreezeV1Bandwidth(addr, 200, 5_000_000) // expires at 5M

	// Unfreeze at time 3M — only first entry expires
	refunded := sdb.UnfreezeV1Bandwidth(addr, 3_000_000)
	if refunded != 300 {
		t.Fatalf("refunded: want 300, got %d", refunded)
	}
	obj := sdb.getStateObject(addr)
	if got := obj.account.TotalFrozenBandwidth(); got != 200 {
		t.Fatalf("remaining frozen: want 200, got %d", got)
	}
}

func TestFreezeV1Energy(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(3)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Energy(addr, 400, 3_000_000)
	obj := sdb.getStateObject(addr)
	if got := obj.account.FrozenEnergyAmount(); got != 400 {
		t.Fatalf("frozen energy: want 400, got %d", got)
	}

	// Add more — accumulates
	sdb.FreezeV1Energy(addr, 100, 4_000_000)
	obj = sdb.getStateObject(addr)
	if got := obj.account.FrozenEnergyAmount(); got != 500 {
		t.Fatalf("accumulated energy: want 500, got %d", got)
	}
	if got := obj.account.FrozenEnergyExpireTime(); got != 4_000_000 {
		t.Fatalf("expire time: want 4000000, got %d", got)
	}
}

func TestUnfreezeV1Energy_NotExpired(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(4)
	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.AddBalance(addr, 1000)

	sdb.FreezeV1Energy(addr, 400, 5_000_000) // expires at 5M

	// Try unfreeze at 3M — not expired
	refunded := sdb.UnfreezeV1Energy(addr, 3_000_000)
	if refunded != 0 {
		t.Fatalf("refunded: want 0, got %d", refunded)
	}

	// Unfreeze at 5M — expired
	refunded = sdb.UnfreezeV1Energy(addr, 5_000_000)
	if refunded != 400 {
		t.Fatalf("refunded: want 400, got %d", refunded)
	}
}

func TestFreezeV1DelegatedBandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(5)
	receiver := testAddr(6)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(receiver, corepb.AccountType_Normal)

	sdb.FreezeV1DelegatedBandwidth(owner, receiver, 300)

	ownerObj := sdb.getStateObject(owner)
	if got := ownerObj.account.DelegatedFrozenBandwidth(); got != 300 {
		t.Fatalf("owner delegated: want 300, got %d", got)
	}
	recvObj := sdb.getStateObject(receiver)
	if got := recvObj.account.AcquiredDelegatedFrozenBandwidth(); got != 300 {
		t.Fatalf("receiver acquired: want 300, got %d", got)
	}
}

func TestUnfreezeV1DelegatedBandwidth(t *testing.T) {
	sdb := newTestStateDB(t)
	owner := testAddr(7)
	receiver := testAddr(8)
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	sdb.CreateAccount(receiver, corepb.AccountType_Normal)

	sdb.FreezeV1DelegatedBandwidth(owner, receiver, 300)
	sdb.UnfreezeV1DelegatedBandwidth(owner, receiver, 300)

	ownerObj := sdb.getStateObject(owner)
	if got := ownerObj.account.DelegatedFrozenBandwidth(); got != 0 {
		t.Fatalf("owner delegated: want 0, got %d", got)
	}
	recvObj := sdb.getStateObject(receiver)
	if got := recvObj.account.AcquiredDelegatedFrozenBandwidth(); got != 0 {
		t.Fatalf("receiver acquired: want 0, got %d", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run "TestFreezeV1|TestUnfreezeV1" -v -count=1`
Expected: compilation errors — methods not yet defined.

- [ ] **Step 3: Add V1 frozen balance accessors to `core/types/account.go`**

Insert after the `SetAcquiredDelegatedFrozenV2BalanceForEnergy` block (~line 219), before `ClearUnfrozenV2`:

```go
// --- V1 Stake (Stake 1.0) frozen balance accessors ---

func (a *Account) FrozenBandwidthList() []*corepb.Account_Frozen {
	return a.pb.Frozen
}

func (a *Account) AddFrozenBandwidth(amount, expireTimeMs int64) {
	a.pb.Frozen = append(a.pb.Frozen, &corepb.Account_Frozen{
		FrozenBalance: amount,
		ExpireTime:    expireTimeMs,
	})
}

func (a *Account) TotalFrozenBandwidth() int64 {
	var total int64
	for _, f := range a.pb.Frozen {
		total += f.FrozenBalance
	}
	return total
}

func (a *Account) RemoveExpiredFrozenBandwidth(blockTimeMs int64) int64 {
	var refunded int64
	remaining := a.pb.Frozen[:0]
	for _, f := range a.pb.Frozen {
		if f.ExpireTime <= blockTimeMs {
			refunded += f.FrozenBalance
		} else {
			remaining = append(remaining, f)
		}
	}
	a.pb.Frozen = remaining
	return refunded
}

func (a *Account) FrozenEnergyAmount() int64 {
	if a.pb.AccountResource == nil || a.pb.AccountResource.FrozenBalanceForEnergy == nil {
		return 0
	}
	return a.pb.AccountResource.FrozenBalanceForEnergy.FrozenBalance
}

func (a *Account) FrozenEnergyExpireTime() int64 {
	if a.pb.AccountResource == nil || a.pb.AccountResource.FrozenBalanceForEnergy == nil {
		return 0
	}
	return a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime
}

func (a *Account) AddFrozenEnergy(amount, expireTimeMs int64) {
	a.ensureAccountResource()
	if a.pb.AccountResource.FrozenBalanceForEnergy == nil {
		a.pb.AccountResource.FrozenBalanceForEnergy = &corepb.Account_Frozen{
			FrozenBalance: amount,
			ExpireTime:    expireTimeMs,
		}
	} else {
		a.pb.AccountResource.FrozenBalanceForEnergy.FrozenBalance += amount
		if expireTimeMs > a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime {
			a.pb.AccountResource.FrozenBalanceForEnergy.ExpireTime = expireTimeMs
		}
	}
}

func (a *Account) ClearFrozenEnergy() {
	if a.pb.AccountResource != nil {
		a.pb.AccountResource.FrozenBalanceForEnergy = nil
	}
}

// V1 delegation: bandwidth
func (a *Account) DelegatedFrozenBandwidth() int64  { return a.pb.DelegatedFrozenBalanceForBandwidth }
func (a *Account) SetDelegatedFrozenBandwidth(v int64) {
	a.pb.DelegatedFrozenBalanceForBandwidth = v
}
func (a *Account) AcquiredDelegatedFrozenBandwidth() int64 {
	return a.pb.AcquiredDelegatedFrozenBalanceForBandwidth
}
func (a *Account) SetAcquiredDelegatedFrozenBandwidth(v int64) {
	a.pb.AcquiredDelegatedFrozenBalanceForBandwidth = v
}

// V1 delegation: energy
func (a *Account) DelegatedFrozenEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.DelegatedFrozenBalanceForEnergy
}
func (a *Account) SetDelegatedFrozenEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.DelegatedFrozenBalanceForEnergy = v
}
func (a *Account) AcquiredDelegatedFrozenEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy
}
func (a *Account) SetAcquiredDelegatedFrozenEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.AcquiredDelegatedFrozenBalanceForEnergy = v
}
```

- [ ] **Step 4: Add V1 StateDB wrapper methods to `core/state/statedb.go`**

Insert after `AddFreezeV2` (~line 168), before `GetWitness`:

```go
// --- V1 Stake (Stake 1.0) StateDB methods ---

func (s *StateDB) FreezeV1Bandwidth(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFrozenBandwidth(amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) UnfreezeV1Bandwidth(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	s.journalAccount(addr, obj)
	refunded := obj.account.RemoveExpiredFrozenBandwidth(blockTimeMs)
	obj.markDirty()
	return refunded
}

func (s *StateDB) FreezeV1Energy(addr tcommon.Address, amount, expireTimeMs int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFrozenEnergy(amount, expireTimeMs)
	obj.markDirty()
}

func (s *StateDB) UnfreezeV1Energy(addr tcommon.Address, blockTimeMs int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	if obj.account.FrozenEnergyExpireTime() > blockTimeMs {
		return 0
	}
	amount := obj.account.FrozenEnergyAmount()
	if amount == 0 {
		return 0
	}
	s.journalAccount(addr, obj)
	obj.account.ClearFrozenEnergy()
	obj.markDirty()
	return amount
}

func (s *StateDB) GetDelegatedFrozenV1Bandwidth(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.DelegatedFrozenBandwidth()
}

func (s *StateDB) GetDelegatedFrozenV1Energy(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.DelegatedFrozenEnergy()
}

func (s *StateDB) FreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() + amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	recvObj.account.SetAcquiredDelegatedFrozenBandwidth(recvObj.account.AcquiredDelegatedFrozenBandwidth() + amount)
	recvObj.markDirty()
}

func (s *StateDB) UnfreezeV1DelegatedBandwidth(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenBandwidth(ownerObj.account.DelegatedFrozenBandwidth() - amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	v := recvObj.account.AcquiredDelegatedFrozenBandwidth() - amount
	if v < 0 {
		v = 0
	}
	recvObj.account.SetAcquiredDelegatedFrozenBandwidth(v)
	recvObj.markDirty()
}

func (s *StateDB) FreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() + amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	recvObj.account.SetAcquiredDelegatedFrozenEnergy(recvObj.account.AcquiredDelegatedFrozenEnergy() + amount)
	recvObj.markDirty()
}

func (s *StateDB) UnfreezeV1DelegatedEnergy(owner, receiver tcommon.Address, amount int64) {
	ownerObj := s.getStateObject(owner)
	if ownerObj == nil {
		return
	}
	s.journalAccount(owner, ownerObj)
	ownerObj.account.SetDelegatedFrozenEnergy(ownerObj.account.DelegatedFrozenEnergy() - amount)
	ownerObj.markDirty()

	recvObj := s.getStateObject(receiver)
	if recvObj == nil {
		return
	}
	s.journalAccount(receiver, recvObj)
	v := recvObj.account.AcquiredDelegatedFrozenEnergy() - amount
	if v < 0 {
		v = 0
	}
	recvObj.account.SetAcquiredDelegatedFrozenEnergy(v)
	recvObj.markDirty()
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run "TestFreezeV1|TestUnfreezeV1" -v -count=1`
Expected: all 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add core/types/account.go core/state/statedb.go core/state/statedb_v1_test.go
git commit -m "feat(state): add V1 Stake 1.0 frozen balance accessors and StateDB methods"
```

---

### Task 2: FreezeBalanceActuator (type 11)

**Files:**
- Create: `actuator/freeze_balance.go`
- Create: `actuator/freeze_balance_test.go`

- [ ] **Step 1: Write failing tests**

Create `actuator/freeze_balance_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeFreezeBalanceTx(ownerByte byte, amount, duration int64, resource corepb.ResourceCode, receiverByte *byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	fc := &contractpb.FreezeBalanceContract{
		OwnerAddress:   owner.Bytes(),
		FrozenBalance:  amount,
		FrozenDuration: duration,
		Resource:       resource,
	}
	if receiverByte != nil {
		recv := makeTestAddr(*receiverByte)
		fc.ReceiverAddress = recv.Bytes()
	}
	any, _ := anypb.New(fc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_FreezeBalanceContract, Parameter: any},
			},
		},
	})
}

func TestFreezeBalanceValidate_Success(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(1, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeBalanceValidate_InsufficientBalance(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 500_000)

	tx := makeFreezeBalanceTx(1, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestFreezeBalanceValidate_DurationTooShort(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)

	tx := makeFreezeBalanceTx(1, 1_000_000, 2, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for duration too short")
	}
}

func TestFreezeBalanceExecute_Bandwidth(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(1, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// Balance decreased
	if got := statedb.GetBalance(owner); got != 9_000_000 {
		t.Fatalf("balance: want 9000000, got %d", got)
	}

	// Frozen bandwidth added
	obj := statedb.GetStateObject(owner)
	if got := obj.TotalFrozenBandwidth(); got != 1_000_000 {
		t.Fatalf("frozen bandwidth: want 1000000, got %d", got)
	}
}

func TestFreezeBalanceExecute_Energy(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(2)
	seedAccount(statedb, owner, 10_000_000)

	tx := makeFreezeBalanceTx(2, 2_000_000, 3, corepb.ResourceCode_ENERGY, nil)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	if got := statedb.GetBalance(owner); got != 8_000_000 {
		t.Fatalf("balance: want 8000000, got %d", got)
	}

	obj := statedb.GetStateObject(owner)
	if got := obj.FrozenEnergyAmount(); got != 2_000_000 {
		t.Fatalf("frozen energy: want 2000000, got %d", got)
	}
}

func TestFreezeBalanceExecute_Delegated(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	receiver := makeTestAddr(4)
	seedAccount(statedb, owner, 10_000_000)
	seedAccount(statedb, receiver, 0)

	recvByte := byte(4)
	tx := makeFreezeBalanceTx(3, 1_000_000, 3, corepb.ResourceCode_BANDWIDTH, &recvByte)
	act := &FreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	if got := statedb.GetBalance(owner); got != 9_000_000 {
		t.Fatalf("owner balance: want 9000000, got %d", got)
	}

	ownerObj := statedb.GetStateObject(owner)
	if got := ownerObj.DelegatedFrozenBandwidth(); got != 1_000_000 {
		t.Fatalf("owner delegated: want 1000000, got %d", got)
	}
	recvObj := statedb.GetStateObject(receiver)
	if got := recvObj.AcquiredDelegatedFrozenBandwidth(); got != 1_000_000 {
		t.Fatalf("receiver acquired: want 1000000, got %d", got)
	}
}
```

**Note:** The tests call `statedb.GetStateObject(addr)` which returns `*Account` — this needs a thin public accessor on StateDB. Add to statedb.go:
```go
func (s *StateDB) GetStateObject(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	return obj.account
}
```
Add this in the same commit as the actuator if it doesn't already exist.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestFreezeBalance" -v -count=1`
Expected: compilation errors.

- [ ] **Step 3: Implement FreezeBalanceActuator**

Create `actuator/freeze_balance.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type FreezeBalanceActuator struct{}

func (a *FreezeBalanceActuator) getContract(ctx *Context) (*contractpb.FreezeBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	fc := &contractpb.FreezeBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(fc); err != nil {
		return nil, errors.New("failed to unmarshal FreezeBalanceContract")
	}
	return fc, nil
}

func (a *FreezeBalanceActuator) Validate(ctx *Context) error {
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance < 1_000_000 {
		return errors.New("frozen balance must be at least 1 TRX")
	}
	if fc.FrozenDuration < 3 {
		return errors.New("frozen duration must be at least 3 days")
	}
	if ctx.State.GetBalance(ownerAddr) < fc.FrozenBalance {
		return errors.New("insufficient balance")
	}
	if fc.Resource != corepb.ResourceCode_BANDWIDTH &&
		fc.Resource != corepb.ResourceCode_ENERGY &&
		fc.Resource != corepb.ResourceCode_TRON_POWER {
		return errors.New("invalid resource type")
	}
	if len(fc.ReceiverAddress) > 0 {
		receiverAddr := common.BytesToAddress(fc.ReceiverAddress)
		if !ctx.State.AccountExists(receiverAddr) {
			return errors.New("receiver account does not exist")
		}
	}
	return nil
}

func (a *FreezeBalanceActuator) Execute(ctx *Context) (*Result, error) {
	fc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if err := ctx.State.SubBalance(ownerAddr, fc.FrozenBalance); err != nil {
		return nil, err
	}

	expireTimeMs := ctx.BlockTime + fc.FrozenDuration*86_400_000
	delegated := len(fc.ReceiverAddress) > 0

	if !delegated {
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1Bandwidth(ownerAddr, fc.FrozenBalance, expireTimeMs)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1Energy(ownerAddr, fc.FrozenBalance, expireTimeMs)
		}
	} else {
		receiverAddr := common.BytesToAddress(fc.ReceiverAddress)
		switch fc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			ctx.State.FreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, fc.FrozenBalance)
		case corepb.ResourceCode_ENERGY:
			ctx.State.FreezeV1DelegatedEnergy(ownerAddr, receiverAddr, fc.FrozenBalance)
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Add `GetStateObject` to StateDB if not present**

Check if `GetStateObject` already exists in `core/state/statedb.go`. If not, add:

```go
func (s *StateDB) GetStateObject(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	return obj.account
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestFreezeBalance" -v -count=1`
Expected: all 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/freeze_balance.go actuator/freeze_balance_test.go core/state/statedb.go
git commit -m "feat(actuator): implement FreezeBalanceActuator (type 11) for Stake 1.0"
```

---

### Task 3: UnfreezeBalanceActuator (type 12)

**Files:**
- Create: `actuator/unfreeze_balance.go`
- Create: `actuator/unfreeze_balance_test.go`

- [ ] **Step 1: Write failing tests**

Create `actuator/unfreeze_balance_test.go`:

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeUnfreezeBalanceTx(ownerByte byte, resource corepb.ResourceCode, receiverByte *byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	uc := &contractpb.UnfreezeBalanceContract{
		OwnerAddress: owner.Bytes(),
		Resource:     resource,
	}
	if receiverByte != nil {
		recv := makeTestAddr(*receiverByte)
		uc.ReceiverAddress = recv.Bytes()
	}
	any, _ := anypb.New(uc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_UnfreezeBalanceContract, Parameter: any},
			},
		},
	})
}

func TestUnfreezeBalanceValidate_NotExpired(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 10_000_000)

	// Freeze with long expiry
	statedb.FreezeV1Bandwidth(owner, 1_000_000, 999_999_999)

	tx := makeUnfreezeBalanceTx(1, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx) // BlockTime = 1_000_000
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for not expired")
	}
}

func TestUnfreezeBalanceValidate_NoFrozen(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 10_000_000)

	tx := makeUnfreezeBalanceTx(1, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no frozen balance")
	}
}

func TestUnfreezeBalanceExecute_Bandwidth(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(2)
	seedAccount(statedb, owner, 10_000_000)

	// Freeze bandwidth — expires at 500_000 (before BlockTime of 1_000_000)
	statedb.FreezeV1Bandwidth(owner, 1_000_000, 500_000)

	tx := makeUnfreezeBalanceTx(2, corepb.ResourceCode_BANDWIDTH, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx) // BlockTime = 1_000_000

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// Balance restored (original 10M + 1M refunded)
	if got := statedb.GetBalance(owner); got != 11_000_000 {
		t.Fatalf("balance: want 11000000, got %d", got)
	}
}

func TestUnfreezeBalanceExecute_Energy(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(3)
	seedAccount(statedb, owner, 10_000_000)

	statedb.FreezeV1Energy(owner, 2_000_000, 500_000)

	tx := makeUnfreezeBalanceTx(3, corepb.ResourceCode_ENERGY, nil)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}
	if got := statedb.GetBalance(owner); got != 12_000_000 {
		t.Fatalf("balance: want 12000000, got %d", got)
	}
}

func TestUnfreezeBalanceExecute_Delegated(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(4)
	receiver := makeTestAddr(5)
	seedAccount(statedb, owner, 10_000_000)
	seedAccount(statedb, receiver, 0)

	statedb.FreezeV1DelegatedBandwidth(owner, receiver, 1_000_000)

	recvByte := byte(5)
	tx := makeUnfreezeBalanceTx(4, corepb.ResourceCode_BANDWIDTH, &recvByte)
	act := &UnfreezeBalanceActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// Owner gets delegated amount back as balance
	if got := statedb.GetBalance(owner); got != 11_000_000 {
		t.Fatalf("owner balance: want 11000000, got %d", got)
	}

	// Delegation fields cleared
	if got := statedb.GetDelegatedFrozenV1Bandwidth(owner); got != 0 {
		t.Fatalf("owner delegated: want 0, got %d", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestUnfreezeBalance" -v -count=1`
Expected: compilation errors.

- [ ] **Step 3: Implement UnfreezeBalanceActuator**

Create `actuator/unfreeze_balance.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UnfreezeBalanceActuator struct{}

func (a *UnfreezeBalanceActuator) getContract(ctx *Context) (*contractpb.UnfreezeBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	uc := &contractpb.UnfreezeBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(uc); err != nil {
		return nil, errors.New("failed to unmarshal UnfreezeBalanceContract")
	}
	return uc, nil
}

func (a *UnfreezeBalanceActuator) Validate(ctx *Context) error {
	uc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}

	delegated := len(uc.ReceiverAddress) > 0
	acct := ctx.State.GetStateObject(ownerAddr)
	if acct == nil {
		return errors.New("account not found")
	}

	if !delegated {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			hasExpired := false
			for _, f := range acct.FrozenBandwidthList() {
				if f.ExpireTime <= ctx.BlockTime {
					hasExpired = true
					break
				}
			}
			if !hasExpired {
				return errors.New("no expired frozen bandwidth")
			}
		case corepb.ResourceCode_ENERGY:
			if acct.FrozenEnergyAmount() == 0 {
				return errors.New("no frozen energy")
			}
			if acct.FrozenEnergyExpireTime() > ctx.BlockTime {
				return errors.New("frozen energy not expired")
			}
		default:
			return errors.New("invalid resource type")
		}
	} else {
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			if acct.DelegatedFrozenBandwidth() <= 0 {
				return errors.New("no delegated frozen bandwidth")
			}
		case corepb.ResourceCode_ENERGY:
			if acct.DelegatedFrozenEnergy() <= 0 {
				return errors.New("no delegated frozen energy")
			}
		default:
			return errors.New("invalid resource type")
		}
	}
	return nil
}

func (a *UnfreezeBalanceActuator) Execute(ctx *Context) (*Result, error) {
	uc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	delegated := len(uc.ReceiverAddress) > 0

	if !delegated {
		var refunded int64
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			refunded = ctx.State.UnfreezeV1Bandwidth(ownerAddr, ctx.BlockTime)
		case corepb.ResourceCode_ENERGY:
			refunded = ctx.State.UnfreezeV1Energy(ownerAddr, ctx.BlockTime)
		}
		ctx.State.AddBalance(ownerAddr, refunded)
	} else {
		receiverAddr := common.BytesToAddress(uc.ReceiverAddress)
		var delegatedAmt int64
		switch uc.Resource {
		case corepb.ResourceCode_BANDWIDTH, corepb.ResourceCode_TRON_POWER:
			delegatedAmt = ctx.State.GetDelegatedFrozenV1Bandwidth(ownerAddr)
			ctx.State.UnfreezeV1DelegatedBandwidth(ownerAddr, receiverAddr, delegatedAmt)
		case corepb.ResourceCode_ENERGY:
			delegatedAmt = ctx.State.GetDelegatedFrozenV1Energy(ownerAddr)
			ctx.State.UnfreezeV1DelegatedEnergy(ownerAddr, receiverAddr, delegatedAmt)
		}
		ctx.State.AddBalance(ownerAddr, delegatedAmt)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestUnfreezeBalance" -v -count=1`
Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/unfreeze_balance.go actuator/unfreeze_balance_test.go
git commit -m "feat(actuator): implement UnfreezeBalanceActuator (type 12) for Stake 1.0"
```

---

### Task 4: Market rawdb Schema + Accessors

**Files:**
- Modify: `core/rawdb/schema.go`
- Create: `core/rawdb/accessors_market.go`
- Create: `core/rawdb/accessors_market_test.go`

- [ ] **Step 1: Write failing tests**

Create `core/rawdb/accessors_market_test.go`:

```go
package rawdb

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWriteReadMarketOrder(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	orderID := []byte("order-1")
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            []byte{0x41, 0x01},
		SellTokenId:             []byte("1000001"),
		SellTokenQuantity:       100,
		BuyTokenId:              []byte("_"),
		BuyTokenQuantity:        50,
		SellTokenQuantityRemain: 100,
		State:                   corepb.MarketOrder_ACTIVE,
	}
	if err := WriteMarketOrder(db, orderID, order); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketOrder(db, orderID)
	if got == nil {
		t.Fatal("order not found")
	}
	if got.SellTokenQuantity != 100 || got.BuyTokenQuantity != 50 {
		t.Fatalf("quantities: want {100,50}, got {%d,%d}", got.SellTokenQuantity, got.BuyTokenQuantity)
	}
	if got.State != corepb.MarketOrder_ACTIVE {
		t.Fatalf("state: want ACTIVE, got %v", got.State)
	}
}

func TestReadMarketOrder_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	got := ReadMarketOrder(db, []byte("nonexistent"))
	if got != nil {
		t.Fatal("expected nil for missing order")
	}
}

func TestWriteReadMarketAccountOrder(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := []byte{0x41, 0x01}
	mao := &corepb.MarketAccountOrder{
		OwnerAddress: addr,
		Orders:       [][]byte{[]byte("o1"), []byte("o2")},
		Count:        2,
		TotalCount:   2,
	}
	if err := WriteMarketAccountOrder(db, addr, mao); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketAccountOrder(db, addr)
	if len(got.Orders) != 2 {
		t.Fatalf("orders: want 2, got %d", len(got.Orders))
	}
	if got.Count != 2 || got.TotalCount != 2 {
		t.Fatalf("counts: want {2,2}, got {%d,%d}", got.Count, got.TotalCount)
	}
}

func TestWriteReadMarketOrderBook(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sellToken := []byte("1000001")
	buyToken := []byte("_")
	pk := PriceKey(2, 1)

	list := &corepb.MarketOrderIdList{
		Head: []byte("o1"),
		Tail: []byte("o2"),
	}
	if err := WriteMarketOrderBook(db, sellToken, buyToken, pk, list); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketOrderBook(db, sellToken, buyToken, pk)
	if got == nil {
		t.Fatal("order book not found")
	}
	if !bytes.Equal(got.Head, []byte("o1")) || !bytes.Equal(got.Tail, []byte("o2")) {
		t.Fatalf("head/tail mismatch")
	}

	// Delete
	if err := DeleteMarketOrderBook(db, sellToken, buyToken, pk); err != nil {
		t.Fatal(err)
	}
	got = ReadMarketOrderBook(db, sellToken, buyToken, pk)
	if got != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestWriteReadMarketPriceList(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	sellToken := []byte("1000001")
	buyToken := []byte("_")
	pl := &corepb.MarketPriceList{
		SellTokenId: sellToken,
		BuyTokenId:  buyToken,
		Prices: []*corepb.MarketPrice{
			{SellTokenQuantity: 2, BuyTokenQuantity: 1},
			{SellTokenQuantity: 3, BuyTokenQuantity: 1},
		},
	}
	if err := WriteMarketPriceList(db, sellToken, buyToken, pl); err != nil {
		t.Fatal(err)
	}
	got := ReadMarketPriceList(db, sellToken, buyToken)
	if len(got.Prices) != 2 {
		t.Fatalf("prices: want 2, got %d", len(got.Prices))
	}
}

func TestPriceKey_Normalization(t *testing.T) {
	// 200/100 and 2/1 should produce the same key
	pk1 := PriceKey(200, 100)
	pk2 := PriceKey(2, 1)
	if pk1 != pk2 {
		t.Fatalf("price keys should match: %v != %v", pk1, pk2)
	}

	// 3/2 and 6/4 should match
	pk3 := PriceKey(3, 2)
	pk4 := PriceKey(6, 4)
	if pk3 != pk4 {
		t.Fatalf("price keys should match: %v != %v", pk3, pk4)
	}

	// 2/1 and 3/2 should differ
	if pk2 == pk3 {
		t.Fatal("different prices should have different keys")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run "TestWriteReadMarket|TestReadMarketOrder_NotFound|TestPriceKey" -v -count=1`
Expected: compilation errors.

- [ ] **Step 3: Add market prefixes and key functions to `core/rawdb/schema.go`**

Add after the existing `assetIssueTimePrefix` variable block:

```go
	marketOrderPrefix        = []byte("mo-")
	marketAccountOrderPrefix = []byte("mao-")
	marketOrderBookPrefix    = []byte("mop-")
	marketPriceListPrefix    = []byte("mpl-")
```

Add after the existing `assetIssueTimeKey` function:

```go
func gcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// PriceKey normalizes a {sellQty, buyQty} pair by GCD and encodes as 16 bytes.
func PriceKey(sellQty, buyQty int64) [16]byte {
	g := gcdInt64(sellQty, buyQty)
	var k [16]byte
	binary.BigEndian.PutUint64(k[:8], uint64(sellQty/g))
	binary.BigEndian.PutUint64(k[8:], uint64(buyQty/g))
	return k
}

func marketOrderKey(orderID []byte) []byte {
	return append(append([]byte{}, marketOrderPrefix...), orderID...)
}

func marketAccountOrderKey(ownerAddr []byte) []byte {
	return append(append([]byte{}, marketAccountOrderPrefix...), ownerAddr...)
}

func marketOrderBookKey(sellTokenID, buyTokenID []byte, pk [16]byte) []byte {
	k := append(append([]byte{}, marketOrderBookPrefix...), sellTokenID...)
	k = append(k, '|')
	k = append(k, buyTokenID...)
	k = append(k, '|')
	return append(k, pk[:]...)
}

func marketPriceListKey(sellTokenID, buyTokenID []byte) []byte {
	k := append(append([]byte{}, marketPriceListPrefix...), sellTokenID...)
	k = append(k, '|')
	return append(k, buyTokenID...)
}
```

- [ ] **Step 4: Create `core/rawdb/accessors_market.go`**

```go
package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func WriteMarketOrder(db ethdb.KeyValueWriter, orderID []byte, order *corepb.MarketOrder) error {
	data, err := proto.Marshal(order)
	if err != nil {
		return err
	}
	return db.Put(marketOrderKey(orderID), data)
}

func ReadMarketOrder(db ethdb.KeyValueReader, orderID []byte) *corepb.MarketOrder {
	data, err := db.Get(marketOrderKey(orderID))
	if err != nil || len(data) == 0 {
		return nil
	}
	var o corepb.MarketOrder
	if err := proto.Unmarshal(data, &o); err != nil {
		return nil
	}
	return &o
}

func WriteMarketAccountOrder(db ethdb.KeyValueWriter, ownerAddr []byte, mao *corepb.MarketAccountOrder) error {
	data, err := proto.Marshal(mao)
	if err != nil {
		return err
	}
	return db.Put(marketAccountOrderKey(ownerAddr), data)
}

func ReadMarketAccountOrder(db ethdb.KeyValueReader, ownerAddr []byte) *corepb.MarketAccountOrder {
	data, err := db.Get(marketAccountOrderKey(ownerAddr))
	if err != nil || len(data) == 0 {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	var mao corepb.MarketAccountOrder
	if err := proto.Unmarshal(data, &mao); err != nil {
		return &corepb.MarketAccountOrder{OwnerAddress: ownerAddr}
	}
	return &mao
}

func WriteMarketOrderBook(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pk [16]byte, list *corepb.MarketOrderIdList) error {
	data, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	return db.Put(marketOrderBookKey(sellTokenID, buyTokenID, pk), data)
}

func ReadMarketOrderBook(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte, pk [16]byte) *corepb.MarketOrderIdList {
	data, err := db.Get(marketOrderBookKey(sellTokenID, buyTokenID, pk))
	if err != nil || len(data) == 0 {
		return nil
	}
	var list corepb.MarketOrderIdList
	if err := proto.Unmarshal(data, &list); err != nil {
		return nil
	}
	return &list
}

func DeleteMarketOrderBook(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pk [16]byte) error {
	return db.Delete(marketOrderBookKey(sellTokenID, buyTokenID, pk))
}

func WriteMarketPriceList(db ethdb.KeyValueWriter, sellTokenID, buyTokenID []byte, pl *corepb.MarketPriceList) error {
	data, err := proto.Marshal(pl)
	if err != nil {
		return err
	}
	return db.Put(marketPriceListKey(sellTokenID, buyTokenID), data)
}

func ReadMarketPriceList(db ethdb.KeyValueReader, sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	data, err := db.Get(marketPriceListKey(sellTokenID, buyTokenID))
	if err != nil || len(data) == 0 {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	var pl corepb.MarketPriceList
	if err := proto.Unmarshal(data, &pl); err != nil {
		return &corepb.MarketPriceList{SellTokenId: sellTokenID, BuyTokenId: buyTokenID}
	}
	return &pl
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run "TestWriteReadMarket|TestReadMarketOrder_NotFound|TestPriceKey" -v -count=1`
Expected: all 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add core/rawdb/schema.go core/rawdb/accessors_market.go core/rawdb/accessors_market_test.go
git commit -m "feat(rawdb): add market order storage schema and accessors"
```

---

### Task 5: MarketSellAssetActuator + Matching Engine (type 52)

**Files:**
- Create: `actuator/market_sell_asset.go`
- Create: `actuator/market_sell_asset_test.go`

- [ ] **Step 1: Write failing tests**

Create `actuator/market_sell_asset_test.go`:

```go
package actuator

import (
	"bytes"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeMarketSellTx(ownerByte byte, sellTokenID []byte, sellQty int64, buyTokenID []byte, buyQty int64) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	mc := &contractpb.MarketSellAssetContract{
		OwnerAddress:      owner.Bytes(),
		SellTokenId:       sellTokenID,
		SellTokenQuantity: sellQty,
		BuyTokenId:        buyTokenID,
		BuyTokenQuantity:  buyQty,
	}
	any, _ := anypb.New(mc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_MarketSellAssetContract, Parameter: any},
			},
		},
	})
}

func TestMarketSellAssetValidate_Success(t *testing.T) {
	statedb := setupStateDB(t)
	seller := makeTestAddr(1)
	seedAccount(statedb, seller, 0)
	statedb.AddTRC10Balance(seller, 1000001, 500)

	tx := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	act := &MarketSellAssetActuator{}
	db := ethrawdb.NewMemoryDatabase()
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMarketSellAssetValidate_InsufficientBalance(t *testing.T) {
	statedb := setupStateDB(t)
	seller := makeTestAddr(1)
	seedAccount(statedb, seller, 0)
	statedb.AddTRC10Balance(seller, 1000001, 50)

	tx := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	act := &MarketSellAssetActuator{}
	db := ethrawdb.NewMemoryDatabase()
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestMarketSellAssetValidate_SameToken(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 1000)

	tx := makeMarketSellTx(1, []byte("_"), 100, []byte("_"), 50)
	act := &MarketSellAssetActuator{}
	db := ethrawdb.NewMemoryDatabase()
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for same token")
	}
}

func TestMarketSellAssetExecute_NoMatch(t *testing.T) {
	statedb := setupStateDB(t)
	seller := makeTestAddr(1)
	seedAccount(statedb, seller, 0)
	statedb.AddTRC10Balance(seller, 1000001, 500)

	tx := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	act := &MarketSellAssetActuator{}
	db := ethrawdb.NewMemoryDatabase()
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// Seller's TRC10 balance decreased
	if got := statedb.GetTRC10Balance(seller, 1000001); got != 400 {
		t.Fatalf("TRC10 balance: want 400, got %d", got)
	}

	// Order in account order list
	mao := rawdb.ReadMarketAccountOrder(db, seller.Bytes())
	if len(mao.Orders) != 1 {
		t.Fatalf("account orders: want 1, got %d", len(mao.Orders))
	}

	// Order in DB with ACTIVE state
	order := rawdb.ReadMarketOrder(db, mao.Orders[0])
	if order == nil {
		t.Fatal("order not found")
	}
	if order.State != corepb.MarketOrder_ACTIVE {
		t.Fatalf("state: want ACTIVE, got %v", order.State)
	}
	if order.SellTokenQuantityRemain != 100 {
		t.Fatalf("remaining: want 100, got %d", order.SellTokenQuantityRemain)
	}
}

func TestMarketSellAssetExecute_FullMatch(t *testing.T) {
	statedb := setupStateDB(t)
	db := ethrawdb.NewMemoryDatabase()

	// Seller A: sells 100 TRX for 200 TRC10
	sellerA := makeTestAddr(1)
	seedAccount(statedb, sellerA, 1000)

	txA := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	actA := &MarketSellAssetActuator{}
	ctxA := setupContext(t, statedb, txA)
	ctxA.DB = db
	if _, err := actA.Execute(ctxA); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	// Seller B: sells 200 TRC10 for 100 TRX — should fully match A
	sellerB := makeTestAddr(2)
	seedAccount(statedb, sellerB, 0)
	statedb.AddTRC10Balance(sellerB, 1000001, 500)

	txB := makeMarketSellTx(2, []byte("1000001"), 200, []byte("_"), 100)
	actB := &MarketSellAssetActuator{}
	ctxB := setupContext(t, statedb, txB)
	ctxB.DB = db
	result, err := actB.Execute(ctxB)
	if err != nil {
		t.Fatalf("execute B: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// Seller A got 200 TRC10
	if got := statedb.GetTRC10Balance(sellerA, 1000001); got != 200 {
		t.Fatalf("A TRC10: want 200, got %d", got)
	}

	// Seller B got 100 TRX
	if got := statedb.GetBalance(sellerB); got != 100 {
		t.Fatalf("B TRX: want 100, got %d", got)
	}

	// B's TRC10 balance: started 500, sold 200 → 300
	if got := statedb.GetTRC10Balance(sellerB, 1000001); got != 300 {
		t.Fatalf("B TRC10: want 300, got %d", got)
	}

	// Order A should be INACTIVE (fully filled)
	maoA := rawdb.ReadMarketAccountOrder(db, sellerA.Bytes())
	orderA := rawdb.ReadMarketOrder(db, maoA.Orders[0])
	if orderA.State != corepb.MarketOrder_INACTIVE {
		t.Fatalf("order A state: want INACTIVE, got %v", orderA.State)
	}

	// Order B should be INACTIVE (fully matched)
	maoB := rawdb.ReadMarketAccountOrder(db, sellerB.Bytes())
	orderB := rawdb.ReadMarketOrder(db, maoB.Orders[0])
	if orderB.State != corepb.MarketOrder_INACTIVE {
		t.Fatalf("order B state: want INACTIVE, got %v", orderB.State)
	}
}

func TestMarketSellAssetExecute_PartialMatch(t *testing.T) {
	statedb := setupStateDB(t)
	db := ethrawdb.NewMemoryDatabase()

	// Seller A: sells 100 TRX for 200 TRC10 (rate: 1 TRX = 2 TRC10)
	sellerA := makeTestAddr(1)
	seedAccount(statedb, sellerA, 1000)

	txA := makeMarketSellTx(1, []byte("_"), 100, []byte("1000001"), 200)
	actA := &MarketSellAssetActuator{}
	ctxA := setupContext(t, statedb, txA)
	ctxA.DB = db
	if _, err := actA.Execute(ctxA); err != nil {
		t.Fatalf("execute A: %v", err)
	}

	// Seller B: sells 400 TRC10 for 200 TRX — only partially matches A
	// A can only give 100 TRX for 200 TRC10, so B fills 200 TRC10 → gets 100 TRX
	// B has 200 TRC10 remaining in the book
	sellerB := makeTestAddr(2)
	seedAccount(statedb, sellerB, 0)
	statedb.AddTRC10Balance(sellerB, 1000001, 500)

	txB := makeMarketSellTx(2, []byte("1000001"), 400, []byte("_"), 200)
	actB := &MarketSellAssetActuator{}
	ctxB := setupContext(t, statedb, txB)
	ctxB.DB = db
	result, err := actB.Execute(ctxB)
	if err != nil {
		t.Fatalf("execute B: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// A got 200 TRC10
	if got := statedb.GetTRC10Balance(sellerA, 1000001); got != 200 {
		t.Fatalf("A TRC10: want 200, got %d", got)
	}

	// B got 100 TRX
	if got := statedb.GetBalance(sellerB); got != 100 {
		t.Fatalf("B TRX: want 100, got %d", got)
	}

	// Order B is still ACTIVE with 200 remaining
	maoB := rawdb.ReadMarketAccountOrder(db, sellerB.Bytes())
	orderB := rawdb.ReadMarketOrder(db, maoB.Orders[0])
	if orderB.State != corepb.MarketOrder_ACTIVE {
		t.Fatalf("order B state: want ACTIVE, got %v", orderB.State)
	}
	if orderB.SellTokenQuantityRemain != 200 {
		t.Fatalf("order B remaining: want 200, got %d", orderB.SellTokenQuantityRemain)
	}

	// Suppress unused import warnings
	_ = bytes.Equal
	_ = tcommon.Address{}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestMarketSellAsset" -v -count=1`
Expected: compilation errors.

- [ ] **Step 3: Implement MarketSellAssetActuator with matching engine**

Create `actuator/market_sell_asset.go`:

```go
package actuator

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type MarketSellAssetActuator struct{}

func (a *MarketSellAssetActuator) getContract(ctx *Context) (*contractpb.MarketSellAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	mc := &contractpb.MarketSellAssetContract{}
	if err := contract.Parameter.UnmarshalTo(mc); err != nil {
		return nil, errors.New("failed to unmarshal MarketSellAssetContract")
	}
	return mc, nil
}

func (a *MarketSellAssetActuator) Validate(ctx *Context) error {
	mc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(mc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if mc.SellTokenQuantity <= 0 {
		return errors.New("sell token quantity must be positive")
	}
	if mc.BuyTokenQuantity <= 0 {
		return errors.New("buy token quantity must be positive")
	}
	if bytes.Equal(mc.SellTokenId, mc.BuyTokenId) {
		return errors.New("cannot sell token for itself")
	}
	if err := checkTokenBalance(ctx.State, ownerAddr, mc.SellTokenId, mc.SellTokenQuantity); err != nil {
		return err
	}
	return nil
}

func (a *MarketSellAssetActuator) Execute(ctx *Context) (*Result, error) {
	mc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(mc.OwnerAddress)

	// 1. Deduct sell tokens from owner
	if err := deductToken(ctx.State, ownerAddr, mc.SellTokenId, mc.SellTokenQuantity); err != nil {
		return nil, fmt.Errorf("deduct sell tokens: %w", err)
	}

	// 2. Generate order ID
	txHash := ctx.Tx.Hash()
	orderID := generateOrderID(ownerAddr, txHash)

	// 3. Create order
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            mc.OwnerAddress,
		CreateTime:              ctx.BlockTime,
		SellTokenId:             mc.SellTokenId,
		SellTokenQuantity:       mc.SellTokenQuantity,
		BuyTokenId:              mc.BuyTokenId,
		BuyTokenQuantity:        mc.BuyTokenQuantity,
		SellTokenQuantityRemain: mc.SellTokenQuantity,
		State:                   corepb.MarketOrder_ACTIVE,
	}

	// 4. Match against existing orders
	remaining, err := matchOrder(ctx.DB, ctx.State, order)
	if err != nil {
		return nil, fmt.Errorf("match order: %w", err)
	}

	// 5. If remaining > 0, add to order book
	if remaining > 0 {
		order.SellTokenQuantityRemain = remaining
		if err := addOrderToBook(ctx.DB, order); err != nil {
			return nil, fmt.Errorf("add to book: %w", err)
		}
	} else {
		order.State = corepb.MarketOrder_INACTIVE
	}

	// 6. Persist order
	if err := rawdb.WriteMarketOrder(ctx.DB, orderID, order); err != nil {
		return nil, fmt.Errorf("write order: %w", err)
	}

	// 7. Update account order list
	mao := rawdb.ReadMarketAccountOrder(ctx.DB, mc.OwnerAddress)
	mao.Orders = append(mao.Orders, orderID)
	mao.TotalCount++
	if order.State == corepb.MarketOrder_ACTIVE {
		mao.Count++
	}
	if err := rawdb.WriteMarketAccountOrder(ctx.DB, mc.OwnerAddress, mao); err != nil {
		return nil, fmt.Errorf("write account order: %w", err)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}

// --- Helpers ---

func gcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func generateOrderID(ownerAddr tcommon.Address, txHash tcommon.Hash) []byte {
	input := append(ownerAddr[:], txHash[:]...)
	h := tcommon.Keccak256(input)
	return h[:]
}

func checkTokenBalance(s *state.StateDB, addr tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		if s.GetBalance(addr) < amount {
			return errors.New("insufficient TRX balance")
		}
	} else {
		id, err := strconv.ParseInt(string(tokenID), 10, 64)
		if err != nil {
			return errors.New("invalid token ID")
		}
		if s.GetTRC10Balance(addr, id) < amount {
			return errors.New("insufficient TRC10 balance")
		}
	}
	return nil
}

func transferToken(s *state.StateDB, to tcommon.Address, tokenID []byte, amount int64) {
	if bytes.Equal(tokenID, []byte("_")) {
		s.AddBalance(to, amount)
	} else {
		id, _ := strconv.ParseInt(string(tokenID), 10, 64)
		s.AddTRC10Balance(to, id, amount)
	}
}

func deductToken(s *state.StateDB, from tcommon.Address, tokenID []byte, amount int64) error {
	if bytes.Equal(tokenID, []byte("_")) {
		return s.SubBalance(from, amount)
	}
	id, _ := strconv.ParseInt(string(tokenID), 10, 64)
	return s.SubTRC10Balance(from, id, amount)
}

// matchOrder tries to match the incoming order against the opposite order book.
// Returns the remaining sell quantity not yet filled.
func matchOrder(db ethdb.KeyValueStore, s *state.StateDB, incoming *corepb.MarketOrder) (int64, error) {
	remaining := incoming.SellTokenQuantity
	var totalBuyReceived int64
	ownerAddr := tcommon.BytesToAddress(incoming.OwnerAddress)

	// Get opposite price list: orders selling incoming.BuyToken for incoming.SellToken
	oppPriceList := rawdb.ReadMarketPriceList(db, incoming.BuyTokenId, incoming.SellTokenId)
	if len(oppPriceList.Prices) == 0 {
		return remaining, nil
	}

	// Filter compatible prices
	var compatible []*corepb.MarketPrice
	for _, p := range oppPriceList.Prices {
		// Compatible if: oppSell * inSell >= inBuy * oppBuy
		lhs := new(big.Int).Mul(big.NewInt(p.SellTokenQuantity), big.NewInt(incoming.SellTokenQuantity))
		rhs := new(big.Int).Mul(big.NewInt(incoming.BuyTokenQuantity), big.NewInt(p.BuyTokenQuantity))
		if lhs.Cmp(rhs) >= 0 {
			compatible = append(compatible, p)
		}
	}
	if len(compatible) == 0 {
		return remaining, nil
	}

	// Sort: best for incoming = highest oppSell/oppBuy ratio (descending)
	sort.Slice(compatible, func(i, j int) bool {
		li := new(big.Int).Mul(big.NewInt(compatible[i].SellTokenQuantity), big.NewInt(compatible[j].BuyTokenQuantity))
		ri := new(big.Int).Mul(big.NewInt(compatible[j].SellTokenQuantity), big.NewInt(compatible[i].BuyTokenQuantity))
		return li.Cmp(ri) > 0
	})

	var pricesToRemove [][16]byte

	for _, price := range compatible {
		if remaining <= 0 {
			break
		}
		pk := rawdb.PriceKey(price.SellTokenQuantity, price.BuyTokenQuantity)
		list := rawdb.ReadMarketOrderBook(db, incoming.BuyTokenId, incoming.SellTokenId, pk)
		if list == nil {
			pricesToRemove = append(pricesToRemove, pk)
			continue
		}

		currentID := list.Head

		for len(currentID) > 0 && remaining > 0 {
			existing := rawdb.ReadMarketOrder(db, currentID)
			if existing == nil {
				currentID = nil
				break
			}

			existingRemain := existing.SellTokenQuantityRemain

			// Check if we can fully fill existing order
			lhs := new(big.Int).Mul(big.NewInt(remaining), big.NewInt(incoming.SellTokenQuantity))
			rhs := new(big.Int).Mul(big.NewInt(existingRemain), big.NewInt(incoming.BuyTokenQuantity))

			if lhs.Cmp(rhs) >= 0 {
				// Full fill of existing order at maker price
				fillBuy := existingRemain
				fillSell := fillBuy * price.BuyTokenQuantity / price.SellTokenQuantity

				transferToken(s, tcommon.BytesToAddress(existing.OwnerAddress), incoming.SellTokenId, fillSell)
				totalBuyReceived += fillBuy
				remaining -= fillSell

				existing.SellTokenQuantityRemain = 0
				existing.State = corepb.MarketOrder_INACTIVE
				if err := rawdb.WriteMarketOrder(db, currentID, existing); err != nil {
					return 0, err
				}

				currentID = existing.Next
			} else {
				// Partial fill — incoming fully satisfied
				fillSell := remaining
				fillBuy := fillSell * price.SellTokenQuantity / price.BuyTokenQuantity

				transferToken(s, tcommon.BytesToAddress(existing.OwnerAddress), incoming.SellTokenId, fillSell)
				totalBuyReceived += fillBuy

				existing.SellTokenQuantityRemain -= fillBuy
				if err := rawdb.WriteMarketOrder(db, currentID, existing); err != nil {
					return 0, err
				}

				remaining = 0
			}
		}

		// Update order book for this price
		if len(currentID) == 0 {
			// All orders consumed
			if err := rawdb.DeleteMarketOrderBook(db, incoming.BuyTokenId, incoming.SellTokenId, pk); err != nil {
				return 0, err
			}
			pricesToRemove = append(pricesToRemove, pk)
		} else if !bytes.Equal(list.Head, currentID) {
			// Some orders consumed — update head
			list.Head = currentID
			if err := rawdb.WriteMarketOrderBook(db, incoming.BuyTokenId, incoming.SellTokenId, pk, list); err != nil {
				return 0, err
			}
		}
	}

	// Remove exhausted prices from oppPriceList
	if len(pricesToRemove) > 0 {
		removeSet := make(map[[16]byte]bool)
		for _, pk := range pricesToRemove {
			removeSet[pk] = true
		}
		var kept []*corepb.MarketPrice
		for _, p := range oppPriceList.Prices {
			if !removeSet[rawdb.PriceKey(p.SellTokenQuantity, p.BuyTokenQuantity)] {
				kept = append(kept, p)
			}
		}
		oppPriceList.Prices = kept
		if err := rawdb.WriteMarketPriceList(db, incoming.BuyTokenId, incoming.SellTokenId, oppPriceList); err != nil {
			return 0, err
		}
	}

	// Credit incoming with accumulated buy tokens
	if totalBuyReceived > 0 {
		transferToken(s, ownerAddr, incoming.BuyTokenId, totalBuyReceived)
	}

	return remaining, nil
}

func addOrderToBook(db ethdb.KeyValueStore, order *corepb.MarketOrder) error {
	g := gcdInt64(order.SellTokenQuantity, order.BuyTokenQuantity)
	normSell := order.SellTokenQuantity / g
	normBuy := order.BuyTokenQuantity / g
	pk := rawdb.PriceKey(order.SellTokenQuantity, order.BuyTokenQuantity)

	// Ensure price in price list
	pl := rawdb.ReadMarketPriceList(db, order.SellTokenId, order.BuyTokenId)
	found := false
	for _, p := range pl.Prices {
		if p.SellTokenQuantity == normSell && p.BuyTokenQuantity == normBuy {
			found = true
			break
		}
	}
	if !found {
		pl.Prices = append(pl.Prices, &corepb.MarketPrice{
			SellTokenQuantity: normSell,
			BuyTokenQuantity:  normBuy,
		})
		if err := rawdb.WriteMarketPriceList(db, order.SellTokenId, order.BuyTokenId, pl); err != nil {
			return err
		}
	}

	// Append to linked list at this price
	list := rawdb.ReadMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk)
	if list == nil {
		list = &corepb.MarketOrderIdList{Head: order.OrderId, Tail: order.OrderId}
	} else {
		// Update previous tail's Next pointer
		if len(list.Tail) > 0 {
			prevTail := rawdb.ReadMarketOrder(db, list.Tail)
			if prevTail != nil {
				prevTail.Next = order.OrderId
				if err := rawdb.WriteMarketOrder(db, list.Tail, prevTail); err != nil {
					return err
				}
			}
		}
		order.Prev = list.Tail
		list.Tail = order.OrderId
	}
	return rawdb.WriteMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk, list)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestMarketSellAsset" -v -count=1`
Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/market_sell_asset.go actuator/market_sell_asset_test.go
git commit -m "feat(actuator): implement MarketSellAssetActuator (type 52) with matching engine"
```

---

### Task 6: MarketCancelOrderActuator (type 53)

**Files:**
- Create: `actuator/market_cancel_order.go`
- Create: `actuator/market_cancel_order_test.go`

- [ ] **Step 1: Write failing tests**

Create `actuator/market_cancel_order_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeMarketCancelTx(ownerByte byte, orderID []byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	mc := &contractpb.MarketCancelOrderContract{
		OwnerAddress: owner.Bytes(),
		OrderId:      orderID,
	}
	any, _ := anypb.New(mc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_MarketCancelOrderContract, Parameter: any},
			},
		},
	})
}

func TestMarketCancelOrderValidate_NotOwner(t *testing.T) {
	statedb := setupStateDB(t)
	db := ethrawdb.NewMemoryDatabase()

	// Create an order owned by addr 1
	ownerAddr := makeTestAddr(1)
	seedAccount(statedb, ownerAddr, 0)
	orderID := []byte("test-order-id")
	order := &corepb.MarketOrder{
		OrderId:                 orderID,
		OwnerAddress:            ownerAddr.Bytes(),
		SellTokenId:             []byte("1000001"),
		SellTokenQuantity:       100,
		BuyTokenId:              []byte("_"),
		BuyTokenQuantity:        50,
		SellTokenQuantityRemain: 100,
		State:                   corepb.MarketOrder_ACTIVE,
	}
	rawdb.WriteMarketOrder(db, orderID, order)

	// Try to cancel as addr 2
	seedAccount(statedb, makeTestAddr(2), 0)
	tx := makeMarketCancelTx(2, orderID)
	act := &MarketCancelOrderActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for not owner")
	}
}

func TestMarketCancelOrderValidate_AlreadyInactive(t *testing.T) {
	statedb := setupStateDB(t)
	db := ethrawdb.NewMemoryDatabase()

	ownerAddr := makeTestAddr(1)
	seedAccount(statedb, ownerAddr, 0)
	orderID := []byte("test-order-id")
	order := &corepb.MarketOrder{
		OrderId:      orderID,
		OwnerAddress: ownerAddr.Bytes(),
		State:        corepb.MarketOrder_INACTIVE,
	}
	rawdb.WriteMarketOrder(db, orderID, order)

	tx := makeMarketCancelTx(1, orderID)
	act := &MarketCancelOrderActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for inactive order")
	}
}

func TestMarketCancelOrderExecute_ReturnsTokens(t *testing.T) {
	statedb := setupStateDB(t)
	db := ethrawdb.NewMemoryDatabase()

	// Place an order first using the sell actuator
	seller := makeTestAddr(1)
	seedAccount(statedb, seller, 0)
	statedb.AddTRC10Balance(seller, 1000001, 500)

	txSell := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	actSell := &MarketSellAssetActuator{}
	ctxSell := setupContext(t, statedb, txSell)
	ctxSell.DB = db
	if _, err := actSell.Execute(ctxSell); err != nil {
		t.Fatalf("sell execute: %v", err)
	}

	// Get the order ID
	mao := rawdb.ReadMarketAccountOrder(db, seller.Bytes())
	orderID := mao.Orders[0]

	// Cancel the order
	tx := makeMarketCancelTx(1, orderID)
	actCancel := &MarketCancelOrderActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	result, err := actCancel.Execute(ctx)
	if err != nil {
		t.Fatalf("cancel execute: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet: want 1, got %d", result.ContractRet)
	}

	// Tokens returned: 500 - 100 (sold) + 100 (returned) = 500
	if got := statedb.GetTRC10Balance(seller, 1000001); got != 500 {
		t.Fatalf("TRC10 balance: want 500, got %d", got)
	}

	// Order is CANCELED
	order := rawdb.ReadMarketOrder(db, orderID)
	if order.State != corepb.MarketOrder_CANCELED {
		t.Fatalf("state: want CANCELED, got %v", order.State)
	}
	if order.SellTokenQuantityRemain != 0 {
		t.Fatalf("remaining: want 0, got %d", order.SellTokenQuantityRemain)
	}
}

func TestMarketCancelOrderExecute_RemovesFromBook(t *testing.T) {
	statedb := setupStateDB(t)
	db := ethrawdb.NewMemoryDatabase()

	seller := makeTestAddr(1)
	seedAccount(statedb, seller, 0)
	statedb.AddTRC10Balance(seller, 1000001, 500)

	txSell := makeMarketSellTx(1, []byte("1000001"), 100, []byte("_"), 50)
	actSell := &MarketSellAssetActuator{}
	ctxSell := setupContext(t, statedb, txSell)
	ctxSell.DB = db
	if _, err := actSell.Execute(ctxSell); err != nil {
		t.Fatalf("sell execute: %v", err)
	}

	mao := rawdb.ReadMarketAccountOrder(db, seller.Bytes())
	orderID := mao.Orders[0]

	// Cancel
	tx := makeMarketCancelTx(1, orderID)
	actCancel := &MarketCancelOrderActuator{}
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	if _, err := actCancel.Execute(ctx); err != nil {
		t.Fatalf("cancel execute: %v", err)
	}

	// Order book should be empty for this price
	pk := rawdb.PriceKey(100, 50)
	list := rawdb.ReadMarketOrderBook(db, []byte("1000001"), []byte("_"), pk)
	if list != nil {
		t.Fatal("order book should be nil after removing only order")
	}

	// Price list should have no prices for this pair
	pl := rawdb.ReadMarketPriceList(db, []byte("1000001"), []byte("_"))
	if len(pl.Prices) != 0 {
		t.Fatalf("price list: want 0 prices, got %d", len(pl.Prices))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestMarketCancelOrder" -v -count=1`
Expected: compilation errors.

- [ ] **Step 3: Implement MarketCancelOrderActuator**

Create `actuator/market_cancel_order.go`:

```go
package actuator

import (
	"bytes"
	"errors"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"

	"github.com/ethereum/go-ethereum/ethdb"
)

type MarketCancelOrderActuator struct{}

func (a *MarketCancelOrderActuator) getContract(ctx *Context) (*contractpb.MarketCancelOrderContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	mc := &contractpb.MarketCancelOrderContract{}
	if err := contract.Parameter.UnmarshalTo(mc); err != nil {
		return nil, errors.New("failed to unmarshal MarketCancelOrderContract")
	}
	return mc, nil
}

func (a *MarketCancelOrderActuator) Validate(ctx *Context) error {
	mc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(mc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if len(mc.OrderId) == 0 {
		return errors.New("order ID required")
	}
	order := rawdb.ReadMarketOrder(ctx.DB, mc.OrderId)
	if order == nil {
		return errors.New("order not found")
	}
	if !bytes.Equal(order.OwnerAddress, mc.OwnerAddress) {
		return errors.New("not order owner")
	}
	if order.State != corepb.MarketOrder_ACTIVE {
		return errors.New("order not active")
	}
	return nil
}

func (a *MarketCancelOrderActuator) Execute(ctx *Context) (*Result, error) {
	mc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	order := rawdb.ReadMarketOrder(ctx.DB, mc.OrderId)
	ownerAddr := tcommon.BytesToAddress(mc.OwnerAddress)

	// Return remaining sell tokens to owner
	if order.SellTokenQuantityRemain > 0 {
		transferToken(ctx.State, ownerAddr, order.SellTokenId, order.SellTokenQuantityRemain)
	}

	// Remove from order book
	pk := rawdb.PriceKey(order.SellTokenQuantity, order.BuyTokenQuantity)
	if err := removeOrderFromBook(ctx.DB, order, pk); err != nil {
		return nil, fmt.Errorf("remove from book: %w", err)
	}

	// Mark as canceled
	order.State = corepb.MarketOrder_CANCELED
	order.SellTokenQuantityReturn = order.SellTokenQuantityRemain
	order.SellTokenQuantityRemain = 0
	if err := rawdb.WriteMarketOrder(ctx.DB, mc.OrderId, order); err != nil {
		return nil, fmt.Errorf("write order: %w", err)
	}

	// Update account order list
	mao := rawdb.ReadMarketAccountOrder(ctx.DB, mc.OwnerAddress)
	if mao.Count > 0 {
		mao.Count--
	}
	if err := rawdb.WriteMarketAccountOrder(ctx.DB, mc.OwnerAddress, mao); err != nil {
		return nil, fmt.Errorf("write account order: %w", err)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}

func removeOrderFromBook(db ethdb.KeyValueStore, order *corepb.MarketOrder, pk [16]byte) error {
	list := rawdb.ReadMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk)
	if list == nil {
		return nil
	}

	// Update prev's Next
	if len(order.Prev) > 0 {
		prev := rawdb.ReadMarketOrder(db, order.Prev)
		if prev != nil {
			prev.Next = order.Next
			if err := rawdb.WriteMarketOrder(db, order.Prev, prev); err != nil {
				return err
			}
		}
	} else {
		list.Head = order.Next
	}

	// Update next's Prev
	if len(order.Next) > 0 {
		next := rawdb.ReadMarketOrder(db, order.Next)
		if next != nil {
			next.Prev = order.Prev
			if err := rawdb.WriteMarketOrder(db, order.Next, next); err != nil {
				return err
			}
		}
	} else {
		list.Tail = order.Prev
	}

	// If list is now empty, remove it and the price entry
	if len(list.Head) == 0 {
		if err := rawdb.DeleteMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk); err != nil {
			return err
		}
		// Remove from price list
		pl := rawdb.ReadMarketPriceList(db, order.SellTokenId, order.BuyTokenId)
		g := gcdInt64(order.SellTokenQuantity, order.BuyTokenQuantity)
		normSell := order.SellTokenQuantity / g
		normBuy := order.BuyTokenQuantity / g
		var remaining []*corepb.MarketPrice
		for _, p := range pl.Prices {
			if !(p.SellTokenQuantity == normSell && p.BuyTokenQuantity == normBuy) {
				remaining = append(remaining, p)
			}
		}
		pl.Prices = remaining
		return rawdb.WriteMarketPriceList(db, order.SellTokenId, order.BuyTokenId, pl)
	}

	return rawdb.WriteMarketOrderBook(db, order.SellTokenId, order.BuyTokenId, pk, list)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -run "TestMarketCancelOrder" -v -count=1`
Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/market_cancel_order.go actuator/market_cancel_order_test.go
git commit -m "feat(actuator): implement MarketCancelOrderActuator (type 53)"
```

---

### Task 7: Register Types 11, 12, 52, 53 in actuator.go

**Files:**
- Modify: `actuator/actuator.go`

- [ ] **Step 1: Add 4 new cases to `CreateActuator` switch**

In `actuator/actuator.go`, add these cases to the switch statement (after the existing `UnfreezeAssetContract` case, before `default`):

```go
	case corepb.Transaction_Contract_FreezeBalanceContract:
		return &FreezeBalanceActuator{}, nil
	case corepb.Transaction_Contract_UnfreezeBalanceContract:
		return &UnfreezeBalanceActuator{}, nil
	case corepb.Transaction_Contract_MarketSellAssetContract:
		return &MarketSellAssetActuator{}, nil
	case corepb.Transaction_Contract_MarketCancelOrderContract:
		return &MarketCancelOrderActuator{}, nil
```

- [ ] **Step 2: Verify compilation**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./actuator/`
Expected: compiles without errors.

- [ ] **Step 3: Run all actuator tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./actuator/ -v -count=1`
Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add actuator/actuator.go
git commit -m "feat(actuator): register FreezeBalance, UnfreezeBalance, MarketSell, MarketCancel (types 11,12,52,53)"
```

---

### Task 8: HTTP Market Query Endpoints

**Files:**
- Modify: `internal/tronapi/backend.go`
- Modify: `internal/tronapi/api.go`
- Modify: `core/tron_backend.go`
- Modify: `internal/tronapi/api_test.go` (add stub methods)

- [ ] **Step 1: Add 3 Backend interface methods**

In `internal/tronapi/backend.go`, add before the closing `}` of the `Backend` interface (after the TRC10 methods):

```go
	// Market queries (Phase 13)
	GetMarketOrderByID(orderID []byte) *corepb.MarketOrder
	GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder
	GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList
```

Add this import if not present: `corepb "github.com/tronprotocol/go-tron/proto/core"` (it should already be present).

- [ ] **Step 2: Implement Backend methods in `core/tron_backend.go`**

Add to `TronBackend`:

```go
func (b *TronBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder {
	return rawdb.ReadMarketOrder(b.chain.db, orderID)
}

func (b *TronBackend) GetMarketOrdersByAccount(addr tcommon.Address) []*corepb.MarketOrder {
	mao := rawdb.ReadMarketAccountOrder(b.chain.db, addr[:])
	var orders []*corepb.MarketOrder
	for _, id := range mao.Orders {
		if o := rawdb.ReadMarketOrder(b.chain.db, id); o != nil {
			orders = append(orders, o)
		}
	}
	return orders
}

func (b *TronBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	return rawdb.ReadMarketPriceList(b.chain.db, sellTokenID, buyTokenID)
}
```

Ensure `"github.com/tronprotocol/go-tron/core/rawdb"` is imported.

- [ ] **Step 3: Add 3 routes + handlers in `internal/tronapi/api.go`**

Add routes in `RegisterRoutes` after the TRC10 routes:

```go
	// Phase 13: Market order queries
	mux.HandleFunc("/wallet/getmarketorderbyid", api.getMarketOrderByID)
	mux.HandleFunc("/wallet/getmarketordersfromaccount", api.getMarketOrdersFromAccount)
	mux.HandleFunc("/wallet/getmarketpricebypair", api.getMarketPriceByPair)
```

Add handler methods:

```go
func (api *API) getMarketOrderByID(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	orderID := common.FromHex(body.Value)
	order := api.backend.GetMarketOrderByID(orderID)
	if order == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, order)
}

func (api *API) getMarketOrdersFromAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Address string `json:"address"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Address == "" {
		body.Address = r.URL.Query().Get("address")
	}
	if body.Address == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.Address))
	orders := api.backend.GetMarketOrdersByAccount(addr)
	var list []map[string]any
	for _, o := range orders {
		list = append(list, marshalMessage(o.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	data, _ := json.Marshal(map[string]any{"orders": list})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getMarketPriceByPair(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SellTokenId string `json:"sell_token_id"`
		BuyTokenId  string `json:"buy_token_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.SellTokenId == "" || body.BuyTokenId == "" {
		http.Error(w, "sell_token_id and buy_token_id required", http.StatusBadRequest)
		return
	}
	pl := api.backend.GetMarketPriceByPair([]byte(body.SellTokenId), []byte(body.BuyTokenId))
	if pl == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, pl)
}
```

- [ ] **Step 4: Add stub methods to `stubBackend` in `internal/tronapi/api_test.go`**

Add these methods to `stubBackend`:

```go
func (s *stubBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder { return nil }
func (s *stubBackend) GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder { return nil }
func (s *stubBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList { return nil }
```

- [ ] **Step 5: Verify compilation and run API tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./... && go test ./internal/tronapi/ -v -count=1`
Expected: compiles and all existing API tests PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/tronapi/backend.go internal/tronapi/api.go internal/tronapi/api_test.go core/tron_backend.go
git commit -m "feat(api): add 3 market query HTTP endpoints"
```

---

### Task 9: System Test Section 12 + Final Verification

**Files:**
- Modify: `scripts/system_test.sh`

- [ ] **Step 1: Add Section 12 to system_test.sh**

Add before the summary section (before the `echo "========..."` results line):

```bash
# ─────────────────────────────────────────────────────────────────
# SECTION 12: Phase 13 — Market Order Query Endpoints
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 12: Market Order Query Endpoints ==="

# 12.1 getmarketorderbyid — unknown returns {}
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getmarketorderbyid" \
    -H "Content-Type: application/json" \
    -d '{"value":"0000000000000000000000000000000000000000000000000000000000000000"}' \
    2>/dev/null || echo "CURL_ERROR")
check "getmarketorderbyid unknown returns {}" "$RESULT" '{}'

# 12.2 getmarketordersfromaccount — unknown account returns orders array
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getmarketordersfromaccount" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$WITNESS_ADDR\"}" 2>/dev/null || echo "CURL_ERROR")
check "getmarketordersfromaccount returns orders key" "$RESULT" '"orders"'

# 12.3 getmarketpricebypair — unknown pair returns sell_token_id
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getmarketpricebypair" \
    -H "Content-Type: application/json" \
    -d '{"sell_token_id":"1000001","buy_token_id":"_"}' 2>/dev/null || echo "CURL_ERROR")
check "getmarketpricebypair returns sell_token_id" "$RESULT" 'sell_token_id'
```

- [ ] **Step 2: Run all package tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ ./core/rawdb/ ./actuator/ ./internal/tronapi/ -v -count=1 2>&1 | tail -30`
Expected: all tests PASS across all 4 packages.

- [ ] **Step 3: Verify full build**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./...`
Expected: compiles without errors.

- [ ] **Step 4: Commit**

```bash
git add scripts/system_test.sh
git commit -m "test(system): add Section 12 market query endpoint checks"
```

- [ ] **Step 5: Final verification — run all tests one more time**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./... -count=1 2>&1 | tail -20`
Expected: all packages PASS.
