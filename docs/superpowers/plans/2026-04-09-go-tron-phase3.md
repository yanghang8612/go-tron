# Phase 3: Block Processing Pipeline + Node Bootstrap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire StateDB, Actuators, Consensus, and BlockChain into a working node that can initialize genesis, process blocks with transactions, and serve API requests.

**Architecture:** Migrate actuators from raw DB to StateDB, build a StateProcessor that executes transactions against StateDB via actuators, then integrate into BlockChain.InsertBlock. Add a minimal TxPool, wire everything into the Node lifecycle with HTTP API, and provide CLI commands (init/run).

**Tech Stack:** Go 1.24, go-ethereum ethdb/trie, protobuf, urfave/cli, Pebble DB

---

## File Structure

### New files
| File | Responsibility |
|------|---------------|
| `core/state_processor.go` | ApplyTransaction, ProcessBlock |
| `core/state_processor_test.go` | Processor tests |
| `core/txpool/pool.go` | Transaction pool |
| `core/txpool/pool_test.go` | Pool tests |
| `core/tron_backend.go` | Backend bridging BlockChain + StateDB + TxPool to API |

### Modified files
| File | Changes |
|------|---------|
| `core/state/statedb.go` | ~16 new methods for actuator support |
| `core/state/statedb_test.go` | Tests for new methods |
| `actuator/actuator.go` | Context uses StateDB instead of ethdb |
| `actuator/transfer.go` | Use StateDB methods |
| `actuator/account.go` | Use StateDB methods |
| `actuator/witness.go` | Use StateDB methods |
| `actuator/freeze_v2.go` | Use StateDB methods |
| `actuator/unfreeze_v2.go` | Use StateDB methods |
| `actuator/vote.go` | Use StateDB methods |
| `actuator/withdraw.go` | Use StateDB methods |
| `actuator/withdraw_expire_unfreeze.go` | Use StateDB methods |
| `actuator/*_test.go` | All tests updated for StateDB context |
| `core/blockchain.go` | Add InsertBlock with state processing |
| `core/blockchain_test.go` | Integration tests |
| `internal/tronapi/backend.go` | Extended Backend interface |
| `internal/tronapi/api.go` | New endpoints |
| `cmd/gtron/main.go` | Full node bootstrap, init command |
| `cmd/gtron/config.go` | Genesis selection |

---

### Task 1: StateDB Additions — Account, Witness, Frozen, Vote, Allowance methods

**Files:**
- Modify: `core/state/statedb.go`
- Modify: `core/state/statedb_test.go`

- [ ] **Step 1: Add account existence and creation methods to `core/state/statedb.go`**

Append after the existing `SubBalance` method (after line 112):

```go
// AccountExists returns whether an account exists (non-nil and not deleted).
func (s *StateDB) AccountExists(addr tcommon.Address) bool {
	obj := s.getStateObject(addr)
	return obj != nil && !obj.deleted
}

// CreateAccount creates a new account at addr with the given type.
// If the account already exists, it returns the existing account.
func (s *StateDB) CreateAccount(addr tcommon.Address, accountType corepb.AccountType) *types.Account {
	obj := s.GetOrCreateAccount(addr)
	obj.account.SetAccountType(accountType)
	obj.markDirty()
	return obj.account
}

// SetIsWitness sets the witness flag on an account.
func (s *StateDB) SetIsWitness(addr tcommon.Address, isWitness bool) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetIsWitness(isWitness)
	obj.markDirty()
}
```

- [ ] **Step 2: Add Frozen V2 methods**

Append after the existing `AddFreezeV2` method:

```go
// GetFrozenV2Amount returns the frozen amount for a specific resource type.
func (s *StateDB) GetFrozenV2Amount(addr tcommon.Address, resourceType corepb.ResourceCode) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.GetFrozenV2Amount(resourceType)
}

// ReduceFreezeV2 reduces the frozen amount for a resource type.
func (s *StateDB) ReduceFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ReduceFreezeV2(resourceType, amount)
	obj.markDirty()
}

// AddUnfreezeV2 adds a pending unfreeze entry with expiration time.
func (s *StateDB) AddUnfreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount, expireTime int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddUnfreezeV2(resourceType, amount, expireTime)
	obj.markDirty()
}

// UnfreezeV2Count returns the number of pending unfreeze entries.
func (s *StateDB) UnfreezeV2Count(addr tcommon.Address) int {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return len(obj.account.UnfrozenV2())
}

// RemoveExpiredUnfreezeV2 removes expired entries and returns the total withdrawn.
func (s *StateDB) RemoveExpiredUnfreezeV2(addr tcommon.Address, now int64) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	s.journalAccount(addr, obj)
	amount := obj.account.RemoveExpiredUnfreezeV2(now)
	if amount > 0 {
		obj.markDirty()
	}
	return amount
}

// TotalFrozenV2 returns the total frozen balance across all resource types.
func (s *StateDB) TotalFrozenV2(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.TotalFrozenV2()
}
```

- [ ] **Step 3: Add Vote methods**

```go
// GetVotes returns the votes for an account.
func (s *StateDB) GetVotes(addr tcommon.Address) []*corepb.Vote {
	obj := s.getStateObject(addr)
	if obj == nil {
		return nil
	}
	return obj.account.Votes()
}

// SetVotes sets the vote list on an account.
func (s *StateDB) SetVotes(addr tcommon.Address, votes []*corepb.Vote) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetVotes(votes)
	obj.markDirty()
}

// ClearVotes clears all votes on an account.
func (s *StateDB) ClearVotes(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ClearVotes()
	obj.markDirty()
}

// AddWitnessVoteCount adds delta to a witness's vote count.
func (s *StateDB) AddWitnessVoteCount(addr tcommon.Address, delta int64) {
	w := s.witnesses[addr]
	if w == nil {
		return
	}
	w.SetVoteCount(w.VoteCount() + delta)
}
```

- [ ] **Step 4: Add Allowance and Withdraw methods**

```go
// GetAllowance returns the witness reward allowance.
func (s *StateDB) GetAllowance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Allowance()
}

// SetAllowance sets the witness reward allowance.
func (s *StateDB) SetAllowance(addr tcommon.Address, allowance int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAllowance(allowance)
	obj.markDirty()
}

// AddAllowance adds amount to the witness reward allowance.
func (s *StateDB) AddAllowance(addr tcommon.Address, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAllowance(obj.account.Allowance() + amount)
	obj.markDirty()
}

// GetLatestWithdrawTime returns the latest withdraw timestamp.
func (s *StateDB) GetLatestWithdrawTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestWithdrawTime()
}

// SetLatestWithdrawTime sets the latest withdraw timestamp.
func (s *StateDB) SetLatestWithdrawTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestWithdrawTime(t)
	obj.markDirty()
}
```

- [ ] **Step 5: Check types.Account has SetAccountType method**

Check if `core/types/account.go` has `SetAccountType`. If not, add it:

```go
func (a *Account) SetAccountType(t corepb.AccountType) {
	a.pb.Type = t
}
```

Also check for `SetAllowance` and `SetLatestWithdrawTime`:

```go
func (a *Account) SetAllowance(v int64) {
	a.pb.Allowance = v
}

func (a *Account) SetLatestWithdrawTime(t int64) {
	a.pb.LatestWithdrawTime = t
}
```

Add any missing setters.

- [ ] **Step 6: Write tests for new StateDB methods in `core/state/statedb_test.go`**

Add tests after the existing tests:

```go
func TestStateDBAccountExists(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)

	if db.AccountExists(addr) {
		t.Fatal("should not exist before creation")
	}

	db.CreateAccount(addr, corepb.AccountType_Normal)
	if !db.AccountExists(addr) {
		t.Fatal("should exist after creation")
	}
}

func TestStateDBFreezeAndUnfreeze(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)
	db.AddBalance(addr, 10_000_000)

	// Freeze
	db.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 5_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 5_000_000 {
		t.Fatalf("frozen: got %d, want 5000000", got)
	}
	if got := db.TotalFrozenV2(addr); got != 5_000_000 {
		t.Fatalf("total frozen: got %d, want 5000000", got)
	}

	// Reduce frozen
	db.ReduceFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 3_000_000 {
		t.Fatalf("after reduce: got %d, want 3000000", got)
	}

	// Add unfreeze entry
	db.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000, 100_000)
	if got := db.UnfreezeV2Count(addr); got != 1 {
		t.Fatalf("unfreeze count: got %d, want 1", got)
	}

	// Remove expired
	withdrawn := db.RemoveExpiredUnfreezeV2(addr, 200_000)
	if withdrawn != 2_000_000 {
		t.Fatalf("withdrawn: got %d, want 2000000", withdrawn)
	}
	if got := db.UnfreezeV2Count(addr); got != 0 {
		t.Fatalf("unfreeze count after remove: got %d, want 0", got)
	}
}

func TestStateDBVotes(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	votes := []*corepb.Vote{
		{VoteAddress: testAddr(0x10).Bytes(), VoteCount: 100},
		{VoteAddress: testAddr(0x11).Bytes(), VoteCount: 200},
	}
	db.SetVotes(addr, votes)
	got := db.GetVotes(addr)
	if len(got) != 2 {
		t.Fatalf("votes: got %d, want 2", len(got))
	}

	db.ClearVotes(addr)
	got = db.GetVotes(addr)
	if len(got) != 0 {
		t.Fatalf("after clear: got %d votes, want 0", len(got))
	}
}

func TestStateDBAllowanceAndWithdraw(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	db.SetAllowance(addr, 1000)
	if got := db.GetAllowance(addr); got != 1000 {
		t.Fatalf("allowance: got %d, want 1000", got)
	}

	db.AddAllowance(addr, 500)
	if got := db.GetAllowance(addr); got != 1500 {
		t.Fatalf("after add: got %d, want 1500", got)
	}

	db.SetLatestWithdrawTime(addr, 999)
	if got := db.GetLatestWithdrawTime(addr); got != 999 {
		t.Fatalf("withdraw time: got %d, want 999", got)
	}
}

func TestStateDBWitnessVoteCount(t *testing.T) {
	db := newTestStateDB(t)
	wAddr := testAddr(0x10)
	db.PutWitness(wAddr, "http://w1.example.com")

	db.AddWitnessVoteCount(wAddr, 100)
	w := db.GetWitness(wAddr)
	if w.VoteCount() != 100 {
		t.Fatalf("vote count: got %d, want 100", w.VoteCount())
	}
	db.AddWitnessVoteCount(wAddr, -30)
	if w.VoteCount() != 70 {
		t.Fatalf("after sub: got %d, want 70", w.VoteCount())
	}
}
```

Note: `newTestStateDB` and `testAddr` should already exist from Phase 2 tests. If not, add:

```go
func newTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := NewDatabase(diskdb)
	statedb, err := New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}
```

- [ ] **Step 7: Run tests**

Run: `go test ./core/state/ -v -count=1`
Expected: All PASS including new tests.

- [ ] **Step 8: Commit**

```bash
git add core/state/statedb.go core/state/statedb_test.go core/types/account.go
git commit -m "state: add StateDB methods for actuator support

AccountExists, CreateAccount, SetIsWitness, FreezeV2/UnfreezeV2
operations, vote management, allowance/withdraw tracking."
```

---

### Task 2: Actuator Context Migration — All 8 actuators use StateDB

**Files:**
- Modify: `actuator/actuator.go`
- Modify: `actuator/transfer.go`, `actuator/account.go`, `actuator/witness.go`, `actuator/freeze_v2.go`, `actuator/unfreeze_v2.go`, `actuator/vote.go`, `actuator/withdraw.go`, `actuator/withdraw_expire_unfreeze.go`
- Modify: All `actuator/*_test.go` files

- [ ] **Step 1: Update `actuator/actuator.go` Context struct**

Replace the entire file:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type Context struct {
	State       *state.StateDB
	DynProps    *state.DynamicProperties
	Tx          *types.Transaction
	BlockTime   int64
	BlockNumber uint64
}

type Result struct {
	Fee int64
}

type Actuator interface {
	Validate(ctx *Context) error
	Execute(ctx *Context) (*Result, error)
}

func CreateActuator(tx *types.Transaction) (Actuator, error) {
	ct := tx.ContractType()
	switch ct {
	case corepb.Transaction_Contract_TransferContract:
		return &TransferActuator{}, nil
	case corepb.Transaction_Contract_AccountCreateContract:
		return &CreateAccountActuator{}, nil
	case corepb.Transaction_Contract_WitnessCreateContract:
		return &WitnessCreateActuator{}, nil
	case corepb.Transaction_Contract_FreezeBalanceV2Contract:
		return &FreezeBalanceV2Actuator{}, nil
	case corepb.Transaction_Contract_UnfreezeBalanceV2Contract:
		return &UnfreezeBalanceV2Actuator{}, nil
	case corepb.Transaction_Contract_VoteWitnessContract:
		return &VoteWitnessActuator{}, nil
	case corepb.Transaction_Contract_WithdrawBalanceContract:
		return &WithdrawBalanceActuator{}, nil
	case corepb.Transaction_Contract_WithdrawExpireUnfreezeContract:
		return &WithdrawExpireUnfreezeActuator{}, nil
	default:
		return nil, errors.New("unsupported contract type")
	}
}
```

- [ ] **Step 2: Update `actuator/transfer.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type TransferActuator struct{}

func (a *TransferActuator) getContract(ctx *Context) (*contractpb.TransferContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	tc := &contractpb.TransferContract{}
	if err := contract.Parameter.UnmarshalTo(tc); err != nil {
		return nil, errors.New("failed to unmarshal TransferContract")
	}
	return tc, nil
}

func (a *TransferActuator) Validate(ctx *Context) error {
	tc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(tc.OwnerAddress)
	toAddr := common.BytesToAddress(tc.ToAddress)
	if ownerAddr == toAddr {
		return errors.New("cannot transfer to self")
	}
	if tc.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetBalance(ownerAddr) < tc.Amount {
		return errors.New("insufficient balance")
	}
	return nil
}

func (a *TransferActuator) Execute(ctx *Context) (*Result, error) {
	tc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(tc.OwnerAddress)
	toAddr := common.BytesToAddress(tc.ToAddress)

	if err := ctx.State.SubBalance(ownerAddr, tc.Amount); err != nil {
		return nil, err
	}
	if !ctx.State.AccountExists(toAddr) {
		ctx.State.CreateAccount(toAddr, corepb.AccountType_Normal)
	}
	ctx.State.AddBalance(toAddr, tc.Amount)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 3: Update `actuator/account.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type CreateAccountActuator struct{}

func (a *CreateAccountActuator) getContract(ctx *Context) (*contractpb.AccountCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	ac := &contractpb.AccountCreateContract{}
	if err := contract.Parameter.UnmarshalTo(ac); err != nil {
		return nil, errors.New("failed to unmarshal AccountCreateContract")
	}
	return ac, nil
}

func (a *CreateAccountActuator) Validate(ctx *Context) error {
	ac, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(ac.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	newAddr := common.BytesToAddress(ac.AccountAddress)
	if ctx.State.AccountExists(newAddr) {
		return errors.New("account already exists")
	}
	return nil
}

func (a *CreateAccountActuator) Execute(ctx *Context) (*Result, error) {
	ac, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	newAddr := common.BytesToAddress(ac.AccountAddress)
	ctx.State.CreateAccount(newAddr, ac.Type)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 4: Update `actuator/witness.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessCreateActuator struct{}

func (a *WitnessCreateActuator) getContract(ctx *Context) (*contractpb.WitnessCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WitnessCreateContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WitnessCreateContract")
	}
	return wc, nil
}

func (a *WitnessCreateActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetWitness(ownerAddr) != nil {
		return errors.New("witness already exists")
	}
	if len(wc.Url) == 0 {
		return errors.New("witness URL is empty")
	}
	return nil
}

func (a *WitnessCreateActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	ctx.State.PutWitness(ownerAddr, string(wc.Url))
	ctx.State.SetIsWitness(ownerAddr, true)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 5: Update `actuator/freeze_v2.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type FreezeBalanceV2Actuator struct{}

func (a *FreezeBalanceV2Actuator) getContract(ctx *Context) (*contractpb.FreezeBalanceV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	fc := &contractpb.FreezeBalanceV2Contract{}
	if err := contract.Parameter.UnmarshalTo(fc); err != nil {
		return nil, errors.New("failed to unmarshal FreezeBalanceV2Contract")
	}
	return fc, nil
}

func (a *FreezeBalanceV2Actuator) Validate(ctx *Context) error {
	fc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance <= 0 {
		return errors.New("frozen balance must be positive")
	}
	if ctx.State.GetBalance(ownerAddr) < fc.FrozenBalance {
		return errors.New("insufficient balance to freeze")
	}
	if fc.Resource != corepb.ResourceCode_BANDWIDTH && fc.Resource != corepb.ResourceCode_ENERGY {
		return errors.New("invalid resource type")
	}
	return nil
}

func (a *FreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
	fc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(fc.OwnerAddress)
	if err := ctx.State.SubBalance(ownerAddr, fc.FrozenBalance); err != nil {
		return nil, err
	}
	ctx.State.AddFreezeV2(ownerAddr, fc.Resource, fc.FrozenBalance)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 6: Update `actuator/unfreeze_v2.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const maxUnfreezeCount = 32

type UnfreezeBalanceV2Actuator struct{}

func (a *UnfreezeBalanceV2Actuator) getContract(ctx *Context) (*contractpb.UnfreezeBalanceV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	uc := &contractpb.UnfreezeBalanceV2Contract{}
	if err := contract.Parameter.UnmarshalTo(uc); err != nil {
		return nil, errors.New("failed to unmarshal UnfreezeBalanceV2Contract")
	}
	return uc, nil
}

func (a *UnfreezeBalanceV2Actuator) Validate(ctx *Context) error {
	uc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if uc.UnfreezeBalance <= 0 {
		return errors.New("unfreeze balance must be positive")
	}
	frozen := ctx.State.GetFrozenV2Amount(ownerAddr, uc.Resource)
	if frozen < uc.UnfreezeBalance {
		return errors.New("insufficient frozen balance")
	}
	if ctx.State.UnfreezeV2Count(ownerAddr) >= maxUnfreezeCount {
		return errors.New("too many pending unfreezes")
	}
	return nil
}

func (a *UnfreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
	uc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(uc.OwnerAddress)
	ctx.State.ReduceFreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance)
	expireTime := ctx.BlockTime + 14*86_400_000
	ctx.State.AddUnfreezeV2(ownerAddr, uc.Resource, uc.UnfreezeBalance, expireTime)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 7: Update `actuator/vote.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type VoteWitnessActuator struct{}

func (a *VoteWitnessActuator) getContract(ctx *Context) (*contractpb.VoteWitnessContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	vc := &contractpb.VoteWitnessContract{}
	if err := contract.Parameter.UnmarshalTo(vc); err != nil {
		return nil, errors.New("failed to unmarshal VoteWitnessContract")
	}
	return vc, nil
}

func (a *VoteWitnessActuator) Validate(ctx *Context) error {
	vc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(vc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if len(vc.Votes) == 0 {
		return errors.New("no votes provided")
	}
	if len(vc.Votes) > params.MaxVoteNumber {
		return errors.New("too many votes")
	}

	tronPower := ctx.State.TotalFrozenV2(ownerAddr) / int64(params.TRXPrecision)
	var totalVoteCount int64
	seen := make(map[common.Address]bool)
	for _, v := range vc.Votes {
		targetAddr := common.BytesToAddress(v.VoteAddress)
		if seen[targetAddr] {
			return errors.New("duplicate vote target")
		}
		seen[targetAddr] = true
		if v.VoteCount <= 0 {
			return errors.New("vote count must be positive")
		}
		totalVoteCount += v.VoteCount
		if ctx.State.GetWitness(targetAddr) == nil {
			return errors.New("vote target is not a witness")
		}
	}
	if totalVoteCount > tronPower {
		return errors.New("total votes exceed tron power")
	}
	return nil
}

func (a *VoteWitnessActuator) Execute(ctx *Context) (*Result, error) {
	vc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(vc.OwnerAddress)

	// Remove old votes from witnesses
	oldVotes := ctx.State.GetVotes(ownerAddr)
	for _, v := range oldVotes {
		targetAddr := common.BytesToAddress(v.VoteAddress)
		ctx.State.AddWitnessVoteCount(targetAddr, -v.VoteCount)
	}

	// Set new votes on account
	newVotes := make([]*corepb.Vote, len(vc.Votes))
	for i, v := range vc.Votes {
		newVotes[i] = &corepb.Vote{
			VoteAddress: v.VoteAddress,
			VoteCount:   v.VoteCount,
		}
	}
	ctx.State.SetVotes(ownerAddr, newVotes)

	// Add new votes to witnesses
	for _, v := range vc.Votes {
		targetAddr := common.BytesToAddress(v.VoteAddress)
		ctx.State.AddWitnessVoteCount(targetAddr, v.VoteCount)
	}

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 8: Update `actuator/withdraw.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const withdrawCooldown = 86_400_000 // 24 hours in ms

type WithdrawBalanceActuator struct{}

func (a *WithdrawBalanceActuator) getContract(ctx *Context) (*contractpb.WithdrawBalanceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WithdrawBalanceContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WithdrawBalanceContract")
	}
	return wc, nil
}

func (a *WithdrawBalanceActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetAllowance(ownerAddr) <= 0 {
		return errors.New("no allowance to withdraw")
	}
	lastWithdraw := ctx.State.GetLatestWithdrawTime(ownerAddr)
	if ctx.BlockTime-lastWithdraw < withdrawCooldown {
		return errors.New("withdraw too frequent")
	}
	return nil
}

func (a *WithdrawBalanceActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	allowance := ctx.State.GetAllowance(ownerAddr)
	ctx.State.AddBalance(ownerAddr, allowance)
	ctx.State.SetAllowance(ownerAddr, 0)
	ctx.State.SetLatestWithdrawTime(ownerAddr, ctx.BlockTime)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 9: Update `actuator/withdraw_expire_unfreeze.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WithdrawExpireUnfreezeActuator struct{}

func (a *WithdrawExpireUnfreezeActuator) getContract(ctx *Context) (*contractpb.WithdrawExpireUnfreezeContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	wc := &contractpb.WithdrawExpireUnfreezeContract{}
	if err := contract.Parameter.UnmarshalTo(wc); err != nil {
		return nil, errors.New("failed to unmarshal WithdrawExpireUnfreezeContract")
	}
	return wc, nil
}

func (a *WithdrawExpireUnfreezeActuator) Validate(ctx *Context) error {
	wc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	acc := ctx.State.GetAccount(ownerAddr)
	hasExpired := false
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= ctx.BlockTime {
			hasExpired = true
			break
		}
	}
	if !hasExpired {
		return errors.New("no expired unfreeze entries")
	}
	return nil
}

func (a *WithdrawExpireUnfreezeActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	withdrawn := ctx.State.RemoveExpiredUnfreezeV2(ownerAddr, ctx.BlockTime)
	ctx.State.AddBalance(ownerAddr, withdrawn)
	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 10: Update all actuator test files**

All test files need to change from rawdb-based setup to StateDB-based setup. Create a shared test helper in `actuator/test_helpers_test.go`:

```go
package actuator

import (
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"testing"
)

func makeTestAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func setupStateDB(t *testing.T) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func setupContext(t *testing.T, statedb *state.StateDB, tx *types.Transaction) *Context {
	t.Helper()
	return &Context{
		State:       statedb,
		DynProps:    state.NewDynamicProperties(),
		Tx:          tx,
		BlockTime:   1000000,
		BlockNumber: 1,
	}
}

func seedAccount(statedb *state.StateDB, addr tcommon.Address, balance int64) {
	statedb.CreateAccount(addr, corepb.AccountType_Normal)
	statedb.AddBalance(addr, balance)
}
```

Then update each test file. Example for `actuator/transfer_test.go`:

```go
package actuator

import (
	"testing"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/core/types"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTransferTx(from, to byte, amount int64) *types.Transaction {
	tc := &contractpb.TransferContract{
		OwnerAddress: makeTestAddr(from).Bytes(),
		ToAddress:    makeTestAddr(to).Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func TestTransferValidate_Success(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 1000000)
	tx := makeTransferTx(1, 2, 100)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTransferValidate_InsufficientBalance(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 50)
	tx := makeTransferTx(1, 2, 100)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected insufficient balance error")
	}
}

func TestTransferValidate_SelfTransfer(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 1000000)
	tx := makeTransferTx(1, 1, 100)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected self-transfer error")
	}
}

func TestTransferExecute(t *testing.T) {
	statedb := setupStateDB(t)
	seedAccount(statedb, makeTestAddr(1), 1000)
	tx := makeTransferTx(1, 2, 300)
	ctx := setupContext(t, statedb, tx)
	act := &TransferActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatal(err)
	}
	if got := statedb.GetBalance(makeTestAddr(1)); got != 700 {
		t.Fatalf("sender balance: got %d, want 700", got)
	}
	if got := statedb.GetBalance(makeTestAddr(2)); got != 300 {
		t.Fatalf("recipient balance: got %d, want 300", got)
	}
}
```

Repeat the same pattern for all other test files. Each test:
1. Creates `setupStateDB(t)` 
2. Seeds accounts with `seedAccount(statedb, addr, balance)`
3. Uses `setupContext(t, statedb, tx)` for Context
4. Validates or executes via StateDB
5. Asserts via StateDB getters

The engineer should rewrite each `*_test.go` file following this pattern, preserving the same test scenarios and assertions but using StateDB instead of rawdb.

- [ ] **Step 11: Run all actuator tests**

Run: `go test ./actuator/ -v -count=1`
Expected: All PASS.

- [ ] **Step 12: Commit**

```bash
git add actuator/
git commit -m "actuator: migrate all actuators from rawdb to StateDB

Context now uses *state.StateDB instead of ethdb.KeyValueStore.
All 8 actuators updated. Test helpers use setupStateDB/seedAccount."
```

---

### Task 3: State Processor — ApplyTransaction and ProcessBlock

**Files:**
- Create: `core/state_processor.go`
- Create: `core/state_processor_test.go`

- [ ] **Step 1: Create `core/state_processor.go`**

```go
package core

import (
	"fmt"

	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

// ApplyTransaction executes a single transaction against the given state.
// Returns the fee charged by the actuator.
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64, blockNum uint64) (int64, error) {
	act, err := actuator.CreateActuator(tx)
	if err != nil {
		return 0, fmt.Errorf("create actuator: %w", err)
	}

	ctx := &actuator.Context{
		State:       statedb,
		DynProps:    dynProps,
		Tx:          tx,
		BlockTime:   blockTime,
		BlockNumber: blockNum,
	}

	if err := act.Validate(ctx); err != nil {
		return 0, fmt.Errorf("validate: %w", err)
	}

	snap := statedb.Snapshot()
	result, err := act.Execute(ctx)
	if err != nil {
		statedb.RevertToSnapshot(snap)
		return 0, fmt.Errorf("execute: %w", err)
	}

	return result.Fee, nil
}

// ProcessBlock executes all transactions in a block and returns the new state root.
func ProcessBlock(statedb *state.StateDB, dynProps *state.DynamicProperties, block *types.Block) (tcommon.Hash, error) {
	for i, tx := range block.Transactions() {
		_, err := ApplyTransaction(statedb, dynProps, tx, block.Timestamp(), block.Number())
		if err != nil {
			return tcommon.Hash{}, fmt.Errorf("tx %d: %w", i, err)
		}
	}

	// Pay block reward to witness
	witnessAddr := block.WitnessAddress()
	if witnessAddr != (tcommon.Address{}) {
		reward := dynProps.WitnessPayPerBlock()
		if reward > 0 {
			statedb.AddAllowance(witnessAddr, reward)
		}
	}

	// Commit state to get new root
	root, err := statedb.Commit()
	if err != nil {
		return tcommon.Hash{}, fmt.Errorf("commit state: %w", err)
	}

	return root, nil
}
```

- [ ] **Step 2: Write tests in `core/state_processor_test.go`**

```go
package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func newTestState(t *testing.T) (*state.StateDB, *state.Database) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb, sdb
}

func testProcessorAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func makeTestTransferTx(from, to byte, amount int64) *types.Transaction {
	tc := &contractpb.TransferContract{
		OwnerAddress: testProcessorAddr(from).Bytes(),
		ToAddress:    testProcessorAddr(to).Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func TestApplyTransaction_Transfer(t *testing.T) {
	statedb, _ := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)

	tx := makeTestTransferTx(1, 2, 300_000)
	fee, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fee != 0 {
		t.Fatalf("fee: got %d, want 0", fee)
	}
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 700_000 {
		t.Fatalf("sender: got %d, want 700000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 300_000 {
		t.Fatalf("recipient: got %d, want 300000", got)
	}
}

func TestApplyTransaction_ValidationFails(t *testing.T) {
	statedb, _ := newTestState(t)
	dynProps := state.NewDynamicProperties()

	// No account seeded — validation should fail
	tx := makeTestTransferTx(1, 2, 100)
	_, err := ApplyTransaction(statedb, dynProps, tx, 3000, 1)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestProcessBlock_WithTransactions(t *testing.T) {
	statedb, _ := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)

	// Commit the initial state so we have a clean base
	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)

	witnessAddr := testProcessorAddr(0xFF)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	root, err := ProcessBlock(statedb, dynProps, block)
	if err != nil {
		t.Fatal(err)
	}
	if root == (tcommon.Hash{}) {
		t.Fatal("expected non-empty state root")
	}

	// Verify: sender lost 3M, recipients got 1M and 2M
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 7_000_000 {
		t.Fatalf("sender: got %d, want 7000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 1_000_000 {
		t.Fatalf("recipient 2: got %d, want 1000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(3)); got != 2_000_000 {
		t.Fatalf("recipient 3: got %d, want 2000000", got)
	}

	// Verify witness reward
	reward := dynProps.WitnessPayPerBlock()
	if got := statedb.GetAllowance(witnessAddr); got != reward {
		t.Fatalf("witness reward: got %d, want %d", got, reward)
	}
}

func TestProcessBlock_FailingTxRevertsState(t *testing.T) {
	statedb, _ := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 100)

	// tx tries to transfer 200 — should fail validation
	tx := makeTestTransferTx(1, 2, 200)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err := ProcessBlock(statedb, dynProps, block)
	if err == nil {
		t.Fatal("expected error for invalid transaction")
	}

	// Balance should be unchanged
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 100 {
		t.Fatalf("balance should be unchanged: got %d, want 100", got)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./core/ -run TestApply -v -count=1 && go test ./core/ -run TestProcess -v -count=1`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add core/state_processor.go core/state_processor_test.go
git commit -m "core: add StateProcessor with ApplyTransaction and ProcessBlock

ApplyTransaction dispatches to actuators via StateDB.
ProcessBlock executes all block txs and pays witness reward."
```

---

### Task 4: Full Block Insertion with State Processing

**Files:**
- Modify: `core/blockchain.go`
- Create: `core/blockchain_insert_test.go`

- [ ] **Step 1: Add InsertBlock to `core/blockchain.go`**

Add these imports and method after `InsertBlockWithoutVerify`:

```go
// Add to imports:
// "github.com/tronprotocol/go-tron/core/state"

// InsertBlock inserts a block with full state processing.
func (bc *BlockChain) InsertBlock(block *types.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}

	bc.chainmu.Lock()
	defer bc.chainmu.Unlock()

	current := bc.CurrentBlock()
	if block.Number() != current.Number()+1 {
		return ErrInvalidNumber
	}
	if block.ParentHash() != current.Hash() {
		return ErrInvalidParent
	}

	// Open StateDB from parent's state root
	parentRoot := current.AccountStateRoot()
	statedb, err := state.New(parentRoot, bc.stateDB)
	if err != nil {
		return fmt.Errorf("open state: %w", err)
	}

	// Load dynamic properties
	dynProps := state.LoadDynamicProperties(bc.db)

	// Process block (execute transactions, pay reward)
	newRoot, err := ProcessBlock(statedb, dynProps, block)
	if err != nil {
		return fmt.Errorf("process block: %w", err)
	}

	// Verify state root if the block has one set
	blockRoot := block.AccountStateRoot()
	if blockRoot != (tcommon.Hash{}) && blockRoot != newRoot {
		return fmt.Errorf("state root mismatch: block=%x computed=%x", blockRoot, newRoot)
	}

	// Update dynamic properties
	dynProps.SetLatestBlockHeaderNumber(int64(block.Number()))
	dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
	dynProps.SetLatestBlockHeaderHash(block.Hash())
	dynProps.Flush(bc.db)

	// Persist block
	rawdb.WriteBlock(bc.db, block)
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())
	bc.currentBlock.Store(block)

	return nil
}

// StateDB returns the state database for reading state.
func (bc *BlockChain) StateDB() *state.Database {
	return bc.stateDB
}
```

Add `"fmt"` to the imports if not already present.

- [ ] **Step 2: Write integration test in `core/blockchain_insert_test.go`**

```go
package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func testInsertAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestBlockChain_InsertBlock_Transfer(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 99_000_000_000_000_000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	sdb := state.NewDatabase(diskdb)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build transfer tx: addr(1) -> addr(2) for 5M TRX
	tc := &contractpb.TransferContract{
		OwnerAddress: testInsertAddr(1).Bytes(),
		ToAddress:    testInsertAddr(2).Bytes(),
		Amount:       5_000_000,
	}
	param, _ := anypb.New(tc)
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     1,
				Timestamp:  3000,
				ParentHash: genesisHash[:],
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})

	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block: got %d, want 1", bc.CurrentBlock().Number())
	}

	// Verify state: open StateDB from the new state root
	newRoot := bc.CurrentBlock().AccountStateRoot()
	// The block was built without AccountStateRoot, so InsertBlock skipped root verification.
	// But the state was committed. Verify by loading DynProps.
	dynProps := state.LoadDynamicProperties(diskdb)
	if got := dynProps.LatestBlockHeaderNumber(); got != 1 {
		t.Fatalf("dynprops block number: got %d, want 1", got)
	}
}

func TestBlockChain_InsertBlock_MultipleBlocks(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testInsertAddr(1), Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	sdb := state.NewDatabase(diskdb)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	// Insert 3 empty blocks
	for i := uint64(1); i <= 3; i++ {
		parent := bc.CurrentBlock()
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(i),
					Timestamp:  int64(i) * 3000,
					ParentHash: parent.Hash().Bytes(),
				},
			},
		})
		if err := bc.InsertBlock(block); err != nil {
			t.Fatalf("block %d: %v", i, err)
		}
	}

	if bc.CurrentBlock().Number() != 3 {
		t.Fatalf("current: got %d, want 3", bc.CurrentBlock().Number())
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./core/ -v -count=1`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add core/blockchain.go core/blockchain_insert_test.go
git commit -m "core: add InsertBlock with full state processing

Opens StateDB from parent root, executes transactions via
ProcessBlock, updates DynamicProperties, persists state."
```

---

### Task 5: Transaction Pool

**Files:**
- Create: `core/txpool/pool.go`
- Create: `core/txpool/pool_test.go`

- [ ] **Step 1: Create `core/txpool/pool.go`**

```go
package txpool

import (
	"errors"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

const defaultMaxPoolSize = 10000

var (
	ErrPoolFull     = errors.New("transaction pool is full")
	ErrAlreadyKnown = errors.New("transaction already in pool")
	ErrNoContract   = errors.New("transaction has no contract")
)

// TxPool holds pending transactions waiting to be included in a block.
type TxPool struct {
	mu      sync.RWMutex
	pending map[tcommon.Hash]*types.Transaction
	maxSize int
}

// New creates a new transaction pool.
func New() *TxPool {
	return &TxPool{
		pending: make(map[tcommon.Hash]*types.Transaction),
		maxSize: defaultMaxPoolSize,
	}
}

// Add validates and adds a transaction to the pool.
func (pool *TxPool) Add(tx *types.Transaction) error {
	if tx.Contract() == nil {
		return ErrNoContract
	}

	hash := tx.Hash()

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if _, exists := pool.pending[hash]; exists {
		return ErrAlreadyKnown
	}
	if len(pool.pending) >= pool.maxSize {
		return ErrPoolFull
	}

	pool.pending[hash] = tx
	return nil
}

// Get returns a transaction by hash, or nil if not found.
func (pool *TxPool) Get(hash tcommon.Hash) *types.Transaction {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.pending[hash]
}

// Pending returns all pending transactions as a slice.
func (pool *TxPool) Pending() []*types.Transaction {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	txs := make([]*types.Transaction, 0, len(pool.pending))
	for _, tx := range pool.pending {
		txs = append(txs, tx)
	}
	return txs
}

// Remove deletes a transaction from the pool.
func (pool *TxPool) Remove(hash tcommon.Hash) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	delete(pool.pending, hash)
}

// RemoveBatch removes multiple transactions from the pool.
func (pool *TxPool) RemoveBatch(hashes []tcommon.Hash) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for _, h := range hashes {
		delete(pool.pending, h)
	}
}

// Count returns the number of pending transactions.
func (pool *TxPool) Count() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return len(pool.pending)
}
```

- [ ] **Step 2: Create `core/txpool/pool_test.go`**

```go
package txpool

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTx(from byte, amount int64) *types.Transaction {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = from
	tc := &contractpb.TransferContract{
		OwnerAddress: addr.Bytes(),
		ToAddress:    addr.Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp: int64(from)*1000 + amount, // unique per combo
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func TestTxPool_AddAndGet(t *testing.T) {
	pool := New()
	tx := makeTx(1, 100)
	if err := pool.Add(tx); err != nil {
		t.Fatal(err)
	}
	if pool.Count() != 1 {
		t.Fatalf("count: got %d, want 1", pool.Count())
	}
	got := pool.Get(tx.Hash())
	if got == nil {
		t.Fatal("transaction not found")
	}
}

func TestTxPool_DuplicateReject(t *testing.T) {
	pool := New()
	tx := makeTx(1, 100)
	pool.Add(tx)
	if err := pool.Add(tx); err != ErrAlreadyKnown {
		t.Fatalf("expected ErrAlreadyKnown, got %v", err)
	}
}

func TestTxPool_NoContractReject(t *testing.T) {
	pool := New()
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{},
	})
	if err := pool.Add(tx); err != ErrNoContract {
		t.Fatalf("expected ErrNoContract, got %v", err)
	}
}

func TestTxPool_Remove(t *testing.T) {
	pool := New()
	tx := makeTx(1, 100)
	pool.Add(tx)
	pool.Remove(tx.Hash())
	if pool.Count() != 0 {
		t.Fatalf("count after remove: got %d, want 0", pool.Count())
	}
}

func TestTxPool_Pending(t *testing.T) {
	pool := New()
	pool.Add(makeTx(1, 100))
	pool.Add(makeTx(2, 200))
	pool.Add(makeTx(3, 300))

	pending := pool.Pending()
	if len(pending) != 3 {
		t.Fatalf("pending: got %d, want 3", len(pending))
	}
}

func TestTxPool_PoolFull(t *testing.T) {
	pool := New()
	pool.maxSize = 2
	pool.Add(makeTx(1, 100))
	pool.Add(makeTx(2, 200))
	if err := pool.Add(makeTx(3, 300)); err != ErrPoolFull {
		t.Fatalf("expected ErrPoolFull, got %v", err)
	}
}
```

- [ ] **Step 3: Run tests**

Run: `go test ./core/txpool/ -v -count=1`
Expected: All PASS.

- [ ] **Step 4: Commit**

```bash
git add core/txpool/
git commit -m "core/txpool: add basic transaction pool

Map-based pending pool with Add/Get/Remove/Pending/Count.
Validates non-nil contract and rejects duplicates."
```

---

### Task 6: TronBackend + API Expansion

**Files:**
- Create: `core/tron_backend.go`
- Modify: `internal/tronapi/backend.go`
- Modify: `internal/tronapi/api.go`
- Modify: `internal/tronapi/server.go` (if needed for lifecycle)

- [ ] **Step 1: Extend `internal/tronapi/backend.go`**

```go
package tronapi

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

type NodeInfo struct {
	Version      string `json:"version"`
	CurrentBlock uint64 `json:"currentBlock"`
}

type Backend interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) (*types.Block, error)
	GetAccount(addr common.Address) (*types.Account, error)
	BroadcastTransaction(tx *types.Transaction) error
	GetNodeInfo() *NodeInfo
	PendingTransactionCount() int
}
```

- [ ] **Step 2: Create `core/tron_backend.go`**

```go
package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/tronapi"
)

// TronBackend implements tronapi.Backend.
type TronBackend struct {
	chain *BlockChain
	pool  *txpool.TxPool
}

func NewTronBackend(chain *BlockChain, pool *txpool.TxPool) *TronBackend {
	return &TronBackend{chain: chain, pool: pool}
}

func (b *TronBackend) CurrentBlock() *types.Block {
	return b.chain.CurrentBlock()
}

func (b *TronBackend) GetBlockByNumber(number uint64) (*types.Block, error) {
	block := b.chain.GetBlockByNumber(number)
	if block == nil {
		return nil, fmt.Errorf("block %d not found", number)
	}
	return block, nil
}

func (b *TronBackend) GetAccount(addr tcommon.Address) (*types.Account, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return nil, fmt.Errorf("account not found")
	}
	return acc, nil
}

func (b *TronBackend) BroadcastTransaction(tx *types.Transaction) error {
	return b.pool.Add(tx)
}

func (b *TronBackend) GetNodeInfo() *tronapi.NodeInfo {
	current := b.chain.CurrentBlock()
	return &tronapi.NodeInfo{
		Version:      "0.2.0-dev",
		CurrentBlock: current.Number(),
	}
}

func (b *TronBackend) PendingTransactionCount() int {
	return b.pool.Count()
}
```

- [ ] **Step 3: Add new API endpoints to `internal/tronapi/api.go`**

Add new route registrations and handlers. Add to `RegisterRoutes`:

```go
mux.HandleFunc("/wallet/broadcasttransaction", api.broadcastTransaction)
mux.HandleFunc("/wallet/getnodeinfo", api.getNodeInfo)
mux.HandleFunc("/wallet/gettransactioncountinpool", api.getTxPoolCount)
```

Add new handler methods:

```go
func (api *API) broadcastTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var pbTx corepb.Transaction
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if err := protojson.Unmarshal(body, &pbTx); err != nil {
		http.Error(w, "invalid transaction JSON", http.StatusBadRequest)
		return
	}
	tx := types.NewTransactionFromPB(&pbTx)
	if err := api.backend.BroadcastTransaction(tx); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := map[string]string{"txhash": fmt.Sprintf("%x", tx.Hash())}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := api.backend.GetNodeInfo()
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getTxPoolCount(w http.ResponseWriter, r *http.Request) {
	count := api.backend.PendingTransactionCount()
	resp := map[string]int{"count": count}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
```

Add new imports to `api.go`: `"io"`, `"fmt"`, `"github.com/tronprotocol/go-tron/core/types"`, `corepb "github.com/tronprotocol/go-tron/proto/core"`, `"google.golang.org/protobuf/encoding/protojson"` (if not already imported).

- [ ] **Step 4: Run tests**

Run: `go vet ./internal/tronapi/... && go vet ./core/...`
Expected: Clean.

- [ ] **Step 5: Commit**

```bash
git add core/tron_backend.go internal/tronapi/backend.go internal/tronapi/api.go
git commit -m "core: add TronBackend and expand HTTP API

TronBackend bridges BlockChain + TxPool to tronapi.Backend.
New endpoints: broadcasttransaction, getnodeinfo, gettransactioncountinpool."
```

---

### Task 7: Node Bootstrap — CLI init + full node startup

**Files:**
- Modify: `cmd/gtron/main.go`
- Modify: `cmd/gtron/config.go`

- [ ] **Step 1: Update `cmd/gtron/config.go`**

```go
package main

import (
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

func defaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".gtron")
}

func makeConfig(ctx *cli.Context) *node.Config {
	return &node.Config{
		DataDir:     ctx.String("datadir"),
		P2PPort:     ctx.Int("p2p.port"),
		HTTPPort:    ctx.Int("http.port"),
		JSONRPCPort: ctx.Int("jsonrpc.port"),
	}
}

func makeGenesis(ctx *cli.Context) *params.Genesis {
	if ctx.Bool("testnet") {
		return params.DefaultNileGenesis()
	}
	return params.DefaultMainnetGenesis()
}

func chainDataDir(dataDir string) string {
	return filepath.Join(dataDir, "gtron", "chaindata")
}
```

- [ ] **Step 2: Update `cmd/gtron/main.go`**

```go
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	"github.com/tronprotocol/go-tron/node"
	"github.com/urfave/cli/v2"
)

var (
	dataDirFlag = &cli.StringFlag{
		Name:  "datadir",
		Usage: "Data directory for the database and keystore",
		Value: defaultDataDir(),
	}
	p2pPortFlag = &cli.IntFlag{
		Name:  "p2p.port",
		Usage: "P2P listening port",
		Value: 18888,
	}
	httpPortFlag = &cli.IntFlag{
		Name:  "http.port",
		Usage: "HTTP API port",
		Value: 8090,
	}
	jsonrpcPortFlag = &cli.IntFlag{
		Name:  "jsonrpc.port",
		Usage: "JSON-RPC port",
		Value: 8545,
	}
	testnetFlag = &cli.BoolFlag{
		Name:  "testnet",
		Usage: "Use Nile testnet",
	}
)

var app = &cli.App{
	Name:    "gtron",
	Usage:   "TRON blockchain node (Go implementation)",
	Version: "0.2.0-dev",
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		testnetFlag,
	},
	Action: gtron,
	Commands: []*cli.Command{
		{
			Name:  "version",
			Usage: "Print version information",
			Action: func(ctx *cli.Context) error {
				fmt.Printf("gtron version %s\n", ctx.App.Version)
				return nil
			},
		},
		{
			Name:  "init",
			Usage: "Initialize genesis block",
			Flags: []cli.Flag{dataDirFlag, testnetFlag},
			Action: initCmd,
		},
	},
}

func initCmd(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis := makeGenesis(ctx)
	dbPath := chainDataDir(cfg.DataDir)

	db, err := rawdb.NewPebbleDB(dbPath, 256, 500)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	config, hash, err := core.SetupGenesisBlock(db, genesis)
	if err != nil {
		return fmt.Errorf("setup genesis: %w", err)
	}
	fmt.Printf("Genesis initialized: chain=%s hash=%x\n", config.ChainID, hash)
	return nil
}

func gtron(ctx *cli.Context) error {
	cfg := makeConfig(ctx)
	genesis := makeGenesis(ctx)
	dbPath := chainDataDir(cfg.DataDir)

	// Open database
	db, err := rawdb.NewPebbleDB(dbPath, 256, 500)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	// Setup genesis (idempotent)
	chainConfig, _, err := core.SetupGenesisBlock(db, genesis)
	if err != nil {
		db.Close()
		return fmt.Errorf("setup genesis: %w", err)
	}

	// Create blockchain
	sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	bc, err := core.NewBlockChain(db, sdb, chainConfig)
	if err != nil {
		db.Close()
		return fmt.Errorf("create blockchain: %w", err)
	}

	// Create transaction pool
	pool := txpool.New()

	// Create backend + API server
	backend := core.NewTronBackend(bc, pool)
	apiServer := tronapi.NewServer(backend, cfg.HTTPPort)

	// Create node and register services
	stack, err := node.New(cfg)
	if err != nil {
		db.Close()
		return err
	}
	stack.RegisterLifecycle(apiServer)

	// Start
	if err := stack.Start(); err != nil {
		db.Close()
		return err
	}

	fmt.Printf("gtron started (chain=%s, block=%d, http=:%d, datadir=%s)\n",
		chainConfig.ChainID, bc.CurrentBlock().Number(), cfg.HTTPPort, cfg.DataDir)

	// Wait for interrupt
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	fmt.Println("\nShutting down...")
	stack.Stop()
	db.Close()
	return nil
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

- [ ] **Step 3: Ensure tronapi.Server implements node.Lifecycle**

Check that `internal/tronapi/server.go`'s `Stop()` method returns `error` to match `node.Lifecycle`. The current Stop returns `error` but Lifecycle.Stop might return nothing. Check `node/lifecycle.go`:

```go
type Lifecycle interface {
    Start() error
    Stop()
}
```

If Server.Stop returns error but Lifecycle.Stop() returns nothing, add a wrapper:

```go
// In internal/tronapi/server.go, change Stop to:
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.httpServer.Shutdown(ctx)
}
```

- [ ] **Step 4: Verify everything compiles**

Run: `go build ./cmd/gtron/`
Expected: Clean build.

- [ ] **Step 5: Commit**

```bash
git add cmd/gtron/ internal/tronapi/server.go
git commit -m "cmd/gtron: add init command and full node bootstrap

gtron init: initialize genesis block in database.
gtron (run): open DB, setup genesis, create blockchain + txpool,
start HTTP API server, wait for shutdown signal."
```

---

## Self-Review

**Spec coverage:**
1. Actuator Context Migration → Task 1 (StateDB additions) + Task 2 (actuator migration) ✓
2. State Processor → Task 3 ✓
3. Full Block Insertion → Task 4 ✓
4. Transaction Pool → Task 5 ✓
5. TronBackend → Task 6 ✓
6. API Expansion → Task 6 (broadcasttransaction, getnodeinfo, gettransactioncountinpool) ✓
7. Node Bootstrap → Task 7 (init + run) ✓
8. Integration Test → Task 4 (blockchain_insert_test.go) ✓

**Placeholder scan:** No TBD/TODO found. All code blocks are complete.

**Type consistency:**
- `Context.State *state.StateDB` — used consistently across actuators and processor
- `Context.DynProps *state.DynamicProperties` — used in processor and actuators
- `TxPool` methods: `Add`, `Get`, `Pending`, `Remove`, `RemoveBatch`, `Count` — consistent
- `TronBackend` implements `tronapi.Backend` — interface matches
- `ProcessBlock` signature matches usage in `BlockChain.InsertBlock`
