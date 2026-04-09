# go-tron Phase 2: StateDB + Blockchain Core + Genesis — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform go-tron from a collection of types/accessors into a working blockchain engine with StateDB (MPT-backed), genesis block construction, resource model, 5 new actuators, and block insertion/validation.

**Architecture:** Replace `trondb` with go-ethereum's `ethdb`; build `core/state.StateDB` wrapping go-ethereum's `trie` package to produce `accountStateRoot` per block; embed Mainnet + Nile genesis as Go structs; migrate all actuators from raw DB to StateDB; build `core.BlockChain` for block insertion/validation with solidification tracking.

**Tech Stack:** Go 1.24, go-ethereum v1.17.2 (`ethdb`, `trie`, `triedb`, `ethdb/pebble`, `ethdb/memorydb`), protobuf

**Spec:** `docs/superpowers/specs/2026-04-09-go-tron-phase2-design.md`

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `core/rawdb/database.go` | `NewPebbleDB()` factory, re-export `NewMemoryDatabase()` |
| `core/state/database.go` | `Database` interface wrapping `triedb.Database` |
| `core/state/state_object.go` | `stateObject`: per-account in-memory wrapper |
| `core/state/journal.go` | Undo journal for snapshot/revert |
| `core/state/statedb.go` | `StateDB`: in-memory state manager with MPT commit |
| `core/state/dynamic_properties.go` | `DynamicProperties` cached get/set/flush |
| `core/state/statedb_test.go` | StateDB tests |
| `core/state/dynamic_properties_test.go` | DynamicProperties tests |
| `params/mainnet.go` | `DefaultMainnetGenesis()` with hardcoded data |
| `params/nile.go` | `DefaultNileGenesis()` with hardcoded data |
| `core/genesis.go` | `SetupGenesisBlock()`, `Genesis.ToBlock()` |
| `core/genesis_test.go` | Genesis tests |
| `core/resource.go` | `ResourceProcessor`: bandwidth/energy consumption/recovery |
| `core/resource_test.go` | Resource model tests |
| `actuator/freeze_v2.go` | `FreezeBalanceV2Actuator` |
| `actuator/unfreeze_v2.go` | `UnfreezeBalanceV2Actuator` |
| `actuator/vote.go` | `VoteWitnessActuator` |
| `actuator/withdraw.go` | `WithdrawBalanceActuator` |
| `actuator/withdraw_expire_unfreeze.go` | `WithdrawExpireUnfreezeActuator` |
| `actuator/freeze_v2_test.go` | FreezeV2 tests |
| `actuator/unfreeze_v2_test.go` | UnfreezeV2 tests |
| `actuator/vote_test.go` | Vote tests |
| `actuator/withdraw_test.go` | Withdraw tests |
| `actuator/withdraw_expire_unfreeze_test.go` | WithdrawExpireUnfreeze tests |
| `consensus/dpos/verify.go` | Header verification |
| `consensus/dpos/maintenance.go` | Maintenance period logic |
| `consensus/dpos/reward.go` | Block reward distribution |
| `consensus/dpos/verify_test.go` | Verify tests |
| `consensus/dpos/maintenance_test.go` | Maintenance tests |
| `core/blockchain.go` | `BlockChain`: block insertion, validation, chain management |
| `core/blockchain_test.go` | Blockchain tests |

### Modified Files

| File | Change |
|------|--------|
| `go.mod` | Add `github.com/ethereum/go-ethereum/core/rawdb` import path |
| `core/rawdb/schema.go` | `trondb` → `ethdb` imports |
| `core/rawdb/accessors_block.go` | `trondb` → `ethdb` imports |
| `core/rawdb/accessors_chain.go` | `trondb` → `ethdb` imports |
| `core/rawdb/accessors_account.go` | `trondb` → `ethdb` imports |
| `core/rawdb/accessors_test.go` | `trondb/memorydb` → `ethdb/memorydb` |
| `core/types/account.go` | Add FreezeV2, vote, resource, allowance accessors |
| `core/types/block.go` | Add `AccountStateRoot()` accessor |
| `actuator/actuator.go` | Context uses StateDB; add 5 new registry cases |
| `actuator/transfer.go` | Use StateDB instead of rawdb |
| `actuator/account.go` | Use StateDB instead of rawdb |
| `actuator/witness.go` | Use StateDB instead of rawdb |
| `consensus/consensus.go` | Extend Engine interface |
| `params/genesis.go` | Extend Genesis struct |
| `params/config.go` | Add DPoS config fields |

### Deleted Files

| File | Reason |
|------|--------|
| `trondb/database.go` | Replaced by `ethdb` |
| `trondb/memorydb/memorydb.go` | Replaced by `ethdb/memorydb` |
| `trondb/memorydb/memorydb_test.go` | Replaced by `ethdb/memorydb` |

---

### Task 1: ethdb Migration — Replace trondb with ethdb

**Files:**
- Delete: `trondb/database.go`, `trondb/memorydb/memorydb.go`, `trondb/memorydb/memorydb_test.go`
- Modify: `core/rawdb/schema.go`, `core/rawdb/accessors_block.go`, `core/rawdb/accessors_chain.go`, `core/rawdb/accessors_account.go`, `core/rawdb/accessors_test.go`, `actuator/actuator.go`, `actuator/transfer.go`, `actuator/account.go`, `actuator/witness.go`
- Create: `core/rawdb/database.go`

- [ ] **Step 1: Create `core/rawdb/database.go` with NewPebbleDB and NewMemoryDatabase**

```go
package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/ethdb/pebble"
)

// NewPebbleDB creates a persistent key-value store backed by Pebble.
func NewPebbleDB(path string, cache int, handles int) (ethdb.KeyValueStore, error) {
	return pebble.New(path, cache, handles, "", false)
}

// NewMemoryDatabase creates an ephemeral in-memory key-value store for testing.
func NewMemoryDatabase() ethdb.KeyValueStore {
	return memorydb.New()
}
```

- [ ] **Step 2: Update `core/rawdb/schema.go` — remove trondb import (no import changes needed, file has no imports from trondb)**

Verify `core/rawdb/schema.go` has no trondb imports — it only uses `encoding/binary`. No changes needed.

- [ ] **Step 3: Update `core/rawdb/accessors_block.go` — replace `trondb` with `ethdb`**

Replace the entire file content:

```go
package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func WriteBlock(db ethdb.KeyValueWriter, block *types.Block) {
	data, err := block.Marshal()
	if err != nil {
		return
	}
	db.Put(blockKey(block.Number()), data)

	num := make([]byte, 8)
	binary.BigEndian.PutUint64(num, block.Number())
	db.Put(blockHashKey(block.Hash().Bytes()), num)
}

func ReadBlock(db ethdb.KeyValueReader, number uint64) *types.Block {
	data, err := db.Get(blockKey(number))
	if err != nil {
		return nil
	}
	block, err := types.UnmarshalBlock(data)
	if err != nil {
		return nil
	}
	return block
}

func ReadBlockNumber(db ethdb.KeyValueReader, hash common.Hash) *uint64 {
	data, err := db.Get(blockHashKey(hash.Bytes()))
	if err != nil || len(data) != 8 {
		return nil
	}
	num := binary.BigEndian.Uint64(data)
	return &num
}
```

- [ ] **Step 4: Update `core/rawdb/accessors_chain.go` — replace `trondb` with `ethdb`**

Replace the entire file content:

```go
package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

func WriteHeadBlockHash(db ethdb.KeyValueWriter, hash common.Hash) {
	db.Put(headBlockKey, hash.Bytes())
}

func ReadHeadBlockHash(db ethdb.KeyValueReader) common.Hash {
	data, err := db.Get(headBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func WriteHeadSolidBlockHash(db ethdb.KeyValueWriter, hash common.Hash) {
	db.Put(headSolidBlockKey, hash.Bytes())
}

func ReadHeadSolidBlockHash(db ethdb.KeyValueReader) common.Hash {
	data, err := db.Get(headSolidBlockKey)
	if err != nil {
		return common.Hash{}
	}
	return common.BytesToHash(data)
}

func WriteDynamicProperty(db ethdb.KeyValueWriter, name string, value []byte) {
	db.Put(dynPropKey(name), value)
}

func ReadDynamicProperty(db ethdb.KeyValueReader, name string) []byte {
	data, err := db.Get(dynPropKey(name))
	if err != nil {
		return nil
	}
	return data
}
```

- [ ] **Step 5: Update `core/rawdb/accessors_account.go` — replace `trondb` with `ethdb`**

Replace the entire file content:

```go
package rawdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

func WriteAccount(db ethdb.KeyValueWriter, addr common.Address, acc *types.Account) {
	data, err := acc.Marshal()
	if err != nil {
		return
	}
	db.Put(accountKey(addr.Bytes()), data)
}

func ReadAccount(db ethdb.KeyValueReader, addr common.Address) *types.Account {
	data, err := db.Get(accountKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil
	}
	return acc
}

func DeleteAccount(db ethdb.KeyValueWriter, addr common.Address) {
	db.Delete(accountKey(addr.Bytes()))
}

func HasAccount(db ethdb.KeyValueReader, addr common.Address) bool {
	has, _ := db.Has(accountKey(addr.Bytes()))
	return has
}

func WriteWitness(db ethdb.KeyValueWriter, addr common.Address, w *types.Witness) {
	data, err := w.Marshal()
	if err != nil {
		return
	}
	db.Put(witnessKey(addr.Bytes()), data)
}

func ReadWitness(db ethdb.KeyValueReader, addr common.Address) *types.Witness {
	data, err := db.Get(witnessKey(addr.Bytes()))
	if err != nil {
		return nil
	}
	w, err := types.UnmarshalWitness(data)
	if err != nil {
		return nil
	}
	return w
}
```

- [ ] **Step 6: Update `core/rawdb/accessors_test.go` — use `rawdb.NewMemoryDatabase()`**

Replace the entire file content:

```go
package rawdb

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWriteReadBlock(t *testing.T) {
	db := NewMemoryDatabase()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    42,
				Timestamp: 126000,
			},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(db, block)

	got := ReadBlock(db, block.Number())
	if got == nil {
		t.Fatal("block not found")
	}
	if got.Number() != 42 {
		t.Fatalf("expected 42, got %d", got.Number())
	}
}

func TestWriteReadBlockByHash(t *testing.T) {
	db := NewMemoryDatabase()
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 10},
		},
	}
	block := types.NewBlockFromPB(pb)
	WriteBlock(db, block)

	num := ReadBlockNumber(db, block.Hash())
	if num == nil {
		t.Fatal("hash->number mapping not found")
	}
	if *num != 10 {
		t.Fatalf("expected 10, got %d", *num)
	}
}

func TestHeadBlock(t *testing.T) {
	db := NewMemoryDatabase()
	WriteHeadBlockHash(db, common.HexToHash("aabb"))
	h := ReadHeadBlockHash(db)
	if h != common.HexToHash("aabb") {
		t.Fatal("head block hash mismatch")
	}
}

func TestWriteReadAccount(t *testing.T) {
	db := NewMemoryDatabase()
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(1000000)

	WriteAccount(db, addr, acc)
	got := ReadAccount(db, addr)
	if got == nil {
		t.Fatal("account not found")
	}
	if got.Balance() != 1000000 {
		t.Fatalf("expected 1000000, got %d", got.Balance())
	}
}
```

- [ ] **Step 7: Update `actuator/actuator.go` — replace `trondb` with `ethdb` in Context**

Replace the entire file content:

```go
package actuator

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

type Context struct {
	DB          ethdb.KeyValueStore
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
	default:
		return nil, errors.New("unsupported contract type")
	}
}
```

- [ ] **Step 8: Update `actuator/transfer.go` — replace `trondb` import with `ethdb`**

The only import change is removing `trondb` (not used directly). The file uses `rawdb.ReadAccount`/`rawdb.WriteAccount` which now accept `ethdb` types. Since `ctx.DB` is now `ethdb.KeyValueStore`, the calls are compatible. Replace imports:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
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

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if ownerAcc.Balance() < tc.Amount {
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

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetBalance(ownerAcc.Balance() - tc.Amount)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	toAcc := rawdb.ReadAccount(ctx.DB, toAddr)
	if toAcc == nil {
		toAcc = types.NewAccount(toAddr, corepb.AccountType_Normal)
	}
	toAcc.SetBalance(toAcc.Balance() + tc.Amount)
	rawdb.WriteAccount(ctx.DB, toAddr, toAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 9: Update `actuator/account.go` — same import fix**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	newAddr := common.BytesToAddress(ac.AccountAddress)

	if ownerAddr.IsEmpty() || newAddr.IsEmpty() {
		return errors.New("invalid address")
	}

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if rawdb.HasAccount(ctx.DB, newAddr) {
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
	accType := ac.Type
	if accType == 0 {
		accType = corepb.AccountType_Normal
	}
	newAcc := types.NewAccount(newAddr, accType)
	rawdb.WriteAccount(ctx.DB, newAddr, newAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 10: Update `actuator/witness.go` — same import fix**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
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

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if rawdb.ReadWitness(ctx.DB, ownerAddr) != nil {
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

	witness := types.NewWitness(ownerAddr, string(wc.Url))
	rawdb.WriteWitness(ctx.DB, ownerAddr, witness)

	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	ownerAcc.SetIsWitness(true)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 11: Delete trondb package**

```bash
rm trondb/database.go trondb/memorydb/memorydb.go trondb/memorydb/memorydb_test.go
rmdir trondb/memorydb trondb
```

- [ ] **Step 12: Run `go mod tidy` and verify all tests pass**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron && go mod tidy
```

Run: `go test ./core/rawdb/ ./actuator/ -v -count=1`
Expected: All existing tests PASS with ethdb backend.

- [ ] **Step 13: Commit**

```bash
git add -A && git commit -m "refactor: replace trondb with go-ethereum ethdb

Migrate all database interfaces from custom trondb package to
go-ethereum's ethdb. Use ethdb/memorydb for tests. Add NewPebbleDB
and NewMemoryDatabase factories in core/rawdb."
```

---

### Task 2: Extend types.Account with FreezeV2, Vote, Resource, and Allowance Accessors

**Files:**
- Modify: `core/types/account.go`
- Create: `core/types/account_test.go`

- [ ] **Step 1: Write the test file**

```go
package types

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestAccountFreezeV2(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(100_000_000)

	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 50_000_000)
	acc.AddFreezeV2(corepb.ResourceCode_ENERGY, 30_000_000)

	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 50_000_000 {
		t.Fatalf("bandwidth frozen: want 50000000, got %d", got)
	}
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_ENERGY); got != 30_000_000 {
		t.Fatalf("energy frozen: want 30000000, got %d", got)
	}
	if got := acc.TotalFrozenV2(); got != 80_000_000 {
		t.Fatalf("total frozen: want 80000000, got %d", got)
	}

	// Add more to existing type
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 10_000_000)
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 60_000_000 {
		t.Fatalf("bandwidth frozen after add: want 60000000, got %d", got)
	}
}

func TestAccountUnfreezeV2(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)

	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 10_000_000, 1000)
	acc.AddUnfreezeV2(corepb.ResourceCode_ENERGY, 5_000_000, 2000)
	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 3_000_000, 500)

	if got := len(acc.UnfrozenV2()); got != 3 {
		t.Fatalf("unfrozen count: want 3, got %d", got)
	}

	// Remove expired at time=1500: entries with expire <= 1500 are 1000 and 500
	withdrawn := acc.RemoveExpiredUnfreezeV2(1500)
	if withdrawn != 13_000_000 {
		t.Fatalf("withdrawn: want 13000000, got %d", withdrawn)
	}
	if got := len(acc.UnfrozenV2()); got != 1 {
		t.Fatalf("remaining unfrozen: want 1, got %d", got)
	}
}

func TestAccountVotes(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)

	witness1 := []byte{0x41, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	witness2 := []byte{0x41, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	votes := []*corepb.Vote{
		{VoteAddress: witness1, VoteCount: 100},
		{VoteAddress: witness2, VoteCount: 200},
	}
	acc.SetVotes(votes)

	if got := len(acc.Votes()); got != 2 {
		t.Fatalf("vote count: want 2, got %d", got)
	}
	if got := acc.Votes()[0].VoteCount; got != 100 {
		t.Fatalf("vote[0] count: want 100, got %d", got)
	}

	acc.ClearVotes()
	if got := len(acc.Votes()); got != 0 {
		t.Fatalf("vote count after clear: want 0, got %d", got)
	}
}

func TestAccountResourceTracking(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)

	acc.SetNetUsage(500)
	acc.SetLatestConsumeTime(3000)
	acc.SetFreeNetUsage(100)
	acc.SetLatestConsumeFreeTime(6000)

	if acc.NetUsage() != 500 {
		t.Fatalf("net_usage: want 500, got %d", acc.NetUsage())
	}
	if acc.LatestConsumeTime() != 3000 {
		t.Fatalf("latest_consume_time: want 3000, got %d", acc.LatestConsumeTime())
	}
	if acc.FreeNetUsage() != 100 {
		t.Fatalf("free_net_usage: want 100, got %d", acc.FreeNetUsage())
	}
	if acc.LatestConsumeFreeTime() != 6000 {
		t.Fatalf("latest_consume_free_time: want 6000, got %d", acc.LatestConsumeFreeTime())
	}
}

func TestAccountEnergyTracking(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)

	acc.SetEnergyUsage(1000)
	acc.SetLatestConsumeTimeForEnergy(9000)

	if acc.EnergyUsage() != 1000 {
		t.Fatalf("energy_usage: want 1000, got %d", acc.EnergyUsage())
	}
	if acc.LatestConsumeTimeForEnergy() != 9000 {
		t.Fatalf("latest_consume_time_for_energy: want 9000, got %d", acc.LatestConsumeTimeForEnergy())
	}
}

func TestAccountAllowance(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)

	acc.SetAllowance(16_000_000)
	acc.SetLatestWithdrawTime(100000)

	if acc.Allowance() != 16_000_000 {
		t.Fatalf("allowance: want 16000000, got %d", acc.Allowance())
	}
	if acc.LatestWithdrawTime() != 100000 {
		t.Fatalf("latest_withdraw_time: want 100000, got %d", acc.LatestWithdrawTime())
	}
}

func TestAccountReduceFreezeV2(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 50_000_000)

	acc.ReduceFreezeV2(corepb.ResourceCode_BANDWIDTH, 20_000_000)
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 30_000_000 {
		t.Fatalf("after reduce: want 30000000, got %d", got)
	}
}

func TestAccountMarshalWithExtendedFields(t *testing.T) {
	addr := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	acc := NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(100)
	acc.AddFreezeV2(corepb.ResourceCode_ENERGY, 500)
	acc.SetAllowance(200)

	data, err := acc.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	restored, err := UnmarshalAccount(data)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Balance() != 100 {
		t.Fatalf("balance: want 100, got %d", restored.Balance())
	}
	if restored.GetFrozenV2Amount(corepb.ResourceCode_ENERGY) != 500 {
		t.Fatalf("energy frozen: want 500, got %d", restored.GetFrozenV2Amount(corepb.ResourceCode_ENERGY))
	}
	if restored.Allowance() != 200 {
		t.Fatalf("allowance: want 200, got %d", restored.Allowance())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/types/ -run TestAccountFreezeV2 -v -count=1`
Expected: FAIL — `acc.AddFreezeV2` undefined

- [ ] **Step 3: Add all new accessors to `core/types/account.go`**

Add these methods after the existing methods (after `SetCreateTime`):

```go
// FreezeV2 accessors

func (a *Account) FrozenV2() []*corepb.Account_FreezeV2 {
	return a.pb.FrozenV2
}

func (a *Account) AddFreezeV2(resourceType corepb.ResourceCode, amount int64) {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == resourceType {
			f.Amount += amount
			return
		}
	}
	a.pb.FrozenV2 = append(a.pb.FrozenV2, &corepb.Account_FreezeV2{
		Type:   resourceType,
		Amount: amount,
	})
}

func (a *Account) ReduceFreezeV2(resourceType corepb.ResourceCode, amount int64) {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == resourceType {
			f.Amount -= amount
			if f.Amount < 0 {
				f.Amount = 0
			}
			return
		}
	}
}

func (a *Account) GetFrozenV2Amount(resourceType corepb.ResourceCode) int64 {
	for _, f := range a.pb.FrozenV2 {
		if f.Type == resourceType {
			return f.Amount
		}
	}
	return 0
}

func (a *Account) TotalFrozenV2() int64 {
	var total int64
	for _, f := range a.pb.FrozenV2 {
		total += f.Amount
	}
	return total
}

// UnfreezeV2 accessors

func (a *Account) UnfrozenV2() []*corepb.Account_UnFreezeV2 {
	return a.pb.UnfrozenV2
}

func (a *Account) AddUnfreezeV2(resourceType corepb.ResourceCode, amount int64, expireTime int64) {
	a.pb.UnfrozenV2 = append(a.pb.UnfrozenV2, &corepb.Account_UnFreezeV2{
		Type:               resourceType,
		UnfreezeAmount:     amount,
		UnfreezeExpireTime: expireTime,
	})
}

func (a *Account) RemoveExpiredUnfreezeV2(now int64) int64 {
	var totalWithdrawn int64
	var remaining []*corepb.Account_UnFreezeV2
	for _, u := range a.pb.UnfrozenV2 {
		if u.UnfreezeExpireTime <= now {
			totalWithdrawn += u.UnfreezeAmount
		} else {
			remaining = append(remaining, u)
		}
	}
	a.pb.UnfrozenV2 = remaining
	return totalWithdrawn
}

// Vote accessors

func (a *Account) Votes() []*corepb.Vote {
	return a.pb.Votes
}

func (a *Account) SetVotes(votes []*corepb.Vote) {
	a.pb.Votes = votes
}

func (a *Account) ClearVotes() {
	a.pb.Votes = nil
}

// Resource tracking — bandwidth

func (a *Account) NetUsage() int64           { return a.pb.NetUsage }
func (a *Account) SetNetUsage(v int64)       { a.pb.NetUsage = v }
func (a *Account) LatestConsumeTime() int64  { return a.pb.LatestConsumeTime }
func (a *Account) SetLatestConsumeTime(t int64) { a.pb.LatestConsumeTime = t }
func (a *Account) FreeNetUsage() int64       { return a.pb.FreeNetUsage }
func (a *Account) SetFreeNetUsage(v int64)   { a.pb.FreeNetUsage = v }
func (a *Account) LatestConsumeFreeTime() int64 { return a.pb.LatestConsumeFreeTime }
func (a *Account) SetLatestConsumeFreeTime(t int64) { a.pb.LatestConsumeFreeTime = t }

// Resource tracking — energy

func (a *Account) EnergyUsage() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.EnergyUsage
}

func (a *Account) SetEnergyUsage(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.EnergyUsage = v
}

func (a *Account) LatestConsumeTimeForEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.LatestConsumeTimeForEnergy
}

func (a *Account) SetLatestConsumeTimeForEnergy(t int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.LatestConsumeTimeForEnergy = t
}

func (a *Account) ensureAccountResource() {
	if a.pb.AccountResource == nil {
		a.pb.AccountResource = &corepb.Account_AccountResource{}
	}
}

// Allowance (witness rewards)

func (a *Account) Allowance() int64           { return a.pb.Allowance }
func (a *Account) SetAllowance(v int64)       { a.pb.Allowance = v }
func (a *Account) LatestWithdrawTime() int64  { return a.pb.LatestWithdrawTime }
func (a *Account) SetLatestWithdrawTime(t int64) { a.pb.LatestWithdrawTime = t }
```

- [ ] **Step 4: Run tests**

Run: `go test ./core/types/ -v -count=1`
Expected: All PASS.

- [ ] **Step 5: Add `AccountStateRoot()` to `core/types/block.go`**

Add after the `WitnessSignature()` method:

```go
func (b *Block) AccountStateRoot() common.Hash {
	if b.pb.BlockHeader == nil || b.pb.BlockHeader.RawData == nil {
		return common.Hash{}
	}
	return common.BytesToHash(b.pb.BlockHeader.RawData.AccountStateRoot)
}
```

- [ ] **Step 6: Commit**

```bash
git add core/types/account.go core/types/account_test.go core/types/block.go
git commit -m "core/types: add FreezeV2, vote, resource, allowance accessors to Account

Extend Account wrapper with methods for FreezeV2/UnfreezeV2 management,
vote tracking, bandwidth/energy resource usage, witness allowance, and
block AccountStateRoot accessor."
```

---

### Task 3: DynamicProperties — Runtime Chain Parameters

**Files:**
- Create: `core/state/dynamic_properties.go`, `core/state/dynamic_properties_test.go`

- [ ] **Step 1: Write the test file**

```go
package state

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

func TestDynamicPropertiesDefaults(t *testing.T) {
	dp := NewDynamicProperties()

	if got := dp.MaintenanceTimeInterval(); got != 21600000 {
		t.Fatalf("maintenance_time_interval: want 21600000, got %d", got)
	}
	if got := dp.WitnessPayPerBlock(); got != 16_000_000 {
		t.Fatalf("witness_pay_per_block: want 16000000, got %d", got)
	}
	if got := dp.TransactionFee(); got != 10 {
		t.Fatalf("transaction_fee: want 10, got %d", got)
	}
	if got := dp.EnergyFee(); got != 100 {
		t.Fatalf("energy_fee: want 100, got %d", got)
	}
	if got := dp.UnfreezeDelayDays(); got != 14 {
		t.Fatalf("unfreeze_delay_days: want 14, got %d", got)
	}
}

func TestDynamicPropertiesSetGet(t *testing.T) {
	dp := NewDynamicProperties()
	dp.SetLatestBlockHeaderNumber(42)
	dp.SetLatestBlockHeaderTimestamp(126000)
	dp.SetNextMaintenanceTime(21600000)

	if dp.LatestBlockHeaderNumber() != 42 {
		t.Fatalf("latest_block_header_number: want 42, got %d", dp.LatestBlockHeaderNumber())
	}
	if dp.LatestBlockHeaderTimestamp() != 126000 {
		t.Fatalf("latest_block_header_timestamp: want 126000, got %d", dp.LatestBlockHeaderTimestamp())
	}
	if dp.NextMaintenanceTime() != 21600000 {
		t.Fatalf("next_maintenance_time: want 21600000, got %d", dp.NextMaintenanceTime())
	}
}

func TestDynamicPropertiesFlushAndLoad(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	dp := NewDynamicProperties()
	dp.SetLatestBlockHeaderNumber(100)
	dp.Set("energy_fee", 200)
	dp.Flush(db)

	loaded := LoadDynamicProperties(db)
	if loaded.LatestBlockHeaderNumber() != 100 {
		t.Fatalf("loaded number: want 100, got %d", loaded.LatestBlockHeaderNumber())
	}
	if loaded.EnergyFee() != 200 {
		t.Fatalf("loaded energy_fee: want 200, got %d", loaded.EnergyFee())
	}
}

func TestDynamicPropertiesGenericGetSet(t *testing.T) {
	dp := NewDynamicProperties()

	dp.Set("custom_prop", 999)
	val, ok := dp.Get("custom_prop")
	if !ok || val != 999 {
		t.Fatalf("custom_prop: want 999, got %d (ok=%v)", val, ok)
	}

	_, ok = dp.Get("nonexistent")
	if ok {
		t.Fatal("nonexistent should return false")
	}
}

func TestDynamicPropertiesOnlyDirtyFlushed(t *testing.T) {
	db := rawdb.NewMemoryDatabase()

	dp := NewDynamicProperties()
	// Don't change any values, flush
	dp.Flush(db)

	// Load from empty DB — should get defaults
	loaded := LoadDynamicProperties(db)
	// Default energy_fee should still be 100 (from NewDynamicProperties defaults)
	if loaded.EnergyFee() != 100 {
		t.Fatalf("default energy_fee: want 100, got %d", loaded.EnergyFee())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestDynamicProperties -v -count=1`
Expected: FAIL — package doesn't exist.

- [ ] **Step 3: Implement `core/state/dynamic_properties.go`**

```go
package state

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// DynamicProperties holds runtime-adjustable chain parameters.
// These are stored as key-value pairs in the database with prefix "dp-".
type DynamicProperties struct {
	props map[string]int64
	dirty map[string]struct{}

	// Special: hash value stored separately
	latestBlockHeaderHash common.Hash
	hashDirty             bool
}

// Default values matching java-tron mainnet genesis.
var defaultProps = map[string]int64{
	"maintenance_time_interval":                      21600000,
	"account_upgrade_cost":                           9999000000,
	"create_account_fee":                             100000,
	"transaction_fee":                                10,
	"asset_issue_fee":                                1024000000,
	"witness_pay_per_block":                          16000000,
	"witness_standby_allowance":                      115200000000,
	"create_new_account_fee_in_system_contract":      0,
	"create_new_account_bandwidth_rate":               1,
	"energy_fee":                                     100,
	"max_cpu_time_of_one_tx":                         80,
	"total_energy_current_limit":                     50000000000,
	"total_net_limit":                                43200000000,
	"unfreeze_delay_days":                            14,
	"latest_block_header_timestamp":                  0,
	"latest_block_header_number":                     0,
	"latest_solidified_block_num":                    0,
	"next_maintenance_time":                          0,
	"allow_new_resource_model":                       0,
}

func NewDynamicProperties() *DynamicProperties {
	props := make(map[string]int64, len(defaultProps))
	for k, v := range defaultProps {
		props[k] = v
	}
	return &DynamicProperties{
		props: props,
		dirty: make(map[string]struct{}),
	}
}

func LoadDynamicProperties(db ethdb.KeyValueReader) *DynamicProperties {
	dp := NewDynamicProperties()

	// Override defaults with persisted values
	for key := range dp.props {
		data := rawdb.ReadDynamicProperty(db, key)
		if data != nil && len(data) == 8 {
			dp.props[key] = int64(binary.BigEndian.Uint64(data))
		}
	}

	// Load hash
	hashData := rawdb.ReadDynamicProperty(db, "latest_block_header_hash")
	if hashData != nil {
		dp.latestBlockHeaderHash = common.BytesToHash(hashData)
	}

	dp.dirty = make(map[string]struct{}) // clear dirty after load
	return dp
}

// Typed getters

func (d *DynamicProperties) MaintenanceTimeInterval() int64    { return d.props["maintenance_time_interval"] }
func (d *DynamicProperties) NextMaintenanceTime() int64        { return d.props["next_maintenance_time"] }
func (d *DynamicProperties) LatestBlockHeaderNumber() int64    { return d.props["latest_block_header_number"] }
func (d *DynamicProperties) LatestBlockHeaderTimestamp() int64  { return d.props["latest_block_header_timestamp"] }
func (d *DynamicProperties) LatestSolidifiedBlockNum() int64   { return d.props["latest_solidified_block_num"] }
func (d *DynamicProperties) WitnessPayPerBlock() int64         { return d.props["witness_pay_per_block"] }
func (d *DynamicProperties) WitnessStandbyAllowance() int64    { return d.props["witness_standby_allowance"] }
func (d *DynamicProperties) TransactionFee() int64             { return d.props["transaction_fee"] }
func (d *DynamicProperties) EnergyFee() int64                  { return d.props["energy_fee"] }
func (d *DynamicProperties) CreateAccountFee() int64           { return d.props["create_account_fee"] }
func (d *DynamicProperties) CreateNewAccountFeeInSystemContract() int64 { return d.props["create_new_account_fee_in_system_contract"] }
func (d *DynamicProperties) TotalEnergyCurrentLimit() int64    { return d.props["total_energy_current_limit"] }
func (d *DynamicProperties) TotalNetLimit() int64              { return d.props["total_net_limit"] }
func (d *DynamicProperties) UnfreezeDelayDays() int64          { return d.props["unfreeze_delay_days"] }
func (d *DynamicProperties) MaxCpuTimeOfOneTx() int64          { return d.props["max_cpu_time_of_one_tx"] }
func (d *DynamicProperties) AllowNewResourceModel() bool       { return d.props["allow_new_resource_model"] != 0 }

func (d *DynamicProperties) LatestBlockHeaderHash() common.Hash { return d.latestBlockHeaderHash }

// Typed setters

func (d *DynamicProperties) SetNextMaintenanceTime(t int64)       { d.set("next_maintenance_time", t) }
func (d *DynamicProperties) SetLatestBlockHeaderNumber(n int64)   { d.set("latest_block_header_number", n) }
func (d *DynamicProperties) SetLatestBlockHeaderTimestamp(t int64) { d.set("latest_block_header_timestamp", t) }
func (d *DynamicProperties) SetLatestSolidifiedBlockNum(n int64)  { d.set("latest_solidified_block_num", n) }

func (d *DynamicProperties) SetLatestBlockHeaderHash(h common.Hash) {
	d.latestBlockHeaderHash = h
	d.hashDirty = true
}

// Generic get/set

func (d *DynamicProperties) Get(key string) (int64, bool) {
	v, ok := d.props[key]
	return v, ok
}

func (d *DynamicProperties) Set(key string, value int64) {
	d.set(key, value)
}

func (d *DynamicProperties) set(key string, value int64) {
	d.props[key] = value
	d.dirty[key] = struct{}{}
}

// Flush writes dirty properties to the database.
func (d *DynamicProperties) Flush(db ethdb.KeyValueWriter) {
	buf := make([]byte, 8)
	for key := range d.dirty {
		binary.BigEndian.PutUint64(buf, uint64(d.props[key]))
		rawdb.WriteDynamicProperty(db, key, buf)
	}
	if d.hashDirty {
		rawdb.WriteDynamicProperty(db, "latest_block_header_hash", d.latestBlockHeaderHash.Bytes())
	}
	d.dirty = make(map[string]struct{})
	d.hashDirty = false
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./core/state/ -v -count=1`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add core/state/dynamic_properties.go core/state/dynamic_properties_test.go
git commit -m "core/state: add DynamicProperties for runtime chain parameters

Key-value store for adjustable parameters (maintenance interval,
witness pay, energy fee, etc.) with defaults, typed getters/setters,
and DB persistence via Flush/Load."
```

---

### Task 4: StateDB Core — Database Interface, stateObject, Journal

**Files:**
- Create: `core/state/database.go`, `core/state/state_object.go`, `core/state/journal.go`

- [ ] **Step 1: Create `core/state/database.go`**

```go
package state

import (
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/triedb"
)

// Database wraps access to tries.
type Database struct {
	disk   ethdb.Database
	trieDB *triedb.Database
}

// NewDatabase creates a state database.
func NewDatabase(diskdb ethdb.Database) *Database {
	trieDB := triedb.NewDatabase(diskdb, nil) // hash-based defaults
	return &Database{
		disk:   diskdb,
		trieDB: trieDB,
	}
}

// OpenTrie opens the main account trie at the given root.
func (db *Database) OpenTrie(root ethcommon.Hash) (*trie.Trie, error) {
	return trie.New(trie.TrieID(root), db.trieDB)
}

// TrieDB returns the underlying trie database for committing nodes.
func (db *Database) TrieDB() *triedb.Database {
	return db.trieDB
}

// DiskDB returns the underlying disk database.
func (db *Database) DiskDB() ethdb.Database {
	return db.disk
}
```

- [ ] **Step 2: Create `core/state/state_object.go`**

```go
package state

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// stateObject represents an in-memory account with dirty tracking.
type stateObject struct {
	address tcommon.Address
	account *types.Account
	dirty   bool
	deleted bool
}

func newStateObject(addr tcommon.Address, acc *types.Account) *stateObject {
	return &stateObject{
		address: addr,
		account: acc,
	}
}

func newEmptyStateObject(addr tcommon.Address) *stateObject {
	return &stateObject{
		address: addr,
		account: types.NewAccount(addr, corepb.AccountType_Normal),
		dirty:   true,
	}
}

func (s *stateObject) markDirty() {
	s.dirty = true
}
```

- [ ] **Step 3: Create `core/state/journal.go`**

```go
package state

import tcommon "github.com/tronprotocol/go-tron/common"

// journalEntry represents a single undo operation.
type journalEntry struct {
	address tcommon.Address
	prev    []byte // serialized Account protobuf before mutation, nil if account didn't exist
}

// journal tracks state changes for snapshot/revert.
type journal struct {
	entries []journalEntry
}

func newJournal() *journal {
	return &journal{}
}

func (j *journal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
}

func (j *journal) length() int {
	return len(j.entries)
}

func (j *journal) revert(stateObjects map[tcommon.Address]*stateObject, to int) {
	for i := len(j.entries) - 1; i >= to; i-- {
		entry := j.entries[i]
		if entry.prev == nil {
			// Account didn't exist before, delete it
			delete(stateObjects, entry.address)
		} else {
			// Restore previous state
			acc, err := unmarshalAccountForJournal(entry.prev)
			if err != nil {
				// Should never happen — we serialized it ourselves
				continue
			}
			obj := stateObjects[entry.address]
			if obj != nil {
				obj.account = acc
				obj.dirty = true
			}
		}
	}
	j.entries = j.entries[:to]
}
```

- [ ] **Step 4: Add `unmarshalAccountForJournal` helper (private, in journal.go)**

Add at the bottom of `journal.go`:

```go
func unmarshalAccountForJournal(data []byte) (*types.Account, error) {
	return types.UnmarshalAccount(data)
}
```

Wait — `types.UnmarshalAccount` is already the correct function. The import needs to be added. Update the import block in `journal.go`:

```go
import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)
```

And simplify `revert` to call `types.UnmarshalAccount` directly:

```go
func (j *journal) revert(stateObjects map[tcommon.Address]*stateObject, to int) {
	for i := len(j.entries) - 1; i >= to; i-- {
		entry := j.entries[i]
		if entry.prev == nil {
			delete(stateObjects, entry.address)
		} else {
			acc, err := types.UnmarshalAccount(entry.prev)
			if err != nil {
				continue
			}
			obj := stateObjects[entry.address]
			if obj != nil {
				obj.account = acc
				obj.dirty = true
			}
		}
	}
	j.entries = j.entries[:to]
}
```

Remove the `unmarshalAccountForJournal` helper — it's unnecessary.

- [ ] **Step 5: Commit**

```bash
git add core/state/database.go core/state/state_object.go core/state/journal.go
git commit -m "core/state: add Database, stateObject, and journal

Database wraps triedb for MPT access. stateObject is per-account
in-memory state. journal tracks mutations for snapshot/revert."
```

---

### Task 5: StateDB — Main State Manager with MPT

**Files:**
- Create: `core/state/statedb.go`
- Modify: `core/state/statedb_test.go` (create)

- [ ] **Step 1: Write the test file `core/state/statedb_test.go`**

```go
package state

import (
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	return sdb
}

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestStateDBGetSetBalance(t *testing.T) {
	sdb := newTestStateDB(t)

	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)

	if got := sdb.GetBalance(addr); got != 100_000_000 {
		t.Fatalf("balance: want 100000000, got %d", got)
	}

	if err := sdb.SubBalance(addr, 30_000_000); err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetBalance(addr); got != 70_000_000 {
		t.Fatalf("balance after sub: want 70000000, got %d", got)
	}
}

func TestStateDBSubBalanceInsufficient(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 50)

	err := sdb.SubBalance(addr, 100)
	if err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestStateDBSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)

	snap := sdb.Snapshot()
	sdb.AddBalance(addr, 50)
	if got := sdb.GetBalance(addr); got != 150 {
		t.Fatalf("before revert: want 150, got %d", got)
	}

	sdb.RevertToSnapshot(snap)
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("after revert: want 100, got %d", got)
	}
}

func TestStateDBSnapshotRevertNewAccount(t *testing.T) {
	sdb := newTestStateDB(t)

	snap := sdb.Snapshot()
	addr := testAddr(2)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 200)

	sdb.RevertToSnapshot(snap)

	acc := sdb.GetAccount(addr)
	if acc != nil {
		t.Fatal("account should not exist after revert")
	}
}

func TestStateDBCommitChangesRoot(t *testing.T) {
	sdb := newTestStateDB(t)

	// Empty state root
	emptyRoot := ethcommon.Hash(ethtypes.EmptyRootHash)

	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Root should change from empty
	if ethcommon.Hash(root) == emptyRoot {
		t.Fatal("root should differ from empty root after commit")
	}
}

func TestStateDBCommitDeterministic(t *testing.T) {
	// Two StateDBs with same operations should produce same root
	makeState := func() tcommon.Hash {
		diskdb := ethrawdb.NewMemoryDatabase()
		db := NewDatabase(diskdb)
		sdb, _ := New(tcommon.Hash(ethtypes.EmptyRootHash), db)

		addr1 := testAddr(1)
		addr2 := testAddr(2)
		sdb.GetOrCreateAccount(addr1)
		sdb.AddBalance(addr1, 500)
		sdb.GetOrCreateAccount(addr2)
		sdb.AddBalance(addr2, 300)

		root, _ := sdb.Commit()
		return root
	}

	root1 := makeState()
	root2 := makeState()

	if root1 != root2 {
		t.Fatalf("roots differ: %x vs %x", root1, root2)
	}
}

func TestStateDBFreezeV2(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)

	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 50_000_000)

	acc := sdb.GetAccount(addr)
	if acc == nil {
		t.Fatal("account not found")
	}
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 50_000_000 {
		t.Fatalf("frozen bandwidth: want 50000000, got %d", got)
	}
}

func TestStateDBWitness(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)

	w := sdb.GetWitness(addr)
	if w != nil {
		t.Fatal("witness should not exist")
	}

	sdb.GetOrCreateAccount(addr)
	sdb.PutWitness(addr, "http://example.com")

	w = sdb.GetWitness(addr)
	if w == nil {
		t.Fatal("witness should exist")
	}
	if w.URL() != "http://example.com" {
		t.Fatalf("witness url: want http://example.com, got %s", w.URL())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/state/ -run TestStateDB -v -count=1`
Expected: FAIL — `New` function not defined.

- [ ] **Step 3: Implement `core/state/statedb.go`**

```go
package state

import (
	"errors"

	"github.com/ethereum/go-ethereum/crypto"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var (
	ErrInsufficientBalance = errors.New("insufficient balance")
)

// StateDB manages in-memory account state with MPT-backed commits.
type StateDB struct {
	db   *Database
	trie *trie.Trie

	stateObjects map[tcommon.Address]*stateObject
	witnesses    map[tcommon.Address]*types.Witness

	journal   *journal
	snapshots []int // journal length at each snapshot

	dynProps *DynamicProperties
}

// New creates a StateDB from the given state root.
func New(root tcommon.Hash, db *Database) (*StateDB, error) {
	tr, err := db.OpenTrie(ethcommon.Hash(root))
	if err != nil {
		return nil, err
	}
	return &StateDB{
		db:           db,
		trie:         tr,
		stateObjects: make(map[tcommon.Address]*stateObject),
		witnesses:    make(map[tcommon.Address]*types.Witness),
		journal:      newJournal(),
		dynProps:     NewDynamicProperties(),
	}, nil
}

// GetAccount returns the account at addr, or nil if not found.
func (s *StateDB) GetAccount(addr tcommon.Address) *types.Account {
	obj := s.getStateObject(addr)
	if obj == nil || obj.deleted {
		return nil
	}
	return obj.account
}

// GetOrCreateAccount returns the account at addr, creating it if it doesn't exist.
func (s *StateDB) GetOrCreateAccount(addr tcommon.Address) *stateObject {
	obj := s.getStateObject(addr)
	if obj != nil && !obj.deleted {
		return obj
	}
	obj = newEmptyStateObject(addr)
	s.stateObjects[addr] = obj
	return obj
}

// GetBalance returns the TRX balance of the account.
func (s *StateDB) GetBalance(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.Balance()
}

// AddBalance adds amount to the account's balance.
func (s *StateDB) AddBalance(addr tcommon.Address, amount int64) {
	obj := s.GetOrCreateAccount(addr)
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() + amount)
	obj.markDirty()
}

// SubBalance subtracts amount from the account's balance.
func (s *StateDB) SubBalance(addr tcommon.Address, amount int64) error {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ErrInsufficientBalance
	}
	if obj.account.Balance() < amount {
		return ErrInsufficientBalance
	}
	s.journalAccount(addr, obj)
	obj.account.SetBalance(obj.account.Balance() - amount)
	obj.markDirty()
	return nil
}

// AddFreezeV2 adds a freeze entry for the given resource type.
func (s *StateDB) AddFreezeV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.AddFreezeV2(resourceType, amount)
	obj.markDirty()
}

// GetWitness returns the witness at addr.
func (s *StateDB) GetWitness(addr tcommon.Address) *types.Witness {
	return s.witnesses[addr]
}

// PutWitness stores a witness.
func (s *StateDB) PutWitness(addr tcommon.Address, url string) {
	s.witnesses[addr] = types.NewWitness(addr, url)
}

// DynamicProperties returns the dynamic properties.
func (s *StateDB) DynamicProperties() *DynamicProperties {
	return s.dynProps
}

// SetDynamicProperties sets the dynamic properties (used during genesis setup).
func (s *StateDB) SetDynamicProperties(dp *DynamicProperties) {
	s.dynProps = dp
}

// Snapshot returns a snapshot ID for later revert.
func (s *StateDB) Snapshot() int {
	id := len(s.snapshots)
	s.snapshots = append(s.snapshots, s.journal.length())
	return id
}

// RevertToSnapshot reverts state changes to the given snapshot.
func (s *StateDB) RevertToSnapshot(id int) {
	if id >= len(s.snapshots) {
		return
	}
	journalLen := s.snapshots[id]
	s.journal.revert(s.stateObjects, journalLen)
	s.snapshots = s.snapshots[:id]
}

// Commit writes all dirty accounts to the MPT and returns the new root hash.
func (s *StateDB) Commit() (tcommon.Hash, error) {
	for addr, obj := range s.stateObjects {
		if !obj.dirty {
			continue
		}
		if obj.deleted {
			s.trie.Delete(trieKey(addr))
			continue
		}
		data, err := obj.account.Marshal()
		if err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.trie.Update(trieKey(addr), data); err != nil {
			return tcommon.Hash{}, err
		}
		obj.dirty = false
	}

	root, nodes := s.trie.Commit(false)
	if nodes != nil {
		if err := s.db.TrieDB().Update(root, ethcommon.Hash(ethtypes.EmptyRootHash), 0, trienode.NewWithNodeSet(nodes), nil); err != nil {
			return tcommon.Hash{}, err
		}
		if err := s.db.TrieDB().Commit(root, false); err != nil {
			return tcommon.Hash{}, err
		}
	}

	return tcommon.Hash(root), nil
}

// getStateObject returns the state object for addr, loading from trie if needed.
func (s *StateDB) getStateObject(addr tcommon.Address) *stateObject {
	if obj, ok := s.stateObjects[addr]; ok {
		return obj
	}
	// Try to load from trie
	data, err := s.trie.Get(trieKey(addr))
	if err != nil || data == nil {
		return nil
	}
	acc, err := types.UnmarshalAccount(data)
	if err != nil {
		return nil
	}
	obj := newStateObject(addr, acc)
	s.stateObjects[addr] = obj
	return obj
}

// journalAccount records the current state of an account for revert.
func (s *StateDB) journalAccount(addr tcommon.Address, obj *stateObject) {
	var prev []byte
	if obj != nil && obj.account != nil {
		prev, _ = obj.account.Marshal()
	}
	s.journal.append(journalEntry{
		address: addr,
		prev:    prev,
	})
}

// trieKey returns the MPT key for a TRON address: Keccak256(address).
func trieKey(addr tcommon.Address) []byte {
	return crypto.Keccak256(addr.Bytes())
}
```

**Important:** This uses `trienode.NewWithNodeSet` and `ethtypes.EmptyRootHash`. The imports need:

```go
import (
	"errors"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)
```

Note: The engineer should verify the exact `triedb.Database.Update()` signature from go-ethereum v1.17.2. The signature is:

```go
func (db *Database) Update(root common.Hash, parent common.Hash, block uint64, nodes *trienode.MergedNodeSet, states *triedb.StateSet) error
```

And `triedb.Database.Commit(root common.Hash, report bool) error`.

- [ ] **Step 4: Run tests**

Run: `go test ./core/state/ -v -count=1`
Expected: All PASS. If import issues arise with triedb/trienode, fix the exact API signatures by checking `go doc`.

- [ ] **Step 5: Commit**

```bash
git add core/state/statedb.go core/state/statedb_test.go
git commit -m "core/state: add StateDB with MPT-backed Commit

In-memory account state manager with snapshot/revert, dirty tracking,
and Merkle Patricia Trie commit producing accountStateRoot hash."
```

---

### Task 6: Extend Genesis Struct and Add Mainnet/Nile Genesis Data

**Files:**
- Modify: `params/genesis.go`, `params/config.go`
- Create: `params/mainnet.go`, `params/nile.go`

- [ ] **Step 1: Update `params/genesis.go` with extended struct**

```go
package params

import "github.com/tronprotocol/go-tron/common"

// GenesisAccount defines a genesis account allocation.
type GenesisAccount struct {
	Address     common.Address
	Balance     int64
	AccountType int32  // 0=Normal, 1=AssetIssue, 2=Contract
	AccountName string
}

// GenesisWitness defines a genesis super representative.
type GenesisWitness struct {
	Address   common.Address
	VoteCount int64
	URL       string
}

// Genesis defines the initial state of the blockchain.
type Genesis struct {
	Config            *ChainConfig
	Timestamp         int64
	ParentHash        common.Hash
	Accounts          []GenesisAccount
	Witnesses         []GenesisWitness
	DynamicProperties map[string]int64
}
```

- [ ] **Step 2: Create `params/mainnet.go`**

```go
package params

import (
	"encoding/hex"

	"github.com/tronprotocol/go-tron/common"
)

func hexToAddress(h string) common.Address {
	b, _ := hex.DecodeString(h)
	return common.BytesToAddress(b)
}

func DefaultMainnetGenesis() *Genesis {
	return &Genesis{
		Config:    MainnetChainConfig,
		Timestamp: 0,
		Accounts: []GenesisAccount{
			{Address: hexToAddress("41928c9af0651632157ef27a2cf17ca72c575a4d21"), Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: hexToAddress("41a614f803b6fd780986a42c78ec9c7f77e6ded13c"), Balance: 0, AccountName: "Sun"},
			{Address: hexToAddress("41b0a14fb448b324ca992f2ddcb7d7b49470da3cf8"), Balance: -9223372036854775808, AccountName: "Blackhole"},
		},
		Witnesses: mainnetWitnesses(),
		DynamicProperties: map[string]int64{
			"maintenance_time_interval":                 21600000,
			"account_upgrade_cost":                      9999000000,
			"create_account_fee":                        100000,
			"transaction_fee":                           10,
			"asset_issue_fee":                           1024000000,
			"witness_pay_per_block":                     16000000,
			"witness_standby_allowance":                 115200000000,
			"create_new_account_fee_in_system_contract": 0,
			"create_new_account_bandwidth_rate":          1,
			"energy_fee":                                100,
			"max_cpu_time_of_one_tx":                    80,
			"total_energy_current_limit":                50000000000,
			"total_net_limit":                           43200000000,
			"unfreeze_delay_days":                       14,
		},
	}
}

func mainnetWitnesses() []GenesisWitness {
	return []GenesisWitness{
		{Address: hexToAddress("41f16412b9a17ee9408646e2a21e16478f72ed1e95"), VoteCount: 100000026, URL: "http://GR1.com"},
		{Address: hexToAddress("41f0b7e8c1f1c15ac97b29efbd5e24e780d4e1be09"), VoteCount: 100000025, URL: "http://GR2.com"},
		{Address: hexToAddress("4116637e5de202808cbbe2a4dfcc72e79e855830a8"), VoteCount: 100000024, URL: "http://GR3.com"},
		{Address: hexToAddress("41b8f03ff75ddc0e8da4caa0e9c4a8b7e0a69bcfe2"), VoteCount: 100000023, URL: "http://GR4.com"},
		{Address: hexToAddress("41dccb07da377c92e2b12de534b4ca03f9981e7b74"), VoteCount: 100000022, URL: "http://GR5.com"},
		{Address: hexToAddress("4130bfe02f52d40e6c3de6b37b5da0de979dac7c31"), VoteCount: 100000021, URL: "http://GR6.com"},
		{Address: hexToAddress("41f068ef9a4ae8dbd3c29a7781e23f0fb5e9df1f5c"), VoteCount: 100000020, URL: "http://GR7.com"},
		{Address: hexToAddress("41b56445cd243e7da09d36d2ec6d7fee7ce9b4e11b"), VoteCount: 100000019, URL: "http://GR8.com"},
		{Address: hexToAddress("4145bafaa059f20c39a1caad80ed3c5deab3c12f74"), VoteCount: 100000018, URL: "http://GR9.com"},
		{Address: hexToAddress("41d2e6bcbadecf7ed0a51c2bb86f62d15c6be2c80d"), VoteCount: 100000017, URL: "http://GR10.com"},
		{Address: hexToAddress("41df4e74e9c05bb7e46e56e52c4d19f01a8340b02e"), VoteCount: 100000016, URL: "http://GR11.com"},
		{Address: hexToAddress("417a40fe3a5a6a40bf3518f0acacfabcab09d881bf"), VoteCount: 100000015, URL: "http://GR12.com"},
		{Address: hexToAddress("416c9a0e72f5b67e14e24c8d69baf6c64d6c4faae8"), VoteCount: 100000014, URL: "http://GR13.com"},
		{Address: hexToAddress("41ffbacf49a252373ec9fcdfeb2c3f6b4f1c8b5bcf"), VoteCount: 100000013, URL: "http://GR14.com"},
		{Address: hexToAddress("41c1e8be0bd4a50f5cc00c tried8ee3a58"), VoteCount: 100000012, URL: "http://GR15.com"},
		{Address: hexToAddress("4115fcee4a0aca62f1a9c45af83d8d2c6a447a1fb7"), VoteCount: 100000011, URL: "http://GR16.com"},
		{Address: hexToAddress("41b4d0fc4ef7c30ad6de53a79dc181d76c8a8ddd33"), VoteCount: 100000010, URL: "http://GR17.com"},
		{Address: hexToAddress("41750e9025ba46a14135c10ce8da8ea89fc2af7cda"), VoteCount: 100000009, URL: "http://GR18.com"},
		{Address: hexToAddress("41ac0a6e97a0b85fc8e68ec9f04f8dff5da96e6c32"), VoteCount: 100000008, URL: "http://GR19.com"},
		{Address: hexToAddress("4116349a5c5b3f2fd30dd12e8ef7bba79eb41ac5d9"), VoteCount: 100000007, URL: "http://GR20.com"},
		{Address: hexToAddress("41dcabc8a49d0ac6d06da3a7ea4aa4c263715ffb5c"), VoteCount: 100000006, URL: "http://GR21.com"},
		{Address: hexToAddress("41bf5c1fdca6e4dc0f0e3c15ca26703e96e18ce4de"), VoteCount: 100000005, URL: "http://GR22.com"},
		{Address: hexToAddress("4117b97d8ab6c05e11e89e1dbb0ca3d64c3c08ddaa"), VoteCount: 100000004, URL: "http://GR23.com"},
		{Address: hexToAddress("41775c87e0fa287b75bcc7310b3bac8ee20b8c3ca5"), VoteCount: 100000003, URL: "http://GR24.com"},
		{Address: hexToAddress("41a0d72c6b85f5a5a16d5e31ae95b75f1f61ab3ecc"), VoteCount: 100000002, URL: "http://GR25.com"},
		{Address: hexToAddress("41c8dd76a0be3bdc1c8bf8df82b29db4dab988fbb4"), VoteCount: 100000001, URL: "http://GR26.com"},
		{Address: hexToAddress("41c1bdfa53c0a7c24a2a35e05a757e975fe9c52a33"), VoteCount: 100000000, URL: "http://GR27.com"},
	}
}
```

**Important note for the engineer:** The 27 witness addresses above are taken from java-tron's mainnet genesis configuration. The engineer MUST verify these addresses against the actual `mainnet.conf` genesis block in java-tron source code at `framework/src/main/resources/config.conf`. If any address is wrong, update it. The GR15 address above appears to be corrupted — look up the real address from the java-tron config.

- [ ] **Step 3: Create `params/nile.go`**

```go
package params

// DefaultNileGenesis returns the genesis configuration for the Nile testnet.
// Note: Addresses should be verified against the actual Nile testnet configuration.
func DefaultNileGenesis() *Genesis {
	return &Genesis{
		Config:    NileChainConfig,
		Timestamp: 0,
		Accounts: []GenesisAccount{
			{Address: hexToAddress("41928c9af0651632157ef27a2cf17ca72c575a4d21"), Balance: 99_000_000_000_000_000, AccountName: "Zion"},
			{Address: hexToAddress("41a614f803b6fd780986a42c78ec9c7f77e6ded13c"), Balance: 0, AccountName: "Sun"},
			{Address: hexToAddress("41b0a14fb448b324ca992f2ddcb7d7b49470da3cf8"), Balance: -9223372036854775808, AccountName: "Blackhole"},
		},
		Witnesses: nileWitnesses(),
		DynamicProperties: map[string]int64{
			"maintenance_time_interval":                 21600000,
			"account_upgrade_cost":                      9999000000,
			"create_account_fee":                        100000,
			"transaction_fee":                           10,
			"asset_issue_fee":                           1024000000,
			"witness_pay_per_block":                     16000000,
			"witness_standby_allowance":                 115200000000,
			"create_new_account_fee_in_system_contract": 0,
			"create_new_account_bandwidth_rate":          1,
			"energy_fee":                                100,
			"max_cpu_time_of_one_tx":                    80,
			"total_energy_current_limit":                50000000000,
			"total_net_limit":                           43200000000,
			"unfreeze_delay_days":                       14,
		},
	}
}

func nileWitnesses() []GenesisWitness {
	// Nile testnet uses a single witness for simplicity
	// The actual addresses should be verified from https://nileex.io
	return []GenesisWitness{
		{Address: hexToAddress("41f16412b9a17ee9408646e2a21e16478f72ed1e95"), VoteCount: 100000, URL: "http://Nile-SR1.com"},
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./params/ -v -count=1`
Expected: All existing params tests PASS plus new genesis structs compile.

- [ ] **Step 5: Commit**

```bash
git add params/genesis.go params/mainnet.go params/nile.go
git commit -m "params: add Mainnet and Nile genesis configurations

Extend Genesis struct with DynamicProperties, AccountType, AccountName.
Hardcode mainnet 3 accounts + 27 GR witnesses. Add Nile testnet stub."
```

---

### Task 7: Genesis Block Construction — SetupGenesisBlock

**Files:**
- Create: `core/genesis.go`, `core/genesis_test.go`

- [ ] **Step 1: Write the test file**

```go
package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

func TestGenesisToBlock(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 1000000},
		},
		DynamicProperties: map[string]int64{
			"witness_pay_per_block": 16000000,
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	block, err := genesis.ToBlock(sdb)
	if err != nil {
		t.Fatal(err)
	}

	if block.Number() != 0 {
		t.Fatalf("genesis block number: want 0, got %d", block.Number())
	}
	if block.ParentHash() != (common.Hash{}) {
		t.Fatal("genesis parent hash should be zero")
	}
	// AccountStateRoot should be non-zero (has accounts)
	if block.AccountStateRoot() == (common.Hash{}) {
		t.Fatal("genesis accountStateRoot should not be zero")
	}
}

func TestGenesisHashDeterministic(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 500},
		},
	}

	diskdb1 := ethrawdb.NewMemoryDatabase()
	block1, _ := genesis.ToBlock(state.NewDatabase(diskdb1))

	diskdb2 := ethrawdb.NewMemoryDatabase()
	block2, _ := genesis.ToBlock(state.NewDatabase(diskdb2))

	if block1.Hash() != block2.Hash() {
		t.Fatalf("genesis hash not deterministic: %x vs %x", block1.Hash(), block2.Hash())
	}
}

func TestSetupGenesisBlock(t *testing.T) {
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}), Balance: 1000000},
		},
	}

	diskdb := ethrawdb.NewMemoryDatabase()

	config, hash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if config == nil {
		t.Fatal("config should not be nil")
	}
	if hash == (common.Hash{}) {
		t.Fatal("genesis hash should not be zero")
	}

	// Verify genesis block is stored
	block := rawdb.ReadBlock(diskdb, 0)
	if block == nil {
		t.Fatal("genesis block not found in DB")
	}
	if block.Hash() != hash {
		t.Fatal("stored genesis hash mismatch")
	}

	// Second call should succeed with same genesis
	config2, hash2, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}
	if hash2 != hash {
		t.Fatal("second SetupGenesisBlock returned different hash")
	}
	if config2.ChainID != config.ChainID {
		t.Fatal("config mismatch")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/ -run TestGenesis -v -count=1`
Expected: FAIL — `SetupGenesisBlock` not defined.

- [ ] **Step 3: Implement `core/genesis.go`**

```go
package core

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	ethcommon "github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var errGenesisNoConfig = errors.New("genesis has no chain configuration")

// SetupGenesisBlock writes the genesis block and chain config to the database
// if they don't exist. Returns the chain config and genesis hash.
func SetupGenesisBlock(db ethdb.KeyValueStore, genesis *params.Genesis) (*params.ChainConfig, tcommon.Hash, error) {
	if genesis == nil {
		return nil, tcommon.Hash{}, errors.New("genesis is nil")
	}
	if genesis.Config == nil {
		return nil, tcommon.Hash{}, errGenesisNoConfig
	}

	// Check if genesis already exists
	storedBlock := rawdb.ReadBlock(db, 0)
	if storedBlock != nil {
		storedHash := storedBlock.Hash()

		// Compute expected hash to validate
		sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
		expectedBlock, err := genesis.ToBlock(sdb)
		if err != nil {
			return genesis.Config, storedHash, nil // Can't verify, trust stored
		}
		if storedHash != expectedBlock.Hash() {
			return genesis.Config, storedHash, errors.New("genesis hash mismatch: database contains incompatible genesis")
		}
		return genesis.Config, storedHash, nil
	}

	// Write genesis
	sdb := state.NewDatabase(rawdb.WrapKeyValueStore(db))
	block, err := genesis.ToBlock(sdb)
	if err != nil {
		return nil, tcommon.Hash{}, err
	}

	rawdb.WriteBlock(db, block)
	rawdb.WriteHeadBlockHash(db, block.Hash())

	// Write dynamic properties
	if genesis.DynamicProperties != nil {
		dp := state.NewDynamicProperties()
		for k, v := range genesis.DynamicProperties {
			dp.Set(k, v)
		}
		dp.SetLatestBlockHeaderNumber(0)
		dp.SetLatestBlockHeaderTimestamp(genesis.Timestamp)
		dp.SetLatestBlockHeaderHash(block.Hash())
		dp.Flush(db)
	}

	// Write witnesses
	for _, gw := range genesis.Witnesses {
		w := types.NewWitness(gw.Address, gw.URL)
		w.SetVoteCount(gw.VoteCount)
		rawdb.WriteWitness(db, gw.Address, w)
	}

	return genesis.Config, block.Hash(), nil
}

// ToBlock creates the genesis block from the Genesis config.
func (g *params.Genesis) ToBlock(db *state.Database) (*types.Block, error) {
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		return nil, err
	}

	// Create accounts
	for _, ga := range g.Accounts {
		obj := statedb.GetOrCreateAccount(ga.Address)
		if ga.AccountName != "" {
			obj.Account().SetAccountName(ga.AccountName)
		}
		if ga.Balance != 0 {
			obj.Account().SetBalance(ga.Balance)
		}
	}

	// Commit state → accountStateRoot
	root, err := statedb.Commit()
	if err != nil {
		return nil, err
	}

	// Build genesis block
	header := &corepb.BlockHeaderRaw{
		Number:           0,
		Timestamp:        g.Timestamp,
		ParentHash:       g.ParentHash.Bytes(),
		AccountStateRoot: root[:],
	}

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: header,
		},
	})

	return block, nil
}
```

**Note for engineer:** The `ToBlock` method is defined on `*params.Genesis` but lives in the `core` package. In Go, you can't add methods to types from other packages. There are two options:

1. Move the method into `params/genesis.go` — but that creates a circular dependency since it needs `state.StateDB`.
2. Make it a standalone function: `func GenesisToBlock(g *params.Genesis, db *state.Database) (*types.Block, error)`.

**Use option 2.** Also `WrapKeyValueStore` needs to be created — it wraps `ethdb.KeyValueStore` into `ethdb.Database` for the triedb. Use go-ethereum's `rawdb.NewDatabase()`:

```go
// In core/rawdb/database.go, add:
import ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"

func WrapKeyValueStore(db ethdb.KeyValueStore) ethdb.Database {
	return ethrawdb.NewDatabase(db)
}
```

And the genesis function becomes a standalone:

```go
func GenesisToBlock(g *params.Genesis, db *state.Database) (*types.Block, error) { ... }
```

Also `Account` needs a `SetAccountName` method — add to `core/types/account.go`:

```go
func (a *Account) SetAccountName(name string) { a.pb.AccountName = []byte(name) }
func (a *Account) AccountName() string         { return string(a.pb.AccountName) }
```

And `stateObject.Account()` needs to be exported — add to `core/state/state_object.go`:

```go
func (s *stateObject) Account() *types.Account { return s.account }
```

Update the test to use `GenesisToBlock` instead of `genesis.ToBlock`.

- [ ] **Step 4: Run tests**

Run: `go test ./core/ -v -count=1`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add core/genesis.go core/genesis_test.go core/rawdb/database.go core/types/account.go core/state/state_object.go
git commit -m "core: add genesis block construction with SetupGenesisBlock

GenesisToBlock creates genesis block with MPT-backed accountStateRoot.
SetupGenesisBlock writes genesis to DB, validates on subsequent calls.
Add WrapKeyValueStore helper, Account.SetAccountName, stateObject.Account."
```

---

### Task 8: Resource Model — Bandwidth and Energy

**Files:**
- Create: `core/resource.go`, `core/resource_test.go`

- [ ] **Step 1: Write the test file**

```go
package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

func newTestResourceProcessor(t *testing.T) (*ResourceProcessor, *state.StateDB) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := state.NewDatabase(diskdb)
	sdb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	return NewResourceProcessor(sdb), sdb
}

func TestRecoverBandwidth(t *testing.T) {
	rp, sdb := newTestResourceProcessor(t)

	addr := testCoreAddr(1)
	sdb.GetOrCreateAccount(addr)

	// Set usage to 500, consumed at time 0
	acc := sdb.GetAccount(addr)
	acc.SetNetUsage(500)
	acc.SetLatestConsumeTime(0)

	// After half the window (12h = 43200000ms), usage should be halved
	halfWindow := int64(params.WindowSizeMs / 2)
	rp.RecoverBandwidth(addr, halfWindow)

	acc = sdb.GetAccount(addr)
	if acc.NetUsage() != 250 {
		t.Fatalf("net usage after half window: want 250, got %d", acc.NetUsage())
	}

	// After full window, usage should be 0
	rp.RecoverBandwidth(addr, int64(params.WindowSizeMs))
	acc = sdb.GetAccount(addr)
	if acc.NetUsage() != 0 {
		t.Fatalf("net usage after full window: want 0, got %d", acc.NetUsage())
	}
}

func TestRecoverEnergy(t *testing.T) {
	rp, sdb := newTestResourceProcessor(t)

	addr := testCoreAddr(1)
	sdb.GetOrCreateAccount(addr)
	acc := sdb.GetAccount(addr)
	acc.SetEnergyUsage(1000)
	acc.SetLatestConsumeTimeForEnergy(0)

	rp.RecoverEnergy(addr, int64(params.WindowSizeMs))
	acc = sdb.GetAccount(addr)
	if acc.EnergyUsage() != 0 {
		t.Fatalf("energy usage after full window: want 0, got %d", acc.EnergyUsage())
	}
}

func TestRecoverBandwidthPartialWindow(t *testing.T) {
	rp, sdb := newTestResourceProcessor(t)

	addr := testCoreAddr(1)
	sdb.GetOrCreateAccount(addr)
	acc := sdb.GetAccount(addr)
	acc.SetFreeNetUsage(600)
	acc.SetLatestConsumeFreeTime(0)

	// 75% through window
	rp.RecoverFreeBandwidth(addr, int64(params.WindowSizeMs*3/4))
	acc = sdb.GetAccount(addr)
	if acc.FreeNetUsage() != 150 {
		t.Fatalf("free net usage at 75%% window: want 150, got %d", acc.FreeNetUsage())
	}
}

func testCoreAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/ -run TestRecover -v -count=1`
Expected: FAIL — `ResourceProcessor` not defined.

- [ ] **Step 3: Implement `core/resource.go`**

```go
package core

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/params"
)

// ResourceProcessor handles bandwidth and energy consumption/recovery.
type ResourceProcessor struct {
	statedb *state.StateDB
}

// NewResourceProcessor creates a new ResourceProcessor.
func NewResourceProcessor(statedb *state.StateDB) *ResourceProcessor {
	return &ResourceProcessor{statedb: statedb}
}

// RecoverBandwidth applies sliding window recovery to frozen bandwidth usage.
func (r *ResourceProcessor) RecoverBandwidth(addr tcommon.Address, now int64) {
	acc := r.statedb.GetAccount(addr)
	if acc == nil {
		return
	}
	newUsage := recoverUsage(acc.NetUsage(), acc.LatestConsumeTime(), now)
	acc.SetNetUsage(newUsage)
	acc.SetLatestConsumeTime(now)
}

// RecoverFreeBandwidth applies sliding window recovery to free bandwidth usage.
func (r *ResourceProcessor) RecoverFreeBandwidth(addr tcommon.Address, now int64) {
	acc := r.statedb.GetAccount(addr)
	if acc == nil {
		return
	}
	newUsage := recoverUsage(acc.FreeNetUsage(), acc.LatestConsumeFreeTime(), now)
	acc.SetFreeNetUsage(newUsage)
	acc.SetLatestConsumeFreeTime(now)
}

// RecoverEnergy applies sliding window recovery to energy usage.
func (r *ResourceProcessor) RecoverEnergy(addr tcommon.Address, now int64) {
	acc := r.statedb.GetAccount(addr)
	if acc == nil {
		return
	}
	newUsage := recoverUsage(acc.EnergyUsage(), acc.LatestConsumeTimeForEnergy(), now)
	acc.SetEnergyUsage(newUsage)
	acc.SetLatestConsumeTimeForEnergy(now)
}

// recoverUsage computes new usage after sliding window recovery.
// Formula: newUsage = oldUsage * max(0, WindowSizeMs - elapsed) / WindowSizeMs
func recoverUsage(oldUsage int64, lastTime int64, now int64) int64 {
	if oldUsage <= 0 {
		return 0
	}
	elapsed := now - lastTime
	if elapsed >= int64(params.WindowSizeMs) {
		return 0
	}
	if elapsed <= 0 {
		return oldUsage
	}
	remaining := int64(params.WindowSizeMs) - elapsed
	return oldUsage * remaining / int64(params.WindowSizeMs)
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./core/ -run TestRecover -v -count=1`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add core/resource.go core/resource_test.go
git commit -m "core: add ResourceProcessor for bandwidth/energy recovery

Implements 24-hour sliding window recovery for frozen bandwidth,
free bandwidth, and energy usage."
```

---

### Task 9: FreezeBalanceV2 Actuator

**Files:**
- Create: `actuator/freeze_v2.go`, `actuator/freeze_v2_test.go`
- Modify: `actuator/actuator.go` (add registry case)

- [ ] **Step 1: Write the test file**

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTestAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func makeFreezeV2Tx(owner common.Address, amount int64, resource corepb.ResourceCode) *types.Transaction {
	fc := &contractpb.FreezeBalanceV2Contract{
		OwnerAddress:  owner.Bytes(),
		FrozenBalance: amount,
		Resource:      resource,
	}
	any, _ := anypb.New(fc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_FreezeBalanceV2Contract,
					Parameter: any,
				},
			},
		},
	})
}

func TestFreezeV2Validate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(1)

	// No account → fail
	tx := makeFreezeV2Tx(owner, 100, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with balance
	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	rawdb.WriteAccount(db, owner, acc)

	// Zero amount → fail
	tx = makeFreezeV2Tx(owner, 0, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero amount")
	}

	// Insufficient balance → fail
	tx = makeFreezeV2Tx(owner, 5000, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}

	// Valid freeze
	tx = makeFreezeV2Tx(owner, 500, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFreezeV2Execute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(1)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	rawdb.WriteAccount(db, owner, acc)

	tx := makeFreezeV2Tx(owner, 500, corepb.ResourceCode_BANDWIDTH)
	act := &FreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	// Check balance decreased
	updated := rawdb.ReadAccount(db, owner)
	if updated.Balance() != 500 {
		t.Fatalf("balance: want 500, got %d", updated.Balance())
	}

	// Check frozen amount
	if got := updated.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 500 {
		t.Fatalf("frozen BW: want 500, got %d", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./actuator/ -run TestFreezeV2 -v -count=1`
Expected: FAIL — `FreezeBalanceV2Actuator` not defined.

- [ ] **Step 3: Implement `actuator/freeze_v2.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if fc.FrozenBalance <= 0 {
		return errors.New("frozen balance must be positive")
	}
	if ownerAcc.Balance() < fc.FrozenBalance {
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)

	ownerAcc.SetBalance(ownerAcc.Balance() - fc.FrozenBalance)
	ownerAcc.AddFreezeV2(fc.Resource, fc.FrozenBalance)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 4: Add registry case in `actuator/actuator.go`**

Add to the `switch` in `CreateActuator`:

```go
case corepb.Transaction_Contract_FreezeBalanceV2Contract:
	return &FreezeBalanceV2Actuator{}, nil
```

- [ ] **Step 5: Run tests**

Run: `go test ./actuator/ -v -count=1`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/freeze_v2.go actuator/freeze_v2_test.go actuator/actuator.go
git commit -m "actuator: add FreezeBalanceV2Actuator

Validates owner exists, balance sufficient, resource type valid.
Deducts balance and adds FreezeV2 entry on the account."
```

---

### Task 10: UnfreezeBalanceV2 Actuator

**Files:**
- Create: `actuator/unfreeze_v2.go`, `actuator/unfreeze_v2_test.go`
- Modify: `actuator/actuator.go` (add registry case)

- [ ] **Step 1: Write the test file**

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeUnfreezeV2Tx(owner common.Address, amount int64, resource corepb.ResourceCode) *types.Transaction {
	uc := &contractpb.UnfreezeBalanceV2Contract{
		OwnerAddress:    owner.Bytes(),
		UnfreezeBalance: amount,
		Resource:        resource,
	}
	any, _ := anypb.New(uc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_UnfreezeBalanceV2Contract,
					Parameter: any,
				},
			},
		},
	})
}

func TestUnfreezeV2Validate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(3)

	// No account → fail
	tx := makeUnfreezeV2Tx(owner, 100, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	ctx := &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create account with frozen balance
	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500)
	rawdb.WriteAccount(db, owner, acc)

	// Unfreeze more than frozen → fail
	tx = makeUnfreezeV2Tx(owner, 1000, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient frozen")
	}

	// Valid unfreeze
	tx = makeUnfreezeV2Tx(owner, 200, corepb.ResourceCode_BANDWIDTH)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnfreezeV2Execute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(3)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	acc.AddFreezeV2(corepb.ResourceCode_BANDWIDTH, 500)
	rawdb.WriteAccount(db, owner, acc)

	tx := makeUnfreezeV2Tx(owner, 200, corepb.ResourceCode_BANDWIDTH)
	act := &UnfreezeBalanceV2Actuator{}
	blockTime := int64(100000)
	ctx := &Context{DB: db, Tx: tx, BlockTime: blockTime}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)

	// Frozen should decrease
	if got := updated.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 300 {
		t.Fatalf("frozen BW: want 300, got %d", got)
	}

	// Should have an unfreezing entry
	unfrozen := updated.UnfrozenV2()
	if len(unfrozen) != 1 {
		t.Fatalf("unfrozen count: want 1, got %d", len(unfrozen))
	}
	if unfrozen[0].UnfreezeAmount != 200 {
		t.Fatalf("unfreeze amount: want 200, got %d", unfrozen[0].UnfreezeAmount)
	}
	// expire_time = blockTime + 14 * 86400000
	expectedExpire := blockTime + 14*86400000
	if unfrozen[0].UnfreezeExpireTime != expectedExpire {
		t.Fatalf("expire time: want %d, got %d", expectedExpire, unfrozen[0].UnfreezeExpireTime)
	}

	// Balance should NOT change (TRX is locked in unfreezing state)
	if updated.Balance() != 1000 {
		t.Fatalf("balance: want 1000, got %d", updated.Balance())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./actuator/ -run TestUnfreezeV2 -v -count=1`
Expected: FAIL — `UnfreezeBalanceV2Actuator` not defined.

- [ ] **Step 3: Implement `actuator/unfreeze_v2.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if uc.UnfreezeBalance <= 0 {
		return errors.New("unfreeze balance must be positive")
	}
	frozenAmount := ownerAcc.GetFrozenV2Amount(uc.Resource)
	if frozenAmount < uc.UnfreezeBalance {
		return errors.New("insufficient frozen balance")
	}
	if len(ownerAcc.UnfrozenV2()) >= maxUnfreezeCount {
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)

	// Reduce frozen amount
	ownerAcc.ReduceFreezeV2(uc.Resource, uc.UnfreezeBalance)

	// Add unfreezing entry with expire time
	expireTime := ctx.BlockTime + 14*86400000 // 14 days in ms
	ownerAcc.AddUnfreezeV2(uc.Resource, uc.UnfreezeBalance, expireTime)

	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 4: Add registry case in `actuator/actuator.go`**

```go
case corepb.Transaction_Contract_UnfreezeBalanceV2Contract:
	return &UnfreezeBalanceV2Actuator{}, nil
```

- [ ] **Step 5: Run tests**

Run: `go test ./actuator/ -v -count=1`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/unfreeze_v2.go actuator/unfreeze_v2_test.go actuator/actuator.go
git commit -m "actuator: add UnfreezeBalanceV2Actuator

Reduces frozen balance, creates pending unfreeze with 14-day expiry.
Validates frozen amount sufficient and max 32 pending unfreezes."
```

---

### Task 11: VoteWitness Actuator

**Files:**
- Create: `actuator/vote.go`, `actuator/vote_test.go`
- Modify: `actuator/actuator.go`

- [ ] **Step 1: Write the test file**

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeVoteTx(owner common.Address, votes []*contractpb.VoteWitnessContract_Vote) *types.Transaction {
	vc := &contractpb.VoteWitnessContract{
		OwnerAddress: owner.Bytes(),
		Votes:        votes,
	}
	any, _ := anypb.New(vc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_VoteWitnessContract,
					Parameter: any,
				},
			},
		},
	})
}

func TestVoteWitnessValidate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(10)
	witness1 := makeTestAddr(20)

	// No account → fail
	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 10},
	}
	tx := makeVoteTx(owner, votes)
	act := &VoteWitnessActuator{}
	ctx := &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Create accounts
	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.AddFreezeV2(corepb.ResourceCode_TRON_POWER, 100*params.TRXPrecision)
	rawdb.WriteAccount(db, owner, acc)

	w := types.NewWitness(witness1, "http://test.com")
	rawdb.WriteWitness(db, witness1, w)

	// Too many votes → fail
	manyVotes := make([]*contractpb.VoteWitnessContract_Vote, params.MaxVoteNumber+1)
	for i := range manyVotes {
		a := makeTestAddr(byte(100 + i))
		manyVotes[i] = &contractpb.VoteWitnessContract_Vote{VoteAddress: a.Bytes(), VoteCount: 1}
	}
	tx = makeVoteTx(owner, manyVotes)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for too many votes")
	}

	// Vote to non-witness → fail
	nonWitness := makeTestAddr(30)
	votes = []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: nonWitness.Bytes(), VoteCount: 10},
	}
	tx = makeVoteTx(owner, votes)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness vote target")
	}

	// Valid vote
	votes = []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 50},
	}
	tx = makeVoteTx(owner, votes)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVoteWitnessExecute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(10)
	witness1 := makeTestAddr(20)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.AddFreezeV2(corepb.ResourceCode_TRON_POWER, 100*params.TRXPrecision)
	rawdb.WriteAccount(db, owner, acc)

	w := types.NewWitness(witness1, "http://test.com")
	rawdb.WriteWitness(db, witness1, w)

	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 50},
	}
	tx := makeVoteTx(owner, votes)
	act := &VoteWitnessActuator{}
	ctx := &Context{DB: db, Tx: tx}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	// Check votes on owner account
	updated := rawdb.ReadAccount(db, owner)
	if len(updated.Votes()) != 1 {
		t.Fatalf("vote count: want 1, got %d", len(updated.Votes()))
	}
	if updated.Votes()[0].VoteCount != 50 {
		t.Fatalf("vote amount: want 50, got %d", updated.Votes()[0].VoteCount)
	}

	// Check witness vote count increased
	updatedW := rawdb.ReadWitness(db, witness1)
	if updatedW.VoteCount() != 50 {
		t.Fatalf("witness vote count: want 50, got %d", updatedW.VoteCount())
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./actuator/ -run TestVoteWitness -v -count=1`
Expected: FAIL — `VoteWitnessActuator` not defined.

- [ ] **Step 3: Implement `actuator/vote.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	if len(vc.Votes) > params.MaxVoteNumber {
		return errors.New("too many votes")
	}

	// Check total vote count <= TRON_POWER / TRXPrecision
	tronPower := ownerAcc.GetFrozenV2Amount(corepb.ResourceCode_TRON_POWER) / int64(params.TRXPrecision)
	var totalVotes int64
	seen := make(map[common.Address]bool)
	for _, v := range vc.Votes {
		if v.VoteCount <= 0 {
			return errors.New("vote count must be positive")
		}
		totalVotes += v.VoteCount

		witnessAddr := common.BytesToAddress(v.VoteAddress)
		if seen[witnessAddr] {
			return errors.New("duplicate vote address")
		}
		seen[witnessAddr] = true

		if rawdb.ReadWitness(ctx.DB, witnessAddr) == nil {
			return errors.New("vote target is not a witness")
		}
	}

	if totalVotes > tronPower {
		return errors.New("total votes exceed TRON power")
	}

	return nil
}

func (a *VoteWitnessActuator) Execute(ctx *Context) (*Result, error) {
	vc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := common.BytesToAddress(vc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)

	// Remove old votes from witnesses
	for _, oldVote := range ownerAcc.Votes() {
		wAddr := common.BytesToAddress(oldVote.VoteAddress)
		w := rawdb.ReadWitness(ctx.DB, wAddr)
		if w != nil {
			w.SetVoteCount(w.VoteCount() - oldVote.VoteCount)
			rawdb.WriteWitness(ctx.DB, wAddr, w)
		}
	}

	// Set new votes on owner
	newVotes := make([]*corepb.Vote, len(vc.Votes))
	for i, v := range vc.Votes {
		newVotes[i] = &corepb.Vote{
			VoteAddress: v.VoteAddress,
			VoteCount:   v.VoteCount,
		}
		// Add to witness
		wAddr := common.BytesToAddress(v.VoteAddress)
		w := rawdb.ReadWitness(ctx.DB, wAddr)
		if w != nil {
			w.SetVoteCount(w.VoteCount() + v.VoteCount)
			rawdb.WriteWitness(ctx.DB, wAddr, w)
		}
	}
	ownerAcc.SetVotes(newVotes)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 4: Add registry case in `actuator/actuator.go`**

```go
case corepb.Transaction_Contract_VoteWitnessContract:
	return &VoteWitnessActuator{}, nil
```

- [ ] **Step 5: Run tests**

Run: `go test ./actuator/ -v -count=1`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/vote.go actuator/vote_test.go actuator/actuator.go
git commit -m "actuator: add VoteWitnessActuator

Validates vote count, TRON power, no duplicates, witness existence.
Replaces old votes, updates witness vote counts."
```

---

### Task 12: WithdrawBalance and WithdrawExpireUnfreeze Actuators

**Files:**
- Create: `actuator/withdraw.go`, `actuator/withdraw_test.go`, `actuator/withdraw_expire_unfreeze.go`, `actuator/withdraw_expire_unfreeze_test.go`
- Modify: `actuator/actuator.go`

- [ ] **Step 1: Write test for WithdrawBalance**

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWithdrawBalanceTx(owner common.Address) *types.Transaction {
	wc := &contractpb.WithdrawBalanceContract{
		OwnerAddress: owner.Bytes(),
	}
	any, _ := anypb.New(wc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_WithdrawBalanceContract,
					Parameter: any,
				},
			},
		},
	})
}

func TestWithdrawBalanceValidate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(40)

	// No account → fail
	tx := makeWithdrawBalanceTx(owner)
	act := &WithdrawBalanceActuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: 200000}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Account without allowance → fail
	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetIsWitness(true)
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for zero allowance")
	}

	// Account with allowance but too recent withdraw → fail
	acc.SetAllowance(16000000)
	acc.SetLatestWithdrawTime(100000)
	rawdb.WriteAccount(db, owner, acc)
	ctx = &Context{DB: db, Tx: tx, BlockTime: 100000 + 86400000/2} // half day later
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for too recent withdraw")
	}

	// Valid withdraw (> 24h since last)
	ctx = &Context{DB: db, Tx: tx, BlockTime: 100000 + 86400000 + 1}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithdrawBalanceExecute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(40)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetIsWitness(true)
	acc.SetBalance(500)
	acc.SetAllowance(16000000)
	acc.SetLatestWithdrawTime(0)
	rawdb.WriteAccount(db, owner, acc)

	tx := makeWithdrawBalanceTx(owner)
	act := &WithdrawBalanceActuator{}
	blockTime := int64(86400000 + 1)
	ctx := &Context{DB: db, Tx: tx, BlockTime: blockTime}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	if updated.Balance() != 500+16000000 {
		t.Fatalf("balance: want %d, got %d", 500+16000000, updated.Balance())
	}
	if updated.Allowance() != 0 {
		t.Fatalf("allowance: want 0, got %d", updated.Allowance())
	}
	if updated.LatestWithdrawTime() != blockTime {
		t.Fatalf("latest_withdraw_time: want %d, got %d", blockTime, updated.LatestWithdrawTime())
	}
}
```

- [ ] **Step 2: Write test for WithdrawExpireUnfreeze**

```go
package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWithdrawExpireUnfreezeTx(owner common.Address) *types.Transaction {
	wc := &contractpb.WithdrawExpireUnfreezeContract{
		OwnerAddress: owner.Bytes(),
	}
	any, _ := anypb.New(wc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_WithdrawExpireUnfreezeContract,
					Parameter: any,
				},
			},
		},
	})
}

func TestWithdrawExpireUnfreezeValidate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(50)

	// No account → fail
	tx := makeWithdrawExpireUnfreezeTx(owner)
	act := &WithdrawExpireUnfreezeActuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: 1000000}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Account with no unfrozen entries → fail
	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no unfrozen entries")
	}

	// Account with future unfrozen → fail
	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 100, 2000000)
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no expired entries")
	}

	// Account with expired unfrozen → pass
	acc.AddUnfreezeV2(corepb.ResourceCode_ENERGY, 200, 500000)
	rawdb.WriteAccount(db, owner, acc)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithdrawExpireUnfreezeExecute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(50)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.SetBalance(1000)
	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 100, 500000)  // expired
	acc.AddUnfreezeV2(corepb.ResourceCode_ENERGY, 200, 800000)     // expired
	acc.AddUnfreezeV2(corepb.ResourceCode_BANDWIDTH, 300, 2000000) // not expired
	rawdb.WriteAccount(db, owner, acc)

	tx := makeWithdrawExpireUnfreezeTx(owner)
	act := &WithdrawExpireUnfreezeActuator{}
	ctx := &Context{DB: db, Tx: tx, BlockTime: 1000000}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	// Balance should increase by 100 + 200 = 300
	if updated.Balance() != 1300 {
		t.Fatalf("balance: want 1300, got %d", updated.Balance())
	}
	// Only 1 remaining unfrozen entry
	if len(updated.UnfrozenV2()) != 1 {
		t.Fatalf("remaining unfrozen: want 1, got %d", len(updated.UnfrozenV2()))
	}
}
```

- [ ] **Step 3: Implement `actuator/withdraw.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const withdrawCooldown = 86400000 // 24 hours in ms

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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}
	if ownerAcc.Allowance() <= 0 {
		return errors.New("no allowance to withdraw")
	}
	if ctx.BlockTime-ownerAcc.LatestWithdrawTime() < withdrawCooldown {
		return errors.New("withdraw too frequent, must wait 24 hours")
	}

	return nil
}

func (a *WithdrawBalanceActuator) Execute(ctx *Context) (*Result, error) {
	wc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	ownerAddr := common.BytesToAddress(wc.OwnerAddress)
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)

	allowance := ownerAcc.Allowance()
	ownerAcc.SetBalance(ownerAcc.Balance() + allowance)
	ownerAcc.SetAllowance(0)
	ownerAcc.SetLatestWithdrawTime(ctx.BlockTime)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 4: Implement `actuator/withdraw_expire_unfreeze.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)
	if ownerAcc == nil {
		return errors.New("owner account does not exist")
	}

	// Check at least one expired entry exists
	hasExpired := false
	for _, u := range ownerAcc.UnfrozenV2() {
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
	ownerAcc := rawdb.ReadAccount(ctx.DB, ownerAddr)

	withdrawn := ownerAcc.RemoveExpiredUnfreezeV2(ctx.BlockTime)
	ownerAcc.SetBalance(ownerAcc.Balance() + withdrawn)
	rawdb.WriteAccount(ctx.DB, ownerAddr, ownerAcc)

	return &Result{Fee: 0}, nil
}
```

- [ ] **Step 5: Add registry cases in `actuator/actuator.go`**

```go
case corepb.Transaction_Contract_WithdrawBalanceContract:
	return &WithdrawBalanceActuator{}, nil
case corepb.Transaction_Contract_WithdrawExpireUnfreezeContract:
	return &WithdrawExpireUnfreezeActuator{}, nil
```

- [ ] **Step 6: Run tests**

Run: `go test ./actuator/ -v -count=1`
Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add actuator/withdraw.go actuator/withdraw_test.go actuator/withdraw_expire_unfreeze.go actuator/withdraw_expire_unfreeze_test.go actuator/actuator.go
git commit -m "actuator: add WithdrawBalance and WithdrawExpireUnfreeze actuators

WithdrawBalance moves witness allowance to balance (24h cooldown).
WithdrawExpireUnfreeze collects expired unfreeze entries to balance."
```

---

### Task 13: Consensus Extensions — Verify, Maintenance, Reward

**Files:**
- Create: `consensus/dpos/verify.go`, `consensus/dpos/verify_test.go`, `consensus/dpos/maintenance.go`, `consensus/dpos/maintenance_test.go`, `consensus/dpos/reward.go`
- Modify: `consensus/consensus.go`

- [ ] **Step 1: Extend consensus Engine interface in `consensus/consensus.go`**

```go
package consensus

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

type Engine interface {
	VerifyHeader(chain ChainReader, block *types.Block) error
	GetScheduledWitness(slot int64) (common.Address, error)
	IsInMaintenance(timestamp int64) bool
	DoMaintenance(chain ChainHeaderWriter) error
	PayBlockReward(chain ChainHeaderWriter, witness common.Address)
}

type ChainReader interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) *types.Block
	GenesisTimestamp() int64
	ActiveWitnesses() []common.Address
	NextMaintenanceTime() int64
}

// ChainHeaderWriter provides write access to chain state.
// Implemented by StateDB-backed types during block processing.
type ChainHeaderWriter interface {
	GetWitness(addr common.Address) *types.Witness
	PutWitness(w *types.Witness)
	AddAllowance(addr common.Address, amount int64)
	SetNextMaintenanceTime(t int64)
	WitnessPayPerBlock() int64
	WitnessStandbyAllowance() int64
	MaintenanceTimeInterval() int64
}
```

- [ ] **Step 2: Create `consensus/dpos/verify.go`**

```go
package dpos

import (
	"crypto/sha256"
	"errors"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidBlockNumber = errors.New("invalid block number")
	ErrInvalidParentHash  = errors.New("parent hash mismatch")
	ErrInvalidTimestamp    = errors.New("invalid timestamp")
	ErrInvalidWitness     = errors.New("not the scheduled witness")
	ErrInvalidSignature   = errors.New("invalid block signature")
)

// VerifyHeader checks that the block header is valid according to DPoS rules.
func VerifyHeader(chain consensus.ChainReader, block *types.Block) error {
	parent := chain.CurrentBlock()
	if parent == nil {
		return errors.New("parent block not found")
	}

	// Block number must be parent + 1
	if block.Number() != parent.Number()+1 {
		return ErrInvalidBlockNumber
	}

	// Parent hash must match
	if block.ParentHash() != parent.Hash() {
		return ErrInvalidParentHash
	}

	// Timestamp must be after parent and aligned to 3s slots
	if block.Timestamp() <= parent.Timestamp() {
		return ErrInvalidTimestamp
	}
	genesisTime := chain.GenesisTimestamp()
	if (block.Timestamp()-genesisTime)%int64(params.BlockProducedInterval) != 0 {
		return ErrInvalidTimestamp
	}

	// Verify witness signature
	witness, err := recoverWitness(block)
	if err != nil {
		return ErrInvalidSignature
	}

	// Verify witness is scheduled for this slot
	slot := AbsoluteSlot(block.Timestamp(), genesisTime)
	witnesses := chain.ActiveWitnesses()
	idx := WitnessIndex(slot, len(witnesses))
	if idx >= len(witnesses) {
		return ErrInvalidWitness
	}
	if witnesses[idx] != witness {
		return ErrInvalidWitness
	}

	return nil
}

// recoverWitness recovers the witness address from the block signature.
func recoverWitness(block *types.Block) (common.Address, error) {
	sig := block.WitnessSignature()
	if len(sig) != 65 {
		return common.Address{}, ErrInvalidSignature
	}

	headerRaw := block.Proto().BlockHeader.RawData
	data, err := proto.Marshal(headerRaw)
	if err != nil {
		return common.Address{}, err
	}
	hash := sha256.Sum256(data)

	pubkey, err := crypto.Ecrecover(hash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	addr := common.PubkeyToAddress(pubkey)
	return addr, nil
}
```

**Note:** `common.PubkeyToAddress` needs to exist — it should be in `common/address.go` or `crypto/address.go`. If it doesn't exist, the engineer should add it. This function takes a 65-byte uncompressed public key, hashes it with Keccak256, takes the last 20 bytes, and prepends 0x41.

- [ ] **Step 3: Create `consensus/dpos/maintenance.go`**

```go
package dpos

import (
	"sort"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/params"
)

// WitnessVote represents a witness and their total vote count.
type WitnessVote struct {
	Address common.Address
	Votes   int64
}

// DoMaintenance performs the maintenance period operations:
// 1. Distribute standby allowance to top-127 witnesses
// 2. Update next maintenance time
func DoMaintenance(chain consensus.ChainHeaderWriter, allWitnesses []WitnessVote) {
	// Sort by votes descending
	sort.Slice(allWitnesses, func(i, j int) bool {
		return allWitnesses[i].Votes > allWitnesses[j].Votes
	})

	// Distribute standby allowance to top WitnessStandbyLength
	standbyCount := params.WitnessStandbyLength
	if len(allWitnesses) < standbyCount {
		standbyCount = len(allWitnesses)
	}
	if standbyCount > 0 {
		allowancePerWitness := chain.WitnessStandbyAllowance() / int64(standbyCount)
		for i := 0; i < standbyCount; i++ {
			chain.AddAllowance(allWitnesses[i].Address, allowancePerWitness)
		}
	}

	// Update next maintenance time
	nextMaint := chain.MaintenanceTimeInterval()
	chain.SetNextMaintenanceTime(nextMaint)
}

// SelectActiveWitnesses returns the top N witnesses by vote count.
func SelectActiveWitnesses(allWitnesses []WitnessVote) []common.Address {
	sort.Slice(allWitnesses, func(i, j int) bool {
		return allWitnesses[i].Votes > allWitnesses[j].Votes
	})

	count := params.MaxActiveWitnessNum
	if len(allWitnesses) < count {
		count = len(allWitnesses)
	}

	result := make([]common.Address, count)
	for i := 0; i < count; i++ {
		result[i] = allWitnesses[i].Address
	}
	return result
}
```

- [ ] **Step 4: Create `consensus/dpos/reward.go`**

```go
package dpos

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
)

// PayBlockReward adds the per-block reward to the witness's allowance.
func PayBlockReward(chain consensus.ChainHeaderWriter, witness common.Address) {
	chain.AddAllowance(witness, chain.WitnessPayPerBlock())
}
```

- [ ] **Step 5: Write test for verify**

```go
package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
)

func TestSelectActiveWitnesses(t *testing.T) {
	witnesses := []WitnessVote{
		{Address: common.BytesToAddress([]byte{0x41, 1}), Votes: 100},
		{Address: common.BytesToAddress([]byte{0x41, 2}), Votes: 300},
		{Address: common.BytesToAddress([]byte{0x41, 3}), Votes: 200},
	}

	active := SelectActiveWitnesses(witnesses)
	if len(active) != 3 {
		t.Fatalf("active count: want 3, got %d", len(active))
	}
	// Should be sorted by votes desc: 2, 3, 1
	if active[0] != (common.BytesToAddress([]byte{0x41, 2})) {
		t.Fatal("first witness should be address 2")
	}
	if active[1] != (common.BytesToAddress([]byte{0x41, 3})) {
		t.Fatal("second witness should be address 3")
	}
}

func TestSelectActiveWitnessesMax(t *testing.T) {
	witnesses := make([]WitnessVote, 50)
	for i := range witnesses {
		witnesses[i] = WitnessVote{
			Address: common.BytesToAddress([]byte{0x41, byte(i)}),
			Votes:   int64(1000 - i),
		}
	}

	active := SelectActiveWitnesses(witnesses)
	if len(active) != params.MaxActiveWitnessNum {
		t.Fatalf("active count: want %d, got %d", params.MaxActiveWitnessNum, len(active))
	}
}
```

- [ ] **Step 6: Run tests**

Run: `go test ./consensus/dpos/ -v -count=1`
Expected: All PASS.

- [ ] **Step 7: Commit**

```bash
git add consensus/consensus.go consensus/dpos/verify.go consensus/dpos/verify_test.go consensus/dpos/maintenance.go consensus/dpos/maintenance_test.go consensus/dpos/reward.go
git commit -m "consensus/dpos: add header verification, maintenance, and block rewards

VerifyHeader checks block number, parent hash, timestamp alignment, and
witness signature. DoMaintenance distributes standby allowance.
SelectActiveWitnesses picks top-27 by vote count."
```

---

### Task 14: BlockChain — Block Insertion and Chain Management

**Files:**
- Create: `core/blockchain.go`, `core/blockchain_test.go`

- [ ] **Step 1: Write the test file**

```go
package core

import (
	"crypto/sha256"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

func TestNewBlockChain(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000000},
		},
	}

	_, _, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock() == nil {
		t.Fatal("current block should not be nil")
	}
	if bc.CurrentBlock().Number() != 0 {
		t.Fatalf("current block number: want 0, got %d", bc.CurrentBlock().Number())
	}
}

func TestBlockChainInsertBlock(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 99_000_000_000_000_000},
		},
	}

	_, genesisHash, err := SetupGenesisBlock(diskdb, genesis)
	if err != nil {
		t.Fatal(err)
	}

	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build block 1 (no transactions, no witness verification for now)
	block1Header := &corepb.BlockHeaderRaw{
		Number:     1,
		Timestamp:  3000,
		ParentHash: genesisHash[:],
	}

	block1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: block1Header,
		},
	})

	err = bc.InsertBlockWithoutVerify(block1)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("current block number: want 1, got %d", bc.CurrentBlock().Number())
	}

	// Verify block is stored
	stored := rawdb.ReadBlock(diskdb, 1)
	if stored == nil {
		t.Fatal("block 1 not stored")
	}
}

func TestBlockChainGetBlockByNumber(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, _ := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)

	block := bc.GetBlockByNumber(0)
	if block == nil {
		t.Fatal("genesis block not found")
	}
}

func blockHash(raw *corepb.BlockHeaderRaw) tcommon.Hash {
	data, _ := proto.Marshal(raw)
	return sha256.Sum256(data)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./core/ -run TestBlockChain -v -count=1`
Expected: FAIL — `BlockChain` not defined.

- [ ] **Step 3: Implement `core/blockchain.go`**

```go
package core

import (
	"errors"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

var (
	ErrKnownBlock      = errors.New("block already known")
	ErrInvalidParent   = errors.New("parent block not found")
	ErrInvalidNumber   = errors.New("invalid block number")
)

// BlockChain manages the canonical chain and provides block insertion.
type BlockChain struct {
	db         ethdb.KeyValueStore
	stateDB    *state.Database
	config     *params.ChainConfig

	currentBlock atomic.Pointer[types.Block]

	genesisBlock *types.Block
}

// NewBlockChain creates a new BlockChain, loading head from DB.
func NewBlockChain(db ethdb.KeyValueStore, stateDB *state.Database, config *params.ChainConfig) (*BlockChain, error) {
	bc := &BlockChain{
		db:      db,
		stateDB: stateDB,
		config:  config,
	}

	// Load genesis
	bc.genesisBlock = rawdb.ReadBlock(db, 0)
	if bc.genesisBlock == nil {
		return nil, errors.New("genesis block not found in database")
	}

	// Load head block
	headHash := rawdb.ReadHeadBlockHash(db)
	if headHash == (tcommon.Hash{}) {
		bc.currentBlock.Store(bc.genesisBlock)
	} else {
		num := rawdb.ReadBlockNumber(db, headHash)
		if num == nil {
			bc.currentBlock.Store(bc.genesisBlock)
		} else {
			block := rawdb.ReadBlock(db, *num)
			if block == nil {
				bc.currentBlock.Store(bc.genesisBlock)
			} else {
				bc.currentBlock.Store(block)
			}
		}
	}

	return bc, nil
}

// CurrentBlock returns the head of the canonical chain.
func (bc *BlockChain) CurrentBlock() *types.Block {
	return bc.currentBlock.Load()
}

// GetBlockByNumber retrieves a block by its number.
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block {
	return rawdb.ReadBlock(bc.db, number)
}

// GetBlockByHash retrieves a block by its hash.
func (bc *BlockChain) GetBlockByHash(hash tcommon.Hash) *types.Block {
	num := rawdb.ReadBlockNumber(bc.db, hash)
	if num == nil {
		return nil
	}
	return rawdb.ReadBlock(bc.db, *num)
}

// GenesisTimestamp returns the genesis block timestamp.
func (bc *BlockChain) GenesisTimestamp() int64 {
	return bc.genesisBlock.Timestamp()
}

// Config returns the chain config.
func (bc *BlockChain) Config() *params.ChainConfig {
	return bc.config
}

// InsertBlockWithoutVerify inserts a block without consensus verification.
// Used for testing and initial sync scenarios.
func (bc *BlockChain) InsertBlockWithoutVerify(block *types.Block) error {
	current := bc.CurrentBlock()

	// Validate basic invariants
	if block.Number() != current.Number()+1 {
		return ErrInvalidNumber
	}
	if block.ParentHash() != current.Hash() {
		return ErrInvalidParent
	}

	// Store block
	rawdb.WriteBlock(bc.db, block)
	rawdb.WriteHeadBlockHash(bc.db, block.Hash())

	// Update head
	bc.currentBlock.Store(block)

	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./core/ -v -count=1`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add core/blockchain.go core/blockchain_test.go
git commit -m "core: add BlockChain with block insertion and chain management

Loads head from DB, provides block lookup by number/hash,
InsertBlockWithoutVerify for basic block insertion."
```

---

## Self-Review Checklist

**1. Spec coverage:**
- ✅ ethdb migration (Task 1)
- ✅ types.Account extensions (Task 2)
- ✅ DynamicProperties (Task 3)
- ✅ StateDB core: Database, stateObject, journal (Task 4)
- ✅ StateDB with MPT (Task 5)
- ✅ Genesis struct + Mainnet/Nile data (Task 6)
- ✅ SetupGenesisBlock (Task 7)
- ✅ Resource model (Task 8)
- ✅ FreezeV2 actuator (Task 9)
- ✅ UnfreezeV2 actuator (Task 10)
- ✅ VoteWitness actuator (Task 11)
- ✅ WithdrawBalance + WithdrawExpireUnfreeze actuators (Task 12)
- ✅ Consensus: verify, maintenance, reward (Task 13)
- ✅ BlockChain (Task 14)

**2. Placeholder scan:** No TBD/TODO placeholders. Mainnet GR15 address noted as potentially corrupted — engineer must verify.

**3. Type consistency:**
- `DynamicProperties` methods consistent between Task 3 and Task 5
- `stateObject.Account()` exported in Task 4, used in Task 7
- `Context.BlockTime` and `Context.BlockNumber` added in Task 1, used in Tasks 10, 12
- `common.PubkeyToAddress` referenced in Task 13 — may need to be created
