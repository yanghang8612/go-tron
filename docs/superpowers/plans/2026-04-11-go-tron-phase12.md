# Phase 12: TRC10 Asset System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the complete TRC10 native token lifecycle: 6 actuators, rawdb asset metadata store with 4 new prefixes, per-account TRC10 balances stored as storage trie slots (contributing to stateRoot), and 5 HTTP query endpoints.

**Architecture:** Asset metadata (global token definitions) is stored in raw KV under `ast-`/`astn-`/`asto-`/`asti-` prefixes. Per-account TRC10 balances live in the account's storage trie as keccak256-keyed slots, so they contribute to the block's `stateRoot`. This matches the user's directive to stay compatible with go-ethereum's account model and enables stateRoot calculation. All 6 contract types (2, 3, 6, 9, 14, 15) are implemented and registered.

**Tech Stack:** Go standard library only. Proto marshaling via `google.golang.org/protobuf/proto`. Address handling via go-tron's 21-byte `common.Address`. Keccak256 via `tcommon.Keccak256`. No new external dependencies.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `core/rawdb/schema.go` | Modify | Add 4 new prefixes + key functions |
| `core/rawdb/accessors_asset.go` | Create | Read/Write/List for asset metadata, indexes, and timestamps |
| `core/rawdb/accessors_asset_test.go` | Create | Unit tests for all rawdb asset functions |
| `core/state/slots.go` | Create | `trc10BalanceSlot`, `trc10FrozenClaimedSlot`, `int64ToSlot`, `slotToInt64` |
| `core/state/statedb.go` | Modify | Add 6 TRC10 state methods |
| `core/state/dynamic_properties.go` | Modify | Add `next_token_id` default + `NextTokenID`, `SetNextTokenID`, `AssetIssueFee` |
| `core/state/statedb_trc10_test.go` | Create | Unit tests for TRC10 state methods |
| `actuator/asset_issue.go` | Create | `AssetIssueActuator` (type 6) |
| `actuator/asset_issue_test.go` | Create | Tests for AssetIssueActuator |
| `actuator/transfer_asset.go` | Create | `TransferAssetActuator` (type 2) |
| `actuator/transfer_asset_test.go` | Create | Tests for TransferAssetActuator |
| `actuator/participate_asset_issue.go` | Create | `ParticipateAssetIssueActuator` (type 9) |
| `actuator/participate_asset_issue_test.go` | Create | Tests for ParticipateAssetIssueActuator |
| `actuator/update_asset.go` | Create | `UpdateAssetActuator` (type 15) |
| `actuator/update_asset_test.go` | Create | Tests for UpdateAssetActuator |
| `actuator/vote_asset.go` | Create | `VoteAssetActuator` (type 3, validated no-op) |
| `actuator/vote_asset_test.go` | Create | Tests for VoteAssetActuator |
| `actuator/unfreeze_asset.go` | Create | `UnfreezeAssetActuator` (type 14) |
| `actuator/unfreeze_asset_test.go` | Create | Tests for UnfreezeAssetActuator |
| `actuator/actuator.go` | Modify | Register all 6 new types |
| `internal/tronapi/backend.go` | Modify | Add 5 asset query Backend methods |
| `internal/tronapi/api.go` | Modify | Add 5 route handlers + register routes |
| `core/tron_backend.go` | Modify | Implement 5 new Backend methods |
| `scripts/system_test.sh` | Modify | Add Section 11: asset query endpoint checks |

---

## Task 1: Raw KV Schema + Asset Accessors

**Files:**
- Modify: `core/rawdb/schema.go`
- Create: `core/rawdb/accessors_asset.go`
- Create: `core/rawdb/accessors_asset_test.go`

- [ ] **Step 1: Write the failing tests**

Create `core/rawdb/accessors_asset_test.go`:

```go
package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestWriteReadAssetIssue(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	c := &contractpb.AssetIssueContract{
		Name:        []byte("MYTOKEN"),
		TotalSupply: 1_000_000,
		Id:          "1000001",
	}
	WriteAssetIssue(db, 1_000_001, c)
	got := ReadAssetIssue(db, 1_000_001)
	if got == nil {
		t.Fatal("expected asset to be found")
	}
	if string(got.Name) != "MYTOKEN" {
		t.Fatalf("name: want MYTOKEN, got %s", got.Name)
	}
}

func TestReadAssetIssue_NotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if got := ReadAssetIssue(db, 9_999_999); got != nil {
		t.Fatal("expected nil for unknown token")
	}
}

func TestAssetNameIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	WriteAssetNameIndex(db, []byte("MYTOKEN"), 1_000_001)
	id, ok := ReadAssetNameIndex(db, []byte("MYTOKEN"))
	if !ok {
		t.Fatal("expected name index to be found")
	}
	if id != 1_000_001 {
		t.Fatalf("tokenID: want 1000001, got %d", id)
	}
	_, ok2 := ReadAssetNameIndex(db, []byte("UNKNOWN"))
	if ok2 {
		t.Fatal("expected not-found for unknown name")
	}
}

func TestAssetOwnerIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	owner := common.Address{0x41, 0x01}
	WriteAssetOwnerIndex(db, owner[:], 1_000_001)
	id, ok := ReadAssetOwnerIndex(db, owner[:])
	if !ok {
		t.Fatal("expected owner index to be found")
	}
	if id != 1_000_001 {
		t.Fatalf("tokenID: want 1000001, got %d", id)
	}
	other := common.Address{0x41, 0x02}
	_, ok2 := ReadAssetOwnerIndex(db, other[:])
	if ok2 {
		t.Fatal("expected not-found for other address")
	}
}

func TestAssetIssueTime(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	WriteAssetIssueTime(db, 1_000_001, 1_713_000_000_000)
	got := ReadAssetIssueTime(db, 1_000_001)
	if got != 1_713_000_000_000 {
		t.Fatalf("issueTime: want 1713000000000, got %d", got)
	}
	if ReadAssetIssueTime(db, 9_999_999) != 0 {
		t.Fatal("expected 0 for unknown token")
	}
}

func TestListAllAssets(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{Name: []byte("AAA")})
	WriteAssetIssue(db, 1_000_002, &contractpb.AssetIssueContract{Name: []byte("BBB")})
	all := ListAllAssets(db)
	if len(all) != 2 {
		t.Fatalf("expected 2 assets, got %d", len(all))
	}
}

func TestListAssetsPaginated(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	for i := int64(0); i < 5; i++ {
		WriteAssetIssue(db, 1_000_001+i, &contractpb.AssetIssueContract{})
	}
	page := ListAssetsPaginated(db, 2, 2)
	if len(page) != 2 {
		t.Fatalf("expected 2 paginated assets, got %d", len(page))
	}
	all := ListAssetsPaginated(db, 0, 100)
	if len(all) != 5 {
		t.Fatalf("expected 5 for limit>total, got %d", len(all))
	}
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go test ./core/rawdb/ -run TestWriteReadAsset -v 2>&1 | head -20
```

Expected: `undefined: WriteAssetIssue` (or similar compile error).

- [ ] **Step 3: Add schema entries to `core/rawdb/schema.go`**

Add after the last prefix variable block (after `brokeragePrefix`):

```go
	assetPrefix          = []byte("ast-")   // tokenID big-endian 8B → AssetIssueContract proto bytes
	assetNamePrefix      = []byte("astn-")  // token name bytes → tokenID big-endian 8B
	assetOwnerPrefix     = []byte("asto-")  // owner address 21B → tokenID big-endian 8B
	assetIssueTimePrefix = []byte("asti-")  // tokenID big-endian 8B → issue timestamp ms big-endian 8B
```

Add after the last key function (after `brokerageKey`):

```go
func assetKey(tokenID int64) []byte {
	k := make([]byte, len(assetPrefix)+8)
	copy(k, assetPrefix)
	binary.BigEndian.PutUint64(k[len(assetPrefix):], uint64(tokenID))
	return k
}

func assetNameKey(name []byte) []byte {
	return append(append([]byte{}, assetNamePrefix...), name...)
}

func assetOwnerKey(ownerAddr []byte) []byte {
	return append(append([]byte{}, assetOwnerPrefix...), ownerAddr...)
}

func assetIssueTimeKey(tokenID int64) []byte {
	k := make([]byte, len(assetIssueTimePrefix)+8)
	copy(k, assetIssueTimePrefix)
	binary.BigEndian.PutUint64(k[len(assetIssueTimePrefix):], uint64(tokenID))
	return k
}
```

- [ ] **Step 4: Create `core/rawdb/accessors_asset.go`**

```go
package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// WriteAssetIssue stores an AssetIssueContract keyed by tokenID.
func WriteAssetIssue(db ethdb.KeyValueWriter, tokenID int64, c *contractpb.AssetIssueContract) {
	data, err := proto.Marshal(c)
	if err != nil {
		return
	}
	db.Put(assetKey(tokenID), data)
}

// ReadAssetIssue returns the AssetIssueContract for tokenID, or nil if not found.
func ReadAssetIssue(db ethdb.KeyValueReader, tokenID int64) *contractpb.AssetIssueContract {
	data, err := db.Get(assetKey(tokenID))
	if err != nil || len(data) == 0 {
		return nil
	}
	c := &contractpb.AssetIssueContract{}
	if err := proto.Unmarshal(data, c); err != nil {
		return nil
	}
	return c
}

// WriteAssetNameIndex stores a name → tokenID mapping.
func WriteAssetNameIndex(db ethdb.KeyValueWriter, name []byte, tokenID int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	db.Put(assetNameKey(name), buf)
}

// ReadAssetNameIndex returns the tokenID for the given name, and whether it exists.
func ReadAssetNameIndex(db ethdb.KeyValueReader, name []byte) (int64, bool) {
	data, err := db.Get(assetNameKey(name))
	if err != nil || len(data) < 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(data[:8])), true
}

// WriteAssetOwnerIndex stores an ownerAddr → tokenID mapping (21-byte TRON address).
func WriteAssetOwnerIndex(db ethdb.KeyValueWriter, ownerAddr []byte, tokenID int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(tokenID))
	db.Put(assetOwnerKey(ownerAddr), buf)
}

// ReadAssetOwnerIndex returns the tokenID issued by ownerAddr, and whether it exists.
func ReadAssetOwnerIndex(db ethdb.KeyValueReader, ownerAddr []byte) (int64, bool) {
	data, err := db.Get(assetOwnerKey(ownerAddr))
	if err != nil || len(data) < 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(data[:8])), true
}

// WriteAssetIssueTime stores the block timestamp (ms) when the token was issued.
func WriteAssetIssueTime(db ethdb.KeyValueWriter, tokenID int64, issueTimeMs int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(issueTimeMs))
	db.Put(assetIssueTimeKey(tokenID), buf)
}

// ReadAssetIssueTime returns the issue timestamp for tokenID, or 0 if not found.
func ReadAssetIssueTime(db ethdb.KeyValueReader, tokenID int64) int64 {
	data, err := db.Get(assetIssueTimeKey(tokenID))
	if err != nil || len(data) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data[:8]))
}

// ListAllAssets iterates the ast- prefix and returns all assets sorted by tokenID ascending.
func ListAllAssets(db ethdb.Iteratee) []*contractpb.AssetIssueContract {
	it := db.NewIterator(assetPrefix, nil)
	defer it.Release()
	var result []*contractpb.AssetIssueContract
	for it.Next() {
		c := &contractpb.AssetIssueContract{}
		if err := proto.Unmarshal(it.Value(), c); err == nil {
			result = append(result, c)
		}
	}
	return result
}

// ListAssetsPaginated returns up to limit assets starting at position offset (0-indexed).
func ListAssetsPaginated(db ethdb.Iteratee, offset, limit int) []*contractpb.AssetIssueContract {
	it := db.NewIterator(assetPrefix, nil)
	defer it.Release()
	var result []*contractpb.AssetIssueContract
	skipped := 0
	for it.Next() {
		if skipped < offset {
			skipped++
			continue
		}
		c := &contractpb.AssetIssueContract{}
		if err := proto.Unmarshal(it.Value(), c); err == nil {
			result = append(result, c)
		}
		if len(result) >= limit {
			break
		}
	}
	return result
}
```

- [ ] **Step 5: Run tests — expect all pass**

```bash
go test ./core/rawdb/ -run "TestWriteReadAsset|TestAssetNameIndex|TestAssetOwnerIndex|TestAssetIssueTime|TestListAllAssets|TestListAssetsPaginated" -v
```

Expected: All 7 tests PASS.

- [ ] **Step 6: Verify full rawdb package compiles**

```bash
go build ./core/rawdb/
```

Expected: no output (success).

- [ ] **Step 7: Commit**

```bash
git add core/rawdb/schema.go core/rawdb/accessors_asset.go core/rawdb/accessors_asset_test.go
git commit -m "feat(rawdb): add TRC10 asset metadata accessors and schema"
```

---

## Task 2: Storage Trie Slots + StateDB TRC10 Methods + DynamicProperties

**Files:**
- Create: `core/state/slots.go`
- Modify: `core/state/statedb.go`
- Modify: `core/state/dynamic_properties.go`
- Create: `core/state/statedb_trc10_test.go`

- [ ] **Step 1: Write the failing tests**

Create `core/state/statedb_trc10_test.go`:

```go
package state

import (
	"testing"
)

func TestGetTRC10Balance_Empty(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	// Non-existent account: GetState returns zero hash → slotToInt64 returns 0
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 0 {
		t.Fatalf("expected 0 for new account, got %d", got)
	}
}

func TestSetGetTRC10Balance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.SetTRC10Balance(addr, 1_000_001, 500_000)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 500_000 {
		t.Fatalf("expected 500000, got %d", got)
	}
}

func TestAddSubTRC10Balance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.AddTRC10Balance(addr, 1_000_001, 1_000_000)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 1_000_000 {
		t.Fatalf("add: expected 1000000, got %d", got)
	}
	if err := sdb.SubTRC10Balance(addr, 1_000_001, 300_000); err != nil {
		t.Fatalf("sub failed: %v", err)
	}
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 700_000 {
		t.Fatalf("after sub: expected 700000, got %d", got)
	}
}

func TestSubTRC10Balance_Insufficient(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.SetTRC10Balance(addr, 1_000_001, 100)
	err := sdb.SubTRC10Balance(addr, 1_000_001, 200)
	if err == nil {
		t.Fatal("expected ErrInsufficientBalance")
	}
}

func TestFrozenClaimed(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	if sdb.IsFrozenClaimed(addr, 1_000_001, 0) {
		t.Fatal("should not be claimed initially")
	}
	sdb.SetFrozenClaimed(addr, 1_000_001, 0)
	if !sdb.IsFrozenClaimed(addr, 1_000_001, 0) {
		t.Fatal("should be claimed after SetFrozenClaimed")
	}
	if sdb.IsFrozenClaimed(addr, 1_000_001, 1) {
		t.Fatal("index 1 should not be claimed independently")
	}
}

func TestTRC10BalanceIndependentSlots(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.SetTRC10Balance(addr, 1_000_001, 100)
	sdb.SetTRC10Balance(addr, 1_000_002, 200)
	if got := sdb.GetTRC10Balance(addr, 1_000_001); got != 100 {
		t.Fatalf("token 1000001: expected 100, got %d", got)
	}
	if got := sdb.GetTRC10Balance(addr, 1_000_002); got != 200 {
		t.Fatalf("token 1000002: expected 200, got %d", got)
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./core/state/ -run TestGetTRC10Balance -v 2>&1 | head -10
```

Expected: `undefined: GetTRC10Balance`.

- [ ] **Step 3: Create `core/state/slots.go`**

```go
package state

import (
	"encoding/binary"

	tcommon "github.com/tronprotocol/go-tron/common"
)

// trc10BalanceSlot returns the storage slot key for an account's TRC10 token balance.
// Key = keccak256("trc10_balance" || big_endian_uint64(tokenID))
func trc10BalanceSlot(tokenID int64) tcommon.Hash {
	buf := make([]byte, len("trc10_balance")+8)
	copy(buf, "trc10_balance")
	binary.BigEndian.PutUint64(buf[len("trc10_balance"):], uint64(tokenID))
	return tcommon.Keccak256(buf)
}

// trc10FrozenClaimedSlot returns the storage slot key indicating whether frozen_supply[index]
// has been claimed by the asset issuer.
// Key = keccak256("trc10_frozen_claimed" || big_endian_uint64(tokenID) || big_endian_uint32(index))
func trc10FrozenClaimedSlot(tokenID int64, index uint32) tcommon.Hash {
	buf := make([]byte, len("trc10_frozen_claimed")+8+4)
	copy(buf, "trc10_frozen_claimed")
	binary.BigEndian.PutUint64(buf[len("trc10_frozen_claimed"):], uint64(tokenID))
	binary.BigEndian.PutUint32(buf[len("trc10_frozen_claimed")+8:], index)
	return tcommon.Keccak256(buf)
}

// int64ToSlot encodes v as a 32-byte hash: value in the last 8 bytes (big-endian).
func int64ToSlot(v int64) tcommon.Hash {
	var h tcommon.Hash
	binary.BigEndian.PutUint64(h[24:], uint64(v))
	return h
}

// slotToInt64 decodes an int64 from a 32-byte hash (value in the last 8 bytes, big-endian).
func slotToInt64(h tcommon.Hash) int64 {
	return int64(binary.BigEndian.Uint64(h[24:]))
}
```

- [ ] **Step 4: Add TRC10 state methods to `core/state/statedb.go`**

Add these 6 methods after the `SubBalance` function (around line 116):

```go
// GetTRC10Balance returns the TRC10 token balance of addr for the given tokenID.
// Returns 0 if the account or token slot does not exist.
func (s *StateDB) GetTRC10Balance(addr tcommon.Address, tokenID int64) int64 {
	return slotToInt64(s.GetState(addr, trc10BalanceSlot(tokenID)))
}

// SetTRC10Balance sets the TRC10 token balance. Used for initial token minting.
// SetState calls GetOrCreateAccount internally, so the account is created if needed.
func (s *StateDB) SetTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	s.SetState(addr, trc10BalanceSlot(tokenID), int64ToSlot(amount))
}

// AddTRC10Balance credits amount TRC10 tokens to addr.
func (s *StateDB) AddTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
	s.SetTRC10Balance(addr, tokenID, s.GetTRC10Balance(addr, tokenID)+amount)
}

// SubTRC10Balance debits amount TRC10 tokens from addr.
// Returns ErrInsufficientBalance if addr has fewer than amount tokens.
func (s *StateDB) SubTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) error {
	current := s.GetTRC10Balance(addr, tokenID)
	if current < amount {
		return ErrInsufficientBalance
	}
	s.SetTRC10Balance(addr, tokenID, current-amount)
	return nil
}

// IsFrozenClaimed returns whether frozen_supply entry at index has been claimed.
func (s *StateDB) IsFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) bool {
	v := s.GetState(addr, trc10FrozenClaimedSlot(tokenID, index))
	return v[31] != 0
}

// SetFrozenClaimed marks frozen_supply entry at index as claimed.
func (s *StateDB) SetFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) {
	var v tcommon.Hash
	v[31] = 0x01
	s.SetState(addr, trc10FrozenClaimedSlot(tokenID, index), v)
}
```

- [ ] **Step 5: Add `next_token_id` and typed accessors to `core/state/dynamic_properties.go`**

In `defaultProps`, add after `"next_proposal_id": 0,`:
```go
	"next_token_id": 1_000_001,
```

After the last typed getter/setter (after `SetNextProposalID`), add:
```go
// NextTokenID returns the next token ID to assign (starts at 1_000_001).
func (dp *DynamicProperties) NextTokenID() int64 { return dp.props["next_token_id"] }

// SetNextTokenID updates the next token ID counter.
func (dp *DynamicProperties) SetNextTokenID(id int64) { dp.Set("next_token_id", id) }

// AssetIssueFee returns the fee (in SUN) required to issue a TRC10 token.
func (dp *DynamicProperties) AssetIssueFee() int64 { return dp.props["asset_issue_fee"] }
```

- [ ] **Step 6: Run the new tests — expect all pass**

```bash
go test ./core/state/ -run "TestGetTRC10|TestSetGetTRC10|TestAddSubTRC10|TestSubTRC10Balance_Insufficient|TestFrozenClaimed|TestTRC10BalanceIndependentSlots" -v
```

Expected: All 6 tests PASS.

- [ ] **Step 7: Run full state package tests to catch regressions**

```bash
go test ./core/state/ -v 2>&1 | tail -20
```

Expected: All existing tests still PASS.

- [ ] **Step 8: Commit**

```bash
git add core/state/slots.go core/state/statedb.go core/state/dynamic_properties.go core/state/statedb_trc10_test.go
git commit -m "feat(state): add TRC10 balance and frozen-claim storage trie methods"
```

---

## Task 3: AssetIssueActuator

**Files:**
- Create: `actuator/asset_issue.go`
- Create: `actuator/asset_issue_test.go`

**Context:** `newTestContext` (defined in `actuator/vm_actuator_test.go`) creates a `*Context` with `ctx.DB == nil`. All TRC10 actuator tests MUST set `ctx.DB = ethrawdb.NewMemoryDatabase()` after calling `newTestContext`. The `DynProps` created by `newTestContext` includes `next_token_id = 1_000_001` (added in Task 2).

- [ ] **Step 1: Write the failing tests**

Create `actuator/asset_issue_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func makeAssetIssueContract(ownerByte byte, name string, totalSupply int64) *contractpb.AssetIssueContract {
	owner := makeTestAddr(ownerByte)
	return &contractpb.AssetIssueContract{
		OwnerAddress: owner.Bytes(),
		Name:         []byte(name),
		Abbr:         []byte("TKN"),
		TotalSupply:  totalSupply,
		TrxNum:       1,
		Num:          1,
		StartTime:    1000,
		EndTime:      2000,
		Precision:    0,
	}
}

func TestAssetIssueValidate_Success(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestAssetIssueValidate_DuplicateName(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee()*2)
	rawdb.WriteAssetNameIndex(db, []byte("MYTOKEN"), 999_999)

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for duplicate name")
	}
}

func TestAssetIssueValidate_AlreadyIssued(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "NEWTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee()*2)
	rawdb.WriteAssetOwnerIndex(db, owner[:], 999_999)

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already issued token")
	}
}

func TestAssetIssueValidate_InsufficientFee(t *testing.T) {
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	owner := makeTestAddr(1)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	// balance = 0, fee = 1_024_000_000

	act := &AssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient fee")
	}
}

func TestAssetIssueExecute(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "MYTOKEN", 1_000_000)
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	tokenID := int64(1_000_001)
	asset := rawdb.ReadAssetIssue(db, tokenID)
	if asset == nil {
		t.Fatal("asset should be stored in rawdb")
	}
	if string(asset.Name) != "MYTOKEN" {
		t.Fatalf("asset name: want MYTOKEN, got %s", asset.Name)
	}
	if ctx.State.GetTRC10Balance(owner, tokenID) != 1_000_000 {
		t.Fatalf("TRC10 balance: want 1000000, got %d", ctx.State.GetTRC10Balance(owner, tokenID))
	}
	if ctx.State.GetBalance(owner) != 0 {
		t.Fatalf("TRX balance after fee: expected 0, got %d", ctx.State.GetBalance(owner))
	}
	if ctx.DynProps.NextTokenID() != 1_000_002 {
		t.Fatalf("next_token_id: want 1000002, got %d", ctx.DynProps.NextTokenID())
	}
}

func TestAssetIssueExecute_WithFrozenSupply(t *testing.T) {
	owner := makeTestAddr(1)
	c := makeAssetIssueContract(1, "FROZENTOKEN", 1_000_000)
	c.FrozenSupply = []*contractpb.AssetIssueContract_FrozenSupply{
		{FrozenAmount: 200_000, FrozenDays: 30},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AssetIssueContract, c, 0)
	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddBalance(owner, ctx.DynProps.AssetIssueFee())

	act := &AssetIssueActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	tokenID := int64(1_000_001)
	// Only the 800,000 free tokens are minted; 200,000 are frozen
	if bal := ctx.State.GetTRC10Balance(owner, tokenID); bal != 800_000 {
		t.Fatalf("TRC10 balance: want 800000 (free portion), got %d", bal)
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./actuator/ -run TestAssetIssue -v 2>&1 | head -10
```

Expected: `undefined: AssetIssueActuator`.

- [ ] **Step 3: Create `actuator/asset_issue.go`**

```go
package actuator

import (
	"errors"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// AssetIssueActuator handles TRC10 token issuance (contract type 6).
type AssetIssueActuator struct{}

func (a *AssetIssueActuator) getContract(ctx *Context) (*contractpb.AssetIssueContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AssetIssueContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AssetIssueContract")
	}
	return c, nil
}

func (a *AssetIssueActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(owner) {
		return errors.New("owner account does not exist")
	}
	if len(c.Name) == 0 {
		return errors.New("token name is required")
	}
	if len(c.Abbr) == 0 {
		return errors.New("token abbreviation is required")
	}
	if c.TotalSupply <= 0 {
		return errors.New("total supply must be positive")
	}
	if c.TrxNum <= 0 {
		return errors.New("trx_num must be positive")
	}
	if c.Num <= 0 {
		return errors.New("num must be positive")
	}
	if c.StartTime >= c.EndTime {
		return errors.New("start_time must be before end_time")
	}
	if c.Precision < 0 || c.Precision > 6 {
		return errors.New("precision must be 0-6")
	}
	var frozenTotal int64
	for _, f := range c.FrozenSupply {
		frozenTotal += f.FrozenAmount
	}
	if frozenTotal > c.TotalSupply {
		return errors.New("frozen supply exceeds total supply")
	}
	if ctx.State.GetBalance(owner) < ctx.DynProps.AssetIssueFee() {
		return errors.New("insufficient balance for asset issue fee")
	}
	if _, ok := rawdb.ReadAssetNameIndex(ctx.DB, c.Name); ok {
		return errors.New("token name already exists")
	}
	if _, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:]); ok {
		return errors.New("address has already issued a token")
	}
	return nil
}

func (a *AssetIssueActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner := common.BytesToAddress(c.OwnerAddress)

	// Assign and increment token ID
	tokenID := ctx.DynProps.NextTokenID()
	ctx.DynProps.SetNextTokenID(tokenID + 1)
	c.Id = strconv.FormatInt(tokenID, 10)

	// Persist metadata and indexes
	rawdb.WriteAssetIssue(ctx.DB, tokenID, c)
	rawdb.WriteAssetNameIndex(ctx.DB, c.Name, tokenID)
	rawdb.WriteAssetOwnerIndex(ctx.DB, owner[:], tokenID)
	rawdb.WriteAssetIssueTime(ctx.DB, tokenID, ctx.BlockTime)

	// Mint free supply to issuer (frozen supply is held until UnfreezeAsset)
	var frozenTotal int64
	for _, f := range c.FrozenSupply {
		frozenTotal += f.FrozenAmount
	}
	freeAmount := c.TotalSupply - frozenTotal
	if freeAmount > 0 {
		ctx.State.SetTRC10Balance(owner, tokenID, freeAmount)
	}

	// Burn issuance fee
	fee := ctx.DynProps.AssetIssueFee()
	if err := ctx.State.SubBalance(owner, fee); err != nil {
		return nil, err
	}

	return &Result{Fee: fee, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Run tests — expect all pass**

```bash
go test ./actuator/ -run TestAssetIssue -v
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/asset_issue.go actuator/asset_issue_test.go
git commit -m "feat(actuator): add AssetIssueActuator (TRC10 type 6)"
```

---

## Task 4: TransferAssetActuator

**Files:**
- Create: `actuator/transfer_asset.go`
- Create: `actuator/transfer_asset_test.go`

**Context:** `asset_name` in the proto contains the token ID as a decimal string (e.g., `"1000001"`). Always parse it with `strconv.ParseInt`. `ctx.DB` must be set in tests.

- [ ] **Step 1: Write the failing tests**

Create `actuator/transfer_asset_test.go`:

```go
package actuator

import (
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTransferAssetTx(ownerByte, toByte byte, tokenID int64, amount int64) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	to := makeTestAddr(toByte)
	c := &contractpb.TransferAssetContract{
		OwnerAddress: owner.Bytes(),
		ToAddress:    to.Bytes(),
		AssetName:    []byte(strconv.FormatInt(tokenID, 10)),
		Amount:       amount,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_TransferAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// seedAssetAndBalance issues a token and gives owner the specified TRC10 balance.
func seedAssetAndBalance(ctx *Context, ownerByte byte, tokenID int64, balance int64) {
	owner := makeTestAddr(ownerByte)
	rawdb.WriteAssetIssue(ctx.DB, tokenID, &contractpb.AssetIssueContract{
		Name: []byte("TOKEN"),
		Id:   strconv.FormatInt(tokenID, 10),
	})
	ctx.State.SetTRC10Balance(owner, tokenID, balance)
}

func TestTransferAssetValidate_Success(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 500_000)

	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")})

	tx := makeTransferAssetTx(1, 2, tokenID, 100_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestTransferAssetValidate_UnknownToken(t *testing.T) {
	statedb := setupStateDB(t)
	statedb.CreateAccount(makeTestAddr(1), corepb.AccountType_Normal)

	tx := makeTransferAssetTx(1, 2, 9_999_999, 100)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase() // empty DB — no token

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for unknown token")
	}
}

func TestTransferAssetValidate_InsufficientBalance(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 50) // only 50 tokens

	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")})

	tx := makeTransferAssetTx(1, 2, tokenID, 100) // wants 100
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &TransferAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestTransferAssetExecute_Success(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")})

	tx := makeTransferAssetTx(1, 2, tokenID, 300_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &TransferAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}
	if got := statedb.GetTRC10Balance(owner, tokenID); got != 700_000 {
		t.Fatalf("sender: want 700000, got %d", got)
	}
	if got := statedb.GetTRC10Balance(to, tokenID); got != 300_000 {
		t.Fatalf("recipient: want 300000, got %d", got)
	}
}

func TestTransferAssetExecute_CreatesRecipient(t *testing.T) {
	const tokenID = int64(1_000_001)
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	to := makeTestAddr(9)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000) // TRX for create_account_fee
	statedb.SetTRC10Balance(owner, tokenID, 1_000_000)

	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, tokenID, &contractpb.AssetIssueContract{Name: []byte("T")})

	tx := makeTransferAssetTx(1, 9, tokenID, 500_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &TransferAssetActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	if !statedb.AccountExists(to) {
		t.Fatal("recipient account should have been created")
	}
	if got := statedb.GetTRC10Balance(to, tokenID); got != 500_000 {
		t.Fatalf("recipient TRC10 balance: want 500000, got %d", got)
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./actuator/ -run TestTransferAsset -v 2>&1 | head -10
```

Expected: `undefined: TransferAssetActuator`.

- [ ] **Step 3: Create `actuator/transfer_asset.go`**

```go
package actuator

import (
	"errors"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TransferAssetActuator handles TRC10 token transfers (contract type 2).
type TransferAssetActuator struct{}

func (a *TransferAssetActuator) getContract(ctx *Context) (*contractpb.TransferAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.TransferAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal TransferAssetContract")
	}
	return c, nil
}

func (a *TransferAssetActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	tokenID, err := strconv.ParseInt(string(c.AssetName), 10, 64)
	if err != nil {
		return errors.New("invalid token ID in asset_name")
	}
	if rawdb.ReadAssetIssue(ctx.DB, tokenID) == nil {
		return errors.New("token not found")
	}
	if c.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	from := common.BytesToAddress(c.OwnerAddress)
	to := common.BytesToAddress(c.ToAddress)
	if from == to {
		return errors.New("cannot transfer to self")
	}
	if ctx.State.GetTRC10Balance(from, tokenID) < c.Amount {
		return errors.New("insufficient TRC10 balance")
	}
	return nil
}

func (a *TransferAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	tokenID, _ := strconv.ParseInt(string(c.AssetName), 10, 64)
	from := common.BytesToAddress(c.OwnerAddress)
	to := common.BytesToAddress(c.ToAddress)

	fee := int64(0)
	if !ctx.State.AccountExists(to) {
		ctx.State.CreateAccount(to, corepb.AccountType_Normal)
		fee = ctx.DynProps.CreateAccountFee()
		if err := ctx.State.SubBalance(from, fee); err != nil {
			return nil, err
		}
	}

	if err := ctx.State.SubTRC10Balance(from, tokenID, c.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10Balance(to, tokenID, c.Amount)

	return &Result{Fee: fee, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Run tests — expect all pass**

```bash
go test ./actuator/ -run TestTransferAsset -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/transfer_asset.go actuator/transfer_asset_test.go
git commit -m "feat(actuator): add TransferAssetActuator (TRC10 type 2)"
```

---

## Task 5: ParticipateAssetIssueActuator

**Files:**
- Create: `actuator/participate_asset_issue.go`
- Create: `actuator/participate_asset_issue_test.go`

**Context:** Buyers purchase tokens from the issuer during the ICO window (`start_time..end_time`). The exchange: `amount` TRX drops buy `amount * num / trx_num` tokens from the issuer's TRC10 balance. `ctx.BlockTime` represents the current time in milliseconds.

- [ ] **Step 1: Write the failing tests**

Create `actuator/participate_asset_issue_test.go`:

```go
package actuator

import (
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

const participateTokenID = int64(1_000_001)

func makeParticipateAssetTx(buyerByte, issuerByte byte, tokenID int64, trxAmount int64) *types.Transaction {
	buyer := makeTestAddr(buyerByte)
	issuer := makeTestAddr(issuerByte)
	c := &contractpb.ParticipateAssetIssueContract{
		OwnerAddress: buyer.Bytes(),
		ToAddress:    issuer.Bytes(),
		AssetName:    []byte(strconv.FormatInt(tokenID, 10)),
		Amount:       trxAmount,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ParticipateAssetIssueContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// makeTestICO writes an asset with ICO window [start, end] and rate trx_num:num.
// Returns a context with the issuer having the full supply as TRC10 balance.
func makeTestICO(t *testing.T, buyerByte, issuerByte byte, trxNum, num int32, startTime, endTime int64) *Context {
	t.Helper()
	buyer := makeTestAddr(buyerByte)
	issuer := makeTestAddr(issuerByte)

	asset := &contractpb.AssetIssueContract{
		Name:        []byte("ICOTOKEN"),
		TotalSupply: 10_000_000,
		TrxNum:      trxNum,
		Num:         num,
		StartTime:   startTime,
		EndTime:     endTime,
		Id:          strconv.FormatInt(participateTokenID, 10),
	}
	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, participateTokenID, asset)

	tx := makeParticipateAssetTx(buyerByte, issuerByte, participateTokenID, 1_000_000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(buyer, corepb.AccountType_Normal)
	statedb.CreateAccount(issuer, corepb.AccountType_Normal)
	statedb.AddBalance(buyer, 100_000_000)
	statedb.SetTRC10Balance(issuer, participateTokenID, 10_000_000)

	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = (startTime + endTime) / 2 // midpoint within ICO window
	return ctx
}

func TestParticipateAssetValidate_Success(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 100, 500, 2000)
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestParticipateAssetValidate_ICONotStarted(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 100, 5000, 10000) // window starts at 5000
	ctx.BlockTime = 100                                 // before ICO
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: ICO not started")
	}
}

func TestParticipateAssetValidate_ICOEnded(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 100, 100, 500) // window ends at 500
	ctx.BlockTime = 1000                             // after ICO
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: ICO ended")
	}
}

func TestParticipateAssetValidate_InsufficientTRX(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 1, 500, 2000)
	buyer := makeTestAddr(1)
	ctx.State.SubBalance(buyer, ctx.State.GetBalance(buyer)) // drain TRX
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: insufficient TRX")
	}
}

func TestParticipateAssetExecute(t *testing.T) {
	// rate: 1 TRX = 100 tokens
	ctx := makeTestICO(t, 1, 2, 1, 100, 500, 2000)
	buyer := makeTestAddr(1)
	issuer := makeTestAddr(2)
	initialBuyerTRX := ctx.State.GetBalance(buyer)

	act := &ParticipateAssetIssueActuator{}
	// tx sends 1_000_000 drops → buyer gets 1_000_000 * 100 / 1 = 100_000_000 tokens
	// But issuer only has 10_000_000 tokens — validate will fail for token amount > supply.
	// Adjust: use 1 drop → 100 tokens; issuer has 10M tokens.
	// Let's use trxNum=1, num=1 so 1 drop → 1 token.
	asset := &contractpb.AssetIssueContract{
		Name:        []byte("ICOTOKEN"),
		TotalSupply: 10_000_000,
		TrxNum:      1,
		Num:         1,
		StartTime:   500,
		EndTime:     2000,
		Id:          strconv.FormatInt(participateTokenID, 10),
	}
	rawdb.WriteAssetIssue(ctx.DB, participateTokenID, asset)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Buyer paid 1_000_000 TRX drops
	if got := ctx.State.GetBalance(buyer); got != initialBuyerTRX-1_000_000 {
		t.Fatalf("buyer TRX: want %d, got %d", initialBuyerTRX-1_000_000, got)
	}
	// Buyer received 1_000_000 tokens (1:1 rate)
	if got := ctx.State.GetTRC10Balance(buyer, participateTokenID); got != 1_000_000 {
		t.Fatalf("buyer TRC10: want 1000000, got %d", got)
	}
	// Issuer received 1_000_000 TRX drops
	issuerInitialTRX := int64(0) // issuer started with 0 TRX
	if got := ctx.State.GetBalance(issuer); got != issuerInitialTRX+1_000_000 {
		t.Fatalf("issuer TRX: want %d, got %d", issuerInitialTRX+1_000_000, got)
	}
	// Issuer lost 1_000_000 tokens
	if got := ctx.State.GetTRC10Balance(issuer, participateTokenID); got != 9_000_000 {
		t.Fatalf("issuer TRC10: want 9000000, got %d", got)
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./actuator/ -run TestParticipateAsset -v 2>&1 | head -10
```

Expected: `undefined: ParticipateAssetIssueActuator`.

- [ ] **Step 3: Create `actuator/participate_asset_issue.go`**

```go
package actuator

import (
	"errors"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ParticipateAssetIssueActuator handles TRC10 ICO participation (contract type 9).
// Buyers send TRX to the issuer in exchange for tokens at the asset's exchange rate.
type ParticipateAssetIssueActuator struct{}

func (a *ParticipateAssetIssueActuator) getContract(ctx *Context) (*contractpb.ParticipateAssetIssueContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ParticipateAssetIssueContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ParticipateAssetIssueContract")
	}
	return c, nil
}

func (a *ParticipateAssetIssueActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	tokenID, err := strconv.ParseInt(string(c.AssetName), 10, 64)
	if err != nil {
		return errors.New("invalid token ID in asset_name")
	}
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	if asset == nil {
		return errors.New("token not found")
	}
	if c.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	if ctx.BlockTime < asset.StartTime {
		return errors.New("ICO has not started yet")
	}
	if ctx.BlockTime > asset.EndTime {
		return errors.New("ICO has ended")
	}
	if asset.TrxNum <= 0 || asset.Num <= 0 {
		return errors.New("invalid exchange rate in asset")
	}
	tokenAmount := c.Amount * int64(asset.Num) / int64(asset.TrxNum)
	if tokenAmount <= 0 {
		return errors.New("token amount rounds to zero")
	}
	buyer := common.BytesToAddress(c.OwnerAddress)
	issuer := common.BytesToAddress(c.ToAddress)
	if buyer == issuer {
		return errors.New("buyer and issuer must be different")
	}
	if ctx.State.GetBalance(buyer) < c.Amount {
		return errors.New("insufficient TRX balance")
	}
	if ctx.State.GetTRC10Balance(issuer, tokenID) < tokenAmount {
		return errors.New("issuer has insufficient token supply")
	}
	return nil
}

func (a *ParticipateAssetIssueActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	tokenID, _ := strconv.ParseInt(string(c.AssetName), 10, 64)
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	tokenAmount := c.Amount * int64(asset.Num) / int64(asset.TrxNum)

	buyer := common.BytesToAddress(c.OwnerAddress)
	issuer := common.BytesToAddress(c.ToAddress)

	// Buyer pays TRX; issuer receives TRX
	if err := ctx.State.SubBalance(buyer, c.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddBalance(issuer, c.Amount)

	// Issuer gives tokens; buyer receives tokens
	if err := ctx.State.SubTRC10Balance(issuer, tokenID, tokenAmount); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10Balance(buyer, tokenID, tokenAmount)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Run tests — expect all pass**

```bash
go test ./actuator/ -run TestParticipateAsset -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/participate_asset_issue.go actuator/participate_asset_issue_test.go
git commit -m "feat(actuator): add ParticipateAssetIssueActuator (TRC10 type 9)"
```

---

## Task 6: UpdateAssetActuator + VoteAssetActuator

**Files:**
- Create: `actuator/update_asset.go`
- Create: `actuator/update_asset_test.go`
- Create: `actuator/vote_asset.go`
- Create: `actuator/vote_asset_test.go`

**Context:** UpdateAsset uses `ReadAssetOwnerIndex` to find the caller's token. VoteAsset was deprecated in modern TRON — it validates the owner exists but makes no state changes.

- [ ] **Step 1: Write the failing tests**

Create `actuator/update_asset_test.go`:

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

func makeUpdateAssetTx(ownerByte byte, desc, url string, newLimit, newPublicLimit int64) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.UpdateAssetContract{
		OwnerAddress:  owner.Bytes(),
		Description:   []byte(desc),
		Url:           []byte(url),
		NewLimit:      newLimit,
		NewPublicLimit: newPublicLimit,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_UpdateAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestUpdateAssetValidate_Success(t *testing.T) {
	owner := makeTestAddr(1)
	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetOwnerIndex(db, owner[:], 1_000_001)
	rawdb.WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{
		Name:  []byte("MYTOKEN"),
		Owner: owner.Bytes(),
	})

	tx := makeUpdateAssetTx(1, "new desc", "http://new.url", 500, 1000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &UpdateAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestUpdateAssetValidate_NotOwner(t *testing.T) {
	nonOwner := makeTestAddr(2)
	db := ethrawdb.NewMemoryDatabase()
	// No entry for nonOwner in owner index

	tx := makeUpdateAssetTx(2, "desc", "url", 0, 0)
	statedb := setupStateDB(t)
	statedb.CreateAccount(nonOwner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &UpdateAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: not token owner")
	}
}

func TestUpdateAssetExecute(t *testing.T) {
	owner := makeTestAddr(1)
	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetOwnerIndex(db, owner[:], 1_000_001)
	rawdb.WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{
		Name:               []byte("MYTOKEN"),
		Description:        []byte("old desc"),
		Url:                []byte("http://old.url"),
		FreeAssetNetLimit:  100,
		PublicFreeAssetNetLimit: 200,
	})

	tx := makeUpdateAssetTx(1, "new desc", "http://new.url", 500, 1000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &UpdateAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	updated := rawdb.ReadAssetIssue(db, 1_000_001)
	if updated == nil {
		t.Fatal("asset should still be in rawdb")
	}
	if string(updated.Description) != "new desc" {
		t.Fatalf("description: want 'new desc', got %s", updated.Description)
	}
	if string(updated.Url) != "http://new.url" {
		t.Fatalf("url: want 'http://new.url', got %s", updated.Url)
	}
	if updated.FreeAssetNetLimit != 500 {
		t.Fatalf("free_asset_net_limit: want 500, got %d", updated.FreeAssetNetLimit)
	}
	if updated.PublicFreeAssetNetLimit != 1000 {
		t.Fatalf("public_free_asset_net_limit: want 1000, got %d", updated.PublicFreeAssetNetLimit)
	}
}
```

Create `actuator/vote_asset_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeVoteAssetTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.VoteAssetContract{
		OwnerAddress: owner.Bytes(),
		Count:        1,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_VoteAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestVoteAssetValidate_Success(t *testing.T) {
	owner := makeTestAddr(1)
	tx := makeVoteAssetTx(1)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()

	act := &VoteAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestVoteAssetValidate_OwnerNotExist(t *testing.T) {
	tx := makeVoteAssetTx(1)
	statedb := setupStateDB(t)
	// No account created
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()

	act := &VoteAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: owner does not exist")
	}
}

func TestVoteAssetExecute_NoStateChange(t *testing.T) {
	owner := makeTestAddr(1)
	tx := makeVoteAssetTx(1)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	beforeBalance := statedb.GetBalance(owner)

	act := &VoteAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}
	if statedb.GetBalance(owner) != beforeBalance {
		t.Fatal("VoteAsset should not change any balance")
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./actuator/ -run "TestUpdateAsset|TestVoteAsset" -v 2>&1 | head -10
```

Expected: `undefined: UpdateAssetActuator`.

- [ ] **Step 3: Create `actuator/update_asset.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// UpdateAssetActuator handles TRC10 token metadata updates (contract type 15).
// Only the original issuer can update their token's description, URL, and bandwidth limits.
type UpdateAssetActuator struct{}

func (a *UpdateAssetActuator) getContract(ctx *Context) (*contractpb.UpdateAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateAssetContract")
	}
	return c, nil
}

func (a *UpdateAssetActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	if !ok {
		return errors.New("no token issued by this address")
	}
	if rawdb.ReadAssetIssue(ctx.DB, tokenID) == nil {
		return errors.New("token not found")
	}
	return nil
}

func (a *UpdateAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, _ := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)

	asset.Description = c.Description
	asset.Url = c.Url
	asset.FreeAssetNetLimit = c.NewLimit
	asset.PublicFreeAssetNetLimit = c.NewPublicLimit

	rawdb.WriteAssetIssue(ctx.DB, tokenID, asset)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Create `actuator/vote_asset.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// VoteAssetActuator handles the deprecated VoteAsset transaction (contract type 3).
// VoteAsset had no lasting on-chain effect in modern TRON. This implementation
// validates the owner exists and returns success with no state changes.
type VoteAssetActuator struct{}

func (a *VoteAssetActuator) getContract(ctx *Context) (*contractpb.VoteAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.VoteAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal VoteAssetContract")
	}
	return c, nil
}

func (a *VoteAssetActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(owner) {
		return errors.New("owner account does not exist")
	}
	return nil
}

func (a *VoteAssetActuator) Execute(ctx *Context) (*Result, error) {
	if _, err := a.getContract(ctx); err != nil {
		return nil, err
	}
	// VoteAsset is deprecated — no state changes.
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 5: Run tests — expect all pass**

```bash
go test ./actuator/ -run "TestUpdateAsset|TestVoteAsset" -v
```

Expected: All 5 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add actuator/update_asset.go actuator/update_asset_test.go actuator/vote_asset.go actuator/vote_asset_test.go
git commit -m "feat(actuator): add UpdateAssetActuator (type 15) and VoteAssetActuator (type 3)"
```

---

## Task 7: UnfreezeAssetActuator

**Files:**
- Create: `actuator/unfreeze_asset.go`
- Create: `actuator/unfreeze_asset_test.go`

**Context:** Token issuers can define `frozen_supply` entries at issue time. These are NOT minted immediately — they're held until `issueTime + frozenDays * 86_400_000ms` elapses, at which point `UnfreezeAsset` credits them. The issue time is stored at `asti-<tokenID>` by `AssetIssueActuator`. Claimed state is tracked per-entry in the issuer's storage trie.

- [ ] **Step 1: Write the failing tests**

Create `actuator/unfreeze_asset_test.go`:

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

const unfreezeTokenID = int64(1_000_001)
const dayMs = int64(86_400_000)

func makeUnfreezeAssetTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.UnfreezeAssetContract{
		OwnerAddress: owner.Bytes(),
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_UnfreezeAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// setupUnfreezeCtx creates a context with:
// - owner at ownerByte has issued a token with frozen_supply[0] = {amount, frozenDays}
// - issue time stored in rawdb
// - ctx.BlockTime = issueTime + blockTimeOffsetMs
func setupUnfreezeCtx(t *testing.T, ownerByte byte, frozenAmount, frozenDays int64, blockTimeOffset int64) *Context {
	t.Helper()
	owner := makeTestAddr(ownerByte)
	issueTime := int64(1_000_000)

	asset := &contractpb.AssetIssueContract{
		Name:        []byte("FROZENTOKEN"),
		TotalSupply: 1_000_000,
		Id:          "1000001",
		FrozenSupply: []*contractpb.AssetIssueContract_FrozenSupply{
			{FrozenAmount: frozenAmount, FrozenDays: frozenDays},
		},
	}
	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, unfreezeTokenID, asset)
	rawdb.WriteAssetOwnerIndex(db, owner[:], unfreezeTokenID)
	rawdb.WriteAssetIssueTime(db, unfreezeTokenID, issueTime)

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	tx := makeUnfreezeAssetTx(ownerByte)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + blockTimeOffset
	return ctx
}

func TestUnfreezeAssetValidate_Success(t *testing.T) {
	// frozen 1 day; blockTime = issueTime + 2 days → eligible
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*dayMs)
	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestUnfreezeAssetValidate_FreezeNotElapsed(t *testing.T) {
	// frozen 30 days; blockTime = issueTime + 1 day → not eligible
	ctx := setupUnfreezeCtx(t, 1, 200_000, 30, 1*dayMs)
	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: freeze period not elapsed")
	}
}

func TestUnfreezeAssetValidate_NotOwner(t *testing.T) {
	// owner at byte 1, trying byte 2
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*dayMs)
	// rebuild tx with a different owner
	otherOwner := makeTestAddr(2)
	ctx.State.CreateAccount(otherOwner, corepb.AccountType_Normal)
	c := &contractpb.UnfreezeAssetContract{OwnerAddress: otherOwner.Bytes()}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_UnfreezeAssetContract, Parameter: anyParam},
			},
		},
	}
	ctx.Tx = types.NewTransactionFromPB(pb)

	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: not token owner")
	}
}

func TestUnfreezeAssetExecute(t *testing.T) {
	// Two frozen entries: entry 0 is past due, entry 1 is still locked.
	owner := makeTestAddr(1)
	issueTime := int64(1_000_000)
	asset := &contractpb.AssetIssueContract{
		Name:        []byte("FROZENTOKEN"),
		TotalSupply: 1_000_000,
		Id:          "1000001",
		FrozenSupply: []*contractpb.AssetIssueContract_FrozenSupply{
			{FrozenAmount: 100_000, FrozenDays: 1},  // entry 0: 1 day
			{FrozenAmount: 200_000, FrozenDays: 30}, // entry 1: 30 days
		},
	}
	db := ethrawdb.NewMemoryDatabase()
	rawdb.WriteAssetIssue(db, unfreezeTokenID, asset)
	rawdb.WriteAssetOwnerIndex(db, owner[:], unfreezeTokenID)
	rawdb.WriteAssetIssueTime(db, unfreezeTokenID, issueTime)

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	tx := makeUnfreezeAssetTx(1)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + 2*dayMs // 2 days: entry 0 eligible, entry 1 not

	act := &UnfreezeAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Only entry 0 (100,000 tokens) should have been credited
	if got := statedb.GetTRC10Balance(owner, unfreezeTokenID); got != 100_000 {
		t.Fatalf("TRC10 balance: want 100000, got %d", got)
	}
	// Entry 0 marked claimed
	if !statedb.IsFrozenClaimed(owner, unfreezeTokenID, 0) {
		t.Fatal("entry 0 should be marked claimed")
	}
	// Entry 1 not claimed
	if statedb.IsFrozenClaimed(owner, unfreezeTokenID, 1) {
		t.Fatal("entry 1 should not be claimed yet")
	}
}

func TestUnfreezeAssetExecute_AlreadyClaimed(t *testing.T) {
	// Entry already claimed — second call should find nothing to unfreeze
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*dayMs)
	owner := makeTestAddr(1)
	// Mark entry 0 as already claimed
	ctx.State.SetFrozenClaimed(owner, unfreezeTokenID, 0)

	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: nothing to unfreeze (already claimed)")
	}
}
```

- [ ] **Step 2: Run — expect compile failure**

```bash
go test ./actuator/ -run TestUnfreezeAsset -v 2>&1 | head -10
```

Expected: `undefined: UnfreezeAssetActuator`.

- [ ] **Step 3: Create `actuator/unfreeze_asset.go`**

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const dayMs = int64(86_400_000) // milliseconds per day

// UnfreezeAssetActuator handles TRC10 frozen supply release (contract type 14).
// Token issuers call this to claim pre-frozen supply after lock-up periods expire.
type UnfreezeAssetActuator struct{}

func (a *UnfreezeAssetActuator) getContract(ctx *Context) (*contractpb.UnfreezeAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UnfreezeAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UnfreezeAssetContract")
	}
	return c, nil
}

// eligibleCount returns the number of frozen_supply entries that can be claimed.
func (a *UnfreezeAssetActuator) eligibleCount(ctx *Context, owner common.Address, tokenID int64, asset *contractpb.AssetIssueContract, issueTime int64) int {
	count := 0
	for i, f := range asset.FrozenSupply {
		if issueTime+f.FrozenDays*dayMs > ctx.BlockTime {
			continue
		}
		if ctx.State.IsFrozenClaimed(owner, tokenID, uint32(i)) {
			continue
		}
		count++
	}
	return count
}

func (a *UnfreezeAssetActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	if !ok {
		return errors.New("no token issued by this address")
	}
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	if asset == nil {
		return errors.New("token not found")
	}
	if len(asset.FrozenSupply) == 0 {
		return errors.New("token has no frozen supply")
	}
	issueTime := rawdb.ReadAssetIssueTime(ctx.DB, tokenID)
	if a.eligibleCount(ctx, owner, tokenID, asset, issueTime) == 0 {
		return errors.New("no frozen supply is currently available to unfreeze")
	}
	return nil
}

func (a *UnfreezeAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, _ := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	issueTime := rawdb.ReadAssetIssueTime(ctx.DB, tokenID)

	for i, f := range asset.FrozenSupply {
		if issueTime+f.FrozenDays*dayMs > ctx.BlockTime {
			continue
		}
		if ctx.State.IsFrozenClaimed(owner, tokenID, uint32(i)) {
			continue
		}
		ctx.State.AddTRC10Balance(owner, tokenID, f.FrozenAmount)
		ctx.State.SetFrozenClaimed(owner, tokenID, uint32(i))
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

**Note:** The constant `dayMs` is already defined in `unfreeze_asset.go`. The test file also defines a constant `dayMs`. This will cause a compile error since both are in package `actuator` (tests are in the same package). Rename the test constant to `testDayMs` in the test file:

In `actuator/unfreeze_asset_test.go`, change:
```go
const dayMs = int64(86_400_000)
```
to:
```go
const testDayMs = int64(86_400_000)
```

And update all references in the test file from `dayMs` to `testDayMs`.

- [ ] **Step 4: Run tests — expect all pass**

```bash
go test ./actuator/ -run TestUnfreezeAsset -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add actuator/unfreeze_asset.go actuator/unfreeze_asset_test.go
git commit -m "feat(actuator): add UnfreezeAssetActuator (TRC10 type 14)"
```

---

## Task 8: Register All 6 Actuators

**Files:**
- Modify: `actuator/actuator.go`

- [ ] **Step 1: Add 6 cases to the switch in `actuator/actuator.go`**

In `CreateActuator`, add before the `default:` case:

```go
	case corepb.Transaction_Contract_TransferAssetContract:
		return &TransferAssetActuator{}, nil
	case corepb.Transaction_Contract_VoteAssetContract:
		return &VoteAssetActuator{}, nil
	case corepb.Transaction_Contract_AssetIssueContract:
		return &AssetIssueActuator{}, nil
	case corepb.Transaction_Contract_ParticipateAssetIssueContract:
		return &ParticipateAssetIssueActuator{}, nil
	case corepb.Transaction_Contract_UnfreezeAssetContract:
		return &UnfreezeAssetActuator{}, nil
	case corepb.Transaction_Contract_UpdateAssetContract:
		return &UpdateAssetActuator{}, nil
```

- [ ] **Step 2: Verify the package compiles**

```bash
go build ./actuator/
```

Expected: no output.

- [ ] **Step 3: Run all actuator tests to check for regressions**

```bash
go test ./actuator/ -v 2>&1 | tail -30
```

Expected: All tests PASS (including all 6 new actuators and all existing actuators).

- [ ] **Step 4: Commit**

```bash
git add actuator/actuator.go
git commit -m "feat(actuator): register 6 TRC10 actuators (types 2, 3, 6, 9, 14, 15)"
```

---

## Task 9: HTTP Query Endpoints

**Files:**
- Modify: `internal/tronapi/backend.go`
- Modify: `internal/tronapi/api.go`
- Modify: `core/tron_backend.go`

**Context:** Five endpoints return asset metadata (not per-account balances). Follow the existing pattern: `writeTronJSON` for single proto responses, `marshalMessage` + JSON marshal for lists. The `b.chain.db` field is the raw DB store accessed from the `core` package (same package as `TronBackend`).

- [ ] **Step 1: Add 5 Backend interface methods to `internal/tronapi/backend.go`**

Add at the end of the `Backend` interface (before the closing `}`):

```go
	// Asset queries (TRC10)
	GetAssetIssueByID(id int64) *contractpb.AssetIssueContract
	GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract
	GetAssetIssueList() []*contractpb.AssetIssueContract
	GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract
	GetAssetIssueByAccount(addr common.Address) *contractpb.AssetIssueContract
```

- [ ] **Step 2: Verify the file still builds (stub check)**

```bash
go build ./internal/tronapi/ 2>&1 | head -10
```

Expected: errors about `TronBackend` not implementing the interface. That is expected — we'll fix in Step 4.

- [ ] **Step 3: Add 5 route handlers to `internal/tronapi/api.go`**

In `RegisterRoutes`, add after the `listnodes` line:

```go
	// Phase 12: TRC10 asset queries
	mux.HandleFunc("/wallet/getassetissuebyid", api.getAssetIssueByID)
	mux.HandleFunc("/wallet/getassetissuebyname", api.getAssetIssueByName)
	mux.HandleFunc("/wallet/getassetissuelist", api.getAssetIssueList)
	mux.HandleFunc("/wallet/getpaginatedassetissuelist", api.getPaginatedAssetIssueList)
	mux.HandleFunc("/wallet/getassetissuebyaccount", api.getAssetIssueByAccount)
```

Add the 5 handler functions at the end of the file:

```go
func (api *API) getAssetIssueByID(w http.ResponseWriter, r *http.Request) {
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
	var id int64
	if _, err := fmt.Sscanf(body.Value, "%d", &id); err != nil {
		http.Error(w, "invalid token ID", http.StatusBadRequest)
		return
	}
	asset := api.backend.GetAssetIssueByID(id)
	if asset == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, asset)
}

func (api *API) getAssetIssueByName(w http.ResponseWriter, r *http.Request) {
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
	asset := api.backend.GetAssetIssueByName([]byte(body.Value))
	if asset == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, asset)
}

func (api *API) getAssetIssueList(w http.ResponseWriter, r *http.Request) {
	assets := api.backend.GetAssetIssueList()
	var list []map[string]any
	for _, a := range assets {
		list = append(list, marshalMessage(a.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	resp := map[string]any{"assetIssue": list}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getPaginatedAssetIssueList(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if body.Limit <= 0 {
		body.Limit = 20
	}
	assets := api.backend.GetAssetIssueListPaginated(body.Offset, body.Limit)
	var list []map[string]any
	for _, a := range assets {
		list = append(list, marshalMessage(a.ProtoReflect()))
	}
	if list == nil {
		list = []map[string]any{}
	}
	resp := map[string]any{"assetIssue": list}
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAssetIssueByAccount(w http.ResponseWriter, r *http.Request) {
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
	asset := api.backend.GetAssetIssueByAccount(addr)
	if asset == nil {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{}"))
		return
	}
	writeTronJSON(w, asset)
}
```

- [ ] **Step 4: Implement the 5 Backend methods in `core/tron_backend.go`**

Add at the end of the file:

```go
func (b *TronBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract {
	return rawdb.ReadAssetIssue(b.chain.db, id)
}

func (b *TronBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	id, ok := rawdb.ReadAssetNameIndex(b.chain.db, name)
	if !ok {
		return nil
	}
	return rawdb.ReadAssetIssue(b.chain.db, id)
}

func (b *TronBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
	return rawdb.ListAllAssets(b.chain.db)
}

func (b *TronBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
	return rawdb.ListAssetsPaginated(b.chain.db, offset, limit)
}

func (b *TronBackend) GetAssetIssueByAccount(addr tcommon.Address) *contractpb.AssetIssueContract {
	id, ok := rawdb.ReadAssetOwnerIndex(b.chain.db, addr[:])
	if !ok {
		return nil
	}
	return rawdb.ReadAssetIssue(b.chain.db, id)
}
```

- [ ] **Step 5: Build the full project to verify no compile errors**

```bash
go build ./...
```

Expected: no output (success).

- [ ] **Step 6: Commit**

```bash
git add internal/tronapi/backend.go internal/tronapi/api.go core/tron_backend.go
git commit -m "feat(api): add 5 TRC10 asset HTTP query endpoints"
```

---

## Task 10: System Test Section 11 + Final Verification

**Files:**
- Modify: `scripts/system_test.sh`

- [ ] **Step 1: Run the full test suite to confirm all unit tests pass**

```bash
go test ./... 2>&1 | tail -30
```

Expected: All packages PASS. Fix any failures before proceeding.

- [ ] **Step 2: Add Section 11 to `scripts/system_test.sh`**

Add the following BEFORE the `# ─── Summary` section (the last section in the file):

```bash
# ─────────────────────────────────────────────────────────────────
# SECTION 11: Phase 12 — TRC10 Asset Query Endpoints
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 11: TRC10 Asset Query Endpoints ==="

# 11.1 getassetissuelist — empty on fresh node
RESULT=$(http_get $SR_HTTP "/wallet/getassetissuelist")
check "getassetissuelist returns assetIssue key" "$RESULT" '"assetIssue"'

# 11.2 getassetissuebyid — unknown ID returns {}
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getassetissuebyid" \
    -H "Content-Type: application/json" \
    -d '{"value":"1000001"}' 2>/dev/null || echo "CURL_ERROR")
check "getassetissuebyid unknown returns {}" "$RESULT" '{}'

# 11.3 getassetissuebyname — unknown name returns {}
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getassetissuebyname" \
    -H "Content-Type: application/json" \
    -d '{"value":"UNKNOWNTOKEN"}' 2>/dev/null || echo "CURL_ERROR")
check "getassetissuebyname unknown returns {}" "$RESULT" '{}'

# 11.4 getpaginatedassetissuelist — empty returns assetIssue array
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getpaginatedassetissuelist" \
    -H "Content-Type: application/json" \
    -d '{"offset":0,"limit":10}' 2>/dev/null || echo "CURL_ERROR")
check "getpaginatedassetissuelist returns assetIssue key" "$RESULT" '"assetIssue"'

# 11.5 getassetissuebyaccount — unknown account returns {}
RESULT=$(curl -sf --max-time 5 -X POST "http://localhost:$SR_HTTP/wallet/getassetissuebyaccount" \
    -H "Content-Type: application/json" \
    -d "{\"address\":\"$WITNESS_ADDR\"}" 2>/dev/null || echo "CURL_ERROR")
check "getassetissuebyaccount unknown returns {}" "$RESULT" '{}'
```

- [ ] **Step 3: Verify the system test script has no syntax errors**

```bash
bash -n scripts/system_test.sh
```

Expected: no output (no syntax errors).

- [ ] **Step 4: Final build verification**

```bash
go build ./...
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add scripts/system_test.sh
git commit -m "test(system): add Section 11 TRC10 asset query endpoint checks"
```

---

## Self-Review Notes

**Spec coverage:**
- 6 actuators: types 2, 3, 6, 9, 14, 15 ✓ (Tasks 3–8)
- rawdb 4 new prefixes: ast-, astn-, asto-, asti- ✓ (Task 1)
- StateDB TRC10 methods: GetTRC10Balance, SetTRC10Balance, AddTRC10Balance, SubTRC10Balance, IsFrozenClaimed, SetFrozenClaimed ✓ (Task 2)
- DynamicProperties: next_token_id, NextTokenID, SetNextTokenID, AssetIssueFee ✓ (Task 2)
- 5 HTTP query endpoints ✓ (Task 9)
- System test Section 11 ✓ (Task 10)

**Type consistency:**
- `tcommon.Address` (21-byte) used throughout — no 20-byte Ethereum addresses
- `tcommon.Keccak256` used in slots.go — no external crypto import
- `dayMs` constant defined only in `unfreeze_asset.go` — test file uses `testDayMs`
- `ListAllAssets`/`ListAssetsPaginated` take `ethdb.Iteratee`; `b.chain.db` satisfies it
- All rawdb write functions match key functions in schema.go

**Important implementation notes:**
1. `newTestContext` sets `ctx.DB = nil` — every TRC10 actuator test must do `ctx.DB = ethrawdb.NewMemoryDatabase()`
2. `UnfreezeAssetActuator.Execute` uses `ReadAssetIssue` result — no nil check needed because `Validate` already confirmed the asset exists
3. `VoteAsset` is deprecated — it returns `ContractRet: 1` with no state changes
4. `AssetIssueActuator.Execute` mints `total_supply - sum(frozen_supply)` to issuer; frozen portions are held until `UnfreezeAsset`
