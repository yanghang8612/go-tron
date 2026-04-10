# Phase 8: Governance, Delegation & Account Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the transaction execution layer from 9 to 20 contract types by adding governance proposals, resource delegation, and account/witness management actuators.

**Architecture:** Each new contract type follows the existing actuator pattern (struct with `getContract()`, `Validate()`, `Execute()` methods registered in `CreateActuator`). New rawdb accessors handle proposal, delegation, and brokerage storage. The maintenance cycle in `block_builder.go` is extended to process pending proposals.

**Tech Stack:** Go, protobuf (existing proto definitions), ethdb (Pebble), existing actuator/rawdb/state patterns.

---

### Task 1: Proposal Storage (rawdb)

**Files:**
- Modify: `core/rawdb/schema.go`
- Create: `core/rawdb/accessors_proposal.go`
- Create: `core/rawdb/accessors_proposal_test.go`

- [ ] **Step 1: Add schema keys for proposals**

In `core/rawdb/schema.go`, add after the existing `proposalPrefix` line (which already exists as `[]byte("p-")`):

```go
// Add these new variables after existing ones:
proposalIndexKey = []byte("propi")
```

Add these key functions at the end of the file:

```go
func proposalKey(id int64) []byte {
	k := make([]byte, len(proposalPrefix)+8)
	copy(k, proposalPrefix)
	binary.BigEndian.PutUint64(k[len(proposalPrefix):], uint64(id))
	return k
}
```

- [ ] **Step 2: Create proposal type and accessors**

Create `core/rawdb/accessors_proposal.go`:

```go
package rawdb

import (
	"encoding/binary"
	"encoding/json"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const (
	ProposalStatePending  = 0
	ProposalStateApproved = 1
	ProposalStateCanceled = 2
)

type Proposal struct {
	ID             int64            `json:"id"`
	Proposer       common.Address   `json:"proposer"`
	Parameters     map[int64]int64  `json:"parameters"`
	CreateTime     int64            `json:"create_time"`
	ExpirationTime int64            `json:"expiration_time"`
	Approvals      []common.Address `json:"approvals"`
	State          int32            `json:"state"`
}

func WriteProposal(db ethdb.KeyValueWriter, id int64, p *Proposal) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return db.Put(proposalKey(id), data)
}

func ReadProposal(db ethdb.KeyValueReader, id int64) *Proposal {
	data, err := db.Get(proposalKey(id))
	if err != nil || len(data) == 0 {
		return nil
	}
	p := &Proposal{}
	if err := json.Unmarshal(data, p); err != nil {
		return nil
	}
	return p
}

func WriteProposalIndex(db ethdb.KeyValueWriter, ids []int64) error {
	buf := make([]byte, 8*len(ids))
	for i, id := range ids {
		binary.BigEndian.PutUint64(buf[i*8:], uint64(id))
	}
	return db.Put(proposalIndexKey, buf)
}

func ReadProposalIndex(db ethdb.KeyValueReader) []int64 {
	data, err := db.Get(proposalIndexKey)
	if err != nil || len(data) == 0 {
		return nil
	}
	ids := make([]int64, len(data)/8)
	for i := range ids {
		ids[i] = int64(binary.BigEndian.Uint64(data[i*8:]))
	}
	return ids
}
```

- [ ] **Step 3: Write tests**

Create `core/rawdb/accessors_proposal_test.go`:

```go
package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestProposalWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	p := &Proposal{
		ID:             1,
		Proposer:       common.Address{0x41, 0x01},
		Parameters:     map[int64]int64{6: 200},
		CreateTime:     1000,
		ExpirationTime: 260200000,
		State:          ProposalStatePending,
	}
	if err := WriteProposal(db, 1, p); err != nil {
		t.Fatal(err)
	}
	got := ReadProposal(db, 1)
	if got == nil {
		t.Fatal("expected proposal")
	}
	if got.ID != 1 || got.Parameters[6] != 200 || got.State != ProposalStatePending {
		t.Fatalf("unexpected proposal: %+v", got)
	}
}

func TestProposalNotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if ReadProposal(db, 999) != nil {
		t.Fatal("expected nil for missing proposal")
	}
}

func TestProposalIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	ids := []int64{1, 2, 3}
	if err := WriteProposalIndex(db, ids); err != nil {
		t.Fatal(err)
	}
	got := ReadProposalIndex(db)
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("unexpected index: %v", got)
	}
}

func TestProposalIndexEmpty(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if ReadProposalIndex(db) != nil {
		t.Fatal("expected nil for empty index")
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./core/rawdb/ -run TestProposal -v`

- [ ] **Step 5: Commit**

```bash
git add core/rawdb/schema.go core/rawdb/accessors_proposal.go core/rawdb/accessors_proposal_test.go
git commit -m "feat(rawdb): add proposal storage accessors"
```

---

### Task 2: Delegation & Brokerage Storage (rawdb)

**Files:**
- Modify: `core/rawdb/schema.go`
- Create: `core/rawdb/accessors_delegation.go`
- Create: `core/rawdb/accessors_delegation_test.go`
- Create: `core/rawdb/accessors_brokerage.go`
- Create: `core/rawdb/accessors_brokerage_test.go`

- [ ] **Step 1: Add schema keys**

In `core/rawdb/schema.go`, add new variables:

```go
delegationPrefix      = []byte("dr-")
delegationIndexPrefix = []byte("dri-")
brokeragePrefix       = []byte("wb-")
```

Add key functions:

```go
func delegationKey(from, to []byte) []byte {
	k := make([]byte, len(delegationPrefix)+len(from)+len(to))
	copy(k, delegationPrefix)
	copy(k[len(delegationPrefix):], from)
	copy(k[len(delegationPrefix)+len(from):], to)
	return k
}

func delegationIndexKey(from []byte) []byte {
	return append(append([]byte{}, delegationIndexPrefix...), from...)
}

func brokerageKey(addr []byte) []byte {
	return append(append([]byte{}, brokeragePrefix...), addr...)
}
```

- [ ] **Step 2: Create delegation accessors**

Create `core/rawdb/accessors_delegation.go`:

```go
package rawdb

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type DelegatedResource struct {
	From                      common.Address `json:"from"`
	To                        common.Address `json:"to"`
	FrozenBalanceForBandwidth int64          `json:"frozen_balance_for_bandwidth"`
	FrozenBalanceForEnergy    int64          `json:"frozen_balance_for_energy"`
	ExpireTimeForBandwidth    int64          `json:"expire_time_for_bandwidth"`
	ExpireTimeForEnergy       int64          `json:"expire_time_for_energy"`
}

func WriteDelegatedResource(db ethdb.KeyValueWriter, from, to common.Address, dr *DelegatedResource) error {
	data, err := json.Marshal(dr)
	if err != nil {
		return err
	}
	return db.Put(delegationKey(from[:], to[:]), data)
}

func ReadDelegatedResource(db ethdb.KeyValueReader, from, to common.Address) *DelegatedResource {
	data, err := db.Get(delegationKey(from[:], to[:]))
	if err != nil || len(data) == 0 {
		return nil
	}
	dr := &DelegatedResource{}
	if err := json.Unmarshal(data, dr); err != nil {
		return nil
	}
	return dr
}

func DeleteDelegatedResource(db ethdb.KeyValueWriter, from, to common.Address) error {
	return db.Delete(delegationKey(from[:], to[:]))
}

func WriteDelegationIndex(db ethdb.KeyValueWriter, from common.Address, receivers []common.Address) error {
	buf := make([]byte, common.AddressLength*len(receivers))
	for i, r := range receivers {
		copy(buf[i*common.AddressLength:], r[:])
	}
	return db.Put(delegationIndexKey(from[:]), buf)
}

func ReadDelegationIndex(db ethdb.KeyValueReader, from common.Address) []common.Address {
	data, err := db.Get(delegationIndexKey(from[:]))
	if err != nil || len(data) == 0 {
		return nil
	}
	count := len(data) / common.AddressLength
	addrs := make([]common.Address, count)
	for i := range addrs {
		copy(addrs[i][:], data[i*common.AddressLength:])
	}
	return addrs
}
```

- [ ] **Step 3: Create delegation tests**

Create `core/rawdb/accessors_delegation_test.go`:

```go
package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestDelegatedResourceWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{
		From:                      from,
		To:                        to,
		FrozenBalanceForBandwidth: 1000000,
		FrozenBalanceForEnergy:    500000,
	}
	if err := WriteDelegatedResource(db, from, to, dr); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegatedResource(db, from, to)
	if got == nil {
		t.Fatal("expected delegation record")
	}
	if got.FrozenBalanceForBandwidth != 1000000 || got.FrozenBalanceForEnergy != 500000 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestDelegatedResourceDelete(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	dr := &DelegatedResource{From: from, To: to, FrozenBalanceForBandwidth: 100}
	WriteDelegatedResource(db, from, to, dr)
	DeleteDelegatedResource(db, from, to)
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestDelegationIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	receivers := []common.Address{{0x41, 0x02}, {0x41, 0x03}}
	if err := WriteDelegationIndex(db, from, receivers); err != nil {
		t.Fatal(err)
	}
	got := ReadDelegationIndex(db, from)
	if len(got) != 2 {
		t.Fatalf("expected 2 receivers, got %d", len(got))
	}
	if got[0] != receivers[0] || got[1] != receivers[1] {
		t.Fatalf("unexpected receivers: %v", got)
	}
}

func TestDelegationNotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	from := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	if ReadDelegatedResource(db, from, to) != nil {
		t.Fatal("expected nil")
	}
	if ReadDelegationIndex(db, from) != nil {
		t.Fatal("expected nil")
	}
}
```

- [ ] **Step 4: Create brokerage accessors**

Create `core/rawdb/accessors_brokerage.go`:

```go
package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const defaultBrokerage int64 = 20

func WriteWitnessBrokerage(db ethdb.KeyValueWriter, addr common.Address, brokerage int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(brokerage))
	return db.Put(brokerageKey(addr[:]), buf)
}

func ReadWitnessBrokerage(db ethdb.KeyValueReader, addr common.Address) int64 {
	data, err := db.Get(brokerageKey(addr[:]))
	if err != nil || len(data) != 8 {
		return defaultBrokerage
	}
	return int64(binary.BigEndian.Uint64(data))
}
```

- [ ] **Step 5: Create brokerage tests**

Create `core/rawdb/accessors_brokerage_test.go`:

```go
package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestBrokerageWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := common.Address{0x41, 0x01}
	if err := WriteWitnessBrokerage(db, addr, 30); err != nil {
		t.Fatal(err)
	}
	if got := ReadWitnessBrokerage(db, addr); got != 30 {
		t.Fatalf("expected 30, got %d", got)
	}
}

func TestBrokerageDefault(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := common.Address{0x41, 0x01}
	if got := ReadWitnessBrokerage(db, addr); got != 20 {
		t.Fatalf("expected default 20, got %d", got)
	}
}
```

- [ ] **Step 6: Run tests**

Run: `go test ./core/rawdb/ -run "TestDelegat|TestBrokerage" -v`

- [ ] **Step 7: Commit**

```bash
git add core/rawdb/schema.go core/rawdb/accessors_delegation.go core/rawdb/accessors_delegation_test.go core/rawdb/accessors_brokerage.go core/rawdb/accessors_brokerage_test.go
git commit -m "feat(rawdb): add delegation and brokerage storage accessors"
```

---

### Task 3: DynamicProperties & StateDB Extensions

**Files:**
- Modify: `core/state/dynamic_properties.go`
- Modify: `core/types/account.go`
- Modify: `core/state/statedb.go`

- [ ] **Step 1: Add next_proposal_id to DynamicProperties**

In `core/state/dynamic_properties.go`, add `"next_proposal_id": 0` to the `defaultProps` map.

Add typed getter/setter methods:

```go
func (dp *DynamicProperties) NextProposalID() int64 {
	return dp.props["next_proposal_id"]
}

func (dp *DynamicProperties) SetNextProposalID(id int64) {
	dp.Set("next_proposal_id", id)
}
```

Also fix `LoadDynamicProperties` — it currently only loads keys from `defaultProps`. Since we added `next_proposal_id` to `defaultProps`, it will be loaded automatically.

- [ ] **Step 2: Add Account type accessors for account_id, permissions, delegated balance**

In `core/types/account.go`, add these methods:

```go
// AccountId accessors.
func (a *Account) AccountId() string        { return string(a.pb.AccountId) }
func (a *Account) SetAccountId(id string)   { a.pb.AccountId = []byte(id) }

// Permission accessors.
func (a *Account) OwnerPermission() *corepb.Permission            { return a.pb.OwnerPermission }
func (a *Account) WitnessPermission() *corepb.Permission          { return a.pb.WitnessPermission }
func (a *Account) ActivePermission() []*corepb.Permission         { return a.pb.ActivePermission }
func (a *Account) SetOwnerPermission(p *corepb.Permission)        { a.pb.OwnerPermission = p }
func (a *Account) SetWitnessPermission(p *corepb.Permission)      { a.pb.WitnessPermission = p }
func (a *Account) SetActivePermission(perms []*corepb.Permission) { a.pb.ActivePermission = perms }

// Delegated frozen V2 balance accessors (resources delegated TO this account).
func (a *Account) DelegatedFrozenV2BalanceForBandwidth() int64 {
	return a.pb.DelegatedFrozenV2BalanceForBandwidth
}
func (a *Account) SetDelegatedFrozenV2BalanceForBandwidth(v int64) {
	a.pb.DelegatedFrozenV2BalanceForBandwidth = v
}
func (a *Account) AcquiredDelegatedFrozenV2BalanceForBandwidth() int64 {
	return a.pb.AcquiredDelegatedFrozenV2BalanceForBandwidth
}
func (a *Account) SetAcquiredDelegatedFrozenV2BalanceForBandwidth(v int64) {
	a.pb.AcquiredDelegatedFrozenV2BalanceForBandwidth = v
}

func (a *Account) DelegatedFrozenV2BalanceForEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.DelegatedFrozenV2BalanceForEnergy
}
func (a *Account) SetDelegatedFrozenV2BalanceForEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.DelegatedFrozenV2BalanceForEnergy = v
}
func (a *Account) AcquiredDelegatedFrozenV2BalanceForEnergy() int64 {
	if a.pb.AccountResource == nil {
		return 0
	}
	return a.pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy
}
func (a *Account) SetAcquiredDelegatedFrozenV2BalanceForEnergy(v int64) {
	a.ensureAccountResource()
	a.pb.AccountResource.AcquiredDelegatedFrozenV2BalanceForEnergy = v
}

// ClearUnfrozenV2 removes all pending unfreeze entries.
func (a *Account) ClearUnfrozenV2() {
	a.pb.UnfrozenV2 = nil
}
```

- [ ] **Step 3: Add StateDB methods for account name/id, permissions, delegated balance**

In `core/state/statedb.go`, add:

```go
// SetAccountName sets the account name.
func (s *StateDB) SetAccountName(addr tcommon.Address, name string) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAccountName(name)
	obj.markDirty()
}

// GetAccountName returns the account name.
func (s *StateDB) GetAccountName(addr tcommon.Address) string {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ""
	}
	return obj.account.AccountName()
}

// SetAccountId sets the account ID.
func (s *StateDB) SetAccountId(addr tcommon.Address, id string) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetAccountId(id)
	obj.markDirty()
}

// GetAccountId returns the account ID.
func (s *StateDB) GetAccountId(addr tcommon.Address) string {
	obj := s.getStateObject(addr)
	if obj == nil {
		return ""
	}
	return obj.account.AccountId()
}

// SetPermissions sets all permissions on the account.
func (s *StateDB) SetPermissions(addr tcommon.Address, owner, witness *corepb.Permission, actives []*corepb.Permission) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetOwnerPermission(owner)
	obj.account.SetWitnessPermission(witness)
	obj.account.SetActivePermission(actives)
	obj.markDirty()
}

// AddDelegatedFrozenV2 adds to the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) AddDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(obj.account.DelegatedFrozenV2BalanceForBandwidth() + amount)
	} else {
		obj.account.SetDelegatedFrozenV2BalanceForEnergy(obj.account.DelegatedFrozenV2BalanceForEnergy() + amount)
	}
	obj.markDirty()
}

// SubDelegatedFrozenV2 subtracts from the delegated (outgoing) frozen balance for a resource.
func (s *StateDB) SubDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		v := obj.account.DelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForBandwidth(v)
	} else {
		v := obj.account.DelegatedFrozenV2BalanceForEnergy() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetDelegatedFrozenV2BalanceForEnergy(v)
	}
	obj.markDirty()
}

// AddAcquiredDelegatedFrozenV2 adds to the acquired (incoming) delegated frozen balance.
func (s *StateDB) AddAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() + amount)
	} else {
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() + amount)
	}
	obj.markDirty()
}

// SubAcquiredDelegatedFrozenV2 subtracts from the acquired (incoming) delegated frozen balance.
func (s *StateDB) SubAcquiredDelegatedFrozenV2(addr tcommon.Address, resourceType corepb.ResourceCode, amount int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	if resourceType == corepb.ResourceCode_BANDWIDTH {
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForBandwidth() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForBandwidth(v)
	} else {
		v := obj.account.AcquiredDelegatedFrozenV2BalanceForEnergy() - amount
		if v < 0 {
			v = 0
		}
		obj.account.SetAcquiredDelegatedFrozenV2BalanceForEnergy(v)
	}
	obj.markDirty()
}

// ClearUnfrozenV2 removes all pending unfreeze entries.
func (s *StateDB) ClearUnfrozenV2(addr tcommon.Address) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.ClearUnfrozenV2()
	obj.markDirty()
}
```

- [ ] **Step 4: Run tests**

Run: `go build ./... && go test ./core/state/ -v`

- [ ] **Step 5: Commit**

```bash
git add core/state/dynamic_properties.go core/types/account.go core/state/statedb.go
git commit -m "feat(state): add account name/id/permission/delegation accessors and next_proposal_id"
```

---

### Task 4: Account Management Actuators (AccountUpdate, SetAccountId, WitnessUpdate)

**Files:**
- Create: `actuator/account_update.go`
- Create: `actuator/account_update_test.go`
- Create: `actuator/set_account_id.go`
- Create: `actuator/set_account_id_test.go`
- Create: `actuator/witness_update.go`
- Create: `actuator/witness_update_test.go`
- Modify: `actuator/actuator.go` (register types 8, 10, 19)

- [ ] **Step 1: Create AccountUpdateActuator**

Create `actuator/account_update.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type AccountUpdateActuator struct{}

func (a *AccountUpdateActuator) getContract(ctx *Context) (*contractpb.AccountUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AccountUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AccountUpdateContract")
	}
	return c, nil
}

func (a *AccountUpdateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if len(c.AccountName) == 0 {
		return errors.New("account name is empty")
	}
	if len(c.AccountName) > 32 {
		return errors.New("account name too long (max 32 bytes)")
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetAccountName(ownerAddr) != "" {
		return errors.New("account name already set")
	}
	return nil
}

func (a *AccountUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.SetAccountName(ownerAddr, string(c.AccountName))
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 2: Create AccountUpdate tests**

Create `actuator/account_update_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestAccountUpdateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte("myaccount"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	act := &AccountUpdateActuator{}

	// Owner doesn't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	// Set name, then try again
	ctx.State.SetAccountName(owner, "existing")
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already-set name")
	}
}

func TestAccountUpdateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte("alice"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1, got %d", result.ContractRet)
	}
	if ctx.State.GetAccountName(owner) != "alice" {
		t.Fatalf("name not set")
	}
}

func TestAccountUpdateEmptyName(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountUpdateContract{
		OwnerAddress: owner[:],
		AccountName:  []byte{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for empty name")
	}
}
```

- [ ] **Step 3: Create SetAccountIdActuator**

Create `actuator/set_account_id.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type SetAccountIdActuator struct{}

func (a *SetAccountIdActuator) getContract(ctx *Context) (*contractpb.SetAccountIdContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.SetAccountIdContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal SetAccountIdContract")
	}
	return c, nil
}

func (a *SetAccountIdActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	if len(c.AccountId) == 0 {
		return errors.New("account id is empty")
	}
	if len(c.AccountId) > 32 {
		return errors.New("account id too long (max 32 bytes)")
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetAccountId(ownerAddr) != "" {
		return errors.New("account id already set")
	}
	return nil
}

func (a *SetAccountIdActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.SetAccountId(ownerAddr, string(c.AccountId))
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Create SetAccountId tests**

Create `actuator/set_account_id_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestSetAccountIdValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("myid"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	act := &SetAccountIdActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}

	ctx.State.SetAccountId(owner, "existing")
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for already-set id")
	}
}

func TestSetAccountIdExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.SetAccountIdContract{
		OwnerAddress: owner[:],
		AccountId:    []byte("user123"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_SetAccountIdContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &SetAccountIdActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if ctx.State.GetAccountId(owner) != "user123" {
		t.Fatal("id not set")
	}
}
```

- [ ] **Step 5: Create WitnessUpdateActuator**

Create `actuator/witness_update.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type WitnessUpdateActuator struct{}

func (a *WitnessUpdateActuator) getContract(ctx *Context) (*contractpb.WitnessUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.WitnessUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal WitnessUpdateContract")
	}
	return c, nil
}

func (a *WitnessUpdateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetWitness(ownerAddr) == nil {
		return errors.New("owner is not a witness")
	}
	if len(c.UpdateUrl) == 0 {
		return errors.New("witness URL is empty")
	}
	if len(c.UpdateUrl) > 256 {
		return errors.New("witness URL too long (max 256 bytes)")
	}
	return nil
}

func (a *WitnessUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.PutWitness(ownerAddr, string(c.UpdateUrl))
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 6: Create WitnessUpdate tests**

Create `actuator/witness_update_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestWitnessUpdateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    []byte("http://new-url.com"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
	act := &WitnessUpdateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness")
	}

	ctx.State.PutWitness(owner, "http://old-url.com")
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestWitnessUpdateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    []byte("http://updated.com"),
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://old.com")

	act := &WitnessUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	w := ctx.State.GetWitness(owner)
	if w.URL() != "http://updated.com" {
		t.Fatalf("URL not updated: %s", w.URL())
	}
}

func TestWitnessUpdateEmptyURL(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.WitnessUpdateContract{
		OwnerAddress: owner[:],
		UpdateUrl:    []byte{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_WitnessUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://old.com")

	act := &WitnessUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for empty URL")
	}
}
```

- [ ] **Step 7: Register types 8, 10, 19 in CreateActuator**

In `actuator/actuator.go`, add these cases to the `CreateActuator` switch:

```go
	case corepb.Transaction_Contract_WitnessUpdateContract:
		return &WitnessUpdateActuator{}, nil
	case corepb.Transaction_Contract_AccountUpdateContract:
		return &AccountUpdateActuator{}, nil
	case corepb.Transaction_Contract_SetAccountIdContract:
		return &SetAccountIdActuator{}, nil
```

- [ ] **Step 8: Run tests**

Run: `go test ./actuator/ -run "TestAccountUpdate|TestSetAccountId|TestWitnessUpdate" -v`

- [ ] **Step 9: Commit**

```bash
git add actuator/account_update.go actuator/account_update_test.go actuator/set_account_id.go actuator/set_account_id_test.go actuator/witness_update.go actuator/witness_update_test.go actuator/actuator.go
git commit -m "feat(actuator): add AccountUpdate, SetAccountId, WitnessUpdate actuators"
```

---

### Task 5: UpdateBrokerage & AccountPermissionUpdate Actuators

**Files:**
- Create: `actuator/update_brokerage.go`
- Create: `actuator/update_brokerage_test.go`
- Create: `actuator/account_permission.go`
- Create: `actuator/account_permission_test.go`
- Modify: `actuator/actuator.go` (register types 46, 49)

- [ ] **Step 1: Create UpdateBrokerageActuator**

Create `actuator/update_brokerage.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UpdateBrokerageActuator struct {
	DB interface {
		Put(key []byte, value []byte) error
	}
}

func (a *UpdateBrokerageActuator) getContract(ctx *Context) (*contractpb.UpdateBrokerageContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateBrokerageContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateBrokerageContract")
	}
	return c, nil
}

func (a *UpdateBrokerageActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetWitness(ownerAddr) == nil {
		return errors.New("owner is not a witness")
	}
	if c.Brokerage < 0 || c.Brokerage > 100 {
		return errors.New("brokerage must be 0-100")
	}
	return nil
}

func (a *UpdateBrokerageActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if a.DB != nil {
		rawdb.WriteWitnessBrokerage(a.DB, ownerAddr, int64(c.Brokerage))
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 2: Create UpdateBrokerage tests**

Create `actuator/update_brokerage_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUpdateBrokerageValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    30,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	act := &UpdateBrokerageActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness")
	}

	ctx.State.PutWitness(owner, "http://w.com")
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUpdateBrokerageOutOfRange(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    101,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")

	act := &UpdateBrokerageActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for brokerage > 100")
	}
}

func TestUpdateBrokerageExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.UpdateBrokerageContract{
		OwnerAddress: owner[:],
		Brokerage:    50,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UpdateBrokerageContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")

	db := ethrawdb.NewMemoryDatabase()
	act := &UpdateBrokerageActuator{DB: db}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	if got := rawdb.ReadWitnessBrokerage(db, owner); got != 50 {
		t.Fatalf("expected brokerage 50, got %d", got)
	}
}
```

- [ ] **Step 3: Create AccountPermissionUpdateActuator**

Create `actuator/account_permission.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const maxPermissionKeys = 5

type AccountPermissionUpdateActuator struct{}

func (a *AccountPermissionUpdateActuator) getContract(ctx *Context) (*contractpb.AccountPermissionUpdateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AccountPermissionUpdateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AccountPermissionUpdateContract")
	}
	return c, nil
}

func (a *AccountPermissionUpdateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if c.Owner == nil {
		return errors.New("owner permission is required")
	}
	if err := validatePermission(c.Owner); err != nil {
		return err
	}
	if c.Witness != nil {
		if ctx.State.GetWitness(ownerAddr) == nil {
			return errors.New("witness permission requires witness account")
		}
		if err := validatePermission(c.Witness); err != nil {
			return err
		}
	}
	totalKeys := len(c.Owner.Keys)
	if c.Witness != nil {
		totalKeys += len(c.Witness.Keys)
	}
	for _, active := range c.Actives {
		if err := validatePermission(active); err != nil {
			return err
		}
		totalKeys += len(active.Keys)
	}
	if totalKeys > maxPermissionKeys {
		return errors.New("too many keys across all permissions (max 5)")
	}
	return nil
}

func validatePermission(p *corepb.Permission) error {
	if len(p.Keys) == 0 {
		return errors.New("permission must have at least 1 key")
	}
	if p.Threshold <= 0 {
		return errors.New("permission threshold must be positive")
	}
	var totalWeight int64
	for _, k := range p.Keys {
		totalWeight += k.Weight
	}
	if p.Threshold > totalWeight {
		return errors.New("permission threshold exceeds total key weight")
	}
	return nil
}

func (a *AccountPermissionUpdateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	ctx.State.SetPermissions(ownerAddr, c.Owner, c.Witness, c.Actives)
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Create AccountPermissionUpdate tests**

Create `actuator/account_permission_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestAccountPermissionValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Threshold: 1,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
			},
		},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	act := &AccountPermissionUpdateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestAccountPermissionNoOwner(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing owner permission")
	}
}

func TestAccountPermissionThresholdExceedsWeight(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Threshold: 10,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
			},
		},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for threshold > weight")
	}
}

func TestAccountPermissionExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	key2 := tcommon.Address{0x41, 0x02}
	c := &contractpb.AccountPermissionUpdateContract{
		OwnerAddress: owner[:],
		Owner: &corepb.Permission{
			Type:      corepb.Permission_Owner,
			Threshold: 2,
			Keys: []*corepb.Key{
				{Address: owner[:], Weight: 1},
				{Address: key2[:], Weight: 1},
			},
		},
		Actives: []*corepb.Permission{
			{
				Type:      corepb.Permission_Active,
				Id:        2,
				Threshold: 1,
				Keys: []*corepb.Key{
					{Address: owner[:], Weight: 1},
				},
			},
		},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_AccountPermissionUpdateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &AccountPermissionUpdateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}
	acc := ctx.State.GetAccount(owner)
	if acc.OwnerPermission() == nil {
		t.Fatal("owner permission not set")
	}
	if acc.OwnerPermission().Threshold != 2 {
		t.Fatalf("expected threshold 2, got %d", acc.OwnerPermission().Threshold)
	}
}
```

- [ ] **Step 5: Register types 46, 49 in CreateActuator**

In `actuator/actuator.go`, add to the switch:

```go
	case corepb.Transaction_Contract_AccountPermissionUpdateContract:
		return &AccountPermissionUpdateActuator{}, nil
	case corepb.Transaction_Contract_UpdateBrokerageContract:
		return &UpdateBrokerageActuator{}, nil
```

- [ ] **Step 6: Run tests**

Run: `go test ./actuator/ -run "TestUpdateBrokerage|TestAccountPermission" -v`

- [ ] **Step 7: Commit**

```bash
git add actuator/update_brokerage.go actuator/update_brokerage_test.go actuator/account_permission.go actuator/account_permission_test.go actuator/actuator.go
git commit -m "feat(actuator): add UpdateBrokerage and AccountPermissionUpdate actuators"
```

---

### Task 6: Governance Actuators (ProposalCreate, ProposalApprove, ProposalDelete)

**Files:**
- Create: `actuator/proposal_create.go`
- Create: `actuator/proposal_create_test.go`
- Create: `actuator/proposal_approve.go`
- Create: `actuator/proposal_approve_test.go`
- Create: `actuator/proposal_delete.go`
- Create: `actuator/proposal_delete_test.go`
- Modify: `actuator/actuator.go` (register types 16, 17, 18)
- Modify: `actuator/actuator.go` (add DB field to Context)

The governance actuators need access to rawdb for proposal storage. Add a `DB` field to the `Context` struct.

- [ ] **Step 1: Add DB to actuator.Context**

In `actuator/actuator.go`, add an `ethdb` import and a `DB` field:

```go
import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/vm"
)

type Context struct {
	State          *state.StateDB
	DynProps       *state.DynamicProperties
	Tx             *types.Transaction
	BlockTime      int64
	BlockNumber    uint64
	DB             ethdb.Database // rawdb access for governance/brokerage
	ActiveWitnesses []common.Address // active witness set for governance checks
}
```

Also add the `common` import since `ActiveWitnesses` uses it:

```go
	"github.com/tronprotocol/go-tron/common"
```

- [ ] **Step 2: Create ProposalCreateActuator**

Create `actuator/proposal_create.go`:

```go
package actuator

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const proposalExpirationMs = 259200000 // 3 days in ms

type ProposalCreateActuator struct{}

func (a *ProposalCreateActuator) getContract(ctx *Context) (*contractpb.ProposalCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ProposalCreateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ProposalCreateContract")
	}
	return c, nil
}

func (a *ProposalCreateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !isActiveWitness(ownerAddr, ctx.ActiveWitnesses) {
		return errors.New("owner is not an active witness")
	}
	if len(c.Parameters) == 0 {
		return errors.New("proposal parameters are empty")
	}
	for k := range c.Parameters {
		if _, ok := ctx.DynProps.Get(fmt.Sprintf("param_%d", k)); !ok {
			// Accept any parameter key that exists in dynProps by its raw name.
			// For simplicity, we accept all int64 keys.
		}
	}
	return nil
}

func (a *ProposalCreateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)

	proposalID := ctx.DynProps.NextProposalID()
	ctx.DynProps.SetNextProposalID(proposalID + 1)

	proposal := &rawdb.Proposal{
		ID:             proposalID,
		Proposer:       ownerAddr,
		Parameters:     c.Parameters,
		CreateTime:     ctx.BlockTime,
		ExpirationTime: ctx.BlockTime + proposalExpirationMs,
		State:          rawdb.ProposalStatePending,
	}

	if ctx.DB != nil {
		if err := rawdb.WriteProposal(ctx.DB, proposalID, proposal); err != nil {
			return nil, err
		}
		index := rawdb.ReadProposalIndex(ctx.DB)
		index = append(index, proposalID)
		if err := rawdb.WriteProposalIndex(ctx.DB, index); err != nil {
			return nil, err
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}

func isActiveWitness(addr common.Address, actives []common.Address) bool {
	for _, a := range actives {
		if a == addr {
			return true
		}
	}
	return false
}
```

- [ ] **Step 3: Create ProposalCreate tests**

Create `actuator/proposal_create_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestProposalCreateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.ActiveWitnesses = []tcommon.Address{owner}
	act := &ProposalCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalCreateNotWitness(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = nil // no active witnesses

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-active witness")
	}
}

func TestProposalCreateEmptyParams(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for empty parameters")
	}
}

func TestProposalCreateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db

	act := &ProposalCreateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	p := rawdb.ReadProposal(db, 0)
	if p == nil {
		t.Fatal("proposal not stored")
	}
	if p.Proposer != owner || p.State != rawdb.ProposalStatePending {
		t.Fatalf("unexpected proposal: %+v", p)
	}
	if ctx.DynProps.NextProposalID() != 1 {
		t.Fatalf("next_proposal_id not incremented")
	}
}
```

- [ ] **Step 4: Create ProposalApproveActuator**

Create `actuator/proposal_approve.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type ProposalApproveActuator struct{}

func (a *ProposalApproveActuator) getContract(ctx *Context) (*contractpb.ProposalApproveContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ProposalApproveContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ProposalApproveContract")
	}
	return c, nil
}

func (a *ProposalApproveActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !isActiveWitness(ownerAddr, ctx.ActiveWitnesses) {
		return errors.New("owner is not an active witness")
	}
	if ctx.DB == nil {
		return errors.New("database not available")
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	if proposal == nil {
		return errors.New("proposal not found")
	}
	if proposal.State != rawdb.ProposalStatePending {
		return errors.New("proposal is not pending")
	}
	if proposal.ExpirationTime <= ctx.BlockTime {
		return errors.New("proposal has expired")
	}
	hasApproved := containsAddress(proposal.Approvals, ownerAddr)
	if c.IsAddApproval && hasApproved {
		return errors.New("already approved")
	}
	if !c.IsAddApproval && !hasApproved {
		return errors.New("not yet approved, cannot revoke")
	}
	return nil
}

func (a *ProposalApproveActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)

	if c.IsAddApproval {
		proposal.Approvals = append(proposal.Approvals, ownerAddr)
	} else {
		proposal.Approvals = removeAddress(proposal.Approvals, ownerAddr)
	}

	if err := rawdb.WriteProposal(ctx.DB, c.ProposalId, proposal); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}

func containsAddress(addrs []common.Address, target common.Address) bool {
	for _, a := range addrs {
		if a == target {
			return true
		}
	}
	return false
}

func removeAddress(addrs []common.Address, target common.Address) []common.Address {
	result := addrs[:0]
	for _, a := range addrs {
		if a != target {
			result = append(result, a)
		}
	}
	return result
}
```

- [ ] **Step 5: Create ProposalApprove tests**

Create `actuator/proposal_approve_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func setupProposalForApprove(t *testing.T, db *ethrawdb.MemoryDatabase, proposer tcommon.Address) {
	t.Helper()
	p := &rawdb.Proposal{
		ID:             0,
		Proposer:       proposer,
		Parameters:     map[int64]int64{6: 200},
		CreateTime:     500,
		ExpirationTime: 500 + 259200000,
		State:          rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})
}

func TestProposalApproveValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    0,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	setupProposalForApprove(t, db.(*ethrawdb.MemoryDatabase), owner)

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalApproveExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    0,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	proposer := tcommon.Address{0x41, 0x02}
	setupProposalForApprove(t, db.(*ethrawdb.MemoryDatabase), proposer)

	act := &ProposalApproveActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	p := rawdb.ReadProposal(db, 0)
	if len(p.Approvals) != 1 || p.Approvals[0] != owner {
		t.Fatalf("approval not recorded: %+v", p.Approvals)
	}
}

func TestProposalApproveDoubleApprove(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    0,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{
		ID: 0, ExpirationTime: 999999999, State: rawdb.ProposalStatePending,
		Approvals: []tcommon.Address{owner},
	}
	rawdb.WriteProposal(db, 0, p)

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for double approve")
	}
}
```

- [ ] **Step 6: Create ProposalDeleteActuator**

Create `actuator/proposal_delete.go`:

```go
package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type ProposalDeleteActuator struct{}

func (a *ProposalDeleteActuator) getContract(ctx *Context) (*contractpb.ProposalDeleteContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ProposalDeleteContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ProposalDeleteContract")
	}
	return c, nil
}

func (a *ProposalDeleteActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.DB == nil {
		return errors.New("database not available")
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	if proposal == nil {
		return errors.New("proposal not found")
	}
	if proposal.State != rawdb.ProposalStatePending {
		return errors.New("proposal is not pending")
	}
	if proposal.Proposer != ownerAddr {
		return errors.New("only the proposer can delete the proposal")
	}
	return nil
}

func (a *ProposalDeleteActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	proposal.State = rawdb.ProposalStateCanceled
	if err := rawdb.WriteProposal(ctx.DB, c.ProposalId, proposal); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 7: Create ProposalDelete tests**

Create `actuator/proposal_delete_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestProposalDeleteValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   0,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{ID: 0, Proposer: owner, State: rawdb.ProposalStatePending}
	rawdb.WriteProposal(db, 0, p)

	act := &ProposalDeleteActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalDeleteNotProposer(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	other := tcommon.Address{0x41, 0x02}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   0,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{ID: 0, Proposer: other, State: rawdb.ProposalStatePending}
	rawdb.WriteProposal(db, 0, p)

	act := &ProposalDeleteActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-proposer")
	}
}

func TestProposalDeleteExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   0,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{ID: 0, Proposer: owner, State: rawdb.ProposalStatePending}
	rawdb.WriteProposal(db, 0, p)

	act := &ProposalDeleteActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStateCanceled {
		t.Fatalf("expected CANCELED, got %d", got.State)
	}
}
```

- [ ] **Step 8: Register types 16, 17, 18 in CreateActuator**

In `actuator/actuator.go`, add to the switch:

```go
	case corepb.Transaction_Contract_ProposalCreateContract:
		return &ProposalCreateActuator{}, nil
	case corepb.Transaction_Contract_ProposalApproveContract:
		return &ProposalApproveActuator{}, nil
	case corepb.Transaction_Contract_ProposalDeleteContract:
		return &ProposalDeleteActuator{}, nil
```

- [ ] **Step 9: Run tests**

Run: `go test ./actuator/ -run "TestProposal" -v`

- [ ] **Step 10: Commit**

```bash
git add actuator/proposal_create.go actuator/proposal_create_test.go actuator/proposal_approve.go actuator/proposal_approve_test.go actuator/proposal_delete.go actuator/proposal_delete_test.go actuator/actuator.go
git commit -m "feat(actuator): add ProposalCreate, ProposalApprove, ProposalDelete actuators"
```

---

### Task 7: Delegation Actuators (DelegateResource, UnDelegateResource, CancelAllUnfreezeV2)

**Files:**
- Create: `actuator/delegate_resource.go`
- Create: `actuator/delegate_resource_test.go`
- Create: `actuator/undelegate_resource.go`
- Create: `actuator/undelegate_resource_test.go`
- Create: `actuator/cancel_unfreeze.go`
- Create: `actuator/cancel_unfreeze_test.go`
- Modify: `actuator/actuator.go` (register types 57, 58, 59)

- [ ] **Step 1: Create DelegateResourceActuator**

Create `actuator/delegate_resource.go`:

```go
package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const maxDelegationLockPeriodMs = 259200000 // 3 days

type DelegateResourceActuator struct{}

func (a *DelegateResourceActuator) getContract(ctx *Context) (*contractpb.DelegateResourceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.DelegateResourceContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal DelegateResourceContract")
	}
	return c, nil
}

func (a *DelegateResourceActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)
	if ownerAddr == receiverAddr {
		return errors.New("cannot delegate to self")
	}
	if c.Balance <= 0 {
		return errors.New("delegation balance must be positive")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !ctx.State.AccountExists(receiverAddr) {
		return errors.New("receiver account does not exist")
	}
	if c.Resource != corepb.ResourceCode_BANDWIDTH && c.Resource != corepb.ResourceCode_ENERGY {
		return errors.New("invalid resource type")
	}
	frozen := ctx.State.GetFrozenV2Amount(ownerAddr, c.Resource)
	if frozen < c.Balance {
		return errors.New("insufficient frozen balance to delegate")
	}
	return nil
}

func (a *DelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)

	// Subtract from owner's frozen balance
	ctx.State.ReduceFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Track outgoing delegation on owner
	ctx.State.AddDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Track incoming delegation on receiver
	ctx.State.AddAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	// Update delegation record in rawdb
	if ctx.DB != nil {
		dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
		if dr == nil {
			dr = &rawdb.DelegatedResource{From: ownerAddr, To: receiverAddr}
		}
		if c.Resource == corepb.ResourceCode_BANDWIDTH {
			dr.FrozenBalanceForBandwidth += c.Balance
			if c.Lock {
				dr.ExpireTimeForBandwidth = ctx.BlockTime + c.LockPeriod
			}
		} else {
			dr.FrozenBalanceForEnergy += c.Balance
			if c.Lock {
				dr.ExpireTimeForEnergy = ctx.BlockTime + c.LockPeriod
			}
		}
		rawdb.WriteDelegatedResource(ctx.DB, ownerAddr, receiverAddr, dr)

		// Update delegation index
		receivers := rawdb.ReadDelegationIndex(ctx.DB, ownerAddr)
		if !containsAddress(receivers, receiverAddr) {
			receivers = append(receivers, receiverAddr)
			rawdb.WriteDelegationIndex(ctx.DB, ownerAddr, receivers)
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 2: Create DelegateResource tests**

Create `actuator/delegate_resource_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDelegateResourceValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	act := &DelegateResourceActuator{}

	// Accounts don't exist
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestDelegateResourceSelfDelegation(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: owner[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	act := &DelegateResourceActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for self-delegation")
	}
}

func TestDelegateResourceExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.DelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_DelegateResourceContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 5000000)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db

	act := &DelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Owner's frozen reduced
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 4000000 {
		t.Fatalf("owner frozen not reduced")
	}

	// Delegation record
	dr := rawdb.ReadDelegatedResource(db, owner, receiver)
	if dr == nil || dr.FrozenBalanceForBandwidth != 1000000 {
		t.Fatalf("delegation record wrong: %+v", dr)
	}
}
```

- [ ] **Step 3: Create UnDelegateResourceActuator**

Create `actuator/undelegate_resource.go`:

```go
package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type UnDelegateResourceActuator struct{}

func (a *UnDelegateResourceActuator) getContract(ctx *Context) (*contractpb.UnDelegateResourceContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UnDelegateResourceContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UnDelegateResourceContract")
	}
	return c, nil
}

func (a *UnDelegateResourceActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if c.Balance <= 0 {
		return errors.New("undelegate balance must be positive")
	}
	if ctx.DB == nil {
		return errors.New("database not available")
	}
	dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
	if dr == nil {
		return errors.New("no delegation record found")
	}
	if c.Resource == corepb.ResourceCode_BANDWIDTH {
		if dr.FrozenBalanceForBandwidth < c.Balance {
			return errors.New("insufficient delegated bandwidth balance")
		}
		if dr.ExpireTimeForBandwidth > ctx.BlockTime {
			return errors.New("delegation is still locked")
		}
	} else {
		if dr.FrozenBalanceForEnergy < c.Balance {
			return errors.New("insufficient delegated energy balance")
		}
		if dr.ExpireTimeForEnergy > ctx.BlockTime {
			return errors.New("delegation is still locked")
		}
	}
	return nil
}

func (a *UnDelegateResourceActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	receiverAddr := tcommon.BytesToAddress(c.ReceiverAddress)

	// Return to owner's frozen balance
	ctx.State.AddFreezeV2(ownerAddr, c.Resource, c.Balance)
	// Reduce outgoing delegation on owner
	ctx.State.SubDelegatedFrozenV2(ownerAddr, c.Resource, c.Balance)
	// Reduce incoming delegation on receiver
	ctx.State.SubAcquiredDelegatedFrozenV2(receiverAddr, c.Resource, c.Balance)

	// Update delegation record
	dr := rawdb.ReadDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
	if c.Resource == corepb.ResourceCode_BANDWIDTH {
		dr.FrozenBalanceForBandwidth -= c.Balance
	} else {
		dr.FrozenBalanceForEnergy -= c.Balance
	}

	if dr.FrozenBalanceForBandwidth <= 0 && dr.FrozenBalanceForEnergy <= 0 {
		rawdb.DeleteDelegatedResource(ctx.DB, ownerAddr, receiverAddr)
		// Remove from delegation index
		receivers := rawdb.ReadDelegationIndex(ctx.DB, ownerAddr)
		receivers = removeAddress(receivers, receiverAddr)
		rawdb.WriteDelegationIndex(ctx.DB, ownerAddr, receivers)
	} else {
		rawdb.WriteDelegatedResource(ctx.DB, ownerAddr, receiverAddr, dr)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 4: Create UnDelegateResource tests**

Create `actuator/undelegate_resource_test.go`:

```go
package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestUnDelegateResourceValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	rawdb.WriteDelegatedResource(db, owner, receiver, dr)

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestUnDelegateResourceLocked(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         500000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.BlockTime = 1000
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
		ExpireTimeForBandwidth:   999999, // locked until far future
	}
	rawdb.WriteDelegatedResource(db, owner, receiver, dr)

	act := &UnDelegateResourceActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for locked delegation")
	}
}

func TestUnDelegateResourceExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	receiver := tcommon.Address{0x41, 0x02}
	c := &contractpb.UnDelegateResourceContract{
		OwnerAddress:    owner[:],
		ReceiverAddress: receiver[:],
		Resource:        corepb.ResourceCode_BANDWIDTH,
		Balance:         1000000,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_UnDelegateResourceContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.CreateAccount(receiver, corepb.AccountType_Normal)

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	dr := &rawdb.DelegatedResource{
		From: owner, To: receiver,
		FrozenBalanceForBandwidth: 1000000,
	}
	rawdb.WriteDelegatedResource(db, owner, receiver, dr)
	rawdb.WriteDelegationIndex(db, owner, []tcommon.Address{receiver})

	act := &UnDelegateResourceActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Delegation fully removed
	if rawdb.ReadDelegatedResource(db, owner, receiver) != nil {
		t.Fatal("delegation should be removed")
	}
	// Owner's frozen restored
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 1000000 {
		t.Fatal("frozen balance not restored")
	}
}
```

- [ ] **Step 5: Create CancelAllUnfreezeV2Actuator**

Create `actuator/cancel_unfreeze.go`:

```go
package actuator

import (
	"errors"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type CancelAllUnfreezeV2Actuator struct{}

func (a *CancelAllUnfreezeV2Actuator) getContract(ctx *Context) (*contractpb.CancelAllUnfreezeV2Contract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.CancelAllUnfreezeV2Contract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal CancelAllUnfreezeV2Contract")
	}
	return c, nil
}

func (a *CancelAllUnfreezeV2Actuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.UnfreezeV2Count(ownerAddr) == 0 {
		return errors.New("no pending unfreeze entries")
	}
	return nil
}

func (a *CancelAllUnfreezeV2Actuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := tcommon.BytesToAddress(c.OwnerAddress)
	acc := ctx.State.GetAccount(ownerAddr)

	// Sum pending unfreezes by resource type
	var bwTotal, energyTotal int64
	for _, u := range acc.UnfrozenV2() {
		if u.Type == corepb.ResourceCode_BANDWIDTH {
			bwTotal += u.UnfreezeAmount
		} else {
			energyTotal += u.UnfreezeAmount
		}
	}

	// Re-freeze
	if bwTotal > 0 {
		ctx.State.AddFreezeV2(ownerAddr, corepb.ResourceCode_BANDWIDTH, bwTotal)
	}
	if energyTotal > 0 {
		ctx.State.AddFreezeV2(ownerAddr, corepb.ResourceCode_ENERGY, energyTotal)
	}

	// Clear unfreeze queue
	ctx.State.ClearUnfrozenV2(ownerAddr)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
```

- [ ] **Step 6: Create CancelAllUnfreezeV2 tests**

Create `actuator/cancel_unfreeze_test.go`:

```go
package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestCancelAllUnfreezeV2Validate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.CancelAllUnfreezeV2Contract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
	act := &CancelAllUnfreezeV2Actuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for no pending unfreezes")
	}

	ctx.State.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1000000, 999999)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestCancelAllUnfreezeV2Execute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.CancelAllUnfreezeV2Contract{
		OwnerAddress: owner[:],
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_CancelAllUnfreezeV2Contract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 0) // ensure entry exists
	ctx.State.AddUnfreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 1000000, 999999)
	ctx.State.AddUnfreezeV2(owner, corepb.ResourceCode_ENERGY, 500000, 999999)

	act := &CancelAllUnfreezeV2Actuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	if ctx.State.UnfreezeV2Count(owner) != 0 {
		t.Fatal("unfreeze queue not cleared")
	}
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH) != 1000000 {
		t.Fatalf("bandwidth not re-frozen: %d", ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_BANDWIDTH))
	}
	if ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY) != 500000 {
		t.Fatalf("energy not re-frozen: %d", ctx.State.GetFrozenV2Amount(owner, corepb.ResourceCode_ENERGY))
	}
}
```

- [ ] **Step 7: Register types 57, 58, 59 in CreateActuator**

In `actuator/actuator.go`, add:

```go
	case corepb.Transaction_Contract_DelegateResourceContract:
		return &DelegateResourceActuator{}, nil
	case corepb.Transaction_Contract_UnDelegateResourceContract:
		return &UnDelegateResourceActuator{}, nil
	case corepb.Transaction_Contract_CancelAllUnfreezeV2Contract:
		return &CancelAllUnfreezeV2Actuator{}, nil
```

- [ ] **Step 8: Run tests**

Run: `go test ./actuator/ -run "TestDelegateResource|TestUnDelegateResource|TestCancelAllUnfreezeV2" -v`

- [ ] **Step 9: Commit**

```bash
git add actuator/delegate_resource.go actuator/delegate_resource_test.go actuator/undelegate_resource.go actuator/undelegate_resource_test.go actuator/cancel_unfreeze.go actuator/cancel_unfreeze_test.go actuator/actuator.go
git commit -m "feat(actuator): add DelegateResource, UnDelegateResource, CancelAllUnfreezeV2 actuators"
```

---

### Task 8: Proposal Finalization in Maintenance

**Files:**
- Create: `core/proposal.go`
- Create: `core/proposal_test.go`
- Modify: `core/block_builder.go`
- Modify: `core/state_processor.go` (pass DB and ActiveWitnesses to actuator Context)

- [ ] **Step 1: Create proposal processing function**

Create `core/proposal.go`:

```go
package core

import (
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// ProcessProposals checks all pending proposals and approves or cancels them
// based on the approval count vs active SR count.
func ProcessProposals(db ethdb.Database, dynProps *state.DynamicProperties, activeCount int, maintenanceTime int64) {
	ids := rawdb.ReadProposalIndex(db)
	for _, id := range ids {
		p := rawdb.ReadProposal(db, id)
		if p == nil || p.State != rawdb.ProposalStatePending {
			continue
		}
		if p.ExpirationTime > maintenanceTime {
			continue // not yet expired
		}

		approvalCount := len(p.Approvals)
		// 70% threshold: approvals * 10 >= activeCount * 7
		if approvalCount*10 >= activeCount*7 {
			// Apply parameters
			for _, v := range sortedKeys(p.Parameters) {
				dynProps.Set(paramIDToName(v), p.Parameters[v])
			}
			p.State = rawdb.ProposalStateApproved
		} else {
			p.State = rawdb.ProposalStateCanceled
		}
		rawdb.WriteProposal(db, id, p)
	}
}

// paramIDToName maps a TRON proposal parameter ID to its DynProps key name.
// This is a simplified mapping; java-tron has a full enum.
func paramIDToName(id int64) string {
	mapping := map[int64]string{
		0:  "maintenance_time_interval",
		1:  "account_upgrade_cost",
		2:  "create_account_fee",
		3:  "transaction_fee",
		4:  "asset_issue_fee",
		5:  "witness_pay_per_block",
		6:  "witness_standby_allowance",
		9:  "create_new_account_fee_in_system_contract",
		10: "create_new_account_bandwidth_rate",
		11: "energy_fee",
		15: "max_cpu_time_of_one_tx",
		19: "total_energy_current_limit",
		22: "total_net_limit",
		27: "unfreeze_delay_days",
		65: "free_net_limit",
	}
	if name, ok := mapping[id]; ok {
		return name
	}
	return ""
}

func sortedKeys(m map[int64]int64) []int64 {
	keys := make([]int64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort for small maps
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}

// ActiveWitnessCount is a helper so callers don't need to know the type.
func ActiveWitnessCount(witnesses []tcommon.Address) int {
	return len(witnesses)
}
```

- [ ] **Step 2: Create proposal processing test**

Create `core/proposal_test.go`:

```go
package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestProcessProposals_Approved(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	dynProps := state.NewDynamicProperties()

	// Create proposal to change witness_pay_per_block (ID 5) to 32000000
	p := &rawdb.Proposal{
		ID:             0,
		Proposer:       tcommon.Address{0x41, 0x01},
		Parameters:     map[int64]int64{5: 32000000},
		CreateTime:     1000,
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})

	// 3 approvals out of 4 SRs = 75% >= 70%
	ProcessProposals(db, dynProps, 4, 3000)

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStateApproved {
		t.Fatalf("expected APPROVED, got %d", got.State)
	}
	if dynProps.WitnessPayPerBlock() != 32000000 {
		t.Fatalf("parameter not applied: %d", dynProps.WitnessPayPerBlock())
	}
}

func TestProcessProposals_Canceled(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	dynProps := state.NewDynamicProperties()

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{5: 32000000},
		ExpirationTime: 2000,
		Approvals:      []tcommon.Address{{0x41, 0x01}}, // 1 of 4 = 25%
		State:          rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})

	ProcessProposals(db, dynProps, 4, 3000)

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStateCanceled {
		t.Fatalf("expected CANCELED, got %d", got.State)
	}
	// Parameter should NOT have changed
	if dynProps.WitnessPayPerBlock() != 16000000 {
		t.Fatalf("parameter should not change: %d", dynProps.WitnessPayPerBlock())
	}
}

func TestProcessProposals_NotExpired(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	dynProps := state.NewDynamicProperties()

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{5: 32000000},
		ExpirationTime: 9999999,
		Approvals:      []tcommon.Address{{0x41, 0x01}},
		State:          rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})

	ProcessProposals(db, dynProps, 1, 1000) // maintenance time < expiration

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStatePending {
		t.Fatalf("expected still PENDING, got %d", got.State)
	}
}
```

- [ ] **Step 3: Wire proposal processing into block_builder.go**

In `core/block_builder.go`, inside the maintenance block (after `dpos.DoMaintenance` and `bc.SetActiveWitnesses`), add:

```go
		ProcessProposals(bc.db, dynProps, len(newActive), timestamp)
```

- [ ] **Step 4: Pass DB and ActiveWitnesses to actuator Context in state_processor.go**

In `core/state_processor.go`, find where `actuator.Context` is created in `ApplyTransaction`. Add `DB` and `ActiveWitnesses` fields. This requires adding the DB and activeWitnesses parameters to `ApplyTransaction`.

Update the `ApplyTransaction` signature to accept these:

The current signature is likely:
```go
func ApplyTransaction(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64, blockNumber uint64) (*actuator.Result, error)
```

Add optional DB and activeWitnesses. To avoid breaking existing callers, add them as fields on the context after construction. The simplest approach: just set them when available. In `block_builder.go` and `blockchain.go` where `ApplyTransaction` is called, pass the bc.db and active witnesses.

Actually, let's keep it simple — add a `SetDB` / `SetActiveWitnesses` pattern or just pass them through. Let me check the actual current signature and callers.

The function constructs the `actuator.Context` internally. We need to make `DB` and `ActiveWitnesses` available. The cleanest way: add them as parameters.

- [ ] **Step 5: Run tests**

Run: `go test ./core/ -run "TestProcessProposal" -v`

- [ ] **Step 6: Commit**

```bash
git add core/proposal.go core/proposal_test.go core/block_builder.go core/state_processor.go
git commit -m "feat(core): add proposal finalization in maintenance cycle"
```

---

### Task 9: API Endpoints & Backend Methods

**Files:**
- Modify: `internal/tronapi/backend.go`
- Modify: `internal/tronapi/api.go`
- Modify: `internal/tronapi/api_test.go`
- Modify: `core/tron_backend.go`

- [ ] **Step 1: Add ProposalInfo type and Backend methods**

In `internal/tronapi/backend.go`, add:

```go
type ProposalInfo struct {
	ProposalID      int64            `json:"proposal_id"`
	ProposerAddress string           `json:"proposer_address"`
	Parameters      map[string]int64 `json:"parameters"`
	ExpirationTime  int64            `json:"expiration_time"`
	CreateTime      int64            `json:"create_time"`
	Approvals       []string         `json:"approvals"`
	State           string           `json:"state"`
}
```

Add 4 new methods to the `Backend` interface:

```go
	BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error)
	BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error)
	BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error)
	ListProposals() ([]*ProposalInfo, error)
```

- [ ] **Step 2: Add API handler methods and routes**

In `internal/tronapi/api.go`, add 4 new routes in `RegisterRoutes`:

```go
	mux.HandleFunc("/wallet/proposalcreate", api.proposalCreate)
	mux.HandleFunc("/wallet/proposalapprove", api.proposalApprove)
	mux.HandleFunc("/wallet/proposaldelete", api.proposalDelete)
	mux.HandleFunc("/wallet/listproposals", api.listProposals)
```

Add the handler methods (similar pattern to existing handlers like `createTransaction`).

- [ ] **Step 3: Implement Backend methods in tron_backend.go**

In `core/tron_backend.go`, implement all 4 methods using rawdb and tronapi.BuildTransaction.

- [ ] **Step 4: Update mockBackend in api_test.go**

Add stub implementations for the 4 new methods to the `mockBackend` in `internal/tronapi/api_test.go`.

- [ ] **Step 5: Run tests**

Run: `go build ./... && go test ./internal/tronapi/ -v && go test ./core/ -v`

- [ ] **Step 6: Commit**

```bash
git add internal/tronapi/backend.go internal/tronapi/api.go internal/tronapi/api_test.go core/tron_backend.go
git commit -m "feat(api): add proposal create/approve/delete and listproposals endpoints"
```

---

### Task 10: Integration — Wire DB & ActiveWitnesses Through ApplyTransaction

**Files:**
- Modify: `core/state_processor.go`
- Modify: `core/block_builder.go`
- Modify: `core/blockchain.go`
- Modify: `core/state_processor_test.go`

The governance and delegation actuators need `ctx.DB` and `ctx.ActiveWitnesses`. Wire these through the transaction processing path.

- [ ] **Step 1: Update ApplyTransaction to accept DB and ActiveWitnesses**

In `core/state_processor.go`, update the `ApplyTransaction` function signature and the `actuator.Context` construction to include `DB` and `ActiveWitnesses` parameters.

- [ ] **Step 2: Update callers**

In `core/block_builder.go`, pass `bc.db` and `bc.ActiveWitnesses()` to `ApplyTransaction`.
In `core/blockchain.go`, pass `bc.db` and `bc.ActiveWitnesses()` to `ApplyTransaction`.

- [ ] **Step 3: Update tests**

Update `core/state_processor_test.go` to pass nil for DB/ActiveWitnesses (existing tests don't need governance).

- [ ] **Step 4: Run full test suite**

Run: `go build ./... && go test ./... `

- [ ] **Step 5: Commit**

```bash
git add core/state_processor.go core/block_builder.go core/blockchain.go core/state_processor_test.go
git commit -m "feat(core): wire DB and ActiveWitnesses through ApplyTransaction for governance actuators"
```

---

### Task 11: Full Build & Test Verification

**Files:** None (verification only)

- [ ] **Step 1: Full build**

Run: `go build ./...`

- [ ] **Step 2: All unit tests**

Run: `go test ./...`

- [ ] **Step 3: System test**

Run: `bash scripts/system_test.sh`

- [ ] **Step 4: Fix any issues and commit**

Fix compilation errors, test failures, etc.
