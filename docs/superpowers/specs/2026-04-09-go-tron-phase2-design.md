# go-tron Phase 2: StateDB + Blockchain Core + Genesis

## Overview

Phase 2 transforms go-tron from a collection of types and accessors into a working blockchain engine. The core additions are:

1. **ethdb migration** — Replace `trondb.Database` with go-ethereum's `ethdb.Database` + Pebble backend
2. **StateDB with MPT** — In-memory account state with Merkle Patricia Trie, producing `accountStateRoot` per block
3. **Genesis** — Mainnet + Nile genesis configs embedded as Go structs, geth factory function pattern
4. **Resource model** — Bandwidth/energy consumption and recovery
5. **5 new actuators** — FreezeV2, UnfreezeV2, VoteWitness, WithdrawBalance, WithdrawExpireUnfreeze
6. **DynamicProperties** — Runtime-adjustable chain parameters
7. **Blockchain** — Block insertion, validation, chain management, solidified block tracking
8. **Consensus extensions** — Maintenance period, block rewards, header verification

No P2P networking in this phase.

---

## 1. ethdb Migration

### Problem

Phase 1 defined `trondb.Database` with custom interfaces. Phase 2 needs go-ethereum's `trie/` package for MPT, which operates on `ethdb.Database`. Maintaining two interface families creates unnecessary indirection.

### Solution

Replace `trondb.Database` entirely with `github.com/ethereum/go-ethereum/ethdb.Database`. Use `ethdb/pebble` directly for the on-disk backend and `ethdb/memorydb` for tests.

### Changes

**Delete:**
- `trondb/database.go`
- `trondb/memorydb/memorydb.go`
- `trondb/memorydb/memorydb_test.go`

**Update all consumers** to import `ethdb` instead of `trondb`:
- `core/rawdb/schema.go` — change function signatures from `trondb.KeyValueReader/Writer` to `ethdb.KeyValueReader/Writer`
- `core/rawdb/accessors_block.go`, `accessors_chain.go`, `accessors_account.go` — same
- `actuator/actuator.go` — `Context.DB` becomes `ethdb.KeyValueStore`
- `actuator/transfer.go`, `actuator/account.go`, `actuator/witness.go` — update imports

**Interface mapping** (`trondb` → `ethdb`):

| trondb | ethdb |
|--------|-------|
| `KeyValueReader` (Has, Get) | `ethdb.KeyValueReader` (identical) |
| `KeyValueWriter` (Put, Delete) | `ethdb.KeyValueWriter` (identical) |
| `KeyValueStore` | `ethdb.KeyValueStore` (superset — adds `KeyValueStater`, `KeyValueSyncer`, `KeyValueRangeDeleter`, `Batcher`, `Iteratee`, `Compacter`) |
| `Database` | `ethdb.Database` (= `KeyValueStore` + `AncientStore`) |
| `Batch` | `ethdb.Batch` |
| `Iterator` | `ethdb.Iterator` |
| `ErrNotFound` | Use `errors.Is(err, pebble.ErrNotFound)` or nil-check pattern |

**Note:** `ethdb.Database` includes `AncientStore` which we don't use yet. For Pebble-only usage, `ethdb/pebble.New()` returns an `ethdb.KeyValueStore`. We'll wrap it with a no-op ancient store via `rawdb.NewDatabase()` (from go-ethereum's `core/rawdb`) to satisfy the full `ethdb.Database` interface where needed, or use `ethdb.KeyValueStore` directly where ancient data isn't needed.

### DB Open Function

```go
// core/rawdb/database.go
package rawdb

import (
    "github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/ethdb/pebble"
)

func NewPebbleDB(path string, cache int, handles int) (ethdb.KeyValueStore, error) {
    return pebble.New(path, cache, handles, "", false)
}
```

---

## 2. StateDB with Merkle Patricia Trie

### Architecture

```
core/state/
├── statedb.go       # StateDB: in-memory state, snapshot/revert, Commit→MPT root
├── state_object.go  # stateObject: per-account in-memory wrapper
├── journal.go       # journal: undo log for snapshot/revert
└── database.go      # Database interface wrapping triedb
```

### StateDB

Central in-memory state manager. All actuators and block processing operate through StateDB, never touching raw DB directly.

```go
package state

import (
    "github.com/ethereum/go-ethereum/common"
    ethcommon "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/ethdb"
    "github.com/ethereum/go-ethereum/trie"
    "github.com/ethereum/go-ethereum/triedb"
    tcommon "github.com/tronprotocol/go-tron/common"
    "github.com/tronprotocol/go-tron/core/types"
)

type StateDB struct {
    db           Database              // trie database
    trie         Trie                  // account trie (MPT)
    stateRoot    tcommon.Hash          // current root

    stateObjects map[tcommon.Address]*stateObject
    dirtyObjects map[tcommon.Address]struct{}

    // Snapshot/revert
    journal      *journal
    snapshots    []int                 // journal length at each snapshot
    nextRevID    int

    // Dynamic properties (in-memory cache, persisted on Commit)
    dynProps     *DynamicProperties
}

func New(root tcommon.Hash, db Database) (*StateDB, error)
func (s *StateDB) GetAccount(addr tcommon.Address) *types.Account
func (s *StateDB) GetOrCreateAccount(addr tcommon.Address) *stateObject
func (s *StateDB) GetBalance(addr tcommon.Address) int64
func (s *StateDB) AddBalance(addr tcommon.Address, amount int64)
func (s *StateDB) SubBalance(addr tcommon.Address, amount int64) error
func (s *StateDB) GetFrozenV2(addr tcommon.Address) []*types.FreezeV2
func (s *StateDB) AddFreezeV2(addr tcommon.Address, resourceType int32, amount int64)
func (s *StateDB) GetBandwidthUsage(addr tcommon.Address) (used int64, lastTime int64)
func (s *StateDB) SetBandwidthUsage(addr tcommon.Address, used int64, lastTime int64)
func (s *StateDB) GetEnergyUsage(addr tcommon.Address) (used int64, lastTime int64)
func (s *StateDB) SetEnergyUsage(addr tcommon.Address, used int64, lastTime int64)
func (s *StateDB) GetWitness(addr tcommon.Address) *types.Witness
func (s *StateDB) PutWitness(w *types.Witness)
func (s *StateDB) DynamicProperties() *DynamicProperties
func (s *StateDB) Snapshot() int
func (s *StateDB) RevertToSnapshot(id int)
func (s *StateDB) Commit() (tcommon.Hash, error)  // returns accountStateRoot
func (s *StateDB) Copy() *StateDB
```

### stateObject

Per-account in-memory wrapper. Tracks dirty state for Commit.

```go
type stateObject struct {
    address  tcommon.Address
    account  *types.Account    // mutable protobuf wrapper
    dirty    bool
    deleted  bool
}
```

The `types.Account` wrapper will be extended with new accessors for FreezeV2, UnfreezeV2, votes, etc. (see Section 5).

### Database Interface

Wraps go-ethereum's `triedb.Database` + `trie.Trie`:

```go
type Database interface {
    OpenTrie(root ethcommon.Hash) (Trie, error)
    CopyTrie(t Trie) Trie
    TrieDB() *triedb.Database
}

type Trie interface {
    GetStorage(addr ethcommon.Address, key []byte) ([]byte, error)
    GetAccount(address ethcommon.Address) (*ethtypes.StateAccount, error)
    UpdateStorage(addr ethcommon.Address, key, value []byte) error
    UpdateAccount(address ethcommon.Address, account *ethtypes.StateAccount) error
    DeleteStorage(addr ethcommon.Address, key []byte) error
    DeleteAccount(address ethcommon.Address) error
    Hash() ethcommon.Hash
    Commit(collectLeaf bool) (ethcommon.Hash, *trienode.NodeSet, error)
}
```

**Key design decision:** We use go-ethereum's `trie` package natively but with a TRON-specific mapping:

- The MPT key for each account is `Keccak256(tronAddress)` (21 bytes → 32 bytes) — this matches Ethereum's account trie key derivation and lets us reuse go-ethereum's trie code unmodified.
- The MPT value for each account is the protobuf-serialized `Account` message (not RLP). This diverges from Ethereum but allows TRON accounts to be stored natively.
- Since we store raw bytes in the trie (not Ethereum's `StateAccount` struct), we use `UpdateStorage`/`GetStorage` on the trie for account data rather than `UpdateAccount`/`GetAccount`. The trie is conceptually a `key→value` map where key = `Keccak256(address)` and value = `proto.Marshal(account)`.

### accountStateRoot

The `accountStateRoot` field already exists in `BlockHeaderRaw` (proto field 11). After executing all transactions in a block:

1. `StateDB.Commit()` serializes all dirty accounts into the MPT
2. Returns the trie root hash as `accountStateRoot`
3. This root is set in the block header before signing

---

## 3. Genesis

### Design

Follow go-ethereum's factory function pattern: hardcoded Go structs + embedded alloc data + `DefaultMainnetGenesis()` / `DefaultNileGenesis()` factory functions + `SetupGenesisBlock(db, genesis)` to write to DB.

### Genesis Struct (extended from Phase 1)

```go
// params/genesis.go
package params

type Genesis struct {
    Config          *ChainConfig
    Timestamp       int64
    ParentHash      common.Hash
    Accounts        []GenesisAccount
    Witnesses       []GenesisWitness
    DynamicProperties map[string]int64   // initial dynamic properties
}

type GenesisAccount struct {
    Address    common.Address
    Balance    int64
    AccountType int32  // 0=Normal, 1=AssetIssue, 2=Contract
    AccountName string
}

type GenesisWitness struct {
    Address   common.Address
    VoteCount int64
    URL       string
}
```

### Mainnet Genesis Data

3 accounts + 27 witnesses (GR1–GR27):

```go
// params/mainnet.go
func DefaultMainnetGenesis() *Genesis {
    return &Genesis{
        Config:    MainnetChainConfig,
        Timestamp: 0,  // 2018-06-25T00:00:00Z encoded as TRON genesis timestamp
        Accounts: []GenesisAccount{
            {Address: hexToAddress("41928c9af0651632157ef27a2cf17ca72c575a4d21"), Balance: 99_000_000_000_000_000, AccountName: "Zion", AccountType: 0},
            {Address: hexToAddress("41a614f803b6fd780986a42c78ec9c7f77e6ded13c"), Balance: 0, AccountName: "Sun", AccountType: 0},
            {Address: hexToAddress("41b0a14fb448b324ca992f2ddcb7d7b49470da3cf8"), Balance: -9223372036854775808, AccountName: "Blackhole", AccountType: 0},
        },
        Witnesses: mainnetWitnesses(),  // 27 GR witnesses
        DynamicProperties: map[string]int64{
            "maintenance_time_interval":       21600000,
            "account_upgrade_cost":            9999000000,
            "create_account_fee":              100000,
            "transaction_fee":                 10,
            "asset_issue_fee":                 1024000000,
            "witness_pay_per_block":           16000000,
            "witness_standby_allowance":       115200000000,
            "create_new_account_fee_in_system_contract": 0,
            "create_new_account_bandwidth_rate": 1,
            "allow_creation_of_contracts":     0,  // enabled later via proposal
            "energy_fee":                      100,
            "max_cpu_time_of_one_tx":          80,
            "allow_tvm_transfer_trc10":        0,
            "total_energy_limit":              50000000000,
            "allow_multi_sign":                0,
            "allow_adaptive_energy":           0,
            "total_energy_current_limit":      50000000000,
            "allow_delegate_resource":         0,
            "allow_tvm_istanbul":              0,
            "allow_new_resource_model":        0,
            "unfreeze_delay_days":             14,
        },
    }
}
```

### Nile Genesis Data

```go
// params/nile.go
// Note: Actual Nile testnet addresses and witness list to be extracted from
// https://nileex.io or the Nile genesis.block configuration during implementation.
func DefaultNileGenesis() *Genesis {
    return &Genesis{
        Config:    NileChainConfig,
        Timestamp: 0,
        Accounts:  nileAccounts(),   // Zion/Sun/Blackhole with Nile-specific addresses
        Witnesses: nileWitnesses(),  // Nile SR list
        DynamicProperties: map[string]int64{
            // Same structure as mainnet, with testnet-appropriate values
            "maintenance_time_interval": 21600000,
            // Most values identical to mainnet defaults
        },
    }
}
```

### SetupGenesisBlock

```go
// core/genesis.go
package core

func SetupGenesisBlock(db ethdb.KeyValueStore, trieDB *triedb.Database, genesis *Genesis) (*params.ChainConfig, common.Hash, error) {
    // 1. Check if chain already exists in DB
    // 2. If no genesis provided and no chain exists → error
    // 3. If genesis provided and no chain exists → write genesis
    // 4. If chain exists → validate genesis hash matches
    // 5. Return chain config, genesis hash
}

func (g *Genesis) ToBlock(db ethdb.KeyValueStore, trieDB *triedb.Database) (*types.Block, error) {
    // 1. Create StateDB with empty root
    // 2. For each account in genesis: statedb.GetOrCreateAccount, set balance
    // 3. For each witness: create witness in statedb
    // 4. Write DynamicProperties to statedb
    // 5. statedb.Commit() → accountStateRoot
    // 6. Build genesis block header with accountStateRoot
    // 7. Return genesis block
}

func (g *Genesis) Hash() common.Hash {
    block, _ := g.ToBlock(rawdb.NewMemoryDatabase(), nil)
    return block.Hash()
}
```

---

## 4. DynamicProperties

Runtime-adjustable chain parameters. Stored as key-value pairs in the database (prefix `dp-`) and cached in StateDB.

### Structure

```go
// core/state/dynamic_properties.go
package state

type DynamicProperties struct {
    props  map[string]int64
    dirty  map[string]struct{}
}

func NewDynamicProperties() *DynamicProperties
func LoadDynamicProperties(db ethdb.KeyValueReader) *DynamicProperties

// Typed getters (most frequently used)
func (d *DynamicProperties) MaintenanceTimeInterval() int64    // default 21600000 (6h)
func (d *DynamicProperties) NextMaintenanceTime() int64
func (d *DynamicProperties) SetNextMaintenanceTime(t int64)
func (d *DynamicProperties) LatestBlockHeaderNumber() int64
func (d *DynamicProperties) SetLatestBlockHeaderNumber(n int64)
func (d *DynamicProperties) LatestBlockHeaderTimestamp() int64
func (d *DynamicProperties) SetLatestBlockHeaderTimestamp(t int64)
func (d *DynamicProperties) LatestBlockHeaderHash() common.Hash
func (d *DynamicProperties) SetLatestBlockHeaderHash(h common.Hash)
func (d *DynamicProperties) LatestSolidifiedBlockNum() int64
func (d *DynamicProperties) SetLatestSolidifiedBlockNum(n int64)
func (d *DynamicProperties) WitnessPayPerBlock() int64         // default 16_000_000 (16 TRX)
func (d *DynamicProperties) WitnessStandbyAllowance() int64    // default 115_200_000_000
func (d *DynamicProperties) TransactionFee() int64             // default 10 sun/byte
func (d *DynamicProperties) EnergyFee() int64                  // default 100 sun/energy
func (d *DynamicProperties) CreateAccountFee() int64           // default 100_000
func (d *DynamicProperties) CreateNewAccountFeeInSystemContract() int64
func (d *DynamicProperties) TotalEnergyCurrentLimit() int64
func (d *DynamicProperties) UnfreezeDelayDays() int64          // default 14
func (d *DynamicProperties) MaxCpuTimeOfOneTx() int64          // default 80ms
func (d *DynamicProperties) AllowNewResourceModel() bool

// Generic get/set for proposal system
func (d *DynamicProperties) Get(key string) (int64, bool)
func (d *DynamicProperties) Set(key string, value int64)

// Persistence
func (d *DynamicProperties) Flush(db ethdb.KeyValueWriter)     // write dirty props to DB
```

### Key Names

Follow java-tron's exact key names (stored as `dp-<name>` in DB):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `latest_block_header_timestamp` | int64 | 0 | Head block timestamp |
| `latest_block_header_number` | int64 | 0 | Head block number |
| `latest_block_header_hash` | bytes | 0x00... | Head block hash |
| `latest_solidified_block_num` | int64 | 0 | Latest solidified block |
| `next_maintenance_time` | int64 | genesis+interval | Next maintenance timestamp |
| `maintenance_time_interval` | int64 | 21600000 | 6 hours in ms |
| `witness_pay_per_block` | int64 | 16000000 | 16 TRX |
| `witness_standby_allowance` | int64 | 115200000000 | Per maintenance period |
| `transaction_fee` | int64 | 10 | sun per byte |
| `energy_fee` | int64 | 100 | sun per energy unit |
| `create_account_fee` | int64 | 100000 | |
| `create_new_account_fee_in_system_contract` | int64 | 0 | |
| `create_new_account_bandwidth_rate` | int64 | 1 | |
| `account_upgrade_cost` | int64 | 9999000000 | |
| `asset_issue_fee` | int64 | 1024000000 | |
| `total_energy_current_limit` | int64 | 50000000000 | |
| `unfreeze_delay_days` | int64 | 14 | Days before unfrozen TRX can be withdrawn |
| `max_cpu_time_of_one_tx` | int64 | 80 | ms |
| `allow_new_resource_model` | int64 | 0 | 0=disabled, 1=enabled |
| `total_net_limit` | int64 | 43200000000 | Total free bandwidth per day |

---

## 5. types.Account Extensions

Extend `core/types/account.go` with accessors for FreezeV2, votes, and resource tracking:

```go
// New accessors added to Account

// FreezeV2
func (a *Account) FrozenV2() []*corepb.Account_FreezeV2
func (a *Account) AddFreezeV2(resourceType corepb.ResourceCode, amount int64)
func (a *Account) GetFrozenV2Amount(resourceType corepb.ResourceCode) int64
func (a *Account) TotalFrozenV2() int64

// UnfreezeV2 (pending unfreezes with expire_time)
func (a *Account) UnfrozenV2() []*corepb.Account_UnFreezeV2
func (a *Account) AddUnfreezeV2(resourceType corepb.ResourceCode, amount int64, expireTime int64)
func (a *Account) RemoveExpiredUnfreezeV2(now int64) int64  // returns total withdrawn amount
func (a *Account) TotalUnfrozenV2() int64

// Votes
func (a *Account) Votes() []*corepb.Vote
func (a *Account) SetVotes(votes []*corepb.Vote)
func (a *Account) ClearVotes()

// Resource tracking
func (a *Account) NetUsage() int64
func (a *Account) SetNetUsage(v int64)
func (a *Account) LatestConsumeTime() int64
func (a *Account) SetLatestConsumeTime(t int64)
func (a *Account) FreeNetUsage() int64
func (a *Account) SetFreeNetUsage(v int64)
func (a *Account) LatestConsumeFreeTime() int64
func (a *Account) SetLatestConsumeFreeTime(t int64)
func (a *Account) EnergyUsage() int64
func (a *Account) SetEnergyUsage(v int64)
func (a *Account) LatestConsumeTimeForEnergy() int64
func (a *Account) SetLatestConsumeTimeForEnergy(t int64)

// Allowance (witness rewards)
func (a *Account) Allowance() int64
func (a *Account) SetAllowance(v int64)
func (a *Account) LatestWithdrawTime() int64
func (a *Account) SetLatestWithdrawTime(t int64)
```

---

## 6. Resource Model

### Design

```go
// core/resource.go
package core

type ResourceProcessor struct {
    statedb  *state.StateDB
}

func NewResourceProcessor(statedb *state.StateDB) *ResourceProcessor
```

### Bandwidth

```go
func (r *ResourceProcessor) ConsumeBandwidth(tx *types.Transaction, addr common.Address) error {
    // 1. Calculate bytes_needed = len(tx.Proto().Marshal())
    // 2. Recover existing bandwidth: RecoverBandwidth(addr, now)
    // 3. Try frozen bandwidth first
    //    frozenBW = calculateFrozenBandwidth(addr)
    //    if frozenBW >= bytes_needed → consume from frozen, return nil
    // 4. Try free bandwidth
    //    if freeNetUsage + bytes_needed <= FreeNetLimit → consume from free, return nil
    // 5. Fallback: burn TRX at TransactionFee sun/byte
    //    cost = bytes_needed * dynProps.TransactionFee()
    //    SubBalance(addr, cost) or return error
}

func (r *ResourceProcessor) RecoverBandwidth(addr common.Address, now int64) {
    // Sliding window recovery:
    // elapsed = now - latestConsumeTime
    // if elapsed >= WindowSizeMs (24h) → reset to 0
    // else → used = used * (WindowSizeMs - elapsed) / WindowSizeMs
}
```

**Bandwidth formula:**
```
frozenBandwidth = (account.frozenV2[BANDWIDTH].amount / totalNetWeight) * totalNetLimit
```
where `totalNetWeight` = sum of all accounts' frozen-for-bandwidth amounts, and `totalNetLimit` = `total_net_limit` dynamic property.

### Energy

```go
func (r *ResourceProcessor) ConsumeEnergy(addr common.Address, energyUsed int64) error {
    // 1. Recover existing energy: RecoverEnergy(addr, now)
    // 2. Try frozen energy first
    //    frozenEnergy = calculateFrozenEnergy(addr)
    //    consume min(frozenEnergy, energyUsed)
    // 3. Remainder → burn TRX at EnergyFee sun/energy
}
```

**Energy formula:**
```
frozenEnergy = (account.frozenV2[ENERGY].amount / totalEnergyWeight) * totalEnergyCurrentLimit
```

### Recovery

Both bandwidth and energy use the same 24-hour sliding window recovery:

```
newUsage = oldUsage * max(0, WindowSizeMs - elapsed) / WindowSizeMs
```

---

## 7. New Actuators

All actuators change from direct DB ops to operating through `StateDB`. The `actuator.Context` is updated:

```go
type Context struct {
    StateDB     *state.StateDB
    Tx          *types.Transaction
    BlockTime   int64
    BlockNumber uint64
}
```

### 7.1 FreezeBalanceV2Actuator

Freezes TRX for bandwidth or energy resources.

```go
// actuator/freeze_v2.go

type FreezeBalanceV2Actuator struct{}

func (a *FreezeBalanceV2Actuator) Validate(ctx *Context) error {
    // 1. Decode FreezeBalanceV2Contract from tx
    // 2. Verify owner exists
    // 3. Verify frozen_balance > 0
    // 4. Verify owner has sufficient balance
    // 5. Verify resource type is BANDWIDTH or ENERGY
}

func (a *FreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
    // 1. Decode FreezeBalanceV2Contract
    // 2. SubBalance(owner, frozen_balance)
    // 3. AddFreezeV2(owner, resource_type, frozen_balance)
    // 4. If resource == TRON_POWER: add to tron_power (for voting weight)
    // 5. Update total weight (totalNetWeight or totalEnergyWeight in DynProps)
}
```

### 7.2 UnfreezeBalanceV2Actuator

Initiates unfreezing. TRX enters a waiting period before withdrawal.

```go
// actuator/unfreeze_v2.go

type UnfreezeBalanceV2Actuator struct{}

func (a *UnfreezeBalanceV2Actuator) Validate(ctx *Context) error {
    // 1. Decode UnfreezeBalanceV2Contract
    // 2. Verify owner exists
    // 3. Verify unfreeze_balance > 0
    // 4. Verify frozen amount for resource_type >= unfreeze_balance
    // 5. Check unfreezing count limit (max 32 pending unfreezes)
}

func (a *UnfreezeBalanceV2Actuator) Execute(ctx *Context) (*Result, error) {
    // 1. Decode UnfreezeBalanceV2Contract
    // 2. Reduce FreezeV2 amount for the resource type
    // 3. Calculate expireTime = now + unfreezeDelayDays * 86400000
    // 4. AddUnfreezeV2(owner, resource_type, unfreeze_balance, expireTime)
    // 5. Update total weight
    // 6. If resource == TRON_POWER: reduce voting power, clear votes if needed
}
```

### 7.3 VoteWitnessActuator

Cast votes for super representative candidates.

```go
// actuator/vote.go

type VoteWitnessActuator struct{}

func (a *VoteWitnessActuator) Validate(ctx *Context) error {
    // 1. Decode VoteWitnessContract
    // 2. Verify owner exists
    // 3. Verify vote count <= MaxVoteNumber (30)
    // 4. Verify total vote_count <= owner's TRON_POWER (frozen balance / TRX_PRECISION)
    // 5. Verify each vote_address is a registered witness
    // 6. Verify no duplicate vote addresses
}

func (a *VoteWitnessActuator) Execute(ctx *Context) (*Result, error) {
    // 1. Decode VoteWitnessContract
    // 2. Clear old votes from owner account
    // 3. For each vote:
    //    a. Subtract old vote count from witness
    //    b. Add new vote count to witness
    // 4. Set new votes on owner account
}
```

### 7.4 WithdrawBalanceActuator

Withdraw accumulated witness block rewards.

```go
// actuator/withdraw.go

type WithdrawBalanceActuator struct{}

func (a *WithdrawBalanceActuator) Validate(ctx *Context) error {
    // 1. Decode WithdrawBalanceContract
    // 2. Verify owner exists and is a witness
    // 3. Verify allowance > 0
    // 4. Verify latestWithdrawTime + 24h <= now
}

func (a *WithdrawBalanceActuator) Execute(ctx *Context) (*Result, error) {
    // 1. Get allowance from account
    // 2. AddBalance(owner, allowance)
    // 3. SetAllowance(owner, 0)
    // 4. SetLatestWithdrawTime(owner, now)
}
```

### 7.5 WithdrawExpireUnfreezeActuator

Withdraw TRX from expired unfreeze entries.

```go
// actuator/withdraw_expire_unfreeze.go

type WithdrawExpireUnfreezeActuator struct{}

func (a *WithdrawExpireUnfreezeActuator) Validate(ctx *Context) error {
    // 1. Decode WithdrawExpireUnfreezeContract
    // 2. Verify owner exists
    // 3. Verify at least one unfreezeV2 entry has expireTime <= now
}

func (a *WithdrawExpireUnfreezeActuator) Execute(ctx *Context) (*Result, error) {
    // 1. Scan all unfreezeV2 entries
    // 2. Sum up all entries where expireTime <= now
    // 3. Remove those entries from the account
    // 4. AddBalance(owner, totalExpired)
}
```

### Actuator Registry Update

```go
// actuator/actuator.go — add to CreateActuator switch
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
```

---

## 8. Blockchain

### Design

```go
// core/blockchain.go
package core

type BlockChain struct {
    db           ethdb.KeyValueStore
    trieDB       *triedb.Database
    stateCache   state.Database       // for opening state tries

    genesisBlock *types.Block
    currentBlock atomic.Pointer[types.Block]

    chainConfig  *params.ChainConfig
    engine       consensus.Engine

    // Solidification tracking
    witnessBlockCount map[common.Address]int64  // witness → confirmed block count
}

func NewBlockChain(db ethdb.KeyValueStore, config *params.ChainConfig, engine consensus.Engine) (*BlockChain, error) {
    // 1. Open trieDB from ethdb
    // 2. Load head block from rawdb
    // 3. If no head → expect genesis to be set up first
    // 4. Validate chain integrity
    // 5. Return blockchain
}
```

### Block Insertion

```go
func (bc *BlockChain) InsertBlock(block *types.Block) error {
    // 1. Validate block header
    //    - engine.VerifyHeader(bc, block)
    //    - parent hash matches current head
    //    - block number == parent + 1
    //    - timestamp is in valid slot
    //    - witness signature verification

    // 2. Create state from parent root
    //    statedb, err := state.New(parentAccountStateRoot, bc.stateCache)

    // 3. Process all transactions
    //    for _, tx := range block.Transactions():
    //        actuator := actuator.CreateActuator(tx)
    //        resource.ConsumeBandwidth(tx, sender)
    //        actuator.Validate(ctx)
    //        actuator.Execute(ctx)

    // 4. Apply block-level effects
    //    - Witness pay: AddBalance(witness, witnessPayPerBlock)
    //    - If maintenance: doMaintenance(statedb)

    // 5. Commit state → accountStateRoot
    //    root, err := statedb.Commit()

    // 6. Verify accountStateRoot matches block header
    //    if root != block.AccountStateRoot(): return ErrStateRootMismatch

    // 7. Write block to rawdb
    //    rawdb.WriteBlock(bc.db, block)
    //    rawdb.WriteHeadBlockHash(bc.db, block.Hash())

    // 8. Update dynamic properties
    //    dynProps.SetLatestBlockHeaderNumber(block.Number())
    //    dynProps.SetLatestBlockHeaderTimestamp(block.Timestamp())
    //    dynProps.SetLatestBlockHeaderHash(block.Hash())
    //    dynProps.Flush(bc.db)

    // 9. Update solidified block
    //    bc.updateSolidifiedBlock(block)

    // 10. Set current block
    //     bc.currentBlock.Store(block)
}
```

### Solidified Block Tracking

A block is solidified when confirmed by 70%+ of active witnesses:

```go
func (bc *BlockChain) updateSolidifiedBlock(block *types.Block) {
    witness := block.WitnessAddress()
    bc.witnessBlockCount[witness] = block.Number()

    // Find the block number where 70% of witnesses have produced blocks at or above
    var heights []int64
    for _, h := range bc.witnessBlockCount {
        heights = append(heights, h)
    }
    sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

    activeCount := len(bc.engine.ActiveWitnesses())
    threshold := activeCount * params.SolidifiedThreshold / 100
    if len(heights) >= threshold {
        solidNum := heights[len(heights)-threshold]
        if solidNum > dynProps.LatestSolidifiedBlockNum() {
            dynProps.SetLatestSolidifiedBlockNum(solidNum)
        }
    }
}
```

### Chain Reader (for consensus)

```go
func (bc *BlockChain) CurrentBlock() *types.Block
func (bc *BlockChain) GetBlockByNumber(number uint64) *types.Block
func (bc *BlockChain) GetBlockByHash(hash common.Hash) *types.Block
func (bc *BlockChain) GenesisTimestamp() int64
func (bc *BlockChain) ActiveWitnesses() []common.Address
func (bc *BlockChain) NextMaintenanceTime() int64
func (bc *BlockChain) Config() *params.ChainConfig
```

---

## 9. Consensus Extensions

### Header Verification

```go
// consensus/dpos/verify.go

func (d *DPoS) VerifyHeader(chain ChainReader, block *types.Block) error {
    // 1. Verify block number == parent.Number + 1
    // 2. Verify parentHash matches
    // 3. Verify timestamp is at a valid slot boundary (aligned to 3s)
    // 4. Verify timestamp > parent.Timestamp
    // 5. Verify witness signature (ecrecover from block header)
    // 6. Verify witness is the scheduled witness for this slot
    //    scheduled := d.GetScheduledWitness(slotForTime(block.Timestamp))
    //    if witness != scheduled → return ErrInvalidWitness
}
```

### Maintenance Period

```go
// consensus/dpos/maintenance.go

func (d *DPoS) DoMaintenance(statedb *state.StateDB) error {
    // 1. Tally votes: aggregate all votes from all accounts
    // 2. Sort witnesses by vote count descending
    // 3. Top MaxActiveWitnessNum become active witnesses
    // 4. Update witness schedule in statedb
    // 5. Distribute standby allowance:
    //    allowance = WitnessStandbyAllowance / WitnessStandbyLength
    //    for each of top-127 witnesses: AddAllowance(witness, allowance)
    // 6. Update NextMaintenanceTime += MaintenanceTimeInterval
}

func (d *DPoS) IsInMaintenance(timestamp int64) bool {
    return timestamp >= nextMaintenanceTime
}
```

### Block Rewards

```go
// consensus/dpos/reward.go

func (d *DPoS) PayBlockReward(statedb *state.StateDB, witness common.Address) {
    // Add WitnessPayPerBlock to witness's allowance (not balance directly)
    account := statedb.GetAccount(witness)
    account.SetAllowance(account.Allowance() + statedb.DynamicProperties().WitnessPayPerBlock())
}
```

---

## 10. Actuator Context Migration

Phase 1 actuators used `trondb.Database` directly. Phase 2 migrates them to `StateDB`:

**Before (Phase 1):**
```go
type Context struct {
    DB trondb.Database
    Tx *types.Transaction
}
```

**After (Phase 2):**
```go
type Context struct {
    StateDB     *state.StateDB
    Tx          *types.Transaction
    BlockTime   int64
    BlockNumber uint64
}
```

All existing actuators (Transfer, CreateAccount, WitnessCreate) are updated to use `ctx.StateDB.GetAccount()`, `ctx.StateDB.AddBalance()`, etc. instead of direct rawdb calls.

---

## 11. File Map

### New Files

| File | Description |
|------|-------------|
| `core/rawdb/database.go` | `NewPebbleDB()` factory using go-ethereum's pebble |
| `core/state/statedb.go` | StateDB: in-memory state + MPT root |
| `core/state/state_object.go` | Per-account state wrapper |
| `core/state/journal.go` | Undo journal for snapshot/revert |
| `core/state/database.go` | Database interface wrapping triedb |
| `core/state/dynamic_properties.go` | DynamicProperties get/set/persist |
| `core/genesis.go` | `SetupGenesisBlock()`, `Genesis.ToBlock()` |
| `core/blockchain.go` | Block insertion, validation, chain management |
| `core/resource.go` | Bandwidth/energy consumption and recovery |
| `params/mainnet.go` | `DefaultMainnetGenesis()` + hardcoded data |
| `params/nile.go` | `DefaultNileGenesis()` + hardcoded data |
| `actuator/freeze_v2.go` | FreezeBalanceV2Actuator |
| `actuator/unfreeze_v2.go` | UnfreezeBalanceV2Actuator |
| `actuator/vote.go` | VoteWitnessActuator |
| `actuator/withdraw.go` | WithdrawBalanceActuator |
| `actuator/withdraw_expire_unfreeze.go` | WithdrawExpireUnfreezeActuator |
| `consensus/dpos/verify.go` | Header verification |
| `consensus/dpos/maintenance.go` | Maintenance period logic |
| `consensus/dpos/reward.go` | Block reward distribution |

### Modified Files

| File | Change |
|------|--------|
| `core/rawdb/schema.go` | `trondb` → `ethdb` imports |
| `core/rawdb/accessors_*.go` | `trondb` → `ethdb` imports |
| `core/types/account.go` | Add FreezeV2, vote, resource accessors |
| `core/types/block.go` | Add `AccountStateRoot()` accessor |
| `actuator/actuator.go` | Context uses StateDB; add 5 new cases to registry |
| `actuator/transfer.go` | Use StateDB instead of raw DB |
| `actuator/account.go` | Use StateDB instead of raw DB |
| `actuator/witness.go` | Use StateDB instead of raw DB |
| `consensus/consensus.go` | Extend Engine interface with DoMaintenance, PayBlockReward |
| `consensus/dpos/schedule.go` | Wire to StateDB for witness list |
| `params/config.go` | Add DPoS config fields |
| `params/genesis.go` | Extend Genesis struct with DynamicProperties, AccountType |
| `go.mod` | Add triedb, pebble transitive deps |

### Deleted Files

| File | Reason |
|------|--------|
| `trondb/database.go` | Replaced by `ethdb.Database` |
| `trondb/memorydb/memorydb.go` | Replaced by `ethdb/memorydb` |
| `trondb/memorydb/memorydb_test.go` | Replaced by `ethdb/memorydb` |

---

## 12. Testing Strategy

- **StateDB tests** (`core/state/statedb_test.go`): Create accounts, set balances, freeze, snapshot/revert, verify MPT root changes
- **Genesis tests** (`core/genesis_test.go`): Build genesis block, verify account balances, verify deterministic hash
- **Blockchain tests** (`core/blockchain_test.go`): Insert blocks, verify state transitions, test solidification
- **Resource tests** (`core/resource_test.go`): Bandwidth consumption/recovery, energy consumption, TRX burn fallback
- **Actuator tests** (`actuator/*_test.go`): Each actuator: validate + execute with crafted transactions via StateDB
- **DynamicProperties tests** (`core/state/dynamic_properties_test.go`): Get/set, persistence, defaults
- All tests use `ethdb/memorydb` (from go-ethereum) for in-memory database

---

## 13. Dependency Changes

### Added (via go-ethereum)

Already in `go.mod` as transitive dependencies from `github.com/ethereum/go-ethereum`:
- `github.com/ethereum/go-ethereum/ethdb` — DB interfaces
- `github.com/ethereum/go-ethereum/ethdb/pebble` — Pebble backend
- `github.com/ethereum/go-ethereum/ethdb/memorydb` — Test backend
- `github.com/ethereum/go-ethereum/trie` — Merkle Patricia Trie
- `github.com/ethereum/go-ethereum/triedb` — Trie database
- `github.com/cockroachdb/pebble` — Pebble storage engine (transitive)

### Removed

- `trondb` package — fully replaced by ethdb
