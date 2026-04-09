# go-tron Phase 3: Block Processing Pipeline + Node Bootstrap

## Overview

Phase 3 wires all Phase 2 components (StateDB, Actuators, Consensus, BlockChain) into a working blockchain node. After this phase, go-tron can:

1. Initialize genesis from CLI (`gtron init`)
2. Process blocks end-to-end (execute transactions against StateDB via actuators)
3. Accept transactions via HTTP API and queue them in a transaction pool
4. Serve blockchain and account data via HTTP API
5. Run as a standalone node (`gtron` / `gtron run`)

No P2P networking, no smart contracts (TVM), no TRC-10 assets in this phase.

---

## 1. Actuator Context Migration

### Problem

Phase 1 actuators use `rawdb.ReadAccount(ctx.DB, addr)` with `ethdb.KeyValueStore`. Phase 2 introduced StateDB with MPT-backed state, snapshot/revert, and dirty tracking. The actuators must use StateDB to participate in the block processing pipeline.

### Solution

Replace `Context.DB ethdb.KeyValueStore` with `Context.State *state.StateDB`. Actuators read/write accounts through StateDB methods, gaining automatic dirty tracking, journaling, and MPT persistence.

### Context struct (new)

```go
type Context struct {
    State       *state.StateDB
    DynProps    *state.DynamicProperties
    Tx          *types.Transaction
    BlockTime   int64
    BlockNumber uint64
}
```

### Actuator migration pattern

Before:
```go
ownerAcc := rawdb.ReadAccount(ctx.DB, addr)
ownerAcc.SetBalance(ownerAcc.Balance() - amount)
rawdb.WriteAccount(ctx.DB, addr, ownerAcc)
```

After:
```go
ownerAcc := ctx.State.GetAccount(addr)
// ... modify account via StateDB methods ...
ctx.State.SubBalance(addr, amount)
```

Key: actuators use StateDB's typed methods (AddBalance, SubBalance, AddFreezeV2, etc.) for operations that StateDB supports directly. For operations not yet on StateDB (e.g., SetVotes, AddUnfreezeV2), add the necessary methods to StateDB.

### StateDB additions needed

```go
func (s *StateDB) SetVotes(addr Address, votes []*corepb.Vote)
func (s *StateDB) AddUnfreezeV2(addr Address, resourceType corepb.ResourceCode, amount, expireTime int64)
func (s *StateDB) ClearUnfreezeV2Expired(addr Address, now int64) int64  // returns collected amount
func (s *StateDB) ReduceFreezeV2(addr Address, resourceType corepb.ResourceCode, amount int64) error
func (s *StateDB) SetLatestConsumeTime(addr Address, t int64)
func (s *StateDB) SetLatestConsumeFreeTime(addr Address, t int64)
func (s *StateDB) SetAllowance(addr Address, allowance int64)
func (s *StateDB) GetAllowance(addr Address) int64
func (s *StateDB) SetLatestWithdrawTime(addr Address, t int64)
func (s *StateDB) GetLatestWithdrawTime(addr Address) int64
func (s *StateDB) AccountExists(addr Address) bool
func (s *StateDB) CreateAccount(addr Address, accountType corepb.AccountType) *types.Account
```

All 8 actuators (Transfer, CreateAccount, WitnessCreate, FreezeV2, UnfreezeV2, VoteWitness, WithdrawBalance, WithdrawExpireUnfreeze) must be migrated.

---

## 2. State Processor

### Design

Two-level processing:

1. **ApplyTransaction** — execute a single transaction against StateDB
2. **ProcessBlock** — execute all transactions in a block, apply consensus rewards, return new state root

```go
// core/state_processor.go

func ApplyTransaction(state *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64, blockNum uint64) (int64, error)

func ProcessBlock(state *state.StateDB, dynProps *state.DynamicProperties, block *types.Block) (common.Hash, error)
```

### ApplyTransaction flow

1. Create actuator from transaction type
2. Build `actuator.Context{State, DynProps, Tx, BlockTime, BlockNumber}`
3. Call `actuator.Validate(ctx)` — if error, revert and return error
4. Take StateDB snapshot
5. Call `actuator.Execute(ctx)` — if error, revert snapshot
6. Return fee from result

### ProcessBlock flow

1. For each transaction in the block:
   - `ApplyTransaction(state, dynProps, tx, block.Timestamp(), block.Number())`
   - Accumulate fees
2. Pay block reward to witness: `state.AddAllowance(witnessAddr, dynProps.WitnessPayPerBlock())`
3. Commit StateDB → new state root
4. Return the new state root hash

### No resource deduction in Phase 3

Bandwidth/energy consumption is complex and involves the ResourceProcessor from Phase 2. For Phase 3, we skip per-transaction resource deduction. Transactions cost their actuator-returned fee only. Resource model integration comes in a future phase.

---

## 3. Full Block Insertion

### Design

Enhance `BlockChain` to process blocks with state execution.

```go
func (bc *BlockChain) InsertBlock(block *types.Block) error
```

### InsertBlock flow

1. Lock `chainmu`
2. Validate: number == current+1, parent hash matches
3. Load parent state root from current block's `AccountStateRoot`
4. Open StateDB from parent state root
5. Load DynamicProperties from DB
6. Call `ProcessBlock(statedb, dynProps, block)` → newRoot
7. If block has a non-zero `AccountStateRoot`, verify it matches newRoot
8. Update DynamicProperties: block number, timestamp, hash, solidified block
9. Flush DynamicProperties to DB
10. Store block via rawdb
11. Update head block hash
12. Update `currentBlock` atomic pointer

### DynamicProperties loading

`BlockChain` loads DynamicProperties from the disk DB at each block insertion. After updating, it flushes back. This ensures properties persist across restarts.

### AccountStateRoot handling

For blocks produced locally (no stateRoot in header yet), skip the root check. For blocks received from peers (has stateRoot), verify match. In Phase 3, since there's no P2P, we only produce blocks locally via `InsertBlockWithoutVerify` or test helpers, so root verification is optional but implemented for future use.

---

## 4. Transaction Pool

### Design

Minimal transaction pool for queuing transactions received via API.

```go
// core/txpool/pool.go

type TxPool struct {
    mu      sync.RWMutex
    pending map[common.Hash]*types.Transaction
    chain   ChainReader
}
```

### Interface

```go
func NewTxPool(chain ChainReader) *TxPool
func (pool *TxPool) Add(tx *types.Transaction) error       // basic validation + add
func (pool *TxPool) Get(hash common.Hash) *types.Transaction
func (pool *TxPool) Pending() []*types.Transaction          // all pending txs
func (pool *TxPool) Remove(hash common.Hash)
func (pool *TxPool) RemoveBatch(hashes []common.Hash)
func (pool *TxPool) Count() int
```

### ChainReader interface (for pool)

```go
type ChainReader interface {
    CurrentBlock() *types.Block
}
```

### Validation on Add

1. Transaction must have a valid contract (non-nil)
2. Transaction hash must not already exist in pool
3. Pool size limit (default 10000)

No signature verification, no balance check, no nonce check in Phase 3. These require more infrastructure (signature recovery, state lookup) and are deferred.

---

## 5. TronBackend

### Design

Bridge between `BlockChain` + `StateDB` + `TxPool` and the API layer.

```go
// core/tron_backend.go

type TronBackend struct {
    chain  *BlockChain
    pool   *txpool.TxPool
}

func NewTronBackend(chain *BlockChain, pool *txpool.TxPool) *TronBackend
```

### Implements tronapi.Backend (extended)

```go
type Backend interface {
    CurrentBlock() *types.Block
    GetBlockByNumber(number uint64) (*types.Block, error)
    GetAccount(addr common.Address) (*types.Account, error)
    BroadcastTransaction(tx *types.Transaction) error
    ListWitnesses() []*types.Witness
    GetNodeInfo() *NodeInfo
    GetDynamicProperties() map[string]int64
    PendingTransactionCount() int
}
```

### GetAccount implementation

Opens a fresh StateDB from the current block's `AccountStateRoot`, then calls `statedb.GetAccount(addr)`. This ensures reads are always against the latest committed state.

---

## 6. API Expansion

### New HTTP endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/wallet/broadcasttransaction` | POST | Submit transaction to pool |
| `/wallet/listwitmesses` | GET/POST | List all witnesses |
| `/wallet/getnodeinfo` | GET | Node version and current block info |
| `/wallet/getdynamicproperties` | GET | Current chain parameters |
| `/wallet/gettransactioncountinpool` | GET | Pending transaction count |

### BroadcastTransaction

Accepts JSON-encoded protobuf Transaction, decodes it, adds to TxPool. Returns transaction hash on success.

---

## 7. Node Bootstrap

### Design

The `gtron` CLI action creates the full node stack:

1. Open Pebble database at `datadir/gtron/chaindata`
2. Call `SetupGenesisBlock(db, genesis)` — idempotent
3. Create `state.Database` wrapping the disk DB
4. Create `BlockChain(db, stateDB, config)`
5. Create `TxPool(chain)`
6. Create `TronBackend(chain, pool)`
7. Create `tronapi.Server(backend, httpPort)`
8. Register API server as node Lifecycle
9. Start node
10. Wait for shutdown signal

### CLI commands

- `gtron` (default action) — Initialize genesis + start node
- `gtron init` — Initialize genesis only (useful for explicit setup)
- `gtron version` — Print version (already exists)

### Genesis selection

- Default: mainnet genesis
- `--testnet` flag: Nile testnet genesis

---

## 8. Integration Test

### Test scenario

```
1. Create in-memory database
2. SetupGenesisBlock with 2 funded accounts
3. Create BlockChain
4. Build block 1 with a TransferContract transaction
5. InsertBlock(block1)
6. Verify: sender balance decreased, recipient balance increased
7. Verify: block 1 is current block
8. Verify: DynamicProperties updated (block number, timestamp)
9. Build block 2 with FreezeBalanceV2Contract
10. InsertBlock(block2)
11. Verify: account has frozen balance
```

This test exercises the full pipeline: genesis → state → actuator → processor → blockchain.

---

## File Summary

### New files
- `core/state_processor.go` — ApplyTransaction, ProcessBlock
- `core/state_processor_test.go` — Processor tests
- `core/txpool/pool.go` — Transaction pool
- `core/txpool/pool_test.go` — Pool tests
- `core/tron_backend.go` — Backend implementation
- `core/blockchain_insert_test.go` — Integration test for full block insertion

### Modified files
- `actuator/actuator.go` — Context struct migration
- `actuator/transfer.go` — Use StateDB
- `actuator/account.go` — Use StateDB
- `actuator/witness.go` — Use StateDB
- `actuator/freeze_v2.go` — Use StateDB
- `actuator/unfreeze_v2.go` — Use StateDB
- `actuator/vote.go` — Use StateDB
- `actuator/withdraw.go` — Use StateDB
- `actuator/withdraw_expire_unfreeze.go` — Use StateDB
- `actuator/*_test.go` — Update test contexts
- `core/state/statedb.go` — Add missing methods
- `core/blockchain.go` — Add InsertBlock with state processing
- `internal/tronapi/backend.go` — Extend Backend interface
- `internal/tronapi/api.go` — New endpoints
- `cmd/gtron/main.go` — Full node bootstrap, init command
- `cmd/gtron/config.go` — Genesis selection

### Not changed
- No P2P
- No TVM / smart contracts
- No TRC-10 assets
- No gRPC
- No JSON-RPC
