# go-tron Phase 4: Block Production + Resource Integration + Maintenance

## Overview

Phase 4 makes go-tron a self-sufficient single-node chain. After this phase, the node can:

1. Produce blocks on schedule as a configured witness (DPoS slot-based production)
2. Sign blocks with the witness private key
3. Consume bandwidth per transaction during block processing
4. Run maintenance at period boundaries (rotate active witnesses, distribute standby allowance)
5. Persist and load the active witness list across restarts

No P2P networking, no TVM, no TRC-10 assets in this phase.

---

## 1. Active Witness List Persistence

### Problem

`consensus.ChainReader` requires `ActiveWitnesses() []common.Address`, but BlockChain doesn't store or load the active witness list. The list is computed by `dpos.SelectActiveWitnesses()` during maintenance but has nowhere to persist.

### Solution

Store the active witness list in rawdb as a simple serialized blob. Load on BlockChain startup. Update after each maintenance cycle.

### rawdb additions

```go
func WriteActiveWitnesses(db ethdb.KeyValueWriter, witnesses []common.Address)
func ReadActiveWitnesses(db ethdb.KeyValueReader) []common.Address
```

Key: `activeWitnesses` → gob-encoded or length-prefixed `[]common.Address`.

### BlockChain additions

```go
type BlockChain struct {
    // ... existing fields ...
    activeWitnesses atomic.Value // []common.Address
}

func (bc *BlockChain) ActiveWitnesses() []common.Address
func (bc *BlockChain) SetActiveWitnesses(witnesses []common.Address)
```

On startup, `NewBlockChain` loads witnesses from rawdb. If empty (fresh chain), derive from genesis witnesses via `SelectActiveWitnesses`.

---

## 2. DPoS Engine Struct

### Problem

Phase 2 created free functions (`VerifyHeader`, `DoMaintenance`, `PayBlockReward`, `GetScheduledWitness`) but no struct implementing `consensus.Engine`. The block producer needs an Engine to check schedules and produce blocks.

### Solution

Create `consensus/dpos/engine.go` with a `DPoS` struct that implements `consensus.Engine` by delegating to existing free functions.

```go
type DPoS struct {
    chain consensus.ChainReader
}

func New(chain consensus.ChainReader) *DPoS

func (d *DPoS) VerifyHeader(chain consensus.ChainReader, block *types.Block) error
func (d *DPoS) GetScheduledWitness(slot int64) (common.Address, error)
func (d *DPoS) IsInMaintenance(timestamp int64) bool
func (d *DPoS) DoMaintenance(chain consensus.ChainHeaderWriter) error
func (d *DPoS) PayBlockReward(chain consensus.ChainHeaderWriter, witness common.Address)
```

`GetScheduledWitness` delegates to `dpos.GetScheduledWitness()` using `chain.CurrentBlock()`, `chain.GenesisTimestamp()`, `chain.ActiveWitnesses()`.

`IsInMaintenance` compares the given timestamp against `chain.NextMaintenanceTime()`.

---

## 3. Block Builder

### Problem

No code exists to assemble a block from pending transactions. Block production requires: select txs from pool → build block header → set state root → sign → insert.

### Solution

`core/block_builder.go` — a stateless function that assembles a block.

```go
func BuildBlock(
    chain *BlockChain,
    stateDB *state.Database,
    pool *txpool.TxPool,
    witnessAddr common.Address,
    timestamp int64,
) (*types.Block, error)
```

### BuildBlock flow

1. Get current head block (parent)
2. Set block number = parent+1, parentHash, timestamp, witnessAddress
3. Open StateDB from parent's AccountStateRoot
4. Load DynamicProperties from disk
5. Pull all pending transactions from pool
6. For each tx, try `ApplyTransaction` — if error, skip the tx (don't fail the block)
7. Collect successful tx protos and track applied tx hashes
8. Commit StateDB → get `accountStateRoot`
9. Set `accountStateRoot` in block header
10. Construct the `Block` from protobuf
11. Return unsigned block (signing is separate)

### Key design decisions

- **Skip failing transactions** rather than aborting the block. A block producer should include as many valid transactions as possible.
- **No block signing here** — signing is done by the producer after building, keeping concerns separate.
- **StateDB commit happens inside BuildBlock** — the resulting root goes into the header. When `InsertBlock` re-processes the block, it will compute the same root and verify it matches.

Wait — this means the block is processed twice (once in BuildBlock to get the root, once in InsertBlock to verify). This is how java-tron works: the producer executes transactions to get the state root, then the block is inserted via the normal path which re-executes and verifies the root matches.

Actually, for self-produced blocks we can optimize: instead of re-executing in InsertBlock, we can add a method `InsertBlockWithState` that takes the already-computed state and skips re-execution. But for simplicity and correctness in Phase 4, we'll re-execute. The overhead is negligible for a single-node chain.

---

## 4. Block Signing

### Problem

Blocks need to be signed by the witness private key. `recoverWitness` in verify.go shows the format: SHA256 of proto-marshaled BlockHeaderRaw, signed with ECDSA.

### Solution

Add `SignBlock` to the block type or as a standalone function:

```go
// core/types/block.go
func (b *Block) SetWitnessSignature(sig []byte)

// core/block_builder.go (or crypto/)
func SignBlock(block *types.Block, privKey *ecdsa.PrivateKey) error
```

`SignBlock`:
1. Marshal `block.Proto().BlockHeader.RawData` with protobuf
2. SHA256 the serialized data
3. `crypto.Sign(hash, privKey)` → 65-byte signature
4. `block.SetWitnessSignature(sig)`
5. Reset cached hash (since header changed)

### Block.SetWitnessSignature

```go
func (b *Block) SetWitnessSignature(sig []byte) {
    if b.pb.BlockHeader == nil {
        b.pb.BlockHeader = &corepb.BlockHeader{}
    }
    b.pb.BlockHeader.WitnessSignature = sig
    b.hashOnce = sync.Once{} // reset cached hash
}
```

Note: `hashOnce` reset is critical because the signature is NOT part of BlockHeaderRaw (the hash), but we still want to ensure consistency. Actually, in TRON, the block hash is SHA256 of BlockHeaderRaw which does NOT include the signature. So the hash doesn't change. But we should still reset to be safe if the hashOnce was computed before accountStateRoot was set.

Actually, looking more carefully: `Block.Hash()` hashes `BlockHeader.RawData` which includes `AccountStateRoot` but NOT `WitnessSignature`. The `WitnessSignature` is in `BlockHeader` not `BlockHeaderRaw`. So setting the signature doesn't change the hash. But we need a way to reset the hash after `BuildBlock` sets `AccountStateRoot` — at that point the hash changes.

Solution: add `ResetHash()` to Block, called after modifying RawData fields.

```go
func (b *Block) ResetHash() {
    b.hashOnce = sync.Once{}
}
```

---

## 5. Block Producer

### Problem

No goroutine exists to drive block production on schedule.

### Solution

`core/producer/producer.go` — a `Producer` struct that implements `node.Lifecycle`.

```go
type Producer struct {
    chain      *core.BlockChain
    stateDB    *state.Database
    pool       *txpool.TxPool
    engine     *dpos.DPoS
    witnessKey *ecdsa.PrivateKey
    witnessAddr common.Address
    quit       chan struct{}
}

func New(chain, stateDB, pool, engine, witnessKey) *Producer
func (p *Producer) Start() error
func (p *Producer) Stop() error
```

### Production loop

```
Start():
    go p.loop()

loop():
    ticker := time.NewTicker(500ms)  // check twice per slot for precision
    for {
        select {
        case <-ticker.C:
            p.tryProduceBlock()
        case <-p.quit:
            return
        }
    }
```

### tryProduceBlock logic

1. Get current time (ms since epoch)
2. Align to next slot boundary: `nextSlot = align to 3s grid from genesis`
3. If `now` is not within a valid slot window, return
4. Calculate which witness should produce at this slot
5. If it's not our witness address, return
6. Build block via `BuildBlock(chain, stateDB, pool, witnessAddr, slotTimestamp)`
7. Sign block via `SignBlock(block, witnessKey)`
8. Insert block via `chain.InsertBlock(block)`
9. Remove applied transactions from pool
10. Log: "Produced block #N at timestamp T"

### Slot alignment

Use existing slot math:
```go
genesisTime := p.chain.GenesisTimestamp()
now := time.Now().UnixMilli()
slotTimestamp := (now / params.BlockProducedInterval) * params.BlockProducedInterval + genesisTime % params.BlockProducedInterval
```

Actually, simpler: use `AbsoluteSlot(now, genesisTime)` to get slot number, then compute `slotTime = genesisTime + slot * BlockProducedInterval`. If we already produced a block at this slot, skip.

### Duplicate production guard

Track `lastProducedSlot int64`. If `currentSlot == lastProducedSlot`, skip.

---

## 6. Maintenance Integration

### Problem

`DoMaintenance` and `SelectActiveWitnesses` exist but are never called during block processing. Active witnesses never rotate. Standby allowance never distributes.

### Solution

Integrate maintenance into `ProcessBlock` (or into `InsertBlock` after ProcessBlock). At each block, check if the block timestamp crosses the maintenance boundary.

### Where to hook

In `BlockChain.InsertBlock`, after `ProcessBlock`:

```go
// Check maintenance
if dynProps.NextMaintenanceTime() > 0 && block.Timestamp() >= dynProps.NextMaintenanceTime() {
    // Gather all witnesses with votes from StateDB
    allWitnesses := gatherWitnessVotes(statedb)
    dpos.DoMaintenance(chainWriter, block.Timestamp(), allWitnesses)
    newActive := dpos.SelectActiveWitnesses(allWitnesses)
    bc.SetActiveWitnesses(newActive)
    rawdb.WriteActiveWitnesses(bc.db, newActive)
}
```

### gatherWitnessVotes

The StateDB has a `witnesses` map (`map[common.Address]*types.Witness`). We need a way to iterate all witnesses to build `[]dpos.WitnessVote`.

Add to StateDB:
```go
func (s *StateDB) AllWitnesses() []WitnessInfo  // returns addr + voteCount pairs
```

Or simpler: BlockChain tracks witnesses in rawdb and loads them. Actually, StateDB.witnesses is in-memory only (loaded via PutWitness during genesis and block processing). For maintenance, we need all witnesses.

**Approach:** Store a witness index in rawdb (list of all witness addresses). On `PutWitness`, add to the index. During maintenance, load all witness addresses from the index, then read vote counts from StateDB.

### rawdb additions for witness index

```go
func WriteWitnessIndex(db ethdb.KeyValueWriter, witnesses []common.Address)
func ReadWitnessIndex(db ethdb.KeyValueReader) []common.Address
func AppendWitnessIndex(db ethdb.KeyValueStore, addr common.Address)
```

### ChainHeaderWriter adapter

`consensus.ChainHeaderWriter` is needed by `DoMaintenance`. Create an adapter that wraps StateDB + DynProps to satisfy the interface:

```go
type chainHeaderAdapter struct {
    statedb  *state.StateDB
    dynProps *state.DynamicProperties
}
```

This implements `GetWitness`, `PutWitness`, `AddAllowance`, `NextMaintenanceTime`, `SetNextMaintenanceTime`, `WitnessPayPerBlock`, `WitnessStandbyAllowance`, `MaintenanceTimeInterval`.

---

## 7. Resource Integration (Bandwidth)

### Problem

`ResourceProcessor` exists with bandwidth/energy recovery but isn't wired into transaction processing. Transactions cost nothing.

### Solution

Add bandwidth consumption to `ApplyTransaction`. For Phase 4, we implement bandwidth only (energy is for TVM which doesn't exist yet).

### Bandwidth model (simplified for Phase 4)

Each transaction consumes bandwidth = serialized transaction size in bytes.

1. Before executing the transaction, compute `txSize = len(proto.Marshal(tx.Proto()))`
2. Recover the sender's bandwidth usage (sliding window)
3. Check if sender has enough bandwidth capacity:
   - Frozen bandwidth capacity = `frozenBW * totalNetLimit / totalFrozenBW` (simplified: just use `frozenBW / TRXPrecision * 1000` as available bandwidth)
   - Free bandwidth = dynProps `free_net_limit` (default: 1500 bytes/day)
4. Consume: add `txSize` to sender's `NetUsage`, update `LatestConsumeTime`
5. If insufficient bandwidth, charge TRX: `txSize * transactionFee` (where transactionFee = dynProps `transaction_fee`, default 10 sun/byte)

### Where to hook

In `ApplyTransaction`, after `Validate` succeeds but before `Execute`:

```go
func ApplyTransaction(...) (int64, error) {
    // ... create actuator, validate ...
    
    // Consume bandwidth
    if err := consumeBandwidth(statedb, dynProps, tx, blockTime); err != nil {
        return 0, fmt.Errorf("bandwidth: %w", err)
    }
    
    // ... snapshot, execute ...
}
```

### consumeBandwidth function

```go
func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, now int64) error
```

1. Get sender address from transaction contract
2. Compute tx size in bytes
3. Recover bandwidth: `recoverUsage(netUsage, latestConsumeTime, now)`
4. Try frozen bandwidth first:
   - Available = `frozenBW` (from account's BANDWIDTH frozen amount, simplified)
   - If `recoveredUsage + txSize <= available`: consume frozen bandwidth, update NetUsage and LatestConsumeTime
5. If frozen bandwidth insufficient, try free bandwidth:
   - Available = `freeNetLimit` (default 1500)
   - If `recoveredFreeUsage + txSize <= freeNetLimit`: consume free, update FreeNetUsage and LatestConsumeFreeTime
6. If both insufficient, burn TRX:
   - Cost = `txSize * dynProps.TransactionFee()`
   - `statedb.SubBalance(sender, cost)` — if insufficient balance, return error
7. Return nil on success

### StateDB additions needed

```go
func (s *StateDB) SetNetUsage(addr, usage int64)
func (s *StateDB) GetNetUsage(addr) int64
func (s *StateDB) SetLatestConsumeTime(addr, t int64)
func (s *StateDB) GetLatestConsumeTime(addr) int64
func (s *StateDB) SetFreeNetUsage(addr, usage int64)
func (s *StateDB) GetFreeNetUsage(addr) int64
func (s *StateDB) SetLatestConsumeFreeTime(addr, t int64)
func (s *StateDB) GetLatestConsumeFreeTime(addr) int64
```

These follow the same journalAccount + markDirty pattern as all other StateDB mutations.

### DynamicProperties additions needed

```go
func (dp *DynamicProperties) TransactionFee() int64        // key: "transaction_fee", default 10
func (dp *DynamicProperties) FreeNetLimit() int64           // key: "free_net_limit", default 1500
func (dp *DynamicProperties) TotalNetLimit() int64          // key: "total_net_limit"
```

Check if these getters already exist. `TransactionFee` and `TotalNetLimit` likely don't have typed getters yet.

---

## 8. CLI Witness Configuration

### Problem

The block producer needs a witness private key to sign blocks. The node needs to know it's a witness.

### Solution

Add `--witness` flag and `--witness.key` flag to the CLI.

```
gtron --witness --witness.key <hex-encoded-private-key>
```

When `--witness` is set:
1. Parse the private key from hex
2. Derive the witness address via `crypto.PubkeyToAddress`
3. Create and start the `Producer` as a node lifecycle

When `--witness` is not set, the node runs as a non-producing full node (same as Phase 3).

---

## 9. Node Bootstrap Update

### Problem

`cmd/gtron/main.go` needs to wire the DPoS engine and block producer into the node lifecycle.

### Solution

Update the `gtron` action:

```go
func gtron(ctx *cli.Context) error {
    // ... existing: open DB, setup genesis, create blockchain, txpool, backend, api ...
    
    // Create DPoS engine
    engine := dpos.New(bc)
    
    // If witness mode, create producer
    if ctx.Bool("witness") {
        key := parseWitnessKey(ctx)
        producer := producer.New(bc, sdb, pool, engine, key)
        stack.RegisterLifecycle(producer)
    }
    
    // ... start, wait for signal ...
}
```

---

## File Summary

### New files
- `consensus/dpos/engine.go` — DPoS Engine struct implementing consensus.Engine
- `core/block_builder.go` — BuildBlock, SignBlock functions
- `core/block_builder_test.go` — Tests
- `core/producer/producer.go` — Block producer with scheduling loop
- `core/producer/producer_test.go` — Producer tests

### Modified files
- `core/rawdb/accessors.go` — WriteActiveWitnesses, ReadActiveWitnesses, witness index functions
- `core/blockchain.go` — ActiveWitnesses(), SetActiveWitnesses(), maintenance hook in InsertBlock
- `core/state/statedb.go` — Bandwidth getter/setter methods (8 new methods)
- `core/state/dynamic_properties.go` — TransactionFee(), FreeNetLimit(), TotalNetLimit() getters
- `core/state_processor.go` — consumeBandwidth in ApplyTransaction
- `core/types/block.go` — SetWitnessSignature(), ResetHash()
- `cmd/gtron/main.go` — Wire engine + producer, witness flags
- `cmd/gtron/config.go` — parseWitnessKey helper

### Not changed
- No P2P
- No TVM / smart contracts
- No TRC-10 assets
- No energy consumption (only bandwidth; energy is for TVM)
- No gRPC / JSON-RPC
