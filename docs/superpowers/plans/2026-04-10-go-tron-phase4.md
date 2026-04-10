# Phase 4: Block Production + Resource Integration + Maintenance

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make go-tron a self-sufficient single-node chain that produces blocks on schedule, consumes bandwidth per transaction, and runs maintenance to rotate witnesses.

**Architecture:** Add active witness persistence (rawdb + BlockChain), a DPoS engine struct wrapping existing free functions, a block builder that assembles+signs blocks, a block producer goroutine on a 500ms ticker, maintenance hooks in InsertBlock, bandwidth consumption in ApplyTransaction, and CLI witness flags to wire it all together.

**Tech Stack:** Go, protobuf, go-ethereum (ethdb, trie, crypto), urfave/cli/v2

---

## File Structure

### New files
| File | Responsibility |
|---|---|
| `consensus/dpos/engine.go` | DPoS Engine struct implementing `consensus.Engine` |
| `consensus/dpos/engine_test.go` | Engine tests |
| `core/block_builder.go` | `BuildBlock` + `SignBlock` functions |
| `core/block_builder_test.go` | Block builder tests |
| `core/producer/producer.go` | Block producer scheduling loop (`node.Lifecycle`) |
| `core/producer/producer_test.go` | Producer tests |
| `core/bandwidth.go` | `consumeBandwidth` function for tx processing |
| `core/bandwidth_test.go` | Bandwidth consumption tests |

### Modified files
| File | Changes |
|---|---|
| `core/rawdb/schema.go` | Add `activeWitnessesKey`, `witnessIndexKey` key constants |
| `core/rawdb/accessors_chain.go` | Add `WriteActiveWitnesses`, `ReadActiveWitnesses`, `WriteWitnessIndex`, `ReadWitnessIndex`, `AppendWitnessIndex` |
| `core/rawdb/accessors_chain_test.go` | Tests for new rawdb functions |
| `core/blockchain.go` | Add `activeWitnesses atomic.Value`, `ActiveWitnesses()`, `SetActiveWitnesses()`, `NextMaintenanceTime()`, maintenance hook in `InsertBlock` |
| `core/state/statedb.go` | Add 8 bandwidth getter/setter methods |
| `core/state/dynamic_properties.go` | Add `FreeNetLimit()` typed getter |
| `core/state_processor.go` | Wire `consumeBandwidth` into `ApplyTransaction`; change `ProcessBlock` to skip (not fail) bad txs during block building |
| `core/types/block.go` | Add `SetWitnessSignature()`, `SetAccountStateRoot()`, `ResetHash()` |
| `cmd/gtron/main.go` | Wire DPoS engine + producer, add `--witness` and `--witness.key` flags |
| `cmd/gtron/config.go` | Add `parseWitnessKey` helper |

---

### Task 1: Active Witness Persistence (rawdb)

**Files:**
- Modify: `core/rawdb/schema.go`
- Modify: `core/rawdb/accessors_chain.go`
- Create: `core/rawdb/accessors_chain_test.go`

- [ ] **Step 1: Write the failing test for WriteActiveWitnesses/ReadActiveWitnesses**

```go
// core/rawdb/accessors_chain_test.go
package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func testAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestActiveWitnesses(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	// Empty read returns nil
	got := ReadActiveWitnesses(db)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}

	// Write and read back
	witnesses := []common.Address{testAddr(1), testAddr(2), testAddr(3)}
	WriteActiveWitnesses(db, witnesses)

	got = ReadActiveWitnesses(db)
	if len(got) != 3 {
		t.Fatalf("expected 3 witnesses, got %d", len(got))
	}
	for i, w := range got {
		if w != witnesses[i] {
			t.Fatalf("witness %d: want %x, got %x", i, witnesses[i], w)
		}
	}

	// Overwrite with different list
	witnesses2 := []common.Address{testAddr(4)}
	WriteActiveWitnesses(db, witnesses2)
	got = ReadActiveWitnesses(db)
	if len(got) != 1 || got[0] != testAddr(4) {
		t.Fatalf("overwrite failed: got %v", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run TestActiveWitnesses -v`
Expected: FAIL — `ReadActiveWitnesses` undefined

- [ ] **Step 3: Add schema keys**

In `core/rawdb/schema.go`, add after the `witnessScheduleKey` line:

```go
	activeWitnessesKey = []byte("ActiveWitnesses")
	witnessIndexKey    = []byte("WitnessIndex")
```

- [ ] **Step 4: Implement WriteActiveWitnesses and ReadActiveWitnesses**

In `core/rawdb/accessors_chain.go`, add:

```go
// WriteActiveWitnesses stores the active witness list as length-prefixed addresses.
func WriteActiveWitnesses(db ethdb.KeyValueWriter, witnesses []common.Address) {
	buf := make([]byte, 4+len(witnesses)*common.AddressLength)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(witnesses)))
	for i, w := range witnesses {
		copy(buf[4+i*common.AddressLength:], w.Bytes())
	}
	db.Put(activeWitnessesKey, buf)
}

// ReadActiveWitnesses loads the active witness list from the database.
func ReadActiveWitnesses(db ethdb.KeyValueReader) []common.Address {
	data, err := db.Get(activeWitnessesKey)
	if err != nil || len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+count*common.AddressLength {
		return nil
	}
	witnesses := make([]common.Address, count)
	for i := 0; i < count; i++ {
		witnesses[i] = common.BytesToAddress(data[4+i*common.AddressLength : 4+(i+1)*common.AddressLength])
	}
	return witnesses
}
```

Add the needed imports at the top of `accessors_chain.go`:

```go
import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run TestActiveWitnesses -v`
Expected: PASS

- [ ] **Step 6: Write the failing test for WitnessIndex (witness address tracking)**

Append to `core/rawdb/accessors_chain_test.go`:

```go
func TestWitnessIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()

	// Empty read
	got := ReadWitnessIndex(db)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}

	// Append witnesses one by one
	AppendWitnessIndex(db, testAddr(1))
	AppendWitnessIndex(db, testAddr(2))
	AppendWitnessIndex(db, testAddr(3))

	got = ReadWitnessIndex(db)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[0] != testAddr(1) || got[1] != testAddr(2) || got[2] != testAddr(3) {
		t.Fatalf("unexpected witnesses: %v", got)
	}

	// Append duplicate — should not add
	AppendWitnessIndex(db, testAddr(2))
	got = ReadWitnessIndex(db)
	if len(got) != 3 {
		t.Fatalf("duplicate added: got %d", len(got))
	}
}
```

- [ ] **Step 7: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run TestWitnessIndex -v`
Expected: FAIL — `ReadWitnessIndex` undefined

- [ ] **Step 8: Implement WitnessIndex functions**

Append to `core/rawdb/accessors_chain.go`:

```go
// WriteWitnessIndex stores the complete witness index (all known witness addresses).
func WriteWitnessIndex(db ethdb.KeyValueWriter, witnesses []common.Address) {
	buf := make([]byte, 4+len(witnesses)*common.AddressLength)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(witnesses)))
	for i, w := range witnesses {
		copy(buf[4+i*common.AddressLength:], w.Bytes())
	}
	db.Put(witnessIndexKey, buf)
}

// ReadWitnessIndex loads all known witness addresses from the database.
func ReadWitnessIndex(db ethdb.KeyValueReader) []common.Address {
	data, err := db.Get(witnessIndexKey)
	if err != nil || len(data) < 4 {
		return nil
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	if len(data) < 4+count*common.AddressLength {
		return nil
	}
	witnesses := make([]common.Address, count)
	for i := 0; i < count; i++ {
		witnesses[i] = common.BytesToAddress(data[4+i*common.AddressLength : 4+(i+1)*common.AddressLength])
	}
	return witnesses
}

// AppendWitnessIndex adds a witness address to the index if not already present.
func AppendWitnessIndex(db ethdb.KeyValueStore, addr common.Address) {
	existing := ReadWitnessIndex(db)
	for _, w := range existing {
		if w == addr {
			return // already in index
		}
	}
	existing = append(existing, addr)
	WriteWitnessIndex(db, existing)
}
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/rawdb/ -run "TestActiveWitnesses|TestWitnessIndex" -v`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add core/rawdb/schema.go core/rawdb/accessors_chain.go core/rawdb/accessors_chain_test.go
git commit -m "core/rawdb: add active witness and witness index persistence"
```

---

### Task 2: BlockChain Active Witness Support

**Files:**
- Modify: `core/blockchain.go`
- Modify: `core/blockchain_test.go`

This task adds `ActiveWitnesses()` and `NextMaintenanceTime()` to `BlockChain`, satisfying the `consensus.ChainReader` interface.

- [ ] **Step 1: Write the failing test**

Append to `core/blockchain_test.go`:

```go
func TestBlockChainActiveWitnesses(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config: params.MainnetChainConfig,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: testCoreAddr(10), VoteCount: 100, URL: "http://w1"},
			{Address: testCoreAddr(11), VoteCount: 200, URL: "http://w2"},
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// On a fresh chain with genesis witnesses, ActiveWitnesses should be derived
	witnesses := bc.ActiveWitnesses()
	if len(witnesses) == 0 {
		t.Fatal("expected non-empty active witnesses")
	}

	// SetActiveWitnesses should update and persist
	newList := []tcommon.Address{testCoreAddr(20), testCoreAddr(21)}
	bc.SetActiveWitnesses(newList)

	got := bc.ActiveWitnesses()
	if len(got) != 2 || got[0] != testCoreAddr(20) || got[1] != testCoreAddr(21) {
		t.Fatalf("unexpected witnesses after set: %v", got)
	}

	// Verify persistence: read directly from rawdb
	persisted := rawdb.ReadActiveWitnesses(diskdb)
	if len(persisted) != 2 {
		t.Fatalf("expected 2 persisted witnesses, got %d", len(persisted))
	}
}

func TestBlockChainNextMaintenanceTime(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 1000,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 1000},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 100000,
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	if bc.NextMaintenanceTime() != 100000 {
		t.Fatalf("expected 100000, got %d", bc.NextMaintenanceTime())
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestBlockChainActiveWitnesses|TestBlockChainNextMaintenanceTime" -v`
Expected: FAIL — `ActiveWitnesses` undefined

- [ ] **Step 3: Implement ActiveWitnesses, SetActiveWitnesses, NextMaintenanceTime on BlockChain**

Modify `core/blockchain.go`:

1. Add `activeWitnesses atomic.Value` field to the `BlockChain` struct (after `genesisBlock`):

```go
type BlockChain struct {
	db      ethdb.KeyValueStore
	stateDB *state.Database
	config  *params.ChainConfig

	currentBlock atomic.Pointer[types.Block]
	chainmu      sync.Mutex // serializes block insertion

	genesisBlock    *types.Block
	activeWitnesses atomic.Value // []common.Address
}
```

2. In `NewBlockChain`, after loading the head block and before the `return bc, nil`, add:

```go
	// Load active witnesses from DB; if empty, derive from genesis witnesses via rawdb
	witnesses := rawdb.ReadActiveWitnesses(db)
	if len(witnesses) == 0 {
		// Derive from genesis witnesses stored in rawdb
		var allWitnesses []dpos.WitnessVote
		// Read witness index for all known witness addresses
		witnessAddrs := rawdb.ReadWitnessIndex(db)
		for _, addr := range witnessAddrs {
			w := rawdb.ReadWitness(db, addr)
			if w != nil {
				allWitnesses = append(allWitnesses, dpos.WitnessVote{
					Address: w.Address(),
					Votes:   w.VoteCount(),
				})
			}
		}
		if len(allWitnesses) > 0 {
			witnesses = dpos.SelectActiveWitnesses(allWitnesses)
			rawdb.WriteActiveWitnesses(db, witnesses)
		}
	}
	if len(witnesses) > 0 {
		bc.activeWitnesses.Store(witnesses)
	}
```

3. Add the new import for `dpos`:

```go
import (
	// ... existing imports ...
	"github.com/tronprotocol/go-tron/consensus/dpos"
)
```

4. Add the three new methods:

```go
// ActiveWitnesses returns the current active witness list.
func (bc *BlockChain) ActiveWitnesses() []common.Address {
	v := bc.activeWitnesses.Load()
	if v == nil {
		return nil
	}
	return v.([]common.Address)
}

// SetActiveWitnesses updates the active witness list in memory and rawdb.
func (bc *BlockChain) SetActiveWitnesses(witnesses []common.Address) {
	bc.activeWitnesses.Store(witnesses)
	rawdb.WriteActiveWitnesses(bc.db, witnesses)
}

// NextMaintenanceTime returns the next maintenance timestamp from dynamic properties.
func (bc *BlockChain) NextMaintenanceTime() int64 {
	dynProps := state.LoadDynamicProperties(bc.db)
	return dynProps.NextMaintenanceTime()
}
```

Note: the import alias for common should use `tcommon` consistent with the rest of the file.

5. Also need to write the witness index during `SetupGenesisBlock`. Modify `core/genesis.go` — in the `SetupGenesisBlock` function, in the witnesses loop, add `AppendWitnessIndex`:

```go
	// Write witnesses
	for _, gw := range genesis.Witnesses {
		w := types.NewWitness(gw.Address, gw.URL)
		w.SetVoteCount(gw.VoteCount)
		rawdb.WriteWitness(db, gw.Address, w)
		rawdb.AppendWitnessIndex(db, gw.Address)
	}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestBlockChainActiveWitnesses|TestBlockChainNextMaintenanceTime" -v`
Expected: PASS

- [ ] **Step 5: Run all existing tests to confirm no regressions**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/... -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add core/blockchain.go core/blockchain_test.go core/genesis.go
git commit -m "core: add ActiveWitnesses and NextMaintenanceTime to BlockChain"
```

---

### Task 3: DPoS Engine Struct

**Files:**
- Create: `consensus/dpos/engine.go`
- Create: `consensus/dpos/engine_test.go`

The `DPoS` struct implements `consensus.Engine` by delegating to existing free functions.

- [ ] **Step 1: Write the failing test**

```go
// consensus/dpos/engine_test.go
package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// mockChainReader implements consensus.ChainReader for testing.
type mockChainReader struct {
	currentBlock   *types.Block
	genesisTime    int64
	witnesses      []common.Address
	maintTime      int64
}

func (m *mockChainReader) CurrentBlock() *types.Block       { return m.currentBlock }
func (m *mockChainReader) GetBlockByNumber(uint64) *types.Block { return nil }
func (m *mockChainReader) GenesisTimestamp() int64           { return m.genesisTime }
func (m *mockChainReader) ActiveWitnesses() []common.Address { return m.witnesses }
func (m *mockChainReader) NextMaintenanceTime() int64        { return m.maintTime }

func testEngineAddr(b byte) common.Address {
	var addr common.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestEngine_GetScheduledWitness(t *testing.T) {
	witnesses := []common.Address{testEngineAddr(1), testEngineAddr(2), testEngineAddr(3)}
	chain := &mockChainReader{
		currentBlock: types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:    0,
					Timestamp: 0,
				},
			},
		}),
		genesisTime: 0,
		witnesses:   witnesses,
	}

	engine := New(chain)

	// Slot 1 should map to one of the witnesses
	addr, err := engine.GetScheduledWitness(1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range witnesses {
		if w == addr {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("scheduled witness %x not in witness list", addr)
	}
}

func TestEngine_GetScheduledWitness_NoWitnesses(t *testing.T) {
	chain := &mockChainReader{
		currentBlock: types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{Number: 0, Timestamp: 0},
			},
		}),
		genesisTime: 0,
		witnesses:   nil,
	}

	engine := New(chain)
	_, err := engine.GetScheduledWitness(1)
	if err == nil {
		t.Fatal("expected error with no witnesses")
	}
}

func TestEngine_IsInMaintenance(t *testing.T) {
	chain := &mockChainReader{
		maintTime: 100000,
	}
	engine := New(chain)

	if engine.IsInMaintenance(99999) {
		t.Fatal("should not be in maintenance before maintenance time")
	}
	if !engine.IsInMaintenance(100000) {
		t.Fatal("should be in maintenance at maintenance time")
	}
	if !engine.IsInMaintenance(100001) {
		t.Fatal("should be in maintenance after maintenance time")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./consensus/dpos/ -run "TestEngine_" -v`
Expected: FAIL — `New` undefined

- [ ] **Step 3: Implement the DPoS engine**

```go
// consensus/dpos/engine.go
package dpos

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

var ErrNoActiveWitnesses = errors.New("no active witnesses")

// DPoS implements consensus.Engine using delegated proof-of-stake.
type DPoS struct {
	chain consensus.ChainReader
}

// New creates a DPoS engine.
func New(chain consensus.ChainReader) *DPoS {
	return &DPoS{chain: chain}
}

func (d *DPoS) VerifyHeader(chain consensus.ChainReader, block *types.Block) error {
	return VerifyHeader(chain, block)
}

func (d *DPoS) GetScheduledWitness(slot int64) (common.Address, error) {
	witnesses := d.chain.ActiveWitnesses()
	if len(witnesses) == 0 {
		return common.Address{}, ErrNoActiveWitnesses
	}
	head := d.chain.CurrentBlock()
	addr := GetScheduledWitness(slot, head.Timestamp(), d.chain.GenesisTimestamp(), witnesses,
		d.IsInMaintenance(head.Timestamp()), params.MaintenanceSkipSlots)
	return addr, nil
}

func (d *DPoS) IsInMaintenance(timestamp int64) bool {
	maintTime := d.chain.NextMaintenanceTime()
	if maintTime <= 0 {
		return false
	}
	return timestamp >= maintTime
}

func (d *DPoS) DoMaintenance(chain consensus.ChainHeaderWriter) error {
	// Caller must provide allWitnesses; this delegates to the free function.
	// This method signature matches the Engine interface but doesn't have witness data.
	// The actual maintenance call is in BlockChain.InsertBlock with full witness data.
	// This is a no-op here; maintenance is driven by InsertBlock directly.
	return nil
}

func (d *DPoS) PayBlockReward(chain consensus.ChainHeaderWriter, witness common.Address) {
	PayBlockReward(chain, witness)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./consensus/dpos/ -run "TestEngine_" -v`
Expected: PASS

- [ ] **Step 5: Run all consensus tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./consensus/... -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add consensus/dpos/engine.go consensus/dpos/engine_test.go
git commit -m "consensus/dpos: add Engine struct implementing consensus.Engine"
```

---

### Task 4: Block Type Mutations (SetWitnessSignature, SetAccountStateRoot, ResetHash)

**Files:**
- Modify: `core/types/block.go`
- Modify: `core/types/block_test.go`

- [ ] **Step 1: Write the failing test**

Append to `core/types/block_test.go`:

```go
func TestBlock_SetWitnessSignature(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	sig := make([]byte, 65)
	sig[0] = 0xAA
	block.SetWitnessSignature(sig)

	if got := block.WitnessSignature(); len(got) != 65 || got[0] != 0xAA {
		t.Fatalf("unexpected signature: %x", got)
	}
}

func TestBlock_SetAccountStateRoot(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1},
		},
	})

	var root common.Hash
	root[0] = 0xBB
	block.SetAccountStateRoot(root)

	if block.AccountStateRoot() != root {
		t.Fatalf("expected root %x, got %x", root, block.AccountStateRoot())
	}
}

func TestBlock_ResetHash(t *testing.T) {
	block := NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3000},
		},
	})

	hash1 := block.Hash()

	// Modify RawData
	block.Proto().BlockHeader.RawData.Timestamp = 6000
	// Hash is cached, so it won't change yet
	if block.Hash() != hash1 {
		t.Fatal("hash should be cached")
	}

	// ResetHash forces recomputation
	block.ResetHash()
	hash2 := block.Hash()
	if hash2 == hash1 {
		t.Fatal("hash should change after ResetHash + modified RawData")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/types/ -run "TestBlock_SetWitnessSignature|TestBlock_SetAccountStateRoot|TestBlock_ResetHash" -v`
Expected: FAIL — `SetWitnessSignature` undefined

- [ ] **Step 3: Implement the three methods**

Add to `core/types/block.go`:

```go
// SetWitnessSignature sets the witness signature on the block header.
func (b *Block) SetWitnessSignature(sig []byte) {
	if b.pb.BlockHeader == nil {
		b.pb.BlockHeader = &corepb.BlockHeader{}
	}
	b.pb.BlockHeader.WitnessSignature = sig
}

// SetAccountStateRoot sets the account state root in the block header raw data.
func (b *Block) SetAccountStateRoot(root common.Hash) {
	if b.pb.BlockHeader == nil {
		b.pb.BlockHeader = &corepb.BlockHeader{}
	}
	if b.pb.BlockHeader.RawData == nil {
		b.pb.BlockHeader.RawData = &corepb.BlockHeaderRaw{}
	}
	b.pb.BlockHeader.RawData.AccountStateRoot = root.Bytes()
}

// ResetHash clears the cached hash so it will be recomputed on next Hash() call.
func (b *Block) ResetHash() {
	b.hashOnce = sync.Once{}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/types/ -run "TestBlock_" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/types/block.go core/types/block_test.go
git commit -m "core/types: add SetWitnessSignature, SetAccountStateRoot, ResetHash to Block"
```

---

### Task 5: StateDB Bandwidth Methods

**Files:**
- Modify: `core/state/statedb.go`
- Modify: `core/state/statedb_test.go`

Add 8 bandwidth getter/setter methods to StateDB following the existing journal+markDirty pattern.

- [ ] **Step 1: Write the failing test**

Append to `core/state/statedb_test.go`:

```go
func TestStateDB_BandwidthMethods(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testStateAddr(1)
	statedb.GetOrCreateAccount(addr)

	// NetUsage
	if statedb.GetNetUsage(addr) != 0 {
		t.Fatal("initial NetUsage should be 0")
	}
	statedb.SetNetUsage(addr, 500)
	if statedb.GetNetUsage(addr) != 500 {
		t.Fatalf("NetUsage: want 500, got %d", statedb.GetNetUsage(addr))
	}

	// LatestConsumeTime
	statedb.SetLatestConsumeTime(addr, 3000)
	if statedb.GetLatestConsumeTime(addr) != 3000 {
		t.Fatalf("LatestConsumeTime: want 3000, got %d", statedb.GetLatestConsumeTime(addr))
	}

	// FreeNetUsage
	statedb.SetFreeNetUsage(addr, 200)
	if statedb.GetFreeNetUsage(addr) != 200 {
		t.Fatalf("FreeNetUsage: want 200, got %d", statedb.GetFreeNetUsage(addr))
	}

	// LatestConsumeFreeTime
	statedb.SetLatestConsumeFreeTime(addr, 6000)
	if statedb.GetLatestConsumeFreeTime(addr) != 6000 {
		t.Fatalf("LatestConsumeFreeTime: want 6000, got %d", statedb.GetLatestConsumeFreeTime(addr))
	}
}

func TestStateDB_BandwidthRevert(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testStateAddr(1)
	statedb.GetOrCreateAccount(addr)
	statedb.SetNetUsage(addr, 100)

	snap := statedb.Snapshot()
	statedb.SetNetUsage(addr, 999)
	if statedb.GetNetUsage(addr) != 999 {
		t.Fatalf("want 999 after set, got %d", statedb.GetNetUsage(addr))
	}

	statedb.RevertToSnapshot(snap)
	if statedb.GetNetUsage(addr) != 100 {
		t.Fatalf("want 100 after revert, got %d", statedb.GetNetUsage(addr))
	}
}
```

Note: The test uses helpers `newTestStateDB` and `testStateAddr` which should already exist in `statedb_test.go`. If not, check the existing test file for the equivalent helper names and adjust accordingly.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run "TestStateDB_Bandwidth" -v`
Expected: FAIL — `GetNetUsage` undefined

- [ ] **Step 3: Implement the 8 bandwidth methods**

Add to `core/state/statedb.go`:

```go
// GetNetUsage returns the frozen bandwidth usage for the account.
func (s *StateDB) GetNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.NetUsage()
}

// SetNetUsage sets the frozen bandwidth usage.
func (s *StateDB) SetNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetNetUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeTime returns the latest frozen bandwidth consume timestamp.
func (s *StateDB) GetLatestConsumeTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeTime()
}

// SetLatestConsumeTime sets the latest frozen bandwidth consume timestamp.
func (s *StateDB) SetLatestConsumeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeTime(t)
	obj.markDirty()
}

// GetFreeNetUsage returns the free bandwidth usage.
func (s *StateDB) GetFreeNetUsage(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.FreeNetUsage()
}

// SetFreeNetUsage sets the free bandwidth usage.
func (s *StateDB) SetFreeNetUsage(addr tcommon.Address, usage int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetFreeNetUsage(usage)
	obj.markDirty()
}

// GetLatestConsumeFreeTime returns the latest free bandwidth consume timestamp.
func (s *StateDB) GetLatestConsumeFreeTime(addr tcommon.Address) int64 {
	obj := s.getStateObject(addr)
	if obj == nil {
		return 0
	}
	return obj.account.LatestConsumeFreeTime()
}

// SetLatestConsumeFreeTime sets the latest free bandwidth consume timestamp.
func (s *StateDB) SetLatestConsumeFreeTime(addr tcommon.Address, t int64) {
	obj := s.getStateObject(addr)
	if obj == nil {
		return
	}
	s.journalAccount(addr, obj)
	obj.account.SetLatestConsumeFreeTime(t)
	obj.markDirty()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run "TestStateDB_Bandwidth" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/state/statedb.go core/state/statedb_test.go
git commit -m "core/state: add bandwidth getter/setter methods to StateDB"
```

---

### Task 6: DynamicProperties FreeNetLimit Getter

**Files:**
- Modify: `core/state/dynamic_properties.go`
- Modify: `core/state/dynamic_properties_test.go`

- [ ] **Step 1: Write the failing test**

Append to `core/state/dynamic_properties_test.go`:

```go
func TestDynamicProperties_FreeNetLimit(t *testing.T) {
	dp := NewDynamicProperties()

	// Default "free_net_limit" is not in defaultProps, so we need to add it.
	// Check: if it IS already there, test the default. If not, default should be 1500.
	got := dp.FreeNetLimit()
	if got != 1500 {
		t.Fatalf("default FreeNetLimit: want 1500, got %d", got)
	}

	dp.Set("free_net_limit", 3000)
	if dp.FreeNetLimit() != 3000 {
		t.Fatalf("after set: want 3000, got %d", dp.FreeNetLimit())
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run TestDynamicProperties_FreeNetLimit -v`
Expected: FAIL — `FreeNetLimit` undefined

- [ ] **Step 3: Implement**

1. Add `"free_net_limit": 1500` to the `defaultProps` map in `core/state/dynamic_properties.go`:

```go
var defaultProps = map[string]int64{
	// ... existing entries ...
	"free_net_limit":                            1500,
}
```

2. Add the typed getter after the existing getters:

```go
func (dp *DynamicProperties) FreeNetLimit() int64 {
	return dp.props["free_net_limit"]
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/state/ -run TestDynamicProperties_FreeNetLimit -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/state/dynamic_properties.go core/state/dynamic_properties_test.go
git commit -m "core/state: add FreeNetLimit to DynamicProperties"
```

---

### Task 7: Bandwidth Consumption

**Files:**
- Create: `core/bandwidth.go`
- Create: `core/bandwidth_test.go`
- Modify: `core/state_processor.go`

- [ ] **Step 1: Write the failing test**

```go
// core/bandwidth_test.go
package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestConsumeBandwidth_FreeBandwidth(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	// Should have consumed free bandwidth
	if statedb.GetFreeNetUsage(sender) != txSize {
		t.Fatalf("free net usage: want %d, got %d", txSize, statedb.GetFreeNetUsage(sender))
	}
	if statedb.GetLatestConsumeFreeTime(sender) != 3000 {
		t.Fatalf("latest consume free time: want 3000, got %d", statedb.GetLatestConsumeFreeTime(sender))
	}
}

func TestConsumeBandwidth_FrozenBandwidth(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)
	// Freeze bandwidth: 1 TRX = 1_000_000 sun
	statedb.AddFreezeV2(sender, corepb.ResourceCode_BANDWIDTH, 1_000_000)

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	// Should have consumed frozen bandwidth (not free)
	if statedb.GetNetUsage(sender) != txSize {
		t.Fatalf("net usage: want %d, got %d", txSize, statedb.GetNetUsage(sender))
	}
	if statedb.GetFreeNetUsage(sender) != 0 {
		t.Fatalf("free net usage should be 0, got %d", statedb.GetFreeNetUsage(sender))
	}
}

func TestConsumeBandwidth_BurnTRX(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 10_000_000)

	// Fill up free bandwidth completely
	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, 3000) // same as block time, no recovery

	tx := makeTestTransferTx(1, 2, 100)
	txSize := int64(tx.Size())

	balBefore := statedb.GetBalance(sender)
	err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err != nil {
		t.Fatalf("consumeBandwidth failed: %v", err)
	}

	// Should have burned TRX
	expectedCost := txSize * dynProps.TransactionFee()
	balAfter := statedb.GetBalance(sender)
	if balBefore-balAfter != expectedCost {
		t.Fatalf("TRX burn: want %d, got %d", expectedCost, balBefore-balAfter)
	}
}

func TestConsumeBandwidth_InsufficientBalance(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	sender := testProcessorAddr(1)
	statedb.CreateAccount(sender, corepb.AccountType_Normal)
	statedb.AddBalance(sender, 1) // very low balance

	// Fill up free bandwidth
	statedb.SetFreeNetUsage(sender, dynProps.FreeNetLimit())
	statedb.SetLatestConsumeFreeTime(sender, 3000)

	tx := makeTestTransferTx(1, 2, 0)
	err := consumeBandwidth(statedb, dynProps, tx, 3000)
	if err == nil {
		t.Fatal("expected error for insufficient balance to pay bandwidth")
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestConsumeBandwidth" -v`
Expected: FAIL — `consumeBandwidth` undefined

- [ ] **Step 3: Implement consumeBandwidth**

```go
// core/bandwidth.go
package core

import (
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// consumeBandwidth charges bandwidth for a transaction.
// Priority: frozen bandwidth -> free bandwidth -> burn TRX.
func consumeBandwidth(statedb *state.StateDB, dynProps *state.DynamicProperties, tx *types.Transaction, blockTime int64) error {
	sender := extractSender(tx)
	if sender == (tcommon.Address{}) {
		return fmt.Errorf("cannot determine sender")
	}

	txSize := int64(tx.Size())

	// Try frozen bandwidth first
	frozenBW := statedb.GetFrozenV2Amount(sender, corepb.ResourceCode_BANDWIDTH)
	if frozenBW > 0 {
		recoveredUsage := recoverUsage(statedb.GetNetUsage(sender), statedb.GetLatestConsumeTime(sender), blockTime)
		if recoveredUsage+txSize <= frozenBW {
			statedb.SetNetUsage(sender, recoveredUsage+txSize)
			statedb.SetLatestConsumeTime(sender, blockTime)
			return nil
		}
	}

	// Try free bandwidth
	freeLimit := dynProps.FreeNetLimit()
	recoveredFreeUsage := recoverUsage(statedb.GetFreeNetUsage(sender), statedb.GetLatestConsumeFreeTime(sender), blockTime)
	if recoveredFreeUsage+txSize <= freeLimit {
		statedb.SetFreeNetUsage(sender, recoveredFreeUsage+txSize)
		statedb.SetLatestConsumeFreeTime(sender, blockTime)
		return nil
	}

	// Burn TRX
	cost := txSize * dynProps.TransactionFee()
	if err := statedb.SubBalance(sender, cost); err != nil {
		return fmt.Errorf("insufficient balance to pay bandwidth: need %d sun", cost)
	}
	return nil
}

// recoverUsage computes usage after sliding window recovery.
// This is the same formula as core.recoverUsage in resource.go.
func recoverUsage(oldUsage, lastTime, now int64) int64 {
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

// extractSender extracts the owner address from the first contract of a transaction.
func extractSender(tx *types.Transaction) tcommon.Address {
	contract := tx.Contract()
	if contract == nil {
		return tcommon.Address{}
	}
	// The owner address is in the contract parameter. We need to extract it
	// based on the contract type. For simplicity, we extract directly from
	// the serialized protobuf — the owner_address field is field 1 (tag bytes
	// 0x0a = field 1, wire type 2 = length-delimited) in all contract types.
	// A cleaner approach: unmarshal the Any and read OwnerAddress.
	// Let's use the actuator approach: unmarshal based on type.
	msg, err := contract.Parameter.UnmarshalNew()
	if err != nil {
		return tcommon.Address{}
	}
	// All TRON contracts have an owner_address field. Use reflection or type switch.
	// Since we already have actuator imports that do this, let's use a type switch
	// over the known contract message types.
	type ownerAddressGetter interface {
		GetOwnerAddress() []byte
	}
	if oag, ok := msg.(ownerAddressGetter); ok {
		return tcommon.BytesToAddress(oag.GetOwnerAddress())
	}
	return tcommon.Address{}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestConsumeBandwidth" -v`
Expected: PASS

- [ ] **Step 5: Wire consumeBandwidth into ApplyTransaction**

Modify `core/state_processor.go`. In `ApplyTransaction`, add the bandwidth consumption call after validation succeeds but before the snapshot+execute:

```go
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

	// Consume bandwidth
	if err := consumeBandwidth(statedb, dynProps, tx, blockTime); err != nil {
		return 0, fmt.Errorf("bandwidth: %w", err)
	}

	snap := statedb.Snapshot()
	result, err := act.Execute(ctx)
	if err != nil {
		statedb.RevertToSnapshot(snap)
		return 0, fmt.Errorf("execute: %w", err)
	}

	return result.Fee, nil
}
```

- [ ] **Step 6: Run all core tests to verify no regressions**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/... -count=1`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add core/bandwidth.go core/bandwidth_test.go core/state_processor.go
git commit -m "core: add bandwidth consumption to transaction processing"
```

---

### Task 8: Block Builder and Signing

**Files:**
- Create: `core/block_builder.go`
- Create: `core/block_builder_test.go`

- [ ] **Step 1: Write the failing test for BuildBlock**

```go
// core/block_builder_test.go
package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestBuildBlock_EmptyPool(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 10_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	witnessAddr := testProcessorAddr(0xFF)

	block, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	if block.Number() != 1 {
		t.Fatalf("block number: want 1, got %d", block.Number())
	}
	if block.Timestamp() != 3000 {
		t.Fatalf("timestamp: want 3000, got %d", block.Timestamp())
	}
	if block.WitnessAddress() != witnessAddr {
		t.Fatalf("witness: want %x, got %x", witnessAddr, block.WitnessAddress())
	}
	if block.AccountStateRoot() == (tcommon.Hash{}) {
		t.Fatal("expected non-empty state root")
	}
	if len(block.Transactions()) != 0 {
		t.Fatalf("expected 0 transactions, got %d", len(block.Transactions()))
	}
}

func TestBuildBlock_WithTransactions(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	sender := testProcessorAddr(1)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: sender, Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	tx := makeTestTransferTx(1, 2, 1_000_000)
	pool.Add(tx)

	witnessAddr := testProcessorAddr(0xFF)
	block, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	if len(block.Transactions()) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(block.Transactions()))
	}
}

func TestBuildBlock_SkipsFailingTx(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testProcessorAddr(1), Balance: 100_000_000},
		},
	}
	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	// Good tx
	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	pool.Add(tx1)
	// Bad tx — sender 3 doesn't exist, will fail validation
	tx2 := makeTestTransferTx(3, 4, 1_000_000)
	pool.Add(tx2)

	witnessAddr := testProcessorAddr(0xFF)
	block, err := BuildBlock(bc, pool, witnessAddr, 3000)
	if err != nil {
		t.Fatal(err)
	}

	// Should include only the good tx
	if len(block.Transactions()) != 1 {
		t.Fatalf("expected 1 transaction (skipped failing), got %d", len(block.Transactions()))
	}
}

func TestSignBlock(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
	})

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	err = SignBlock(block, key)
	if err != nil {
		t.Fatal(err)
	}

	sig := block.WitnessSignature()
	if len(sig) != 65 {
		t.Fatalf("signature length: want 65, got %d", len(sig))
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestBuildBlock|TestSignBlock" -v`
Expected: FAIL — `BuildBlock` undefined

- [ ] **Step 3: Implement BuildBlock and SignBlock**

```go
// core/block_builder.go
package core

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"google.golang.org/protobuf/proto"
)

// BuildBlock assembles a new block from pending transactions.
// Failing transactions are skipped rather than aborting the block.
// The returned block is unsigned — call SignBlock separately.
func BuildBlock(bc *BlockChain, pool *txpool.TxPool, witnessAddr tcommon.Address, timestamp int64) (*types.Block, error) {
	parent := bc.CurrentBlock()

	// Open StateDB from parent's state root
	parentRoot := parent.AccountStateRoot()
	statedb, err := state.New(parentRoot, bc.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}

	dynProps := state.LoadDynamicProperties(bc.db)

	// Pull all pending transactions
	pendingTxs := pool.Pending()

	// Execute transactions, collecting successful ones
	var appliedTxProtos []*corepb.Transaction
	var appliedHashes []tcommon.Hash
	blockNum := parent.Number() + 1

	for _, tx := range pendingTxs {
		_, err := ApplyTransaction(statedb, dynProps, tx, timestamp, blockNum)
		if err != nil {
			continue // skip failing transactions
		}
		appliedTxProtos = append(appliedTxProtos, tx.Proto())
		appliedHashes = append(appliedHashes, tx.Hash())
	}

	// Pay block reward to witness
	reward := dynProps.WitnessPayPerBlock()
	if reward > 0 {
		statedb.AddAllowance(witnessAddr, reward)
	}

	// Commit state to get the root
	root, err := statedb.Commit()
	if err != nil {
		return nil, fmt.Errorf("commit state: %w", err)
	}

	// Construct the block
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:           int64(blockNum),
				Timestamp:        timestamp,
				ParentHash:       parent.Hash().Bytes(),
				WitnessAddress:   witnessAddr.Bytes(),
				AccountStateRoot: root.Bytes(),
			},
		},
		Transactions: appliedTxProtos,
	})

	return block, nil
}

// SignBlock signs the block with the witness private key.
// The signature is SHA256(marshaled BlockHeaderRaw) signed with ECDSA.
func SignBlock(block *types.Block, privKey *ecdsa.PrivateKey) error {
	headerRaw := block.Proto().BlockHeader.RawData
	data, err := proto.Marshal(headerRaw)
	if err != nil {
		return fmt.Errorf("marshal header: %w", err)
	}

	hash := sha256.Sum256(data)
	sig, err := crypto.Sign(hash[:], privKey)
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}

	block.SetWitnessSignature(sig)
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run "TestBuildBlock|TestSignBlock" -v`
Expected: PASS

- [ ] **Step 5: Run all core tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/... -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add core/block_builder.go core/block_builder_test.go
git commit -m "core: add BuildBlock and SignBlock for block production"
```

---

### Task 9: Maintenance Integration in InsertBlock

**Files:**
- Modify: `core/blockchain.go`
- Modify: `core/blockchain_insert_test.go` (or `core/blockchain_test.go`)

- [ ] **Step 1: Write the failing test**

Append to `core/blockchain_test.go`:

```go
func TestBlockChainInsertBlock_Maintenance(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := testCoreAddr(10)
	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: testCoreAddr(1), Balance: 100_000_000},
			{Address: witnessAddr, Balance: 1_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://w1"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 6000, // maintenance at timestamp 6000
		},
	}

	SetupGenesisBlock(diskdb, genesis)
	bc, err := NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Build block 1 at timestamp 3000 (before maintenance)
	block1 := buildTestBlock(bc, witnessAddr, 3000)
	if err := bc.InsertBlock(block1); err != nil {
		t.Fatal(err)
	}

	// Check: maintenance should NOT have run yet
	dynProps := state.LoadDynamicProperties(diskdb)
	if dynProps.NextMaintenanceTime() != 6000 {
		t.Fatalf("maintenance should not have run yet, next_maintenance_time should be 6000, got %d", dynProps.NextMaintenanceTime())
	}

	// Build block 2 at timestamp 6000 (at maintenance boundary)
	block2 := buildTestBlock(bc, witnessAddr, 6000)
	if err := bc.InsertBlock(block2); err != nil {
		t.Fatal(err)
	}

	// Check: maintenance should have advanced the next_maintenance_time
	dynProps = state.LoadDynamicProperties(diskdb)
	if dynProps.NextMaintenanceTime() <= 6000 {
		t.Fatalf("next_maintenance_time should have advanced past 6000, got %d", dynProps.NextMaintenanceTime())
	}
}

// buildTestBlock creates a minimal block for testing, with correct parent linkage.
func buildTestBlock(bc *BlockChain, witnessAddr tcommon.Address, timestamp int64) *types.Block {
	parent := bc.CurrentBlock()
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         int64(parent.Number() + 1),
				Timestamp:      timestamp,
				ParentHash:     parent.Hash().Bytes(),
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
	})
}
```

- [ ] **Step 2: Run the test to verify it fails (or passes without maintenance hook — the key is next_maintenance_time should NOT advance without the hook)**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run TestBlockChainInsertBlock_Maintenance -v`
Expected: FAIL — `next_maintenance_time` doesn't advance past 6000 because maintenance hook doesn't exist yet.

- [ ] **Step 3: Add maintenance hook to InsertBlock**

In `core/blockchain.go`, modify the `InsertBlock` method. After `dynProps.Flush(bc.db)` and before the `// Persist block` section, add the maintenance check:

```go
	// Check maintenance
	if dynProps.NextMaintenanceTime() > 0 && block.Timestamp() >= dynProps.NextMaintenanceTime() {
		// Gather all witnesses with votes
		allWitnesses := bc.gatherWitnessVotes(statedb)
		dpos.DoMaintenance(&chainHeaderAdapter{statedb: statedb, dynProps: dynProps}, block.Timestamp(), allWitnesses)

		// Re-select active witnesses
		newActive := dpos.SelectActiveWitnesses(allWitnesses)
		bc.SetActiveWitnesses(newActive)

		// Flush updated maintenance time
		dynProps.Flush(bc.db)
	}
```

Add these helper methods and types to `core/blockchain.go`:

```go
// gatherWitnessVotes collects all witness addresses and their vote counts.
func (bc *BlockChain) gatherWitnessVotes(statedb *state.StateDB) []dpos.WitnessVote {
	addrs := rawdb.ReadWitnessIndex(bc.db)
	var result []dpos.WitnessVote
	for _, addr := range addrs {
		w := statedb.GetWitness(addr)
		if w == nil {
			// Try rawdb fallback if not loaded in statedb
			w = rawdb.ReadWitness(bc.db, addr)
		}
		if w != nil {
			result = append(result, dpos.WitnessVote{
				Address: w.Address(),
				Votes:   w.VoteCount(),
			})
		}
	}
	return result
}

// chainHeaderAdapter adapts StateDB + DynProps to consensus.ChainHeaderWriter.
type chainHeaderAdapter struct {
	statedb *state.StateDB
	dynProps *state.DynamicProperties
}

func (a *chainHeaderAdapter) GetWitness(addr tcommon.Address) *types.Witness {
	return a.statedb.GetWitness(addr)
}

func (a *chainHeaderAdapter) PutWitness(w *types.Witness) {
	a.statedb.PutWitness(w.Address(), w.URL())
}

func (a *chainHeaderAdapter) AddAllowance(addr tcommon.Address, amount int64) {
	a.statedb.AddAllowance(addr, amount)
}

func (a *chainHeaderAdapter) NextMaintenanceTime() int64 {
	return a.dynProps.NextMaintenanceTime()
}

func (a *chainHeaderAdapter) SetNextMaintenanceTime(t int64) {
	a.dynProps.SetNextMaintenanceTime(t)
}

func (a *chainHeaderAdapter) WitnessPayPerBlock() int64 {
	return a.dynProps.WitnessPayPerBlock()
}

func (a *chainHeaderAdapter) WitnessStandbyAllowance() int64 {
	return a.dynProps.WitnessStandbyAllowance()
}

func (a *chainHeaderAdapter) MaintenanceTimeInterval() int64 {
	return a.dynProps.MaintenanceTimeInterval()
}
```

Also ensure the `statedb` in `InsertBlock` loads witnesses from rawdb so `gatherWitnessVotes` can find them. In the `InsertBlock` method, after `statedb` is created and before `ProcessBlock`, add:

```go
	// Load witnesses into statedb for maintenance access
	witnessAddrs := rawdb.ReadWitnessIndex(bc.db)
	for _, addr := range witnessAddrs {
		if statedb.GetWitness(addr) == nil {
			w := rawdb.ReadWitness(bc.db, addr)
			if w != nil {
				statedb.PutWitness(addr, w.URL())
				// Preserve vote count from rawdb
				statedb.AddWitnessVoteCount(addr, w.VoteCount())
			}
		}
	}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/ -run TestBlockChainInsertBlock_Maintenance -v`
Expected: PASS

- [ ] **Step 5: Run all core tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/... -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add core/blockchain.go core/blockchain_test.go
git commit -m "core: integrate maintenance into InsertBlock"
```

---

### Task 10: Block Producer

**Files:**
- Create: `core/producer/producer.go`
- Create: `core/producer/producer_test.go`

- [ ] **Step 1: Write the failing test**

```go
// core/producer/producer_test.go
package producer

import (
	"crypto/ecdsa"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func setupTestChain(t *testing.T, witnessKey *ecdsa.PrivateKey) (*core.BlockChain, *txpool.TxPool, *dpos.DPoS) {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)

	witnessAddr := crypto.PubkeyToAddress(&witnessKey.PublicKey)

	genesis := &params.Genesis{
		Config:    params.MainnetChainConfig,
		Timestamp: 0,
		Accounts: []params.GenesisAccount{
			{Address: witnessAddr, Balance: 100_000_000},
		},
		Witnesses: []params.GenesisWitness{
			{Address: witnessAddr, VoteCount: 1000, URL: "http://test"},
		},
		DynamicProperties: map[string]int64{
			"next_maintenance_time": 21600000, // 6 hours
		},
	}

	core.SetupGenesisBlock(diskdb, genesis)
	bc, err := core.NewBlockChain(diskdb, sdb, params.MainnetChainConfig)
	if err != nil {
		t.Fatal(err)
	}

	pool := txpool.New()
	engine := dpos.New(bc)

	return bc, pool, engine
}

func TestProducer_New(t *testing.T) {
	key, _ := crypto.GenerateKey()
	bc, pool, engine := setupTestChain(t, key)

	p := New(bc, pool, engine, key)
	if p == nil {
		t.Fatal("producer should not be nil")
	}
}

func TestProducer_ProduceBlock(t *testing.T) {
	key, _ := crypto.GenerateKey()
	bc, pool, engine := setupTestChain(t, key)
	_ = engine // used by producer internally

	p := New(bc, pool, engine, key)

	// Directly call produceBlock to test block production without the scheduling loop
	witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
	timestamp := int64(params.BlockProducedInterval) // slot 1

	err := p.produceBlock(witnessAddr, timestamp)
	if err != nil {
		t.Fatal(err)
	}

	if bc.CurrentBlock().Number() != 1 {
		t.Fatalf("expected block 1, got %d", bc.CurrentBlock().Number())
	}
	if bc.CurrentBlock().Timestamp() != timestamp {
		t.Fatalf("expected timestamp %d, got %d", timestamp, bc.CurrentBlock().Timestamp())
	}

	// Verify the block was signed
	sig := bc.CurrentBlock().WitnessSignature()
	if len(sig) != 65 {
		t.Fatalf("expected 65-byte signature, got %d", len(sig))
	}
}

func TestProducer_StartStop(t *testing.T) {
	key, _ := crypto.GenerateKey()
	bc, pool, engine := setupTestChain(t, key)

	p := New(bc, pool, engine, key)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	p.Stop()
	// Should not panic or hang

	// Verify Start returns error if already stopped
	// (no-op in our implementation, but stop should be clean)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/producer/ -run "TestProducer_" -v`
Expected: FAIL — package not found

- [ ] **Step 3: Implement the Producer**

```go
// core/producer/producer.go
package producer

import (
	"crypto/ecdsa"
	"log"
	"sync"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/params"
)

// Producer drives block production on a DPoS schedule.
// It implements the node.Lifecycle interface.
type Producer struct {
	chain       *core.BlockChain
	pool        *txpool.TxPool
	engine      *dpos.DPoS
	witnessKey  *ecdsa.PrivateKey
	witnessAddr tcommon.Address

	lastProducedSlot int64
	quit             chan struct{}
	wg               sync.WaitGroup
}

// New creates a new block producer.
func New(chain *core.BlockChain, pool *txpool.TxPool, engine *dpos.DPoS, witnessKey *ecdsa.PrivateKey) *Producer {
	return &Producer{
		chain:       chain,
		pool:        pool,
		engine:      engine,
		witnessKey:  witnessKey,
		witnessAddr: crypto.PubkeyToAddress(&witnessKey.PublicKey),
		quit:        make(chan struct{}),
	}
}

// Start begins the block production loop.
func (p *Producer) Start() error {
	p.wg.Add(1)
	go p.loop()
	log.Printf("Block producer started (witness=%x)", p.witnessAddr[:6])
	return nil
}

// Stop signals the production loop to exit and waits for it.
func (p *Producer) Stop() error {
	close(p.quit)
	p.wg.Wait()
	log.Println("Block producer stopped")
	return nil
}

func (p *Producer) loop() {
	defer p.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.tryProduceBlock()
		case <-p.quit:
			return
		}
	}
}

func (p *Producer) tryProduceBlock() {
	now := time.Now().UnixMilli()
	genesisTime := p.chain.GenesisTimestamp()
	interval := int64(params.BlockProducedInterval)

	// Align to slot boundary
	slotTimestamp := (now / interval) * interval
	// Adjust for genesis offset
	offset := genesisTime % interval
	slotTimestamp = slotTimestamp + offset
	if slotTimestamp > now {
		slotTimestamp -= interval
	}

	// Check we haven't already produced this slot
	currentSlot := dpos.AbsoluteSlot(slotTimestamp, genesisTime)
	if currentSlot <= p.lastProducedSlot {
		return
	}

	// Check if this is our slot
	head := p.chain.CurrentBlock()
	headSlot := dpos.SlotForTime(slotTimestamp, head.Timestamp(), genesisTime,
		p.engine.IsInMaintenance(head.Timestamp()), params.MaintenanceSkipSlots)
	if headSlot <= 0 {
		return
	}

	scheduled, err := p.engine.GetScheduledWitness(headSlot)
	if err != nil {
		return
	}
	if scheduled != p.witnessAddr {
		return
	}

	if err := p.produceBlock(p.witnessAddr, slotTimestamp); err != nil {
		log.Printf("Failed to produce block: %v", err)
		return
	}

	p.lastProducedSlot = currentSlot
}

// produceBlock builds, signs, and inserts a block.
func (p *Producer) produceBlock(witnessAddr tcommon.Address, timestamp int64) error {
	// Build block
	block, err := core.BuildBlock(p.chain, p.pool, witnessAddr, timestamp)
	if err != nil {
		return err
	}

	// Sign block
	if err := core.SignBlock(block, p.witnessKey); err != nil {
		return err
	}

	// Insert block
	if err := p.chain.InsertBlock(block); err != nil {
		return err
	}

	// Remove applied transactions from pool
	var hashes []tcommon.Hash
	for _, tx := range block.Transactions() {
		hashes = append(hashes, tx.Hash())
	}
	if len(hashes) > 0 {
		p.pool.RemoveBatch(hashes)
	}

	log.Printf("Produced block #%d at timestamp %d (%d txs)",
		block.Number(), block.Timestamp(), len(block.Transactions()))
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./core/producer/ -run "TestProducer_" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add core/producer/producer.go core/producer/producer_test.go
git commit -m "core/producer: add block production scheduling loop"
```

---

### Task 11: CLI Witness Configuration and Node Bootstrap

**Files:**
- Modify: `cmd/gtron/main.go`
- Modify: `cmd/gtron/config.go`

- [ ] **Step 1: Add witness flags and parseWitnessKey helper**

In `cmd/gtron/config.go`, add:

```go
import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/params"
	"github.com/urfave/cli/v2"
)

// parseWitnessKey reads the witness private key from the --witness.key flag.
func parseWitnessKey(ctx *cli.Context) (*ecdsa.PrivateKey, error) {
	hexKey := ctx.String("witness.key")
	if hexKey == "" {
		return nil, fmt.Errorf("--witness.key is required when --witness is set")
	}
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid hex key: %w", err)
	}
	return crypto.BytesToPrivateKey(keyBytes)
}
```

- [ ] **Step 2: Add witness flags to main.go**

In `cmd/gtron/main.go`, add the flag declarations:

```go
	witnessFlag = &cli.BoolFlag{
		Name:  "witness",
		Usage: "Enable block production",
	}
	witnessKeyFlag = &cli.StringFlag{
		Name:  "witness.key",
		Usage: "Witness private key (hex-encoded)",
	}
```

Add them to the app's Flags list:

```go
	Flags: []cli.Flag{
		dataDirFlag,
		p2pPortFlag,
		httpPortFlag,
		jsonrpcPortFlag,
		testnetFlag,
		witnessFlag,
		witnessKeyFlag,
	},
```

- [ ] **Step 3: Wire the DPoS engine and producer into the gtron function**

Add imports to `cmd/gtron/main.go`:

```go
import (
	// ... existing imports ...
	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core/producer"
	"github.com/tronprotocol/go-tron/crypto"
)
```

In the `gtron` function, after `pool := txpool.New()`, add:

```go
	// Create DPoS engine
	engine := dpos.New(bc)

	// If witness mode, create producer
	if ctx.Bool("witness") {
		key, err := parseWitnessKey(ctx)
		if err != nil {
			db.Close()
			return fmt.Errorf("witness key: %w", err)
		}
		witnessAddr := crypto.PubkeyToAddress(&key.PublicKey)
		fmt.Printf("Witness mode enabled (address=%x)\n", witnessAddr[:6])
		prod := producer.New(bc, pool, engine, key)
		stack.RegisterLifecycle(prod)
	}
```

Also update the version string:

```go
	Version: "0.3.0-dev",
```

- [ ] **Step 4: Verify the code compiles**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go build ./cmd/gtron/`
Expected: SUCCESS (no errors)

- [ ] **Step 5: Run all tests**

Run: `cd /Users/asuka/Projects/asuka/go/go-tron && go test ./... -count=1`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add cmd/gtron/main.go cmd/gtron/config.go
git commit -m "cmd/gtron: add --witness flag and wire block producer"
```

---

## Self-Review Checklist

### Spec coverage
| Spec Section | Task |
|---|---|
| 1. Active Witness List Persistence | Task 1 (rawdb), Task 2 (BlockChain) |
| 2. DPoS Engine Struct | Task 3 |
| 3. Block Builder | Task 8 |
| 4. Block Signing | Task 4 (block mutations), Task 8 (SignBlock) |
| 5. Block Producer | Task 10 |
| 6. Maintenance Integration | Task 9 |
| 7. Resource Integration (Bandwidth) | Task 5 (StateDB methods), Task 6 (FreeNetLimit), Task 7 (consumeBandwidth) |
| 8. CLI Witness Configuration | Task 11 |
| 9. Node Bootstrap Update | Task 11 |

All spec sections are covered.

### Type consistency check
- `WriteActiveWitnesses`/`ReadActiveWitnesses` — consistent across Task 1 and Task 2
- `BuildBlock(bc *BlockChain, pool *txpool.TxPool, witnessAddr common.Address, timestamp int64)` — consistent in Task 8 and Task 10
- `SignBlock(block *types.Block, privKey *ecdsa.PrivateKey)` — consistent in Task 8 and Task 10
- `New(chain consensus.ChainReader) *DPoS` — consistent in Task 3 and Task 10+11
- `consumeBandwidth(statedb, dynProps, tx, blockTime)` — consistent in Task 7
- `Producer.produceBlock(witnessAddr, timestamp)` — consistent within Task 10
- `chainHeaderAdapter` methods match `consensus.ChainHeaderWriter` interface — verified
