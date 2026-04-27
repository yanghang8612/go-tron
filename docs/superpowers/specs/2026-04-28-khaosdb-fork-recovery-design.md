# KhaosDB Fork Recovery — Design Spec

**Date:** 2026-04-28
**Status:** Active
**Milestone:** M3.1 slice-2b — 10-block fork recovery
**Gates:** G1 (can follow chain)

---

## 1. Goals

Implement an in-memory branching block buffer (KhaosDB) that enables go-tron to:

1. Accept blocks that arrive on a competing chain tip (not just `current.Hash()`)
2. Detect when a competing fork becomes longer than the canonical chain
3. Roll back canonical state to the lowest common ancestor (LCA)
4. Re-apply the competing branch on top of LCA state
5. Switch `currentBlock` atomically to the new tip

This mirrors `KhaosDatabase` in java-tron (`chainbase/.../db/KhaosDatabase.java`) and `Manager.switchFork` (`framework/.../db/Manager.java`).

---

## 2. Background — java-tron Reference

### KhaosBlock
```java
class KhaosBlock {
    BlockCapsule blk;
    WeakReference<KhaosBlock> parent;  // weak to avoid GC cycles
    BlockId id;
    long num;
}
```

### KhaosStore
Two backing maps:
- `hashKblkMap: ConcurrentHashMap<BlockId, KhaosBlock>` — O(1) hash lookup
- `numKblkMap: LinkedHashMap<Long, ArrayList<KhaosBlock>>` — insertion-ordered, drives eviction

Eviction: on every insert, `removeEldestEntry` scans entries with `num < head.num - maxCapacity` and deletes them from both maps. Default `maxCapacity = 1024`.

### KhaosDatabase
- `miniStore` — linked blocks (full ancestor chain known)
- `miniUnlinkedStore` — blocks whose parent is not yet in miniStore (out-of-order delivery)
- `head` — highest-number KhaosBlock across all branches

### State rollback in java-tron
java-tron uses `revokingStore.fastPop()` — an MVCC snapshot store that can undo writes. Go-tron doesn't have this; instead we exploit the fact that every committed block's state root is durably stored in a hash-based MPT (`triedb.NewDatabase(diskdb, nil)` — hash-based defaults). We rewind by calling `state.New(lcaRoot, bc.stateDB)`.

**Verified:** `TestStateRollbackViaOldRoot` (ad-hoc, run before spec implementation) confirmed that `state.New(root1, db)` after a subsequent `Commit()` to `root2` correctly returns balance from `root1`, not `root2`. Hash-based trie nodes are never deleted on commit, so historical roots remain accessible.

### switchFork algorithm
1. `getBranch(newHead, currentHead)` → `(newBranch, oldBranch)` — two lists back to LCA (LCA not included)
2. Pop canonical chain back to LCA: undo each block in `oldBranch` (state rollback via LCA root)
3. Apply `newBranch` (reversed, LCA+1 to newHead) on top of LCA state
4. On any error: remove orphan blocks from KhaosDB

---

## 3. Data Model — Go

### `KhaosBlock`
```go
type KhaosBlock struct {
    block  *types.Block
    parent *KhaosBlock   // nil = no parent known; set by Push when parent found in miniStore
    id     tcommon.Hash
    num    uint64
}
```

Go doesn't need `WeakReference` — store-level eviction removes the pointer from `byHash` which is the only strong reference. Once a `KhaosBlock` is evicted, traversal stops naturally (`parent` links to already-evicted nodes become unreachable through the store).

### `KhaosStore`
```go
type KhaosStore struct {
    mu          sync.Mutex
    byHash      map[tcommon.Hash]*KhaosBlock
    byNum       map[uint64][]*KhaosBlock  // insertion order unimportant; eviction by num key
    maxCapacity int
}
```

`byNum` is a plain `map` (not `LinkedHashMap`); eviction is triggered explicitly on every `insert` call: remove all entries with `num ≤ headNum - maxCapacity`.

### `KhaosDB`
```go
type KhaosDB struct {
    mu               sync.RWMutex
    miniStore        *KhaosStore
    miniUnlinkedStore *KhaosStore
    head             *KhaosBlock
}
```

---

## 4. KhaosStore Operations

### `insert(block *KhaosBlock, headNum uint64)`
1. Add to `byHash[block.id] = block`
2. Append to `byNum[block.num]`
3. Evict: for each `num ≤ headNum - maxCapacity`, delete all KhaosBlocks in `byNum[num]` from `byHash`, then delete `byNum[num]`

### `remove(hash tcommon.Hash) bool`
1. Look up in `byHash`; if absent return false
2. Remove from `byNum[block.num]` (splice); if slice empty delete key
3. Delete from `byHash`
4. Return true

### `getByHash(hash) *KhaosBlock` — O(1) lookup
### `getByNum(num) []*KhaosBlock` — return slice or nil

---

## 5. KhaosDB Operations

### `Start(block *types.Block)`
Initialize: wrap in KhaosBlock, insert into miniStore, set as head.  
Called on chain start with the genesis block or the current head at node startup.

### `Push(block *types.Block) (*KhaosBlock, error)`
```
kb := &KhaosBlock{block: block, id: block.Hash(), num: block.Number()}
parent := miniStore.getByHash(block.ParentHash())
if parent == nil:
    if block.ParentHash() != zeroHash:
        miniUnlinkedStore.insert(kb, head.num)
        return nil, ErrUnlinkedBlock
    // genesis: no parent expected
else:
    if block.Number() != parent.num+1:
        return nil, ErrBadBlockNumber
    kb.parent = parent
miniStore.insert(kb, head.num)
if kb.num > head.num:
    head = kb
return head.block, nil
```

After inserting, scan miniUnlinkedStore for blocks whose parent is now `kb.id` — promote them to miniStore (recursive). This handles delayed out-of-order delivery.

### `GetBranch(hash1, hash2 tcommon.Hash) (branch1, branch2 []*KhaosBlock, err error)`
Find LCA of two tips, returning the list of blocks on each side (not including LCA):

```
kb1 = miniStore.getByHash(hash1)
kb2 = miniStore.getByHash(hash2)
if either nil: return ErrNonCommonBlock

// Phase 1: equalize heights
while kb1.num > kb2.num:
    branch1 = append(branch1, kb1)
    kb1 = kb1.parent; if nil: return ErrNonCommonBlock
while kb2.num > kb1.num:
    branch2 = append(branch2, kb2)
    kb2 = kb2.parent; if nil: return ErrNonCommonBlock

// Phase 2: walk together until equal
while kb1 != kb2 (by id):
    branch1 = append(branch1, kb1)
    branch2 = append(branch2, kb2)
    kb1 = kb1.parent; if nil: return ErrNonCommonBlock
    kb2 = kb2.parent; if nil: return ErrNonCommonBlock

return branch1, branch2, nil
// LCA = kb1 = kb2 (not included in either list)
// LCA.block.Hash() = branch1[last].block.ParentHash() (if branch1 non-empty)
//                  = hash2 (if branch1 is empty, hash2 is ancestor of hash1)
```

### `RemoveBlk(hash tcommon.Hash)`
Remove from miniStore; if not found, remove from miniUnlinkedStore.
After removing, update `head` to the maximum `num` across remaining miniStore entries.

### `Pop() bool`
```
prev := head.parent
if prev == nil: return false
head = prev
return true
```
Does not modify state. Just rewinds the in-memory head pointer.

### `SetMaxSize(n int)`
Sets `maxCapacity` on both stores.

### `SetHead(block *types.Block)`
Finds the KhaosBlock for `block.Hash()` in miniStore and sets it as `head`. Used in error-recovery paths (mirrors java-tron `khaosDb.setHead(...)`).

### Accessors
- `ContainsBlock(hash) bool` — miniStore or miniUnlinkedStore
- `ContainsInMiniStore(hash) bool` — miniStore only
- `GetBlock(hash) *types.Block` — from either store
- `Head() *types.Block`
- `HasData() bool`

---

## 6. Integration with `core/blockchain.go`

### 6.1 BlockChain struct additions
```go
type BlockChain struct {
    // ... existing fields ...
    khaosDB *KhaosDB
}
```

Initialize in `NewBlockChain`: `bc.khaosDB = NewKhaosDB()`, then `bc.khaosDB.Start(genesisBlock)` (or current head if resuming).

### 6.2 Modified `InsertBlock`

Current code rejects any block where `block.ParentHash() != current.Hash()`. Replace with:

```go
func (bc *BlockChain) InsertBlock(block *types.Block) error {
    bc.chainmu.Lock()
    defer bc.chainmu.Unlock()

    current := bc.CurrentBlock()

    // Duplicate check: already committed on canonical chain
    if block.Number() <= current.Number() && bc.khaosDB.ContainsInMiniStore(block.Hash()) {
        return nil
    }

    // Push to KhaosDB — validates parent linkage and block number
    newHead, err := bc.khaosDB.Push(block)
    if err != nil {
        return err
    }

    // Fork detection: new head doesn't extend canonical tip
    if newHead.ParentHash() != current.Hash() {
        if newHead.Number() > current.Number() {
            // Longer competing chain: switch
            if err := bc.switchFork(newHead); err != nil {
                bc.khaosDB.RemoveBlk(block.Hash())
                return fmt.Errorf("switchFork: %w", err)
            }
        }
        // Equal-or-shorter fork: just keep block in KhaosDB, don't switch
        return nil
    }

    // Normal linear extension
    if err := bc.applyBlock(block); err != nil {
        bc.khaosDB.RemoveBlk(block.Hash())
        return err
    }
    return nil
}
```

### 6.3 `applyBlock` (extracted from current InsertBlock body)
Encapsulates: open state from parentRoot, ProcessBlock, maintenance, Commit, persist block+txInfos, update currentBlock.  
Signature: `applyBlock(block *types.Block) error`

### 6.4 `switchFork`
```go
func (bc *BlockChain) switchFork(newHead *types.Block) error {
    currentHash := bc.CurrentBlock().Hash()
    newBranch, oldBranch, err := bc.khaosDB.GetBranch(newHead.Hash(), currentHash)
    if err != nil {
        // Can't find LCA: remove new branch blocks
        tmp := newHead
        for tmp != nil {
            bc.khaosDB.RemoveBlk(tmp.Hash())
            tmp = bc.khaosDB.GetBlock(tmp.ParentHash())
        }
        return err
    }

    // LCA block hash
    var lcaHash tcommon.Hash
    if len(oldBranch) == 0 {
        lcaHash = currentHash // newHead branches at current (impossible in normal flow but safe)
    } else {
        lcaHash = oldBranch[len(oldBranch)-1].ParentHash()
    }

    lcaBlock := rawdb.ReadBlock(bc.db, lcaHash)
    if lcaBlock == nil {
        return fmt.Errorf("LCA block %x not in DB", lcaHash)
    }

    // Set currentBlock to LCA before the loop so applyBlock reads the correct parentRoot.
    // applyBlock does: parentRoot := bc.CurrentBlock().AccountStateRoot(); state.New(parentRoot, ...)
    // After each successful applyBlock, it updates bc.currentBlock to the newly applied block.
    bc.currentBlock.Store(lcaBlock)

    // Apply new branch (newBranch is ordered newHead→LCA+1; reverse to LCA+1→newHead)
    reversed := reverseKhaosBlocks(newBranch)
    for _, kb := range reversed {
        if err := bc.applyBlock(kb); err != nil {
            // Roll back: currentBlock is now somewhere on the new branch (possibly LCA).
            // Remove new-branch blocks from KhaosDB; leave currentBlock wherever it is.
            // The next successful push will recover from here.
            for _, failed := range newBranch {
                bc.khaosDB.RemoveBlk(failed.Hash())
            }
            return fmt.Errorf("apply fork block %d: %w", kb.Number(), err)
        }
    }
    return nil
}
```

State rewind is explicit via `bc.currentBlock.Store(lcaBlock)` before the loop. `applyBlock` reads `bc.CurrentBlock().AccountStateRoot()` as its parent root, so the first new-branch block opens state from LCA's committed root — effectively rewinding without any explicit undo. Each subsequent `applyBlock` call advances `currentBlock`, providing the correct parent for the next block.

### 6.5 Error types (add to `core/errors.go` or inline)
```go
var (
    ErrUnlinkedBlock  = errors.New("block parent not in KhaosDB")
    ErrNonCommonBlock = errors.New("no common ancestor in KhaosDB window")
    ErrBadBlockNumber = errors.New("block number not parent+1")
)
```

---

## 7. State Rollback — Detailed Rationale

Java-tron uses MVCC snapshots (`revokingStore`) to undo state. Go-tron doesn't need this because:

- Every block successfully committed via `applyBlock` persists its state root in the block header AND durably flushes the trie to the KV store.
- `state.New(root, db)` re-opens *any* previously committed root — including the LCA's root.
- The StateDB opened from LCA root is a fresh view with no pending changes; applying blocks on top is identical to the initial chain sync path.

Constraint: the LCA block must be within the KhaosDB window AND must have been on the canonical chain at some point (so its state is persisted in `bc.stateDB`). Both are guaranteed if `maxCapacity ≥ fork depth` (1024 >> any realistic TRON fork depth).

---

## 8. Unlinked Block Promotion

When a block arrives before its parent, it lands in `miniUnlinkedStore`. When a new block is inserted into `miniStore`, scan `miniUnlinkedStore` for entries whose `ParentHash()` matches the newly inserted block's hash. Promote matches to `miniStore` (set parent link, remove from unlinked store).

This handles late-arriving blocks without requiring the caller to retry.

---

## 9. Pruning / Eviction

Trigger: on every `KhaosStore.insert()`, evict all entries with `num ≤ headNum - maxCapacity`.

Default `maxCapacity = 1024`. java-tron also calls `setMaxSize` with a value that tracks the solidified block distance, but since TRON's practical fork depth is <10 blocks, 1024 is safe without dynamic adjustment for now.

Eviction removes entries from both `byHash` and `byNum`. Since parent pointers in KhaosBlock are plain Go pointers, evicted blocks become unreachable through the store but may be reachable via a traversal in progress — that's fine; `GetBranch` will hit a `nil` parent and return `ErrNonCommonBlock`, which is the expected behavior when the LCA falls outside the window.

---

## 10. Files to Create / Modify

### New files
| File | Content |
|---|---|
| `core/khaosdb.go` | `KhaosBlock`, `KhaosStore`, `KhaosDB`, all methods above |
| `core/khaosdb_test.go` | Unit tests: push, duplicate, getBranch LCA, eviction, unlinked promotion |

### Modified files
| File | Change |
|---|---|
| `core/blockchain.go` | Add `khaosDB *KhaosDB` field; initialize on construction; refactor InsertBlock → applyBlock + switchFork logic |
| `core/blockchain_test.go` | Fork-switch tests: 10-block common stem + 3-block competing branches; confirm switch to longer |

---

## 11. Exit Gate (M3.1 slice-2b)

| Test | Assertion |
|---|---|
| `TestKhaosDB_Push_Linear` | Push 10 blocks in order; head = block 10 |
| `TestKhaosDB_Push_Unlinked` | Push block 5 before parent; once parent arrives, promoted to miniStore |
| `TestKhaosDB_GetBranch_NoFork` | Two equal hashes → empty both branches |
| `TestKhaosDB_GetBranch_10Block` | 5-block common stem, 3-block fork A, 3-block fork B → correct branch lists |
| `TestKhaosDB_Eviction` | Insert 1100 blocks; entries below `head.num - 1024` evicted |
| `TestBlockChain_ForkSwitch_10Block` | Build 10-block canonical chain, then 11-block competing fork; InsertBlock triggers switchFork; currentBlock = tip of longer chain |
| `make test` | Full suite green |

---

## 12. Out of Scope

- PBFT-based solidification floor (M6 dependency): solidified blocks cannot be forked away; for now, `maxCapacity=1024` is the only floor.
- Transaction re-injection after fork switch (java-tron re-adds popped transactions to `rePushTransactions`): deferred to a later slice.
- Metrics/counters for fork events: deferred.
- **Mid-fork failure partial-rewind**: if `applyBlock` fails on block N of the new branch (N > LCA+1), `currentBlock` ends up pointing at block N-1 on the new chain — neither the original canonical tip nor the new tip. The next successful push will recover naturally if a valid block arrives there. java-tron avoids this by fully re-applying the old branch on error; go-tron does not attempt that recovery (the old-branch state roots are still accessible via the hash-based MPT, but the re-apply loop was omitted as out of scope for this milestone).
