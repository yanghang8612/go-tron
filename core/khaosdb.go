package core

import (
	"errors"
	"sync"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

var (
	ErrUnlinkedBlock  = errors.New("block parent not in KhaosDB")
	ErrNonCommonBlock = errors.New("no common ancestor in KhaosDB window")
	ErrBadBlockNumber = errors.New("block number not parent+1")
)

// KhaosBlock is a node in the in-memory fork tree. It deliberately holds NO
// pointer to its parent block: parents are resolved on demand through the
// store's by-hash index (see resolveParent), so a block that slides out of the
// eviction window becomes unreachable and collectable. java-tron achieves the
// same with a WeakReference parent (KhaosDatabase.KhaosBlock); a strong parent
// pointer here would chain head→…→genesis and leak every block ever seen.
type KhaosBlock struct {
	block *types.Block
	id    tcommon.Hash
	num   uint64
}

func (kb *KhaosBlock) Block() *types.Block    { return kb.block }
func (kb *KhaosBlock) ParentHash() tcommon.Hash { return kb.block.ParentHash() }

// khaosStore is a dual-indexed in-memory block store with capacity-based eviction.
type khaosStore struct {
	mu          sync.Mutex
	byHash      map[tcommon.Hash]*KhaosBlock
	byNum       map[uint64][]*KhaosBlock
	maxCapacity int
}

func newKhaosStore(maxCapacity int) *khaosStore {
	return &khaosStore{
		byHash:      make(map[tcommon.Hash]*KhaosBlock),
		byNum:       make(map[uint64][]*KhaosBlock),
		maxCapacity: maxCapacity,
	}
}

func (s *khaosStore) setMaxCapacity(n int) {
	s.mu.Lock()
	s.maxCapacity = n
	s.mu.Unlock()
}

func (s *khaosStore) insert(kb *KhaosBlock, headNum uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byHash[kb.id] = kb
	s.byNum[kb.num] = append(s.byNum[kb.num], kb)
	// Evict entries older than the window. Use the prospective head (whichever is
	// higher: current head or the block being inserted) so the window is always
	// exactly maxCapacity blocks after each insert.
	effectiveHead := headNum
	if kb.num > effectiveHead {
		effectiveHead = kb.num
	}
	if effectiveHead >= uint64(s.maxCapacity) {
		threshold := effectiveHead - uint64(s.maxCapacity)
		for num := range s.byNum {
			if num <= threshold {
				for _, evicted := range s.byNum[num] {
					delete(s.byHash, evicted.id)
				}
				delete(s.byNum, num)
			}
		}
	}
}

// remove returns true if the block was found and removed.
func (s *khaosStore) remove(hash tcommon.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	kb, ok := s.byHash[hash]
	if !ok {
		return false
	}
	list := s.byNum[kb.num]
	for i, b := range list {
		if b.id == hash {
			s.byNum[kb.num] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(s.byNum[kb.num]) == 0 {
		delete(s.byNum, kb.num)
	}
	delete(s.byHash, hash)
	return true
}

func (s *khaosStore) getByHash(hash tcommon.Hash) *KhaosBlock {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byHash[hash]
}

func (s *khaosStore) maxNum() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var max uint64
	for num := range s.byNum {
		if num > max {
			max = num
		}
	}
	return max
}

func (s *khaosStore) empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byHash) == 0
}

// KhaosDB is the in-memory fork-aware block buffer. It tracks a sliding window
// of recent blocks across all competing chain tips, enabling fork detection and
// LCA computation. Mirrors java-tron's KhaosDatabase.
type KhaosDB struct {
	mu               sync.RWMutex
	miniStore        *khaosStore
	miniUnlinkedStore *khaosStore
	head             *KhaosBlock
}

const khaosDefaultCapacity = 1024

// NewKhaosDB creates an empty KhaosDB.
func NewKhaosDB() *KhaosDB {
	return &KhaosDB{
		miniStore:        newKhaosStore(khaosDefaultCapacity),
		miniUnlinkedStore: newKhaosStore(khaosDefaultCapacity),
	}
}

// Start initializes KhaosDB with the current chain head (genesis or last committed block).
func (k *KhaosDB) Start(block *types.Block) {
	k.mu.Lock()
	defer k.mu.Unlock()
	kb := &KhaosBlock{block: block, id: block.Hash(), num: block.Number()}
	k.miniStore.insert(kb, kb.num)
	k.head = kb
}

// SetMaxSize sets the sliding-window capacity on both stores.
func (k *KhaosDB) SetMaxSize(n int) {
	k.miniStore.setMaxCapacity(n)
	k.miniUnlinkedStore.setMaxCapacity(n)
}

// Push adds a block to KhaosDB, linking it to its parent if known.
// Returns the current KhaosDB head (highest block seen) and an error if the
// block's parent is not in miniStore (ErrUnlinkedBlock) or its number is wrong.
func (k *KhaosDB) Push(block *types.Block) (*types.Block, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	kb := &KhaosBlock{block: block, id: block.Hash(), num: block.Number()}
	zeroHash := (tcommon.Hash{})
	parentHash := block.ParentHash()

	if k.head != nil && parentHash != zeroHash {
		parent := k.miniStore.getByHash(parentHash)
		if parent == nil {
			k.miniUnlinkedStore.insert(kb, k.head.num)
			return nil, ErrUnlinkedBlock
		}
		if block.Number() != parent.num+1 {
			return nil, ErrBadBlockNumber
		}
	}

	k.miniStore.insert(kb, k.head.num)
	if k.head == nil || kb.num > k.head.num {
		k.head = kb
	}

	// Promote any previously unlinked blocks whose parent just arrived.
	k.promoteUnlinked(kb)

	return k.head.block, nil
}

// promoteUnlinked scans miniUnlinkedStore for blocks whose parent is now available.
// Must be called with k.mu held.
func (k *KhaosDB) promoteUnlinked(parent *KhaosBlock) {
	// Collect candidate num = parent.num+1 from unlinked store.
	childNum := parent.num + 1
	k.miniUnlinkedStore.mu.Lock()
	var promote []*KhaosBlock
	for _, kb := range k.miniUnlinkedStore.byNum[childNum] {
		if kb.ParentHash() == parent.id {
			promote = append(promote, kb)
		}
	}
	for _, kb := range promote {
		delete(k.miniUnlinkedStore.byHash, kb.id)
		list := k.miniUnlinkedStore.byNum[childNum]
		for i, b := range list {
			if b.id == kb.id {
				k.miniUnlinkedStore.byNum[childNum] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(k.miniUnlinkedStore.byNum[childNum]) == 0 {
			delete(k.miniUnlinkedStore.byNum, childNum)
		}
	}
	k.miniUnlinkedStore.mu.Unlock()

	for _, kb := range promote {
		k.miniStore.insert(kb, k.head.num)
		if kb.num > k.head.num {
			k.head = kb
		}
		// Recurse: this newly promoted block may unblock further children.
		k.promoteUnlinked(kb)
	}
}

// resolveParent returns the in-window parent of kb, or nil if that parent has
// been evicted from the window or was never linked (genesis / an unlinked
// block). It mirrors java-tron's KhaosBlock.getParent() returning null once the
// weak parent reference is cleared: the parent is found by hash through the
// store's index rather than followed through a strong pointer, so an evicted
// ancestor is reported absent (and stays collectable). Callers hold k.mu.
func (k *KhaosDB) resolveParent(kb *KhaosBlock) *KhaosBlock {
	return k.miniStore.getByHash(kb.ParentHash())
}

// GetBranch returns the list of KhaosBlocks on each side of the two tips,
// back to (but not including) their lowest common ancestor.
// branch1 goes from hash1 toward LCA; branch2 from hash2 toward LCA.
// The LCA is the (in-window) parent of branch1[last] and of branch2[last].
// Returns ErrNonCommonBlock if either walk leaves the window before meeting.
func (k *KhaosDB) GetBranch(hash1, hash2 tcommon.Hash) (branch1, branch2 []*KhaosBlock, err error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	kb1 := k.miniStore.getByHash(hash1)
	if kb1 == nil {
		return nil, nil, ErrNonCommonBlock
	}
	kb2 := k.miniStore.getByHash(hash2)
	if kb2 == nil {
		return nil, nil, ErrNonCommonBlock
	}

	// Phase 1: equalize heights.
	for kb1.num > kb2.num {
		branch1 = append(branch1, kb1)
		kb1 = k.resolveParent(kb1)
		if kb1 == nil {
			return nil, nil, ErrNonCommonBlock
		}
	}
	for kb2.num > kb1.num {
		branch2 = append(branch2, kb2)
		kb2 = k.resolveParent(kb2)
		if kb2 == nil {
			return nil, nil, ErrNonCommonBlock
		}
	}

	// Phase 2: walk together until LCA.
	for kb1.id != kb2.id {
		branch1 = append(branch1, kb1)
		branch2 = append(branch2, kb2)
		kb1 = k.resolveParent(kb1)
		if kb1 == nil {
			return nil, nil, ErrNonCommonBlock
		}
		kb2 = k.resolveParent(kb2)
		if kb2 == nil {
			return nil, nil, ErrNonCommonBlock
		}
	}

	return branch1, branch2, nil
}

// RemoveBlk removes a block from miniStore or miniUnlinkedStore, then
// updates head to the highest remaining block in miniStore.
func (k *KhaosDB) RemoveBlk(hash tcommon.Hash) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.miniStore.remove(hash) {
		k.miniUnlinkedStore.remove(hash)
	}
	// Recompute head.
	maxN := k.miniStore.maxNum()
	k.miniStore.mu.Lock()
	var newHead *KhaosBlock
	for _, list := range k.miniStore.byNum[maxN] {
		newHead = list
		break
	}
	k.miniStore.mu.Unlock()
	if newHead != nil {
		k.head = newHead
	}
}

// Pop rewinds the in-memory head pointer by one (does not touch committed state).
// Returns false if there is no in-window parent to rewind to.
func (k *KhaosDB) Pop() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.head == nil {
		return false
	}
	parent := k.resolveParent(k.head)
	if parent == nil {
		return false
	}
	k.head = parent
	return true
}

// SetHead sets the in-memory head to the KhaosBlock for the given block.
// Used in error-recovery paths.
func (k *KhaosDB) SetHead(block *types.Block) {
	k.mu.Lock()
	defer k.mu.Unlock()
	kb := k.miniStore.getByHash(block.Hash())
	if kb != nil {
		k.head = kb
	}
}

// ContainsBlock returns true if the hash is in either store.
func (k *KhaosDB) ContainsBlock(hash tcommon.Hash) bool {
	return k.miniStore.getByHash(hash) != nil || k.miniUnlinkedStore.getByHash(hash) != nil
}

// ContainsInMiniStore returns true if the hash is in the linked store.
func (k *KhaosDB) ContainsInMiniStore(hash tcommon.Hash) bool {
	return k.miniStore.getByHash(hash) != nil
}

// GetBlock returns the block for the given hash from either store, or nil.
func (k *KhaosDB) GetBlock(hash tcommon.Hash) *types.Block {
	if kb := k.miniStore.getByHash(hash); kb != nil {
		return kb.block
	}
	if kb := k.miniUnlinkedStore.getByHash(hash); kb != nil {
		return kb.block
	}
	return nil
}

// Head returns the current highest block in the linked store.
func (k *KhaosDB) Head() *types.Block {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.head == nil {
		return nil
	}
	return k.head.block
}

// HasData reports whether the linked store has any blocks.
func (k *KhaosDB) HasData() bool {
	return !k.miniStore.empty()
}
