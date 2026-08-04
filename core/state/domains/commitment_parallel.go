package domains

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ParallelFoldMinOps gates the parallel root fold: a Fold whose resolved-op count
// is at least this value splits its 16 first-nibble subtries across goroutines.
// 0 disables the parallel path entirely (pure sequential fold). Both paths
// produce byte-identical roots AND byte-identical branch rows — this is purely a
// performance knob and an operational kill switch, never a consensus toggle.
//
// The threshold is the op count above which the parallel split (goroutine spawn +
// a private bufferedBranchStore per non-empty subtrie) pays for itself. It is
// grounded in BenchmarkFoldCrossover against a deep pre-populated trie: at base
// 500k, sequential is faster only up to ~8 ops, while parallel already wins by
// ~1.16x at 16 ops and climbs to ~1.68x by 64. The live chain trie is far deeper
// (every resolved key costs more keccak + branch reads), so its crossover sits
// even lower; 16 is the conservative choice that captures the win on production
// state while keeping trivially small commits sequential.
//
// The prior default of 64 was set on the assumption that per-block commits touch
// "hundreds–thousands" of keys. Live Nile profiling disproved this: under a
// concentrated-write surge (many txs hammering a few hot contracts) the coalesced
// per-block op count routinely lands BELOW 64, so the expensive deep-trie folds
// ran sequentially on one core while ~12 stayed idle. Each key's keccak path
// (keyPath) spreads uniformly across the 16 first-nibble subtries regardless of
// how concentrated the original keys are, so the split parallelizes well even for
// a single hot contract.
var ParallelFoldMinOps = defaultParallelFoldMinOps

const defaultParallelFoldMinOps = 16

// maxFoldNibbles is the branching factor at every trie level: the root fans out
// into at most 16 independent first-nibble subtries.
const maxFoldNibbles = 16

// bufferedBranchStore wraps a base branchStore with read-through reads and
// locally-buffered writes. The parallel root fold gives each first-nibble subtrie
// its own bufferedBranchStore so the subtries can fold concurrently while sharing
// the base for reads. As each subtrie completes, opted-in rawdb production stores
// flush its disjoint sibling buffer immediately; stores that do not explicitly
// advertise concurrent read/write safety retain the serial path after all workers
// join.
//
// Correctness rests on three properties of a single Fold descent:
//
//   - The 16 first-nibble subtries write DISJOINT branch-key prefixes (every row
//     a nibble-nb subtrie touches begins with nibble nb), so no two buffers ever
//     hold the same prefix and flush order cannot affect the final base state.
//   - The descent is single-pass and bottom-up: a subtrie writes a branch only
//     after computing it from its children, and never re-reads a prefix it has
//     written within the same fold. Read-through to the unmodified base is
//     therefore correct. (Buffer-first reads are kept anyway, so the store is
//     correct even if that invariant ever changes.)
//   - Before a worker finishes, its writes stay private. An opted-in production
//     blockbuffer may publish that FINISHED nibble while siblings still read
//     their disjoint prefixes; its immutable topology view plus shard locks make
//     those concurrent reads/writes safe. All other stores retain a serial flush
//     after every worker has joined.
type bufferedBranchStore struct {
	base branchStore
	// keyArena owns immutable prefix bytes for puts. Map lookups from caller
	// []byte values remain allocation-free; only a first insertion appends the
	// prefix here and publishes a read-only string view. The arena is reset only
	// after every map entry has been removed, so later appends and pool reuse
	// cannot mutate a live key.
	keyArena []byte
	// puts holds the latest buffered branch per prefix (one byte per nibble). A
	// re-PUT of a prefix OVERWRITES, so the map is bounded by the number of
	// DISTINCT prefixes the subtrie touches — not by how many times each is
	// rebuilt. (An earlier design appended every PUT's encoding to a grow-only
	// arena; a branch near a busy subtrie root is rebuilt once per op passing
	// through it, so a large fold appended thousands of stale encodings — a >10x
	// allocation blowup that made the parallel path lose to sequential at high op
	// counts. Overwriting removes it.)
	//
	// Map values are pooled pointers rather than BranchData values. BranchData is
	// kilobyte-scale; storing it inline makes every map bucket large and forces
	// the runtime to copy those large values again while a growing map evacuates
	// buckets. A pointer-sized map plus one pool-borrowed destination per distinct
	// prefix keeps re-PUT overwrite semantics while reusing the large objects
	// across folds.
	//
	// Copying the decoded BranchData into the pooled destination is safe even
	// though the caller returns its source *child to branchPool immediately after
	// PutBranch: a branch's only reference-typed field is the immutable leafKey.
	// SetLeafChild always owns a fresh string and fold-internal aliases point into
	// fold-lifetime storage, so their backing bytes outlive the source reuse.
	puts map[string]*BranchData
	dels map[string]struct{} // prefix -> tombstone
}

const (
	initialBufferedBranchKeyArena   = 4 << 10
	maxPooledBufferedBranchKeyArena = 64 << 10
)

var bufferedBranchStorePool = sync.Pool{
	New: func() any {
		return &bufferedBranchStore{
			keyArena: make([]byte, 0, initialBufferedBranchKeyArena),
		}
	},
}

const maxPooledBranchFlushKeys = 4096

var branchFlushKeysPool = sync.Pool{
	New: func() any {
		keys := make([]string, 0, 256)
		return &keys
	},
}

func borrowBranchFlushKeys(size int) *[]string {
	keysPtr := branchFlushKeysPool.Get().(*[]string)
	if cap(*keysPtr) < size {
		*keysPtr = make([]string, 0, size)
	} else {
		*keysPtr = (*keysPtr)[:0]
	}
	return keysPtr
}

func returnBranchFlushKeys(keysPtr *[]string) {
	keys := *keysPtr
	clear(keys)
	if cap(keys) <= maxPooledBranchFlushKeys {
		*keysPtr = keys[:0]
		branchFlushKeysPool.Put(keysPtr)
	}
}

// concurrentSiblingFlushStore is an opt-in marker for a branchStore whose
// PutBranch/DelBranch methods may safely execute concurrently with each other
// and with reads of disjoint keys. The parallel root fold never reads or writes
// the same prefix from two sibling workers.
type concurrentSiblingFlushStore interface {
	branchStore
	concurrentSiblingFlushSafe() bool
}

// branchBatchStore accepts the final writes from one sibling fold in arbitrary
// order. The rawdb adapter publishes them into blockbuffer maps and the later
// durable flush sorts the globally coalesced keys, so sorting each concurrent
// sibling here would add work without creating an observable order. Generic
// stores retain the sorted one-PutBranch-at-a-time fallback below.
type branchBatchStore interface {
	putBranches(keys []string, branches map[string]*BranchData, batchCount int) error
}

func newBufferedBranchStore(base branchStore) *bufferedBranchStore {
	return &bufferedBranchStore{base: base}
}

func borrowBufferedBranchStore(base branchStore) *bufferedBranchStore {
	s := bufferedBranchStorePool.Get().(*bufferedBranchStore)
	s.base = base
	return s
}

func returnBufferedBranchStore(s *bufferedBranchStore) {
	if s == nil {
		return
	}
	s.releasePuts()
	clear(s.dels)
	s.base = nil
	bufferedBranchStorePool.Put(s)
}

func returnSiblingBuffers(buffers [maxFoldNibbles]*bufferedBranchStore) {
	for _, buf := range buffers {
		returnBufferedBranchStore(buf)
	}
}

func (s *bufferedBranchStore) GetBranch(prefix []byte) (BranchData, bool, error) {
	k := string(prefix)
	if _, tomb := s.dels[k]; tomb {
		return BranchData{}, false, nil
	}
	if b, ok := s.puts[k]; ok {
		return *b, true, nil
	}
	return s.base.GetBranch(prefix)
}

func (s *bufferedBranchStore) GetBranchInto(prefix []byte, dst *BranchData) (bool, error) {
	k := string(prefix)
	if _, tomb := s.dels[k]; tomb {
		*dst = BranchData{}
		return false, nil
	}
	if b, ok := s.puts[k]; ok {
		*dst = *b
		return true, nil
	}
	return s.base.GetBranchInto(prefix, dst)
}

func (s *bufferedBranchStore) PutBranch(prefix []byte, b BranchData) error {
	// Direct []byte-to-string map lookups do not allocate. Keep the transient
	// lookup separate from the insertion so a hot upper branch that is rebuilt
	// repeatedly only owns its prefix string on the first PUT.
	delete(s.dels, string(prefix))
	if dst := s.puts[string(prefix)]; dst != nil {
		*dst = b
		return nil
	}
	if s.puts == nil {
		s.puts = make(map[string]*BranchData)
	}
	dst := borrowBranch()
	s.puts[s.ownPrefix(prefix)] = dst
	*dst = b
	return nil
}

func (s *bufferedBranchStore) ownPrefix(prefix []byte) string {
	if len(prefix) == 0 {
		return ""
	}
	start := len(s.keyArena)
	s.keyArena = append(s.keyArena, prefix...)
	return unsafe.String(unsafe.SliceData(s.keyArena[start:]), len(prefix))
}

func (s *bufferedBranchStore) DelBranch(prefix []byte) error {
	k := string(prefix)
	if b := s.puts[k]; b != nil {
		returnBranch(b)
		delete(s.puts, k)
	}
	if s.dels == nil {
		s.dels = make(map[string]struct{})
	}
	s.dels[k] = struct{}{}
	return nil
}

// flush applies the buffered mutations to base. dels and puts hold disjoint
// prefixes, and across all sibling buffers every prefix is written at most once,
// so the resulting base state is independent of flush order. Deletes and the
// generic one-at-a-time put fallback stay sorted for deterministic stores. The
// rawdb batch path may skip that local sort because blockbuffer is map-backed and
// its durable flush sorts the globally coalesced write stream. Each surviving
// branch is encoded here exactly once (inside the batch or base.PutBranch).
func (s *bufferedBranchStore) flush(base branchStore, batchCount int) error {
	// A buffered store is single-use: after applyRootParallel flushes it, no
	// caller reads it again. Return every large BranchData destination even when
	// the base write fails so the next fold can reuse the storage.
	defer s.releasePuts()
	keyCount := len(s.puts)
	if len(s.dels) > keyCount {
		keyCount = len(s.dels)
	}
	if keyCount == 0 {
		return nil
	}
	keysPtr := borrowBranchFlushKeys(keyCount)
	defer returnBranchFlushKeys(keysPtr)

	if len(s.dels) > 0 {
		for k := range s.dels {
			*keysPtr = append(*keysPtr, k)
		}
		sort.Strings(*keysPtr)
		for _, k := range *keysPtr {
			if err := base.DelBranch([]byte(k)); err != nil {
				return err
			}
		}
		clear(*keysPtr)
		*keysPtr = (*keysPtr)[:0]
	}
	if len(s.puts) > 0 {
		for k := range s.puts {
			*keysPtr = append(*keysPtr, k)
		}
		if batch, ok := base.(branchBatchStore); ok {
			return batch.putBranches(*keysPtr, s.puts, batchCount)
		}
		sort.Strings(*keysPtr)
		for _, k := range *keysPtr {
			if err := base.PutBranch([]byte(k), *s.puts[k]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *bufferedBranchStore) releasePuts() {
	for k, b := range s.puts {
		returnBranch(b)
		delete(s.puts, k)
	}
	if cap(s.keyArena) <= maxPooledBufferedBranchKeyArena {
		s.keyArena = s.keyArena[:0]
	} else {
		s.keyArena = nil
	}
}

// applyRootParallel is the parallel counterpart of apply at the root (prefix nil,
// depth 0). It buckets ops by their first nibble and folds each non-empty
// first-nibble subtrie concurrently, each against a private bufferedBranchStore,
// then flushes the buffers into the shared store (concurrently when it opts in)
// and returns the updated root branch plus whether any sibling changed.
//
// The shared root branch is safe to mutate concurrently: each subtrie touches
// only its own children[nb] slot (an independent array element), while
// BranchData's shared presence mask uses atomic bit updates. errs/WaitGroup
// establish the happens-before edge that makes those writes visible to the
// caller after Wait.
func (t *commitmentTrie) applyRootParallel(branch *BranchData, ops []op) (*BranchData, bool, error) {
	if branch == nil {
		branch = &BranchData{}
	}

	// buildOps has already sorted the full paths, so first-nibble groups are
	// contiguous and can be lent directly to workers. Retaining only their
	// boundaries avoids a second full op copy before every parallel fold.
	var counts [maxFoldNibbles]int
	var starts [maxFoldNibbles]int
	for start := 0; start < len(ops); {
		nb := pathNibble(ops[start].path, 0)
		end := start + 1
		for end < len(ops) && pathNibble(ops[end].path, 0) == nb {
			end++
		}
		starts[nb] = start
		counts[nb] = end - start
		start = end
	}

	limit := t.parallelLimit
	if limit <= 0 {
		limit = runtime.GOMAXPROCS(0)
	}
	if limit > maxFoldNibbles {
		limit = maxFoldNibbles
	}
	if limit < 1 {
		limit = 1
	}
	concurrentFlush := false
	if store, ok := t.store.(concurrentSiblingFlushStore); ok && limit > 1 {
		concurrentFlush = store.concurrentSiblingFlushSafe()
	}
	var activeNibbles [maxFoldNibbles]uint8
	activeBatches := 0
	for nb, count := range counts {
		if count > 0 {
			activeNibbles[activeBatches] = uint8(nb)
			activeBatches++
		}
	}

	var (
		buffers [maxFoldNibbles]*bufferedBranchStore
		changed [maxFoldNibbles]bool
		errs    [maxFoldNibbles]error
		wg      sync.WaitGroup
		next    atomic.Uint32
	)
	// Run at most limit long-lived workers and let each claim successive
	// first-nibble groups. The prior one-goroutine-per-group loop used a channel
	// semaphore to cap execution but still created up to 16 goroutine stacks per
	// fold in waves. A bounded worker set preserves the same maximum parallelism
	// and dynamic load balancing while removing the semaphore allocation and the
	// excess short-lived goroutines when limit is below the branching factor.
	workers := min(limit, activeBatches)
	if t.foldStats != nil {
		t.foldStats.parallelCalls++
		t.foldStats.parallelSplits += uint64(activeBatches)
		t.foldStats.parallelWorkers += uint64(workers)
	}
	siblingStats := borrowCommitmentSiblingFoldStats()
	defer returnCommitmentSiblingFoldStats(siblingStats)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			hasher := borrowKeccak()
			defer returnKeccak(hasher)
			sub := commitmentTrie{hasher: hasher}
			var path [pathLen]byte
			for {
				task := int(next.Add(1)) - 1
				if task >= activeBatches {
					return
				}
				nb := activeNibbles[task]
				n := counts[nb]
				group := ops[starts[nb] : starts[nb]+n]
				buf := borrowBufferedBranchStore(t.store)
				buffers[nb] = buf

				// Each subtrie folds sequentially against its private buffer. Keep
				// this tiny owner, hasher, and path on the worker stack. They are reused
				// after each claimed group because recursive stores consume it before
				// applyNibble returns.
				sub.store = buf
				sub.foldStats = &(*siblingStats)[nb]
				nibbleChanged, err := sub.applyNibble(path[:0], 0, branch, nb, group)
				changed[nb] = nibbleChanged
				if err == nil && nibbleChanged && concurrentFlush {
					// This worker only reads/writes prefixes beginning with nb. Publishing
					// its finished buffer cannot affect any still-running sibling, so overlap
					// encoding/writes with their computation and avoid a second goroutine wave.
					err = buf.flush(t.store, activeBatches)
				}
				errs[nb] = err
			}
		}()
	}
	wg.Wait()
	if t.foldStats != nil {
		for nb := range siblingStats {
			t.foldStats.merge(&(*siblingStats)[nb])
		}
	}

	for nb := 0; nb < maxFoldNibbles; nb++ {
		if errs[nb] != nil {
			returnSiblingBuffers(buffers)
			return nil, false, errs[nb]
		}
	}
	if !concurrentFlush {
		if err := flushSiblingBuffersSerial(t.store, buffers, activeBatches); err != nil {
			returnSiblingBuffers(buffers)
			return nil, false, err
		}
	}
	returnSiblingBuffers(buffers)

	anyChanged := false
	for _, nibbleChanged := range changed {
		anyChanged = anyChanged || nibbleChanged
	}
	if branch.childCount() == 0 {
		return nil, anyChanged, nil
	}
	return branch, anyChanged, nil
}

// flushSiblingBuffersSerial publishes first-nibble buffers in deterministic
// order for stores that do not opt into concurrent read/write access.
func flushSiblingBuffersSerial(base branchStore, buffers [maxFoldNibbles]*bufferedBranchStore, batchCount int) error {
	for nb := 0; nb < maxFoldNibbles; nb++ {
		if buffers[nb] == nil {
			continue
		}
		if err := buffers[nb].flush(base, batchCount); err != nil {
			return err
		}
	}
	return nil
}
