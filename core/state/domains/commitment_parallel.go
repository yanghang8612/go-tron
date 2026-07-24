package domains

import (
	"runtime"
	"sort"
	"sync"
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
	// roughly 1 KiB; storing it inline makes every map bucket large and forces
	// the runtime to copy those large values again while a growing map evacuates
	// buckets. A pointer-sized map plus one pool-borrowed destination per distinct
	// prefix keeps re-PUT overwrite semantics while reusing the large objects
	// across folds.
	//
	// Copying the decoded BranchData into the pooled destination is safe even
	// though the caller returns its source *child to branchPool immediately after
	// PutBranch: a branch's only reference-typed field is leafKey, which is
	// write-once — SetLeafChild always allocates a fresh slice and nothing mutates
	// leafKey in place — so the shared backing arrays outlive the source reuse.
	puts map[string]*BranchData
	dels map[string]struct{} // prefix -> tombstone
}

var bufferedBranchStorePool = sync.Pool{
	New: func() any { return new(bufferedBranchStore) },
}

// concurrentSiblingFlushStore is an opt-in marker for a branchStore whose
// PutBranch/DelBranch methods may safely execute concurrently with each other
// and with reads of disjoint keys. The parallel root fold never reads or writes
// the same prefix from two sibling workers.
type concurrentSiblingFlushStore interface {
	branchStore
	concurrentSiblingFlushSafe() bool
}

// branchBatchStore accepts the sorted final writes from one sibling fold. The
// rawdb adapter uses this seam to arena-pack immutable encodings; generic test
// stores keep the ordinary one-PutBranch-at-a-time path.
type branchBatchStore interface {
	putBranchesSorted(keys []string, branches map[string]*BranchData, batchCount int) error
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
	// This conversion escapes into the map and owns the immutable prefix. It is
	// reached exactly once per distinct prefix in this buffered sibling fold.
	s.puts[string(prefix)] = dst
	*dst = b
	return nil
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
// so the resulting base state is independent of flush order; the sorted
// iteration only makes the emitted write stream deterministic. Each surviving
// branch is encoded here exactly once (inside base.PutBranch). Sorting stabilizes
// each sibling's stream; opted-in concurrent siblings may interleave freely.
func (s *bufferedBranchStore) flush(base branchStore, batchCount int) error {
	// A buffered store is single-use: after applyRootParallel flushes it, no
	// caller reads it again. Return every large BranchData destination even when
	// the base write fails so the next fold can reuse the storage.
	defer s.releasePuts()

	if len(s.dels) > 0 {
		keys := make([]string, 0, len(s.dels))
		for k := range s.dels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := base.DelBranch([]byte(k)); err != nil {
				return err
			}
		}
	}
	if len(s.puts) > 0 {
		keys := make([]string, 0, len(s.puts))
		for k := range s.puts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if batch, ok := base.(branchBatchStore); ok {
			return batch.putBranchesSorted(keys, s.puts, batchCount)
		}
		for _, k := range keys {
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
}

// applyRootParallel is the parallel counterpart of apply at the root (prefix nil,
// depth 0). It buckets ops by their first nibble and folds each non-empty
// first-nibble subtrie concurrently, each against a private bufferedBranchStore,
// then flushes the buffers into the shared store (concurrently when it opts in)
// and returns the updated root branch.
//
// The shared root branch is safe to mutate concurrently: each subtrie touches
// only its own children[nb] slot (an independent array element), and the slots
// carry no shared bitmap. errs/WaitGroup establish the happens-before edge that
// makes those slot writes visible to the caller after Wait.
func (t *commitmentTrie) applyRootParallel(branch *BranchData, ops []op) (*BranchData, error) {
	if branch == nil {
		branch = &BranchData{}
	}

	// Bucket ops by first nibble via the same counting sort apply uses. The
	// per-nibble groups must stay alive and isolated for the whole concurrent
	// phase, so this borrows a SEPARATE long-lived ops buffer from the pool
	// (distinct from the short-lived scratch each inner apply borrows/returns),
	// released at function exit after the flush.
	var counts [maxFoldNibbles]int
	for i := range ops {
		counts[pathNibble(ops[i].path, 0)]++
	}
	var starts [maxFoldNibbles]int
	for i := 1; i < maxFoldNibbles; i++ {
		starts[i] = starts[i-1] + counts[i-1]
	}
	groupedP := borrowOpsBuf(len(ops))
	defer returnOpsBuf(groupedP)
	grouped := *groupedP
	heads := starts
	for i := range ops {
		nb := pathNibble(ops[i].path, 0)
		grouped[heads[nb]] = ops[i]
		heads[nb]++
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
	activeBatches := 0
	for _, count := range counts {
		if count > 0 {
			activeBatches++
		}
	}

	var (
		buffers [maxFoldNibbles]*bufferedBranchStore
		errs    [maxFoldNibbles]error
		wg      sync.WaitGroup
		sem     = make(chan struct{}, limit)
	)
	for nb := 0; nb < maxFoldNibbles; nb++ {
		n := counts[nb]
		if n == 0 {
			continue
		}
		group := grouped[starts[nb] : starts[nb]+n]
		buf := borrowBufferedBranchStore(t.store)
		buffers[nb] = buf
		wg.Add(1)
		sem <- struct{}{}
		go func(nb uint8, buf *bufferedBranchStore, group []op) {
			defer wg.Done()
			defer func() { <-sem }()
			// Each subtrie folds sequentially against its private buffer. Keep
			// this tiny owner on the goroutine stack instead of allocating one
			// beside every spawned worker.
			sub := commitmentTrie{store: buf}
			// The worker owns its path backing array, so recursive appends can
			// reuse all 64 nibble slots without aliasing sibling workers.
			var path [pathLen]byte
			err := sub.applyNibble(path[:0], 0, branch, nb, group)
			if err == nil && concurrentFlush {
				// This worker only reads/writes prefixes beginning with nb. Publishing
				// its finished buffer cannot affect any still-running sibling, so overlap
				// encoding/writes with their computation and avoid a second goroutine wave.
				err = buf.flush(t.store, activeBatches)
			}
			errs[nb] = err
		}(uint8(nb), buf, group)
	}
	wg.Wait()

	for nb := 0; nb < maxFoldNibbles; nb++ {
		if errs[nb] != nil {
			returnSiblingBuffers(buffers)
			return nil, errs[nb]
		}
	}
	if !concurrentFlush {
		if err := flushSiblingBuffersSerial(t.store, buffers, activeBatches); err != nil {
			returnSiblingBuffers(buffers)
			return nil, err
		}
	}
	returnSiblingBuffers(buffers)

	if branch.childCount() == 0 {
		return nil, nil
	}
	return branch, nil
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
