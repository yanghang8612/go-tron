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
// The default is chosen so that trivially small commits (a handful of touched
// keys) stay sequential and never pay goroutine spawn / buffer-allocation
// overhead, while realistic per-block commits (typically hundreds–thousands of
// touched keys on an active TRON chain) fan out across cores. The commitment
// fold is keccak-bound (~28% of sync CPU, single-threaded before this), so the
// split recovers idle cores on the hot path.
var ParallelFoldMinOps = defaultParallelFoldMinOps

const defaultParallelFoldMinOps = 64

// maxFoldNibbles is the branching factor at every trie level: the root fans out
// into at most 16 independent first-nibble subtries.
const maxFoldNibbles = 16

// bufferedBranchStore wraps a base branchStore with read-through reads and
// locally-buffered writes. The parallel root fold gives each first-nibble subtrie
// its own bufferedBranchStore so the subtries can fold concurrently while sharing
// the base for reads; the buffers are flushed to the base serially after every
// subtrie has completed.
//
// Correctness rests on three properties of a single Fold descent:
//
//   - The 16 first-nibble subtries write DISJOINT branch-key prefixes (every row
//     a nibble-nb subtrie touches begins with nibble nb), so no two buffers ever
//     hold the same prefix and the serial flush order cannot affect the final
//     base state.
//   - The descent is single-pass and bottom-up: a subtrie writes a branch only
//     after computing it from its children, and never re-reads a prefix it has
//     written within the same fold. Read-through to the unmodified base is
//     therefore correct. (Buffer-first reads are kept anyway, so the store is
//     correct even if that invariant ever changes.)
//   - Base reads are concurrent-safe and the base is not mutated during the
//     parallel phase (writes are buffered): blockbuffer Get/GetNoCopy take a read
//     lock and are documented safe to call concurrently with mutators, pebble Get
//     is concurrent-safe, and go-ethereum memorydb guards reads with an RWMutex.
type bufferedBranchStore struct {
	base branchStore
	puts map[string][]byte   // prefix (one byte per nibble) -> encoded BranchData
	dels map[string]struct{} // prefix -> tombstone
}

func newBufferedBranchStore(base branchStore) *bufferedBranchStore {
	return &bufferedBranchStore{base: base}
}

func (s *bufferedBranchStore) GetBranch(prefix []byte) (BranchData, bool, error) {
	k := string(prefix)
	if _, tomb := s.dels[k]; tomb {
		return BranchData{}, false, nil
	}
	if enc, ok := s.puts[k]; ok {
		b, err := DecodeBranchData(enc)
		if err != nil {
			return BranchData{}, false, err
		}
		return b, true, nil
	}
	return s.base.GetBranch(prefix)
}

func (s *bufferedBranchStore) GetBranchInto(prefix []byte, dst *BranchData) (bool, error) {
	k := string(prefix)
	if _, tomb := s.dels[k]; tomb {
		*dst = BranchData{}
		return false, nil
	}
	if enc, ok := s.puts[k]; ok {
		if err := DecodeBranchDataInto(enc, dst); err != nil {
			return false, err
		}
		return true, nil
	}
	return s.base.GetBranchInto(prefix, dst)
}

func (s *bufferedBranchStore) PutBranch(prefix []byte, b BranchData) error {
	k := string(prefix)
	delete(s.dels, k)
	if s.puts == nil {
		s.puts = make(map[string][]byte)
	}
	// Encode eagerly: the caller (linkChild) returns *child to branchPool right
	// after PutBranch, so the value must not be retained by reference.
	s.puts[k] = b.Encode()
	return nil
}

func (s *bufferedBranchStore) DelBranch(prefix []byte) error {
	k := string(prefix)
	delete(s.puts, k)
	if s.dels == nil {
		s.dels = make(map[string]struct{})
	}
	s.dels[k] = struct{}{}
	return nil
}

// flush applies the buffered mutations to base. dels and puts hold disjoint
// prefixes, and across all sibling buffers every prefix is written at most once,
// so the resulting base state is independent of flush order; the sorted iteration
// only makes the emitted write stream deterministic.
func (s *bufferedBranchStore) flush(base branchStore) error {
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
		// Fast path: write the buffered encoded bytes straight through, so the
		// expensive BranchData encode already happened inside the (parallel)
		// subtrie goroutine and the serial flush is just a KV write.
		if ep, ok := base.(encodedBranchPutter); ok {
			for _, k := range keys {
				if err := ep.putBranchEncoded([]byte(k), s.puts[k]); err != nil {
					return err
				}
			}
			return nil
		}
		for _, k := range keys {
			b, err := DecodeBranchData(s.puts[k])
			if err != nil {
				return err
			}
			if err := base.PutBranch([]byte(k), b); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodedBranchPutter is an optional branchStore capability: writing an
// already-encoded BranchData row without a Decode/Encode round trip. The
// production rawdbBranchStore implements it; the parallel flush uses it when
// available and otherwise falls back to PutBranch.
type encodedBranchPutter interface {
	putBranchEncoded(prefix, encoded []byte) error
}

// applyRootParallel is the parallel counterpart of apply at the root (prefix nil,
// depth 0). It buckets ops by their first nibble and folds each non-empty
// first-nibble subtrie concurrently, each against a private bufferedBranchStore,
// then flushes the buffers into the shared store serially and returns the updated
// root branch.
//
// The shared root branch is safe to mutate concurrently: each subtrie touches
// only its own children[nb] slot (an independent array element), and the slots
// carry no shared bitmap. errs/WaitGroup establish the happens-before edge that
// makes those slot writes visible to the caller after Wait.
func (t *commitmentTrie) applyRootParallel(branch *BranchData, ops []op) (*BranchData, error) {
	if branch == nil {
		branch = &BranchData{}
	}

	// Bucket ops by first nibble via the same counting sort apply uses, but into
	// a dedicated backing slice (not the pooled scratch) because the per-nibble
	// groups must stay alive and isolated for the whole concurrent phase.
	var counts [maxFoldNibbles]int
	for i := range ops {
		counts[ops[i].path[0]]++
	}
	var starts [maxFoldNibbles]int
	for i := 1; i < maxFoldNibbles; i++ {
		starts[i] = starts[i-1] + counts[i-1]
	}
	grouped := make([]op, len(ops))
	heads := starts
	for i := range ops {
		nb := ops[i].path[0]
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
		buf := newBufferedBranchStore(t.store)
		buffers[nb] = buf
		// Each subtrie folds sequentially against its private buffer.
		sub := &commitmentTrie{store: buf}
		wg.Add(1)
		sem <- struct{}{}
		go func(nb uint8, sub *commitmentTrie, group []op) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[nb] = sub.applyNibble(nil, 0, branch, nb, group)
		}(uint8(nb), sub, group)
	}
	wg.Wait()

	for nb := 0; nb < maxFoldNibbles; nb++ {
		if errs[nb] != nil {
			return nil, errs[nb]
		}
	}
	// Serial flush in nibble order. Disjoint keyspaces make the base state
	// order-independent; nibble order keeps the emitted write stream deterministic.
	for nb := 0; nb < maxFoldNibbles; nb++ {
		if buffers[nb] == nil {
			continue
		}
		if err := buffers[nb].flush(t.store); err != nil {
			return nil, err
		}
	}

	if branch.childCount() == 0 {
		return nil, nil
	}
	return branch, nil
}
