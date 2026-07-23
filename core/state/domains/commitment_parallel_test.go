package domains

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// newParallelTrie returns a trie that forces the parallel root fold for every
// non-empty fold (parallelMinOps = 1), so equivalence tests exercise the
// concurrent path even for tiny batches.
func newParallelTrie(store branchStore) *commitmentTrie {
	t := newCommitmentTrie(store)
	t.parallelMinOps = 1
	return t
}

type concurrentFlushProbe struct {
	mu      sync.Mutex
	base    *mapBranchStore
	entered chan struct{}
	release chan struct{}
	active  atomic.Int32
	max     atomic.Int32
	failOn  int
}

func newConcurrentFlushProbe() *concurrentFlushProbe {
	return &concurrentFlushProbe{
		base:    newMapBranchStore(),
		entered: make(chan struct{}, maxFoldNibbles),
		release: make(chan struct{}),
		failOn:  -1,
	}
}

func (*concurrentFlushProbe) concurrentSiblingFlushSafe() bool { return true }

func (s *concurrentFlushProbe) GetBranch(prefix []byte) (BranchData, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base.GetBranch(prefix)
}

func (s *concurrentFlushProbe) GetBranchInto(prefix []byte, dst *BranchData) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base.GetBranchInto(prefix, dst)
}

func (s *concurrentFlushProbe) PutBranch(prefix []byte, branch BranchData) error {
	active := s.active.Add(1)
	for max := s.max.Load(); active > max && !s.max.CompareAndSwap(max, active); max = s.max.Load() {
	}
	defer s.active.Add(-1)
	s.entered <- struct{}{}
	<-s.release
	if len(prefix) > 0 && int(prefix[0]) == s.failOn {
		return fmt.Errorf("flush probe failure at prefix %x", prefix)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base.PutBranch(prefix, branch)
}

func (s *concurrentFlushProbe) DelBranch(prefix []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base.DelBranch(prefix)
}

func siblingFlushOps(nibbles ...uint8) []op {
	ops := make([]op, 0, len(nibbles)*2)
	for _, nb := range nibbles {
		for child := uint8(0); child < 2; child++ {
			o := op{
				key:     []byte{0xff, nb, child},
				valHash: common.Hash{nb + 1, child + 1},
			}
			o.path[0] = nb<<4 | child
			ops = append(ops, o)
		}
	}
	return ops
}

func TestApplyRootParallelFlushesOptedInStoreConcurrently(t *testing.T) {
	probe := newConcurrentFlushProbe()
	trie := newCommitmentTrie(probe)
	trie.parallelLimit = 2

	type result struct {
		root *BranchData
		err  error
	}
	done := make(chan result, 1)
	go func() {
		root, err := trie.applyRootParallel(nil, siblingFlushOps(0, 1))
		done <- result{root: root, err: err}
	}()
	timer := time.NewTimer(2 * time.Second)
	concurrent := true
	for i := 0; i < 2; i++ {
		select {
		case <-probe.entered:
		case <-timer.C:
			concurrent = false
			i = 2
		}
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	close(probe.release)
	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.root == nil || got.root.childCount() != 2 {
		t.Fatalf("parallel root = %#v, want two children", got.root)
	}
	if !concurrent || probe.max.Load() < 2 {
		t.Fatalf("flush max concurrency = %d, want at least 2", probe.max.Load())
	}
	if rows := probe.base.rowSet(); len(rows) != 2 {
		t.Fatalf("parallel flush wrote %d rows, want 2", len(rows))
	}
}

func TestRawdbBranchStoreConcurrentFlushRequiresMarkedDB(t *testing.T) {
	direct := newRawdbBranchStore(rawdb.NewMemoryDatabase())
	if direct.concurrentSiblingFlushSafe() {
		t.Fatal("unmarked memory database unexpectedly opted into concurrent flush")
	}
	buffered := newRawdbBranchStore(blockbuffer.New(rawdb.NewMemoryDatabase()))
	if !buffered.concurrentSiblingFlushSafe() {
		t.Fatal("blockbuffer database did not opt into concurrent flush")
	}
	buf := blockbuffer.New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(common.Hash{1}, 1)
	h, ok := buf.NewestInflight()
	if !ok {
		t.Fatal("missing in-flight blockbuffer layer")
	}
	layer := newRawdbBranchStore(buf.ViewLayer(h))
	if !layer.concurrentSiblingFlushSafe() {
		t.Fatal("blockbuffer layer view did not opt into concurrent flush")
	}
}

func TestApplyRootParallelJoinsSiblingFlushesAfterError(t *testing.T) {
	probe := newConcurrentFlushProbe()
	probe.failOn = 1
	close(probe.release)
	trie := newCommitmentTrie(probe)
	trie.parallelLimit = 3
	if _, err := trie.applyRootParallel(nil, siblingFlushOps(0, 1, 2)); err == nil {
		t.Fatal("parallel flush unexpectedly succeeded")
	}
	// Siblings are joined rather than abandoned on the first error, so the two
	// non-failing disjoint writes both complete.
	if rows := probe.base.rowSet(); len(rows) != 2 {
		t.Fatalf("parallel error path completed %d sibling rows, want 2", len(rows))
	}
}

func cloneUpdates(in []Update) []Update {
	out := make([]Update, len(in))
	for i, u := range in {
		out[i] = Update{
			Key:    append([]byte(nil), u.Key...),
			Value:  append([]byte(nil), u.Value...),
			Delete: u.Delete,
		}
	}
	return out
}

// keyUniverse generates realistic incremental update batches: new inserts,
// overwrites of live keys, and deletes of live keys. Keys are fully random so
// their keccak paths (and therefore first nibbles) spread across all 16 subtries.
type keyUniverse struct {
	rng  *rand.Rand
	live [][]byte
}

func (u *keyUniverse) newKey() []byte {
	k := make([]byte, 32)
	_, _ = u.rng.Read(k)
	return k
}

func (u *keyUniverse) value() []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, u.rng.Uint64())
	return v
}

// batch returns n updates mixing inserts, overwrites, and deletes.
func (u *keyUniverse) batch(n int, delFrac, owFrac float64) []Update {
	ups := make([]Update, 0, n)
	for i := 0; i < n; i++ {
		r := u.rng.Float64()
		switch {
		case len(u.live) > 0 && r < delFrac:
			idx := u.rng.Intn(len(u.live))
			ups = append(ups, del(u.live[idx]))
			u.live[idx] = u.live[len(u.live)-1]
			u.live = u.live[:len(u.live)-1]
		case len(u.live) > 0 && r < delFrac+owFrac:
			idx := u.rng.Intn(len(u.live))
			ups = append(ups, put(u.live[idx], u.value()))
		default:
			k := u.newKey()
			ups = append(ups, put(k, u.value()))
			u.live = append(u.live, k)
		}
	}
	return ups
}

// assertFoldEquivalent folds the same batch into a sequential and a parallel trie
// (sharing identical prior state) and asserts byte-identical root + branch rows.
func assertFoldEquivalent(t *testing.T, label string, seqTrie, parTrie *commitmentTrie, seqStore, parStore *mapBranchStore, batch []Update) {
	t.Helper()
	seqRoot, err := seqTrie.Fold(cloneUpdates(batch))
	if err != nil {
		t.Fatalf("%s: sequential fold: %v", label, err)
	}
	parRoot, err := parTrie.Fold(cloneUpdates(batch))
	if err != nil {
		t.Fatalf("%s: parallel fold: %v", label, err)
	}
	if seqRoot != parRoot {
		t.Fatalf("%s: root mismatch\n  seq=%x\n  par=%x", label, seqRoot, parRoot)
	}
	assertRowSetsEqual(t, seqStore.rowSet(), parStore.rowSet())
}

// TestBufferedBranchStoreRePutOverwrites locks the two contracts the parallel
// fold's per-subtrie buffer relies on. (1) Re-PUTting a prefix OVERWRITES, so a
// branch rebuilt once per op passing through it costs one slot — not one stale
// copy per rebuild, the append-only-arena blowup that made the parallel path
// lose to sequential at high op counts. (2) The buffer keeps an independent
// value, so mutating the source after PutBranch (its *child is pool-recycled)
// cannot corrupt what was buffered.
func TestBufferedBranchStoreRePutOverwrites(t *testing.T) {
	base := newMapBranchStore()
	buf := newBufferedBranchStore(base)
	prefix := []byte{0x0a}

	var a BranchData
	a.SetHashChild(1, common.Hash{0xaa})
	for i := 0; i < 100; i++ { // same prefix rebuilt many times
		if err := buf.PutBranch(prefix, a); err != nil {
			t.Fatal(err)
		}
	}
	firstSlot := buf.puts[string(prefix)]
	var final BranchData
	final.SetHashChild(1, common.Hash{0xbb})
	final.SetLeafChild(2, []byte{0x05}, common.Hash{0xcc})
	if err := buf.PutBranch(prefix, final); err != nil {
		t.Fatal(err)
	}
	if len(buf.puts) != 1 {
		t.Fatalf("re-PUT must overwrite: buffer holds %d entries, want 1", len(buf.puts))
	}
	if buf.puts[string(prefix)] != firstSlot {
		t.Fatal("re-PUT replaced its pooled destination instead of reusing it")
	}
	got, ok, err := buf.GetBranch(prefix)
	if err != nil || !ok {
		t.Fatalf("GetBranch after re-PUT: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.Encode(), final.Encode()) {
		t.Fatal("GetBranch after re-PUT returned a stale value")
	}

	// Independence: mutate the source after PUT; the buffered value must not move.
	src := final
	if err := buf.PutBranch([]byte{0x0b}, src); err != nil {
		t.Fatal(err)
	}
	src.SetLeafChild(2, []byte{0x09}, common.Hash{0xee})
	got2, _, _ := buf.GetBranch([]byte{0x0b})
	if !bytes.Equal(got2.Encode(), final.Encode()) {
		t.Fatal("buffered value changed when the source was mutated after PUT")
	}

	if err := buf.flush(base); err != nil {
		t.Fatal(err)
	}
	if len(buf.puts) != 0 {
		t.Fatalf("flush retained %d pooled branches, want 0", len(buf.puts))
	}
	if rows := base.rowSet(); len(rows) != 2 {
		t.Fatalf("flush emitted %d rows, want 2 (one per distinct prefix)", len(rows))
	}
}

// TestParallelFoldMatchesSequential_Incremental drives many rounds of mixed
// inserts/overwrites/deletes through a sequential trie and a forced-parallel
// trie, asserting byte-identical root and branch keyspace after every round.
// Because each round folds onto the prior round's (parallel-written) state, this
// proves the parallel path stays equivalent across an incrementally built trie —
// the real production scenario where every commit is parallel.
func TestParallelFoldMatchesSequential_Incremental(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 1009, 65537} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			seqStore, parStore := newMapBranchStore(), newMapBranchStore()
			seqTrie := newCommitmentTrie(seqStore)
			parTrie := newParallelTrie(parStore)
			u := &keyUniverse{rng: rand.New(rand.NewSource(seed))}
			for round := 0; round < 40; round++ {
				n := 1 + u.rng.Intn(500)
				batch := u.batch(n, 0.25, 0.20)
				assertFoldEquivalent(t, fmt.Sprintf("round=%d/n=%d", round, n), seqTrie, parTrie, seqStore, parStore, batch)
			}
		})
	}
}

// TestParallelFoldMatchesSequential_RawdbStore drives the same incremental mix
// through rawdbBranchStore over an in-memory KV, proving the production encoding
// remains byte-identical across sequential and parallel subtree computation.
func TestParallelFoldMatchesSequential_RawdbStore(t *testing.T) {
	seqDB, parDB := rawdb.NewMemoryDatabase(), rawdb.NewMemoryDatabase()
	seqTrie := newCommitmentTrie(newRawdbBranchStore(seqDB))
	parTrie := newCommitmentTrie(newRawdbBranchStore(parDB))
	parTrie.parallelMinOps = 1 // force parallel subtree computation

	u := &keyUniverse{rng: rand.New(rand.NewSource(2024))}
	for round := 0; round < 30; round++ {
		n := 1 + u.rng.Intn(400)
		batch := u.batch(n, 0.25, 0.20)
		seqRoot, err := seqTrie.Fold(cloneUpdates(batch))
		if err != nil {
			t.Fatalf("round %d: sequential fold: %v", round, err)
		}
		parRoot, err := parTrie.Fold(cloneUpdates(batch))
		if err != nil {
			t.Fatalf("round %d: parallel fold: %v", round, err)
		}
		if seqRoot != parRoot {
			t.Fatalf("round %d: root mismatch\n  seq=%x\n  par=%x", round, seqRoot, parRoot)
		}
		assertRowSetsEqual(t, snapshotBranches(t, seqDB), snapshotBranches(t, parDB))
	}
}

// TestParallelFoldOverBlockbuffer_RaceAndRootMatch folds over the REAL production
// base store — a blockbuffer.Buffer, whose GetNoCopy is the exact concurrent read
// path the production commitment fold uses. Run under -race, it proves the
// parallel fold's 16 concurrent subtries safely read the live blockbuffer via
// immutable topology views and sharded maps. It also exercises the opted-in
// concurrent sibling flush and verifies the resulting root matches sequential.
func TestParallelFoldOverBlockbuffer_RaceAndRootMatch(t *testing.T) {
	foldOverBuffer := func(parallel, layerView bool) common.Hash {
		buf := blockbuffer.New(rawdb.NewMemoryDatabase())
		buf.BeginBlock(common.Hash{0x01}, 1)
		var db CommitmentDB = buf
		if layerView {
			h, ok := buf.NewestInflight()
			if !ok {
				t.Fatal("missing in-flight blockbuffer layer")
			}
			db = buf.ViewLayer(h)
		}
		trie := newCommitmentTrie(newRawdbBranchStore(db))
		if parallel {
			trie.parallelMinOps = 1
		}
		universe := &keyUniverse{rng: rand.New(rand.NewSource(5))}
		// Base trie the parallel subtries will read concurrently via GetNoCopy.
		if _, err := trie.Fold(universe.batch(2000, 0, 0)); err != nil {
			t.Fatal(err)
		}
		// A 500-key mix of inserts, overwrites, and deletes forces concurrent
		// PutBranch and DelBranch publication while sibling workers still read.
		root, err := trie.Fold(universe.batch(500, 0.25, 0.40))
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	for _, tc := range []struct {
		name      string
		layerView bool
	}{
		{name: "buffer"},
		{name: "layer_view", layerView: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seqRoot := foldOverBuffer(false, tc.layerView)
			parRoot := foldOverBuffer(true, tc.layerView)
			if seqRoot != parRoot {
				t.Fatalf("blockbuffer parallel fold root mismatch\n  seq=%x\n  par=%x", seqRoot, parRoot)
			}
		})
	}
}

// TestParallelFoldMatchesSequential_FromScratch folds one large batch into empty
// tries at a range of sizes spanning the threshold.
func TestParallelFoldMatchesSequential_FromScratch(t *testing.T) {
	for _, n := range []int{1, 2, 16, 63, 64, 65, 256, 2048, 8192} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			seqStore, parStore := newMapBranchStore(), newMapBranchStore()
			batch := buildRandomPuts(rand.New(rand.NewSource(int64(n)*31+1)), n)
			seqRoot, err := newCommitmentTrie(seqStore).Fold(cloneUpdates(batch))
			if err != nil {
				t.Fatal(err)
			}
			parRoot, err := newParallelTrie(parStore).Fold(cloneUpdates(batch))
			if err != nil {
				t.Fatal(err)
			}
			if seqRoot != parRoot {
				t.Fatalf("root mismatch n=%d\n  seq=%x\n  par=%x", n, seqRoot, parRoot)
			}
			assertRowSetsEqual(t, seqStore.rowSet(), parStore.rowSet())
		})
	}
}

// TestParallelFoldMatchesSequential_EdgeCases pins the structurally tricky shapes:
// a batch confined to a single nibble (one active subtrie), deletes that empty
// the whole trie (root → nil), and a delete that collapses the root to a single
// leaf.
func TestParallelFoldMatchesSequential_EdgeCases(t *testing.T) {
	// Single-nibble concentration: keys whose keccak path starts with the same
	// nibble all land in one subtrie, so only one goroutine does work.
	t.Run("single_nibble", func(t *testing.T) {
		seqStore, parStore := newMapBranchStore(), newMapBranchStore()
		rng := rand.New(rand.NewSource(99))
		var batch []Update
		for len(batch) < 200 {
			k := make([]byte, 32)
			_, _ = rng.Read(k)
			if pathNibble(keyPath(k), 0) == 0x0a { // confine to nibble 0xa
				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, rng.Uint64())
				batch = append(batch, put(k, v))
			}
		}
		seqRoot, err := newCommitmentTrie(seqStore).Fold(cloneUpdates(batch))
		if err != nil {
			t.Fatal(err)
		}
		parRoot, err := newParallelTrie(parStore).Fold(cloneUpdates(batch))
		if err != nil {
			t.Fatal(err)
		}
		if seqRoot != parRoot {
			t.Fatalf("root mismatch\n  seq=%x\n  par=%x", seqRoot, parRoot)
		}
		assertRowSetsEqual(t, seqStore.rowSet(), parStore.rowSet())
	})

	// Insert a large batch, then delete ALL of it in the same parallel path:
	// every subtrie collapses, the root must vanish to the zero hash.
	t.Run("full_delete", func(t *testing.T) {
		seqStore, parStore := newMapBranchStore(), newMapBranchStore()
		seqTrie, parTrie := newCommitmentTrie(seqStore), newParallelTrie(parStore)
		batch := buildRandomPuts(rand.New(rand.NewSource(7)), 1000)
		assertFoldEquivalent(t, "insert", seqTrie, parTrie, seqStore, parStore, batch)
		dels := make([]Update, len(batch))
		for i, u := range batch {
			dels[i] = del(u.Key)
		}
		assertFoldEquivalent(t, "delete_all", seqTrie, parTrie, seqStore, parStore, dels)
		root, err := parTrie.Fold(nil)
		if err != nil {
			t.Fatal(err)
		}
		if root != emptyRoot {
			t.Fatalf("emptied trie root = %x, want zero", root)
		}
		if len(parStore.rowSet()) != 0 {
			t.Fatalf("emptied trie left %d branch rows", len(parStore.rowSet()))
		}
	})
}
