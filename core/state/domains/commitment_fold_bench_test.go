package domains

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/blockbuffer"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// buildRandomPuts returns n deterministic pseudo-random put updates with 32-byte
// keys and 8-byte values drawn from rng. Distinct seeds yield disjoint key sets.
func buildRandomPuts(rng *rand.Rand, n int) []Update {
	ups := make([]Update, n)
	for i := 0; i < n; i++ {
		key := make([]byte, 32)
		_, _ = rng.Read(key)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, rng.Uint64())
		ups[i] = Update{Key: key, Value: val}
	}
	return ups
}

// keyValueStoreWithoutPointSnapshot intentionally hides Pebble's optional
// pointread.Snapshotter method while retaining the complete ethdb surface. It
// gives the Pebble fold benchmark an exact fallback baseline.
type keyValueStoreWithoutPointSnapshot struct {
	ethdb.KeyValueStore
}

func (s keyValueStoreWithoutPointSnapshot) View(key []byte, fn func([]byte) error) error {
	return s.KeyValueStore.(interface {
		View([]byte, func([]byte) error) error
	}).View(key, fn)
}

func (s keyValueStoreWithoutPointSnapshot) IsKeyNotFound(err error) bool {
	return s.KeyValueStore.(interface{ IsKeyNotFound(error) bool }).IsKeyNotFound(err)
}

// mapBase / rawdbBase are the two benchmark base stores. mapBase re-encodes on
// every read (worst case for the parallel split — inflates serial store cost);
// rawdbBase is the production branchStore over an in-memory KV and is the
// faithful number (decode-on-read, encode-on-write, no read round-trip).
func mapBase() branchStore   { return newMapBranchStore() }
func rawdbBase() branchStore { return newRawdbBranchStore(rawdb.NewMemoryDatabase()) }

// serialFlushBranchStore deliberately hides concurrentSiblingFlushStore while
// preserving the same reads/writes. It gives the blockbuffer benchmark an
// apples-to-apples serial-flush baseline without disabling parallel subtree
// computation.
type serialFlushBranchStore struct{ branchStore }

func rawdbBlockbufferBase(parallelFlush bool) branchStore {
	buf := blockbuffer.New(rawdb.NewMemoryDatabase())
	buf.BeginBlock(common.Hash{1}, 1)
	h, ok := buf.NewestInflight()
	if !ok {
		panic("missing benchmark blockbuffer layer")
	}
	// Async mainnet commitment folds use the layer-bound view, not Buffer's
	// newest-inflight convenience writer. Keep the benchmark on that exact path.
	store := branchStore(newRawdbBranchStore(buf.ViewLayer(h)))
	if !parallelFlush {
		store = &serialFlushBranchStore{branchStore: store}
	}
	return store
}

// benchFoldIncremental measures folding a batch of N updates onto a pre-populated
// base trie, approximating a per-block commit on a large existing state. With
// parallel=false it characterizes the sequential fold; with parallel=true it
// measures the actual speedup and reveals the crossover size.
func benchFoldIncremental(b *testing.B, parallel bool, newBase func() branchStore) {
	benchFoldIncrementalInput(b, parallel, false, newBase)
}

func benchFoldIncrementalInput(b *testing.B, parallel, sortByRawKey bool, newBase func() branchStore) {
	const base = 100_000
	store := newBase()
	trie := newCommitmentTrie(store)
	if parallel {
		trie.parallelMinOps = 1
	}
	if _, err := trie.Fold(buildRandomPuts(rand.New(rand.NewSource(1)), base)); err != nil {
		b.Fatal(err)
	}

	for _, n := range []int{16, 64, 256, 1024, 4096} {
		batch := buildRandomPuts(rand.New(rand.NewSource(int64(1000+n))), n)
		if sortByRawKey {
			sort.Slice(batch, func(i, j int) bool {
				return bytes.Compare(batch[i].Key, batch[j].Key) < 0
			})
		}
		if _, err := trie.Fold(batch); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("base=%d/batch=%d", base, n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for j := range batch {
					binary.BigEndian.PutUint64(batch[j].Value, uint64(i)<<20|uint64(j))
				}
				if _, err := trie.Fold(batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/update")
		})
	}
}

func BenchmarkFoldSeqMap(b *testing.B)   { benchFoldIncremental(b, false, mapBase) }
func BenchmarkFoldParMap(b *testing.B)   { benchFoldIncremental(b, true, mapBase) }
func BenchmarkFoldSeqRawdb(b *testing.B) { benchFoldIncremental(b, false, rawdbBase) }
func BenchmarkFoldParRawdb(b *testing.B) { benchFoldIncremental(b, true, rawdbBase) }

// BenchmarkFoldParRawdbCoalesced matches the production staged-store input:
// last-writer-wins has already run and the raw keys are strictly sorted.
func BenchmarkFoldParRawdbCoalesced(b *testing.B) {
	benchFoldIncrementalInput(b, true, true, rawdbBase)
}

func BenchmarkFoldParBlockbufferSerialFlushCoalesced(b *testing.B) {
	benchFoldIncrementalInput(b, true, true, func() branchStore { return rawdbBlockbufferBase(false) })
}

func BenchmarkSortOps(b *testing.B) {
	for _, n := range []int{16, 256} {
		updates := buildRandomPuts(rand.New(rand.NewSource(int64(2000+n))), n)
		template := make([]op, n)
		for i, update := range updates {
			template[i] = resolveOp(update)
		}
		ops := make([]op, n)
		b.Run(fmt.Sprintf("ops=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				copy(ops, template)
				sortOps(ops)
			}
		})
	}
}

func BenchmarkFoldParBlockbufferParallelFlushCoalesced(b *testing.B) {
	benchFoldIncrementalInput(b, true, true, func() branchStore { return rawdbBlockbufferBase(true) })
}

// BenchmarkAsyncFoldParentBranchReads measures the complete production-shaped
// one-block fold: a deep immutable base, a fresh in-flight block layer, and 64
// incremental updates. The async constructor skips guaranteed misses in that
// fresh layer while the normal constructor preserves ordinary read-your-own-
// writes semantics. A fresh buffer per iteration keeps both arms on identical
// parent state and excludes setup from the timed region.
func BenchmarkAsyncFoldParentBranchReads(b *testing.B) {
	const baseSize = 100_000
	rng := rand.New(rand.NewSource(441))
	seedRaw := buildRandomPuts(rng, baseSize)
	seed := make([]rawdb.StateCommitmentUpdate, len(seedRaw))
	for i := range seedRaw {
		seed[i] = rawdb.NewStateCommitmentPut(seedRaw[i].Key, seedRaw[i].Value)
	}
	base := rawdb.NewMemoryDatabase()
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(base), seed); err != nil {
		b.Fatal(err)
	}

	updates := make([]rawdb.StateCommitmentUpdate, 64)
	for i := range updates {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, uint64(i+1))
		updates[i] = rawdb.NewStateCommitmentPut(seedRaw[i*997].Key, value)
	}

	for _, tc := range []struct {
		name  string
		store func(CommitmentDB) LatestCommitmentStore
	}{
		{name: "ordinary", store: NewStagedCommitmentStore},
		{name: "async_parent", store: NewStagedCommitmentStoreForAsyncFold},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var expected common.Hash
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				buf := blockbuffer.New(base)
				buf.BeginBlock(common.Hash{byte(i + 1)}, uint64(i+1))
				h, ok := buf.NewestInflight()
				if !ok {
					b.Fatal("missing benchmark in-flight layer")
				}
				store := tc.store(buf.ViewLayer(h))
				b.StartTimer()
				root, err := ApplyLatestCommitmentWithStore(store, updates)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if expected == (common.Hash{}) {
					expected = root
				} else if root != expected {
					b.Fatalf("root = %x, want %x", root, expected)
				}
			}
		})
	}
}

// BenchmarkAsyncFoldPebbleParentReadSession compares the old per-branch
// DB.Get path with the production cursor-backed parent session over an actual
// compacted Pebble database. Setup builds and flushes the parent trie once;
// each measured iteration writes only to a throwaway in-flight layer.
func BenchmarkAsyncFoldPebbleParentReadSession(b *testing.B) {
	const baseSize = 50_000
	rng := rand.New(rand.NewSource(9441))
	seedRaw := buildRandomPuts(rng, baseSize)
	seed := make([]rawdb.StateCommitmentUpdate, len(seedRaw))
	for i := range seedRaw {
		seed[i] = rawdb.NewStateCommitmentPut(seedRaw[i].Key, seedRaw[i].Value)
	}
	base, err := rawdb.NewPebbleDB(b.TempDir(), 64, 128)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = base.Close() })
	seedBuffer := blockbuffer.New(base)
	seedBuffer.BeginBlock(common.Hash{1}, 1)
	h, _ := seedBuffer.NewestInflight()
	if _, err := ApplyLatestCommitmentWithStore(NewStagedCommitmentStore(seedBuffer.ViewLayer(h)), seed); err != nil {
		b.Fatal(err)
	}
	if err := seedBuffer.CommitInflight(h); err != nil {
		b.Fatal(err)
	}
	if err := seedBuffer.FlushUpTo(1, base); err != nil {
		b.Fatal(err)
	}
	if err := base.Compact(nil, nil); err != nil {
		b.Fatal(err)
	}

	const batchCount = 256
	batches := make([][]rawdb.StateCommitmentUpdate, batchCount)
	for batch := range batches {
		batches[batch] = make([]rawdb.StateCommitmentUpdate, 64)
		for i := range batches[batch] {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, uint64(batch*64+i+1))
			index := ((batch*64 + i) * 733) % baseSize
			batches[batch][i] = rawdb.NewStateCommitmentPut(seedRaw[index].Key, value)
		}
	}
	for _, tc := range []struct {
		name string
		base ethdb.KeyValueStore
	}{
		{name: "db_get", base: keyValueStoreWithoutPointSnapshot{base}},
		{name: "snapshot_cursor", base: base},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			expected := make([]common.Hash, len(batches))
			buf := blockbuffer.New(tc.base)
			buf.SetBaseReadCacheSize(16 << 20)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				buf.BeginBlock(common.Hash{byte(i + 1)}, uint64(i+1))
				h, _ := buf.NewestInflight()
				store := NewStagedCommitmentStoreForAsyncFold(buf.ViewLayer(h))
				batch := i % len(batches)
				b.StartTimer()
				root, err := ApplyLatestCommitmentWithStore(store, batches[batch])
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if expected[batch] == (common.Hash{}) {
					expected[batch] = root
				} else if root != expected[batch] {
					b.Fatalf("root = %x, want %x", root, expected[batch])
				}
				buf.DiscardInflight(h)
			}
		})
	}
}

// BenchmarkBuildOps isolates the production coalesced+raw-key-sorted fast path
// from the arbitrary-order fallback that must retain last-writer-wins semantics.
func BenchmarkBuildOps(b *testing.B) {
	const n = 1024
	unsorted := buildRandomPuts(rand.New(rand.NewSource(99)), n)
	sorted := append([]Update(nil), unsorted...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].Key, sorted[j].Key) < 0
	})
	for _, tc := range []struct {
		name    string
		updates []Update
	}{
		{name: "sorted-coalesced", updates: sorted},
		{name: "arbitrary-order", updates: unsorted},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				opsP, err := buildOps(tc.updates)
				if err != nil {
					b.Fatal(err)
				}
				returnOpsBuf(opsP)
			}
		})
	}
}

// benchFoldCrossover sweeps small batch sizes against a DEEP pre-populated trie
// to locate the sequential→parallel crossover op count — the data that justifies
// ParallelFoldMinOps. A deeper base raises per-op traversal cost (more keccak +
// branch reads per resolved key), so parallel wins at a LOWER op count than a
// shallow trie; the live chain trie is far deeper still, so the measured
// crossover here is a conservative upper bound on the production crossover.
func benchFoldCrossover(b *testing.B, parallel bool, base int) {
	store := rawdbBase()
	trie := newCommitmentTrie(store)
	if parallel {
		trie.parallelMinOps = 1
	}
	if _, err := trie.Fold(buildRandomPuts(rand.New(rand.NewSource(1)), base)); err != nil {
		b.Fatal(err)
	}
	for _, n := range []int{8, 16, 24, 32, 48, 64} {
		batch := buildRandomPuts(rand.New(rand.NewSource(int64(7000+n))), n)
		if _, err := trie.Fold(batch); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("base=%d/batch=%d", base, n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for j := range batch {
					binary.BigEndian.PutUint64(batch[j].Value, uint64(i)<<20|uint64(j))
				}
				if _, err := trie.Fold(batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(n), "ns/update")
		})
	}
}

func BenchmarkFoldCrossoverSeq(b *testing.B) { benchFoldCrossover(b, false, 500_000) }
func BenchmarkFoldCrossoverPar(b *testing.B) { benchFoldCrossover(b, true, 500_000) }
