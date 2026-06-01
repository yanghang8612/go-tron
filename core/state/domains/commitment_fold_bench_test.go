package domains

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

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

// mapBase / rawdbBase are the two benchmark base stores. mapBase re-encodes on
// every read (worst case for the parallel split — inflates serial store cost);
// rawdbBase is the production branchStore over an in-memory KV and is the
// faithful number (decode-on-read, encode-on-write, no read round-trip).
func mapBase() branchStore   { return newMapBranchStore() }
func rawdbBase() branchStore { return newRawdbBranchStore(rawdb.NewMemoryDatabase()) }

// benchFoldIncremental measures folding a batch of N updates onto a pre-populated
// base trie, approximating a per-block commit on a large existing state. With
// parallel=false it characterizes the sequential fold; with parallel=true it
// measures the actual speedup and reveals the crossover size.
func benchFoldIncremental(b *testing.B, parallel bool, newBase func() branchStore) {
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
