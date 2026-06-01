package snapshots

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type cbRec struct {
	off  uint64
	data []byte
}

// writeCompressedBlockRecords writes recs (variable-length) and returns the path
// plus each record's reported uncompressed offset.
func writeCompressedBlockRecords(t *testing.T, dir string, blockSize int, recs [][]byte) (string, []cbRec) {
	t.Helper()
	w, err := newCompressedBlockWriter(dir, blockSize)
	if err != nil {
		t.Fatalf("newCompressedBlockWriter: %v", err)
	}
	offs := make([]cbRec, len(recs))
	for i, rec := range recs {
		off, err := w.Append(rec)
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		offs[i] = cbRec{off: off, data: rec}
	}
	path := filepath.Join(dir, fmt.Sprintf("seg-%d.cb", blockSize))
	if err := w.Finish(path); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	return path, offs
}

func randomRecords(seed int64, n, maxLen int) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	recs := make([][]byte, n)
	for i := range recs {
		d := make([]byte, 1+rng.Intn(maxLen))
		_, _ = rng.Read(d)
		recs[i] = d
	}
	return recs
}

// TestCompressedBlockRandomAccessEquivalence proves the block-table addresses
// every record by its uncompressed offset (the keyed-lookup path) and that a
// block-by-block walk reconstructs the exact record stream (the range path).
func TestCompressedBlockRandomAccessEquivalence(t *testing.T) {
	for _, blockSize := range []int{1, 7, 128, 333} {
		t.Run(fmt.Sprintf("B=%d", blockSize), func(t *testing.T) {
			dir := t.TempDir()
			recs := randomRecords(int64(blockSize)*7+1, 5000, 200)
			path, offs := writeCompressedBlockRecords(t, dir, blockSize, recs)

			r, err := openCompressedBlockReader(path)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer r.Close()
			if r.recCount != uint64(len(recs)) {
				t.Fatalf("recCount = %d, want %d", r.recCount, len(recs))
			}

			// Keyed: each record's tail begins with exactly that record's bytes.
			for i, rc := range offs {
				tail, err := r.RecordTailAt(rc.off)
				if err != nil {
					t.Fatalf("RecordTailAt(%d) rec %d: %v", rc.off, i, err)
				}
				if uint64(len(tail)) < uint64(len(rc.data)) || !bytes.Equal(tail[:len(rc.data)], rc.data) {
					t.Fatalf("record %d at off %d mismatch", i, rc.off)
				}
			}

			// Range: walk blocks; concatenated decompressed bytes == all records.
			var rebuilt []byte
			for off := uint64(0); off < r.uncSize; {
				blk, start, err := r.BlockAt(off)
				if err != nil {
					t.Fatalf("BlockAt(%d): %v", off, err)
				}
				if start != off {
					t.Fatalf("BlockAt(%d) start = %d, want block-aligned", off, start)
				}
				rebuilt = append(rebuilt, blk...)
				off = start + uint64(len(blk))
			}
			var all []byte
			for _, rc := range recs {
				all = append(all, rc...)
			}
			if !bytes.Equal(rebuilt, all) {
				t.Fatalf("range reconstruction mismatch: got %d bytes, want %d", len(rebuilt), len(all))
			}
		})
	}
}

// TestCompressedBlockEdgeCases covers empty, single, exact-block, and
// one-past-block record counts.
func TestCompressedBlockEdgeCases(t *testing.T) {
	for _, n := range []int{0, 1, 128, 129} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			dir := t.TempDir()
			recs := randomRecords(int64(n)+11, n, 64)
			path, offs := writeCompressedBlockRecords(t, dir, 128, recs)
			r, err := openCompressedBlockReader(path)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer r.Close()
			if r.recCount != uint64(n) {
				t.Fatalf("recCount = %d, want %d", r.recCount, n)
			}
			if n == 0 {
				if _, err := r.RecordTailAt(0); err == nil {
					t.Fatal("RecordTailAt(0) on empty segment should error")
				}
				return
			}
			for i, rc := range offs {
				tail, err := r.RecordTailAt(rc.off)
				if err != nil {
					t.Fatalf("RecordTailAt rec %d: %v", i, err)
				}
				if !bytes.HasPrefix(tail, rc.data) {
					t.Fatalf("record %d mismatch", i)
				}
			}
		})
	}
}

// TestCompressedBlockRatio sanity-checks that compressible (repetitive) records
// actually shrink on disk — the whole point of the format.
func TestCompressedBlockRatio(t *testing.T) {
	dir := t.TempDir()
	// Highly redundant records (like history's repeated block-hash/owner prefix).
	recs := make([][]byte, 4000)
	prefix := bytes.Repeat([]byte{0xAB}, 60)
	rng := rand.New(rand.NewSource(5))
	var raw int
	for i := range recs {
		tail := make([]byte, 8)
		_, _ = rng.Read(tail)
		recs[i] = append(append([]byte(nil), prefix...), tail...)
		raw += len(recs[i])
	}
	path, _ := writeCompressedBlockRecords(t, dir, 128, recs)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() >= int64(raw) {
		t.Fatalf("compressed file %d not smaller than raw %d", st.Size(), raw)
	}
	t.Logf("ratio %.2fx (raw %d -> file %d)", float64(raw)/float64(st.Size()), raw, st.Size())
}

// TestCompressedBlockConcurrentReads exercises the shared reader from many
// goroutines; run under -race it proves the mu + private-copy design is safe.
func TestCompressedBlockConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	recs := randomRecords(99, 3000, 120)
	path, offs := writeCompressedBlockRecords(t, dir, 64, recs)
	r, err := openCompressedBlockReader(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for k := 0; k < 2000; k++ {
				rc := offs[rng.Intn(len(offs))]
				tail, err := r.RecordTailAt(rc.off)
				if err != nil || !bytes.HasPrefix(tail, rc.data) {
					t.Errorf("concurrent read mismatch at off %d: err=%v", rc.off, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
