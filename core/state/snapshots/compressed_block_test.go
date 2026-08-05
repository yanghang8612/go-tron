package snapshots

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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

func TestCompressedBlockSequentialReaderReusesDecodedBlock(t *testing.T) {
	dir := t.TempDir()
	recs := [][]byte{
		bytes.Repeat([]byte{'a'}, 1024),
		bytes.Repeat([]byte{'b'}, 1024),
		bytes.Repeat([]byte{'c'}, 1024),
	}
	path, _ := writeCompressedBlockRecords(t, dir, 1, recs)
	r, err := openCompressedBlockReaderWithCacheLimit(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	var got [1]byte
	if _, err := r.ReadAt(got[:], 0); err != nil {
		t.Fatal(err)
	}
	if got[0] != 'a' || len(r.cache) != 1 {
		t.Fatalf("first read = %q with %d cached blocks", got[:], len(r.cache))
	}
	first := &r.cache[0].bytes[0]
	if _, err := r.ReadAt(got[:], int64(r.table[1].uncompressedStart)); err != nil {
		t.Fatal(err)
	}
	if got[0] != 'b' || len(r.cache) != 1 {
		t.Fatalf("second read = %q with %d cached blocks", got[:], len(r.cache))
	}
	if first != &r.cache[0].bytes[0] {
		t.Fatal("sequential reader allocated a new decoded block instead of reusing the evicted buffer")
	}
}

func TestStateDomainChangeHistoryCompressionChunkPool(t *testing.T) {
	chunk := acquireStateDomainChangeHistoryCompressionChunk(historyCompressChunkSize)
	chunk = append(chunk, bytes.Repeat([]byte{0x5a}, historyCompressChunkSize)...)
	releaseStateDomainChangeHistoryCompressionChunk(&chunk, historyCompressChunkSize)
	if chunk != nil {
		t.Fatal("released compression chunk still reachable through caller")
	}

	chunk = acquireStateDomainChangeHistoryCompressionChunk(historyCompressChunkSize)
	defer releaseStateDomainChangeHistoryCompressionChunk(&chunk, historyCompressChunkSize)
	if len(chunk) != 0 || cap(chunk) != historyCompressChunkSize {
		t.Fatalf("reused compression chunk len/cap = %d/%d, want 0/%d", len(chunk), cap(chunk), historyCompressChunkSize)
	}
}

func TestStateDomainChangeHistoryEncodedChunkPool(t *testing.T) {
	encoder, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	encoded := acquireStateDomainChangeHistoryEncodedChunk(encoder, historyCompressChunkSize)
	if cap(encoded) < encoder.MaxEncodedSize(historyCompressChunkSize) {
		t.Fatalf("encoded chunk capacity %d below maximum %d", cap(encoded), encoder.MaxEncodedSize(historyCompressChunkSize))
	}
	encoded = encoder.EncodeAll(bytes.Repeat([]byte{0xa5}, historyCompressChunkSize), encoded[:0])
	releaseStateDomainChangeHistoryEncodedChunk(&encoded, historyCompressChunkSize)
	if encoded != nil {
		t.Fatal("released encoded chunk still reachable through caller")
	}

	encoded = acquireStateDomainChangeHistoryEncodedChunk(encoder, historyCompressChunkSize)
	defer releaseStateDomainChangeHistoryEncodedChunk(&encoded, historyCompressChunkSize)
	if len(encoded) != 0 || cap(encoded) != stateDomainChangeHistoryEncodedChunkCapacity {
		t.Fatalf("reused encoded chunk len/cap = %d/%d, want 0/%d", len(encoded), cap(encoded), stateDomainChangeHistoryEncodedChunkCapacity)
	}
}

func TestCompressedBlockReaderRejectsOutOfFileBlockBeforeAlloc(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.cb")
	var buf bytes.Buffer
	buf.WriteString(compressedBlockMagic)
	writeUint32(&buf, compressedBlockVersion)
	writeUint32(&buf, 1)
	writeUint64(&buf, 1)
	writeUint64(&buf, 1)
	writeUint64(&buf, 1)
	writeUint64(&buf, compressedBlockHeaderSize+compressedBlockTableEntry)
	writeUint64(&buf, 0)
	writeUint64(&buf, 0)
	writeUint64(&buf, 1<<32)
	writeUint32(&buf, 1)
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if r, err := openCompressedBlockReader(path); err == nil {
		_ = r.Close()
		t.Fatal("openCompressedBlockReader accepted out-of-file compressed block")
	} else if !strings.Contains(err.Error(), "outside file size") {
		t.Fatalf("openCompressedBlockReader error = %v, want file-size bound", err)
	}
}

func TestDecompressBlockBlobRejectsHugeUncompressedSizeBeforeAlloc(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(compressedBlockMagic)
	writeUint32(&buf, compressedBlockVersion)
	writeUint32(&buf, 1)
	writeUint64(&buf, 0)
	writeUint64(&buf, 0)
	writeUint64(&buf, ^uint64(0))
	writeUint64(&buf, compressedBlockHeaderSize)
	if _, err := decompressBlockBlob(buf.Bytes()); err == nil || !strings.Contains(err.Error(), "empty compressed-block table") {
		t.Fatalf("decompressBlockBlob error = %v, want empty-table size rejection", err)
	}
}

func TestCompressedBlockRejectsHugeDecodedBlockBeforeAlloc(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "huge-decoded.cb")
	huge := uint64(compressedBlockMaxDecodedBlockSize + 1)
	var buf bytes.Buffer
	buf.WriteString(compressedBlockMagic)
	writeUint32(&buf, compressedBlockVersion)
	writeUint32(&buf, 1)
	writeUint64(&buf, 1)
	writeUint64(&buf, 1)
	writeUint64(&buf, huge)
	writeUint64(&buf, compressedBlockHeaderSize+compressedBlockTableEntry)
	writeUint64(&buf, 0)
	writeUint64(&buf, 0)
	writeUint64(&buf, 1)
	writeUint32(&buf, 1)
	buf.WriteByte(0)
	data := buf.Bytes()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := openCompressedBlockReader(path)
	if err != nil {
		t.Fatalf("open compressed block: %v", err)
	}
	defer r.Close()
	if _, _, err := r.BlockAt(0); err == nil || !strings.Contains(err.Error(), "decoded block limit") {
		t.Fatalf("BlockAt error = %v, want decoded-block limit", err)
	}
	if _, err := decompressBlockBlob(data); err == nil || !strings.Contains(err.Error(), "decoded block limit") {
		t.Fatalf("decompressBlockBlob error = %v, want decoded-block limit", err)
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

func TestHistoryCompressionConcurrency(t *testing.T) {
	for _, tc := range []struct {
		procs int
		want  int
	}{
		{procs: 0, want: 1},
		{procs: 1, want: 1},
		{procs: 2, want: 2},
		{procs: 4, want: 4},
		{procs: 64, want: 4},
	} {
		if got := historyCompressionConcurrency(tc.procs); got != tc.want {
			t.Fatalf("historyCompressionConcurrency(%d) = %d, want %d", tc.procs, got, tc.want)
		}
	}
}

func TestCompressedBlockStreamWriterEquivalent(t *testing.T) {
	const chunkSize = 16 << 10
	for _, size := range []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, 80*chunkSize + 731} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			dir := t.TempDir()
			payload := make([]byte, size)
			rng := rand.New(rand.NewSource(int64(size) + 91))
			_, _ = rng.Read(payload)
			for off := 0; off+256 <= len(payload); off += 1024 {
				copy(payload[off:off+256], bytes.Repeat([]byte{byte(off >> 10)}, 256))
			}

			stream, err := newCompressedBlockStreamWriter(dir, chunkSize, 4)
			if err != nil {
				t.Fatal(err)
			}
			for off := 0; off < len(payload); {
				n := 1 + (off*31+17)%70001
				if n > len(payload)-off {
					n = len(payload) - off
				}
				written, err := stream.Write(payload[off : off+n])
				if err != nil {
					t.Fatalf("stream write at %d: %v", off, err)
				}
				if written != n {
					t.Fatalf("stream write at %d wrote %d, want %d", off, written, n)
				}
				off += n
			}
			if len(payload) >= 36 {
				patch := []byte("count123")
				if n, err := stream.WriteAt(patch, 28); err != nil || n != len(patch) {
					t.Fatalf("stream WriteAt = %d, %v", n, err)
				}
				copy(payload[28:36], patch)
			}

			gotPath := filepath.Join(dir, "stream.cb")
			if err := stream.Finish(gotPath); err != nil {
				t.Fatalf("finish stream: %v", err)
			}
			wantPath := filepath.Join(dir, "blob.cb")
			if err := compressBlobToFile(dir, wantPath, payload, chunkSize); err != nil {
				t.Fatalf("compress expected blob: %v", err)
			}
			got, err := os.ReadFile(gotPath)
			if err != nil {
				t.Fatal(err)
			}
			want, err := os.ReadFile(wantPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("stream output differs from canonical blob compression: %d versus %d bytes", len(got), len(want))
			}
		})
	}
}

func TestCompressedBlockStreamWriterReset(t *testing.T) {
	const chunkSize = 16 << 10
	dir := t.TempDir()
	stream, err := newCompressedBlockStreamWriter(dir, chunkSize, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write(bytes.Repeat([]byte("discard"), 200_000)); err != nil {
		t.Fatal(err)
	}
	if err := stream.Reset(); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("retained-after-reset"), 80_000)
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	var patch [8]byte
	binary.BigEndian.PutUint64(patch[:], 12345)
	if _, err := stream.WriteAt(patch[:], 28); err != nil {
		t.Fatal(err)
	}
	copy(payload[28:36], patch[:])
	path := filepath.Join(dir, "reset.cb")
	if err := stream.Finish(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decompressBlockBlob(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatal("reset stream retained bytes from discarded generation")
	}
}

func TestCompressedBlockStreamWriterReusesEncodedScratch(t *testing.T) {
	const chunkSize = 64
	stream, err := newCompressedBlockStreamWriter(t.TempDir(), chunkSize, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Abort()
	payload := bytes.Repeat([]byte{0x5a}, chunkSize)
	if _, err := stream.Write(append(append([]byte(nil), payload...), payload...)); err != nil {
		t.Fatal(err)
	}
	if len(stream.body.encoded) == 0 {
		t.Fatal("stream writer did not retain encoded output scratch")
	}
	base := &stream.body.encoded[0]
	if _, err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}
	if len(stream.body.encoded) == 0 || &stream.body.encoded[0] != base {
		t.Fatal("stream writer replaced reusable encoded output scratch")
	}
}

func TestCompressedHistoryTempAbortRemovesScratch(t *testing.T) {
	dir := t.TempDir()
	tmp, err := createStateDomainChangeHistoryTemp(dir, "history/abort.seg", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write(bytes.Repeat([]byte("bounded-compressed-history"), 80_000)); err != nil {
		t.Fatal(err)
	}
	finalScratch := tmp.tmpName
	bodyScratch := tmp.compressed.body.tmpName
	tmp.Close()
	for _, path := range []string{finalScratch, bodyScratch} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("aborted compressed history left scratch %q: %v", path, err)
		}
	}
}

func BenchmarkCompressedBlockStreamWriter(b *testing.B) {
	const chunkSize = 16 << 10
	dir := b.TempDir()
	payload := make([]byte, 8<<20)
	rng := rand.New(rand.NewSource(77))
	for off := 0; off < len(payload); off += 256 {
		end := off + 256
		if end > len(payload) {
			end = len(payload)
		}
		for i := off; i < end; i++ {
			payload[i] = byte(off / (256 * 31))
		}
		_, _ = rng.Read(payload[end-8 : end])
	}
	for _, size := range []int{512 << 10, 2 << 20, 8 << 20} {
		b.Run(fmt.Sprintf("size=%dKiB", size>>10), func(b *testing.B) {
			for _, workers := range []int{1, 4} {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					out := filepath.Join(dir, fmt.Sprintf("size-%d-workers-%d.cb", size, workers))
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						stream, err := newCompressedBlockStreamWriter(dir, chunkSize, workers)
						if err != nil {
							b.Fatal(err)
						}
						for off := 0; off < size; off += 256 << 10 {
							end := off + 256<<10
							if end > size {
								end = size
							}
							if _, err := stream.Write(payload[off:end]); err != nil {
								b.Fatal(err)
							}
						}
						if err := stream.Finish(out); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
