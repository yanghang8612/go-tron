package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
)

const previousHistoryCompressChunkSize = 64 << 10

// writeHistoryChunkLayout creates one real V7 history trio and rewrites only
// its seekable payload with the requested chunk size. The logical bytes and V7
// accessor offsets remain identical, which makes comparisons isolate page size
// instead of accidentally comparing different record/index encodings.
func writeHistoryChunkLayout(tb testing.TB, chunkSize int, changes []*rawdb.StateDomainChange) (string, SegmentRef, SegmentRef, SegmentRef, uint64) {
	tb.Helper()
	if len(changes) == 0 {
		tb.Fatal("history chunk layout requires changes")
	}
	from := changes[0].TxNum
	to := changes[len(changes)-1].TxNum
	dir := tb.TempDir()
	previous := CompressHistorySegments
	CompressHistorySegments = false
	segment, index, accessor, err := writeHistorySegmentFiles(dir, SegmentRef{
		Dataset:   SegmentDatasetStateDomainChange,
		Kind:      SegmentHistory,
		FromTxNum: from,
		ToTxNum:   to,
		Path:      "history/state-domain-change-test.seg",
	}, changes)
	CompressHistorySegments = previous
	if err != nil {
		tb.Fatalf("write raw V7 history trio: %v", err)
	}
	path := filepath.Join(dir, segment.Path)
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Fatal(err)
	}
	tmp := path + fmt.Sprintf(".%d", chunkSize)
	if err := compressBlobToFile(dir, tmp, raw, chunkSize); err != nil {
		tb.Fatalf("compress V7 history at %d: %v", chunkSize, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		tb.Fatal(err)
	}
	segment.Size, segment.Checksum, err = stateDomainChangeBinaryFileMetadata(path)
	if err != nil {
		tb.Fatal(err)
	}
	return dir, segment, index, accessor, uint64(len(raw))
}

func TestHistoryCurrentChunkRandomReadAndCrossBlockRecord(t *testing.T) {
	if historyCompressChunkSize != 128<<10 {
		t.Fatalf("production history chunk = %d, want 128 KiB", historyCompressChunkSize)
	}
	chunk := historyCompressChunkSize
	// Start the frame two bytes before a chunk boundary so both its length prefix
	// and its payload exercise multi-block ReadAt. The payload crosses the next
	// boundary too, covering the large-record path used by account protobufs.
	offset := chunk - 2
	payload := bytes.Repeat([]byte("state-history-frame-"), chunk/20+200)
	blob := bytes.Repeat([]byte{0x7f}, offset)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	blob = append(blob, length[:]...)
	blob = append(blob, payload...)
	blob = append(blob, bytes.Repeat([]byte{0x3c}, chunk)...)

	dir := t.TempDir()
	path := filepath.Join(dir, "cross-block.seg")
	if err := compressBlobToFile(dir, path, blob, chunk); err != nil {
		t.Fatal(err)
	}
	r, err := openCompressedBlockReaderWithCacheLimit(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i, block := range r.table {
		end := r.uncSize
		if i+1 < len(r.table) {
			end = r.table[i+1].uncompressedStart
		}
		if span := end - block.uncompressedStart; i+1 < len(r.table) && span != uint64(chunk) || i+1 == len(r.table) && span > uint64(chunk) {
			t.Fatalf("block %d span = %d, chunk = %d", i, span, chunk)
		}
	}
	got, next, borrowed, err := r.ReadRecordFrameAt(uint64(offset), nil)
	if err != nil {
		t.Fatal(err)
	}
	if borrowed || next != uint64(offset+4+len(payload)) || !bytes.Equal(got, payload) {
		t.Fatalf("cross-block frame borrowed=%t next=%d len=%d", borrowed, next, len(got))
	}

	rng := rand.New(rand.NewSource(128))
	for i := 0; i < 4096; i++ {
		start := rng.Intn(len(blob) - 257)
		length := 1 + rng.Intn(256)
		buf := make([]byte, length)
		if _, err := r.ReadAt(buf, int64(start)); err != nil {
			t.Fatalf("ReadAt(%d,%d): %v", start, length, err)
		}
		if !bytes.Equal(buf, blob[start:start+length]) {
			t.Fatalf("ReadAt(%d,%d) mismatch", start, length)
		}
	}
}

func TestHistory128KiBLayoutPreservesV7ReadsAndManifest(t *testing.T) {
	changes := buildHistoryStructs(320, 40)
	dir64, segment64, _, _, raw64 := writeHistoryChunkLayout(t, previousHistoryCompressChunkSize, changes)
	dir128, segment128, index128, accessor128, raw128 := writeHistoryChunkLayout(t, historyCompressChunkSize, changes)
	if raw64 != raw128 {
		t.Fatalf("logical sizes differ: 64K=%d 128K=%d", raw64, raw128)
	}
	if segment128.Size > segment64.Size {
		t.Fatalf("128 KiB history grew: 64K=%d 128K=%d", segment64.Size, segment128.Size)
	}
	t.Logf("same V7 payload: 64 KiB=%d, 128 KiB=%d, saving=%.2f%%", segment64.Size, segment128.Size, 100*(1-float64(segment128.Size)/float64(segment64.Size)))

	manifest := NewManifest(segment128.FromTxNum, segment128.ToTxNum, []SegmentRef{segment128, index128, accessor128})
	if err := PublishManifest(dir128, manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProductionManifest(dir128)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Segments) != 3 || loaded.Segments[0].Checksum == "" {
		t.Fatalf("published manifest lost 128 KiB refs: %+v", loaded.Segments)
	}

	normalized := normalizeStateDomainChangesForBinary(changes)
	keys := make([][]byte, 64)
	for i := range keys {
		keys[i] = stateDomainChangeBinaryAccessorKey(normalized[i*len(normalized)/len(keys)])
	}
	found := 0
	if err := iterateStateDomainChangeBinarySegmentByAccessorKeysFile(context.Background(), dir128, segment128, accessor128, keys, segment128.FromTxNum, segment128.ToTxNum,
		func(_ []byte, _ *rawdb.StateDomainChange) (bool, error) {
			found++
			return true, nil
		}); err != nil {
		t.Fatal(err)
	}
	if found != len(keys) {
		t.Fatalf("V7 batch lookup found %d records, want %d", found, len(keys))
	}

	// The previous layout remains readable even though it is no longer current.
	if current, err := historyUsesCurrentCompression(filepath.Join(dir64, segment64.Path)); err != nil || current {
		t.Fatalf("64 KiB current=%t err=%v, want readable legacy layout", current, err)
	}
}

func TestHistoryCompressionPipelineMemoryBoundAt128KiB(t *testing.T) {
	workers := historyCompressionConcurrency(64)
	inFlight := workers * 2
	// The ordered pipeline owns inFlight raw and encoded buffers. The stream
	// writer additionally retains its first logical page and one encoded prefix
	// destination. This is the capacity bound; buffers are reused, not allocated
	// per record.
	rawBuffers := inFlight + 1
	encodedBuffers := inFlight + 1
	peak := uint64(rawBuffers*historyCompressChunkSize + encodedBuffers*stateDomainChangeHistoryEncodedChunkCapacity)
	if workers != maxHistoryCompressionWorkers {
		t.Fatalf("workers = %d, want bounded maximum %d", workers, maxHistoryCompressionWorkers)
	}
	if peak > 3<<20 {
		t.Fatalf("compression pipeline buffers = %d bytes, want <= 3 MiB", peak)
	}
	t.Logf("128 KiB compression pipeline buffer bound: %d bytes (%d raw + %d encoded buffers)", peak, rawBuffers, encodedBuffers)
}

func TestMigrateHistoryV7Rewrites64KiBAndPublishes128KiB(t *testing.T) {
	changes := buildHistoryStructs(180, 32)
	dir, segment, index, accessor, _ := writeHistoryChunkLayout(t, previousHistoryCompressChunkSize, changes)
	manifest := NewManifest(segment.FromTxNum, segment.ToTxNum, []SegmentRef{segment, index, accessor})
	manifest.Generation = 19
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	result, err := MigrateHistoryV7(dir, HistoryV7MigrationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalTrios != 1 || result.AlreadyCurrent != 0 || result.MigratedTrios != 1 || result.RemainingTrios != 0 {
		t.Fatalf("migration result = %+v", result)
	}
	loaded, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Generation <= manifest.Generation || len(loaded.Retired) != 3 {
		t.Fatalf("migration manifest generation=%d retired=%d", loaded.Generation, len(loaded.Retired))
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	candidates := historyCompactionCandidates(loaded, cfg)
	if len(candidates) != 1 {
		t.Fatalf("active trios = %d, want 1", len(candidates))
	}
	current, err := historyCandidateUsesCurrentV7(dir, candidates[0])
	if err != nil || !current {
		t.Fatalf("published 128 KiB trio current=%t err=%v", current, err)
	}
	r, err := openCompressedBlockReader(filepath.Join(dir, candidates[0].history.Path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if len(r.table) > 1 && r.table[1].uncompressedStart != historyCompressChunkSize {
		t.Fatalf("published second block starts at %d, want %d", r.table[1].uncompressedStart, historyCompressChunkSize)
	}
}

func benchmarkHistoryChunkPointReads(b *testing.B, chunkSize, batch int) {
	changes := buildHistoryStructs(1200, 48)
	dir, segment, _, accessor, _ := writeHistoryChunkLayout(b, chunkSize, changes)
	normalized := normalizeStateDomainChangesForBinary(changes)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = stateDomainChangeBinaryAccessorKey(normalized[i*len(normalized)/len(keys)])
	}
	selected := make([][]byte, batch)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range selected {
			selected[j] = keys[(i*batch+j)%len(keys)]
		}
		found := 0
		if err := iterateStateDomainChangeBinarySegmentByAccessorKeysFile(context.Background(), dir, segment, accessor, selected, segment.FromTxNum, segment.ToTxNum,
			func(_ []byte, _ *rawdb.StateDomainChange) (bool, error) {
				found++
				return true, nil
			}); err != nil {
			b.Fatal(err)
		}
		if found != batch {
			b.Fatalf("found %d, want %d", found, batch)
		}
	}
	b.ReportMetric(float64(segment.Size), "segment-B")
	b.ReportMetric(float64(chunkSize), "max-decode-B")
}

// BenchmarkHistoryV7PointReadChunk measures the state-history portion shared
// by eth_getBalance/eth_getCode and the multi-key storage reads of eth_call.
// Code blobs themselves live in the code snapshot; this benchmark intentionally
// isolates the account/KV history page whose chunk size changes here.
func BenchmarkHistoryV7PointReadChunk(b *testing.B) {
	for _, batch := range []int{1, 32} {
		for _, chunk := range []int{previousHistoryCompressChunkSize, historyCompressChunkSize} {
			b.Run(fmt.Sprintf("batch=%d/chunk=%dK", batch, chunk>>10), func(b *testing.B) {
				benchmarkHistoryChunkPointReads(b, chunk, batch)
			})
		}
	}
}

func benchmarkHistoryReadAtChunk(b *testing.B, chunkSize int, cross bool) {
	rng := rand.New(rand.NewSource(8181))
	blob := make([]byte, chunkSize*128)
	_, _ = rng.Read(blob)
	for off := 0; off+4096 <= len(blob); off += 4096 {
		copy(blob[off:off+2048], bytes.Repeat([]byte{byte(off >> 12)}, 2048))
	}
	dir := b.TempDir()
	path := filepath.Join(dir, "readat.seg")
	if err := compressBlobToFile(dir, path, blob, chunkSize); err != nil {
		b.Fatal(err)
	}
	r, err := openCompressedBlockReaderWithCacheLimit(path, 1)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	buf := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block := (i*37 + 11) % 126
		offset := block*chunkSize + 73
		if cross {
			offset = (block+1)*chunkSize - len(buf)/2
		}
		if _, err := r.ReadAt(buf, int64(offset)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(chunkSize), "max-decode-B")
}

func BenchmarkHistoryReadAtChunk(b *testing.B) {
	for _, cross := range []bool{false, true} {
		for _, chunk := range []int{previousHistoryCompressChunkSize, historyCompressChunkSize} {
			b.Run(fmt.Sprintf("cross=%t/chunk=%dK", cross, chunk>>10), func(b *testing.B) {
				benchmarkHistoryReadAtChunk(b, chunk, cross)
			})
		}
	}
}
