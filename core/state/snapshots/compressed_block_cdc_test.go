package snapshots

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

func cdcRepeatedInput(size, versions int) []byte {
	base := make([]byte, size)
	rand.New(rand.NewSource(1901)).Read(base)
	out := []byte("retained metadata")
	for i := 0; i < versions; i++ {
		at := 997 + i*193
		value := append(append(append([]byte{}, base[:at]...), byte(i), 0, 255), base[at:]...)
		out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	return out
}

func writeCDCForTest(t testing.TB, data []byte, prefix int) ([]byte, cdcWriteStats) {
	t.Helper()
	dir := t.TempDir()
	w, err := newCDCStreamWriter(context.Background(), dir, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for off := 0; off < len(data); {
		end := min(len(data), off+17293)
		if _, err := w.Write(data[off:end]); err != nil {
			t.Fatal(err)
		}
		off = end
	}
	path := filepath.Join(dir, "encoded.seg")
	meta, err := w.FinishWithMetadataContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.size != uint64(len(encoded)) || meta.checksum != sha256.Sum256(encoded) {
		t.Fatal("streamed metadata mismatch")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("scratch remained: %v %v", entries, err)
	}
	return encoded, w.stats
}

func TestCDCRoundTripAndBoundedRandomRead(t *testing.T) {
	data := cdcRepeatedInput(2<<20, 8)
	encoded, stats := writeCDCForTest(t, data, historychunk.MaxSize)
	if len(encoded) >= len(data)/3 || stats.ReusedBytes < uint64(len(data)*7/10) || stats.References == 0 {
		t.Fatalf("no useful reuse: raw=%d encoded=%d stats=%+v", len(data), len(encoded), stats)
	}
	got, err := decompressBlockBlob(encoded)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("roundtrip err=%v", err)
	}
	r, err := openCDCReader(bytes.NewReader(encoded), uint64(len(encoded)), encoded[:compressedBlockHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 300; i++ {
		off := rng.Intn(len(data))
		buf := make([]byte, min(8191, len(data)-off))
		if n, err := r.ReadAt(buf, int64(off)); err != nil || n != len(buf) || !bytes.Equal(buf, data[off:off+len(buf)]) {
			t.Fatalf("random offset %d: n=%d err=%v", off, n, err)
		}
	}
	if len(r.pages) > 2 || len(r.cache) > 2 || len(r.compressed) > cdcMaxEncodedChunk {
		t.Fatal("cache cap exceeded")
	}
	for _, page := range r.cache {
		if len(page.bytes) > historychunk.MaxSize {
			t.Fatal("decoded page cap")
		}
	}
	if _, err := r.ReadAt(make([]byte, 1), int64(len(data))); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, 1), -1); err == nil {
		t.Fatal("negative offset accepted")
	}
	buf := make([]byte, 10)
	if n, err := r.ReadAt(buf, int64(len(data)-3)); n != 3 || !errors.Is(err, io.EOF) {
		t.Fatalf("partial EOF: %d %v", n, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			off := i * 133713
			buf := make([]byte, 4096)
			for j := 0; j < 20; j++ {
				if _, err := r.ReadAt(buf, int64(off)); err != nil || !bytes.Equal(buf, data[off:off+len(buf)]) {
					t.Errorf("concurrent read: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestCDCRetainedPrefixResetCancellationAndCleanup(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	w, err := newCDCStreamWriter(ctx, dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte{3}, 2*historychunk.MaxSize)); err != nil {
		t.Fatal(err)
	}
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{9}, historychunk.MaxSize+17)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte{1, 2, 3}, 5); err != nil {
		t.Fatal(err)
	}
	copy(data[5:8], []byte{1, 2, 3})
	if _, err := w.WriteAt([]byte{1}, 64); err == nil {
		t.Fatal("prefix overflow allowed")
	}
	path := filepath.Join(dir, "final")
	if err := w.Finish(path); err != nil {
		t.Fatal(err)
	}
	encoded, _ := os.ReadFile(path)
	got, err := decompressBlockBlob(encoded)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("reset/prefix mismatch", err)
	}
	w, err = newCDCStreamWriter(ctx, dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := w.Write([]byte("cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	w.Abort()
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 || entries[0].Name() != "final" {
		t.Fatal("abort removed final or left scratch", entries)
	}
	// A rename failure cleans only owned scratch, preserving the destination.
	w, err = newCDCStreamWriter(context.Background(), dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	w.Write(data)
	target := filepath.Join(dir, "directory")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(target); err == nil {
		t.Fatal("rename over directory succeeded")
	}
	entries, _ = os.ReadDir(dir)
	if len(entries) != 2 {
		t.Fatal("failed finish leaked scratch", entries)
	}
}

func mutateCDCEntry(t *testing.T, encoded []byte, index uint64, mutation func(*cdcEntry)) []byte {
	t.Helper()
	out := bytes.Clone(encoded)
	footer := out[len(out)-cdcFooterSize:]
	count := binary.BigEndian.Uint64(footer[8:16])
	tableOff := binary.BigEndian.Uint64(footer[24:32])
	pageIndex := index / cdcEntriesPerPage
	pageCount := min(uint64(cdcEntriesPerPage), count-pageIndex*cdcEntriesPerPage)
	pageOff := tableOff + pageIndex*(cdcEntriesPerPage*cdcEntrySize+4)
	page := out[pageOff : pageOff+pageCount*cdcEntrySize+4]
	entryOff := index % cdcEntriesPerPage * cdcEntrySize
	e := getCDCEntry(page[entryOff:])
	mutation(&e)
	putCDCEntry(page[entryOff:], e)
	binary.BigEndian.PutUint32(page[len(page)-4:], crc32.ChecksumIEEE(page[:len(page)-4]))
	return out
}

func TestCDCRejectsCorruptionForwardAndChainedReferences(t *testing.T) {
	encoded, _ := writeCDCForTest(t, cdcRepeatedInput(256<<10, 6), historychunk.MaxSize)
	r, err := openCDCReader(bytes.NewReader(encoded), uint64(len(encoded)), encoded[:48])
	if err != nil {
		t.Fatal(err)
	}
	var refIndex, refOffset uint64
	for i := uint64(0); i < r.count; i++ {
		e, _, err := r.entry(i)
		if err != nil {
			t.Fatal(err)
		}
		if e.anchor != cdcAnchor {
			refIndex, refOffset = i, e.logical
			break
		}
	}
	if refIndex == 0 {
		t.Fatal("fixture has no reference")
	}
	for name, mutate := range map[string]func(*cdcEntry){
		"self":              func(e *cdcEntry) { e.anchor = uint32(refIndex) },
		"forward":           func(e *cdcEntry) { e.anchor = uint32(refIndex + 1) },
		"stored-mismatch":   func(e *cdcEntry) { e.stored++ },
		"physical-mismatch": func(e *cdcEntry) { e.physical++ },
		"physical-overflow": func(e *cdcEntry) { e.physical = math.MaxUint64 },
	} {
		t.Run(name, func(t *testing.T) {
			data := mutateCDCEntry(t, encoded, refIndex, mutate)
			r, err := openCDCReader(bytes.NewReader(data), uint64(len(data)), data[:48])
			if err == nil {
				_, err = r.ReadAt(make([]byte, 1), int64(refOffset))
			}
			if err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
	e, _, _ := r.entry(refIndex)
	chained := mutateCDCEntry(t, encoded, uint64(e.anchor), func(e *cdcEntry) { e.anchor = 0 })
	r2, err := openCDCReader(bytes.NewReader(chained), uint64(len(chained)), chained[:48])
	if err == nil {
		_, err = r2.ReadAt(make([]byte, 1), int64(refOffset))
	}
	if err == nil {
		t.Fatal("reference-to-reference accepted")
	}
	for _, cut := range []int{0, 47, len(encoded) - 1, len(encoded) - cdcFooterSize} {
		if _, err := decompressBlockBlob(encoded[:cut]); err == nil {
			t.Fatalf("truncated %d accepted", cut)
		}
	}
	for _, pos := range []int{8, len(encoded) - 8, len(encoded) - cdcFooterSize - 1} {
		bad := bytes.Clone(encoded)
		bad[pos] ^= 1
		if _, err := decompressBlockBlob(bad); err == nil {
			t.Fatalf("corrupt byte %d accepted", pos)
		}
	}
}

// This fixture has a large logical stream and many metadata pages but just one
// complete anchor. It tests directory costs without allocating the logical body.
func syntheticCDC(t *testing.T, count int) []byte {
	t.Helper()
	enc, _, err := cbCodec()
	if err != nil {
		t.Fatal(err)
	}
	chunk := enc.EncodeAll(make([]byte, historychunk.MaxSize), nil)
	out := make([]byte, compressedBlockHeaderSize)
	copy(out[:8], compressedBlockMagic)
	binary.BigEndian.PutUint32(out[8:12], compressedBlockCDCVersion)
	binary.BigEndian.PutUint32(out[12:16], historychunk.MaxSize)
	out = append(out, chunk...)
	tableOff := len(out)
	var sparse, page []byte
	for i := 0; i < count; i++ {
		if i%cdcEntriesPerPage == 0 {
			sparse = binary.BigEndian.AppendUint64(sparse, uint64(i)*historychunk.MaxSize)
		}
		anchor := uint32(0)
		if i == 0 {
			anchor = cdcAnchor
		}
		var raw [cdcEntrySize]byte
		putCDCEntry(raw[:], cdcEntry{logical: uint64(i) * historychunk.MaxSize, physical: 48, stored: uint64(len(chunk)), anchor: anchor})
		page = append(page, raw[:]...)
		if i%cdcEntriesPerPage == cdcEntriesPerPage-1 || i+1 == count {
			out = append(out, page...)
			out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(page))
			page = page[:0]
		}
	}
	tableLen := len(out) - tableOff
	out = append(out, sparse...)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(sparse))
	footer := make([]byte, cdcFooterSize)
	copy(footer[:8], compressedBlockCDCEndMagic)
	binary.BigEndian.PutUint64(footer[8:16], uint64(count))
	binary.BigEndian.PutUint64(footer[16:24], uint64(count)*historychunk.MaxSize)
	binary.BigEndian.PutUint64(footer[24:32], uint64(tableOff))
	binary.BigEndian.PutUint64(footer[32:40], uint64(tableLen))
	binary.BigEndian.PutUint32(footer[40:44], crc32.ChecksumIEEE(footer[:40]))
	return append(out, footer...)
}

type cdcCountReader struct {
	src      io.ReaderAt
	bytes    uint64
	requests int
}

func (r *cdcCountReader) ReadAt(p []byte, off int64) (int, error) {
	r.bytes += uint64(len(p))
	r.requests++
	return r.src.ReadAt(p, off)
}

func TestCDCLargeDirectoryOpenDoesNotScanPages(t *testing.T) {
	data := syntheticCDC(t, 1<<20) // 128GiB logical, 28MiB metadata, 8KiB sparse
	source := &cdcCountReader{src: bytes.NewReader(data)}
	r, err := openCDCReader(source, uint64(len(data)), data[:48])
	if err != nil {
		t.Fatal(err)
	}
	if source.bytes > 9<<10 || source.requests != 2 || len(r.pages) != 0 {
		t.Fatalf("open scanned pages: bytes=%d calls=%d", source.bytes, source.requests)
	}
	if _, err := r.ReadAt(make([]byte, 1), int64(r.logical-1)); err != nil {
		t.Fatal(err)
	}
	if source.bytes > 80<<10 || len(r.pages) > 2 || len(r.cache) > 2 {
		t.Fatalf("random access unbounded: %+v", source)
	}
}

func TestCDCProductionBuilderAndMixedMergeOracle(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "3")
	first := v6StreamChanges(1, 18, 1, 35)
	second := v6StreamChanges(19, 18, 1, 35)
	base := make([]byte, 256<<10)
	rand.New(rand.NewSource(23)).Read(base)
	for i, c := range append(append([]*rawdb.StateDomainChange{}, first...), second...) {
		if i%3 == 0 {
			c.PrevExists = true
			c.Prev = append(bytes.Clone(base), byte(i))
		}
	}
	dir := t.TempDir()
	// Production hot-history build, including original dictionary/tx ranges.
	db := rawdb.NewMemoryDatabase()
	for _, c := range first {
		if err := rawdb.WriteStateTxRange(db, c.BlockNum, c.BlockHash, c.TxNum, c.TxNum); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, c); err != nil {
			t.Fatal(err)
		}
	}
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 18, "history/state-domain-change-first.seg")
	if err != nil {
		t.Fatal(err)
	}
	refs = append(refs, writeV6StateDomainHistorySegmentForTest(t, dir, 19, 36, second)...)
	manifest := NewManifest(1, 36, refs)
	if _, err := VerifyLoadedManifestFiles(dir, manifest, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
	if err := PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	result, err := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if err != nil || !result.Merged {
		t.Fatalf("mixed merge: %+v %v", result, err)
	}
	merged := NewManifest(1, 36, result.Segments)
	if _, err := VerifyLoadedManifestFiles(dir, merged, VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
		t.Fatal(err)
	}
	want := append(first, second...)
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	var history, accessor SegmentRef
	for _, ref := range result.Segments {
		if ref.Kind == SegmentHistory {
			history = ref
		}
		if ref.Kind == SegmentAccessor {
			accessor = ref
		}
	}
	position := 0
	err = cfg.IterateHistoryRange(dir, merged, history, 1, 36, func(c *rawdb.StateDomainChange) (bool, error) {
		expected := want[position]
		if c.TxNum != expected.TxNum || c.BlockNum != expected.BlockNum || c.BlockHash != expected.BlockHash || c.PrevExists != expected.PrevExists || !bytes.Equal(c.Prev, expected.Prev) || !bytes.Equal(c.Key, expected.Key) {
			t.Fatalf("merged record %d mismatch", position)
		}
		position++
		return true, nil
	})
	if err != nil || position != len(want) {
		t.Fatalf("full oracle count=%d err=%v", position, err)
	}
	for _, expected := range want {
		found := false
		err := iterateStateDomainChangeBinarySegmentByAccessorFile(dir, history, accessor, stateDomainChangeBinaryAccessorKey(expected), expected.TxNum, expected.TxNum, func(c *rawdb.StateDomainChange) (bool, error) {
			found = true
			if !bytes.Equal(c.Prev, expected.Prev) || c.PrevExists != expected.PrevExists {
				t.Fatal("point query mismatch")
			}
			return true, nil
		})
		if err != nil || !found {
			t.Fatalf("point tx=%d found=%v err=%v", expected.TxNum, found, err)
		}
	}
}

func TestCDCHistoryFinalRenameSyncFailurePreservesTarget(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, "scratch.tmp")
	target := filepath.Join(dir, "history", "final.seg")
	payload := []byte("complete synced artifact")
	if err := os.WriteFile(tmp, payload, 0600); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected directory sync failure")
	calls := 0
	err := publishStateDomainChangeBinaryTempWithDirSync(tmp, target, func(path string) error {
		calls++
		if path != filepath.Dir(target) {
			t.Fatalf("wrong directory: %s", path)
		}
		got, err := os.ReadFile(target)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("sync must follow rename")
		}
		return failure
	})
	if !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("got %v, calls %d", err, calls)
	}
	got, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("publication uncertainty must retain artifact: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("old temp still exists: %v", err)
	}
}

func TestCDCVariableChunksRetainBoundedDecodeStorage(t *testing.T) {
	input := cdcRepeatedInput(1<<20, 3)
	encoded, _ := writeCDCForTest(t, input, historyCompressChunkSize)
	r, err := openCDCReader(bytes.NewReader(encoded), uint64(len(encoded)), encoded[:48])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, len(input)), 0); err != nil {
		t.Fatal(err)
	}
	if len(r.cache) != 2 {
		t.Fatal("expected full cache")
	}
	for _, entry := range r.cache {
		if cap(entry.bytes) != historychunk.MaxSize {
			t.Fatalf("cache capacity shrank to %d", cap(entry.bytes))
		}
	}
}

func TestCDCAutoBuildAndMergeSelection(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprintf("large=%v", large), func(t *testing.T) {
			t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "auto")
			dir := t.TempDir()
			db := rawdb.NewMemoryDatabase()
			for i := uint64(1); i <= 2; i++ {
				c := binaryStateDomainChange(i, i, 1, "same-key")
				c.Owner = binaryAddress(7)
				c.Generation = 1
				c.PrevExists = true
				c.Prev = []byte("small account-like value")
				if large {
					c.Prev = bytes.Repeat([]byte("large repeated arbitrary byte history\x00"), 8000)
				}
				if err := rawdb.WriteStateTxRange(db, c.BlockNum, c.BlockHash, c.TxNum, c.TxNum); err != nil {
					t.Fatal(err)
				}
				if err := rawdb.WriteStateDomainChange(db, c); err != nil {
					t.Fatal(err)
				}
			}
			refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 2, "history/state-domain-change-auto.seg")
			if err != nil {
				t.Fatal(err)
			}
			var history SegmentRef
			for _, ref := range refs {
				if ref.Kind == SegmentHistory {
					history = ref
				}
			}
			data, err := os.ReadFile(filepath.Join(dir, history.Path))
			if err != nil {
				t.Fatal(err)
			}
			want := compressedBlockFooterVersion
			if large {
				want = compressedBlockCDCVersion
			}
			if got := binary.BigEndian.Uint32(data[8:12]); got != want {
				t.Fatalf("auto format %d want %d", got, want)
			}
			if _, err := VerifyLoadedManifestFiles(dir, NewManifest(1, 2, refs), VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); err != nil {
				t.Fatal(err)
			}
			format, err := historyMergeCompressionFormat(dir, []stateDomainChangeBinaryCompactionSource{{history: history}})
			if err != nil || format != fmt.Sprint(want) {
				t.Fatalf("merge selection %q %v", format, err)
			}
		})
	}
}
