package snapshots

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"github.com/ethereum/go-ethereum/ethdb"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
)

func recordColumnSelectedPages(t testing.TB, path string) int {
	t.Helper()
	r, e := openCompressedBlockReader(path)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	if !r.recordColumns || r.accountDelta || r.cdc != nil {
		t.Fatal("wrong physical codec")
	}
	n := 0
	for _, b := range r.table {
		var flag [1]byte
		if _, e = r.f.ReadAt(flag[:], int64(compressedBlockHeaderSize+b.compressedStart)); e != nil {
			t.Fatal(e)
		}
		if flag[0] == 1 {
			n++
		}
	}
	return n
}

func TestRecordColumnPhysicalBufferedStream(t *testing.T) {
	dir := t.TempDir()
	w, e := newRecordColumnStreamWriter(dir, 128)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	buffer := bufio.NewWriterSize(w, stateDomainChangeHistoryWriteBufferSize)
	raw := bytes.Repeat([]byte{0xa5}, 128)
	if _, e = buffer.Write(raw); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 20000; i++ {
		if e = w.NoteRecordHeader(uint64(len(raw))); e != nil {
			t.Fatal(i, e)
		}
		n := 17 + (i*17)%120
		if i == 777 {
			n = 3*historyRecordColumnPageMax + 17
		}
		frame := binary.BigEndian.AppendUint32(nil, uint32(17+n))
		frame = binary.BigEndian.AppendUint32(frame, uint32((i*7919)%987655))
		frame = binary.BigEndian.AppendUint64(frame, uint64(i/7+1))
		frame = append(frame, 1)
		frame = binary.BigEndian.AppendUint32(frame, uint32(n))
		sum := sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(i)))
		for j := 0; j < n; j++ {
			frame = append(frame, sum[j%32])
		}
		raw = append(raw, frame...)
		if _, e = buffer.Write(frame); e != nil {
			t.Fatal(e)
		}
	}
	if e = buffer.Flush(); e != nil {
		t.Fatal(e)
	}
	copy(raw[:8], "backfill")
	if _, e = w.WriteAt(raw[:8], 0); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "columns.seg")
	m, e := w.FinishWithMetadata(path)
	if e != nil {
		t.Fatal(e)
	}
	blob, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !recordColumnFooterVersion(blob) || m.size != uint64(len(blob)) || m.checksum != sha256.Sum256(blob) {
		t.Fatal("metadata")
	}
	got, e := decompressBlockBlob(blob)
	if e != nil || !bytes.Equal(got, raw) {
		t.Fatal("whole canonical oracle", e)
	}
	if recordColumnSelectedPages(t, path) == 0 {
		t.Fatal("annotations were swallowed by buffered writes")
	}
	for _, limit := range []int{1, 16} {
		r, e := openCompressedBlockReaderWithCacheLimit(path, limit)
		if e != nil {
			t.Fatal(e)
		}
		other, e := openCompressedBlockReaderWithCacheLimit(path, limit)
		if e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		for seed := int64(0); seed < 4; seed++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				reader := r
				if seed%2 != 0 {
					reader = other
				}
				for j := 0; j < 40; j++ {
					off := rng.Intn(len(raw))
					n := min(2048, len(raw)-off)
					b := make([]byte, n)
					count, e := reader.ReadAt(b, int64(off))
					if e != nil || count != n || !bytes.Equal(b, raw[off:off+n]) {
						t.Errorf("random read %d %v", off, e)
						return
					}
				}
			}(seed)
		}
		wg.Wait()
		for _, b := range r.table {
			var one [1]byte
			if _, e = r.ReadAt(one[:], int64(b.uncompressedStart)); e != nil || one[0] != raw[b.uncompressedStart] {
				t.Fatal(e)
			}
		}
		retained := 0
		for _, c := range r.cache {
			retained += cap(c.bytes)
		}
		if retained > limit*historyRecordColumnPageMax || cap(r.columnScratch) > historyRecordColumnPageMax+historyRecordColumnHeader {
			t.Fatal("cache/scratch budget", retained, cap(r.columnScratch))
		}
		r.Close()
		other.Close()
	}
}

func TestRecordColumnStreamResetAndBounds(t *testing.T) {
	dir := t.TempDir()
	w, e := newRecordColumnStreamWriter(dir, 128)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	if e = w.NoteRecordHeader(128); e != nil {
		t.Fatal(e)
	}
	if _, e = w.Write(make([]byte, 200)); e != nil {
		t.Fatal(e)
	}
	if e = w.Reset(); e != nil {
		t.Fatal(e)
	}
	if len(w.recordHeaders.offsets) != 0 || w.recordHeaders.haveLast {
		t.Fatal("stale annotations after reset")
	}
	raw := []byte("short retained prefix")
	if _, e = w.Write(raw); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "reset.seg")
	if e = w.Finish(path); e != nil {
		t.Fatal(e)
	}
	b, _ := os.ReadFile(path)
	got, e := decompressBlockBlob(b)
	if e != nil || !bytes.Equal(got, raw) || !recordColumnFooterVersion(b) {
		t.Fatal("reset format", e)
	}
	if e = w.NoteRecordHeader(0); e == nil {
		t.Fatal("closed observer")
	}
	w, e = newRecordColumnStreamWriter(dir, 128)
	if e != nil {
		t.Fatal(e)
	}
	defer w.Abort()
	if e = w.NoteRecordHeader(1 << 40); e == nil {
		t.Fatal("unbounded future annotation")
	}
	w.Write(make([]byte, 300))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path = filepath.Join(dir, "cancelled.seg")
	if _, e = w.FinishWithMetadataContext(ctx, path); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e = os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("cancelled output installed")
	}
}

func TestRecordColumnDecoderPhysicalGuards(t *testing.T) {
	enc, _, e := cbCodec()
	if e != nil {
		t.Fatal(e)
	}
	bad := append([]byte{1}, enc.EncodeAll(make([]byte, 2<<20), nil)...)
	_, scratch, e := decodeRecordColumnStoredPage(bad, 128, nil, nil)
	if e == nil || cap(scratch) > 128+historyRecordColumnHeader {
		t.Fatal("oversized transformed output accepted", e)
	}
	for _, mode := range []byte{2, 255} {
		if _, _, e = decodeRecordColumnStoredPage([]byte{mode, 1}, 128, nil, nil); e == nil {
			t.Fatal("mode")
		}
	}
	if e = validateRecordColumnBlocks([]cbBlock{{uncompressedStart: 0, compressedLen: 2}}, historyRecordColumnPageMax+1); e == nil {
		t.Fatal("large physical page")
	}
}

func TestRecordColumnV6BufferedBuild(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "5")
	dir := t.TempDir()
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	changes := v6StreamChanges(1, 6000, 100, 96)
	for i, c := range changes {
		c.Seq = uint64(i%100 + 1)
	}
	for first := 0; first < len(changes); first += 100 {
		rows := changes[first : first+100]
		if e := rawdb.WriteStateTxRange(db, rows[0].BlockNum, rows[0].BlockHash, rows[0].TxNum, rows[99].TxNum); e != nil {
			t.Fatal(e)
		}
		if e := rawdb.WriteStateDomainChangeBlockRows(db, rows); e != nil {
			t.Fatal(e)
		}
	}
	cfg, _ := DefaultDomainRegistry().Dataset(SegmentDatasetStateDomainChange)
	built, e := buildStateDomainChangeHistoryBinarySegmentsFromDBRange(db, dir, SegmentRef{Dataset: SegmentDatasetStateDomainChange, Kind: SegmentHistory, FromTxNum: 1, ToTxNum: 6000, Path: "history/state-domain-change-1-6000.seg"}, cfg, etl.Options{}, &stateDomainChangeHistoryBlockRange{from: 1, to: 60})
	if e != nil {
		t.Fatal(e)
	}
	if built.recordETL.Collected != 0 {
		t.Fatal("unexpected fallback")
	}
	for _, ref := range built.refs {
		if ref.Kind == SegmentHistory && recordColumnSelectedPages(t, filepath.Join(dir, ref.Path)) == 0 {
			t.Fatal("production bufio lost all annotations")
		}
	}
	if _, e = VerifyLoadedManifestFiles(dir, NewManifest(1, 6000, built.refs), VerifyManifestOptions{RequireRegistered: true, RequireChecksums: true}); e != nil {
		t.Fatal(e)
	}
}

func TestRecordColumnOfflineTrioAndFallback(t *testing.T) {
	for _, randomLarge := range []bool{false, true} {
		t.Run(map[bool]string{false: "columns", true: "plain-fallback"}[randomLarge], func(t *testing.T) {
			dir := t.TempDir()
			count := uint64(6000)
			if randomLarge {
				count = 3
			}
			changes := v6StreamChanges(1, count, 1, 80)
			if randomLarge {
				rng := rand.New(rand.NewSource(901))
				for _, c := range changes {
					c.PrevExists = true
					c.Prev = make([]byte, 1<<20)
					rng.Read(c.Prev)
				}
			}
			refs := writeV6StateDomainHistorySegmentForTest(t, dir, 1, count, changes)
			for i, ref := range refs {
				if ref.Kind != SegmentHistory {
					continue
				}
				path := filepath.Join(dir, ref.Path)
				b, _ := os.ReadFile(path)
				w, e := newCompressedBlockStreamWriterWithPrefix(dir, historyCompressChunkSize, historyHeaderPrefixBytes, 1, true)
				if e != nil {
					t.Fatal(e)
				}
				w.Write(b)
				if e = w.Finish(path); e != nil {
					t.Fatal(e)
				}
				refs[i].Size, refs[i].Checksum, e = stateDomainChangeBinaryFileMetadata(path)
				if e != nil {
					t.Fatal(e)
				}
			}
			before := map[string][]byte{}
			for _, r := range refs {
				b, e := os.ReadFile(filepath.Join(dir, r.Path))
				if e != nil {
					t.Fatal(e)
				}
				before[r.Path] = b
			}
			result, stats, e := ReencodeHistoryRecordColumnsTrioContext(context.Background(), dir, refs)
			if e != nil {
				t.Fatal(e)
			}
			if stats.Converted == randomLarge || stats.AfterBytes > stats.BeforeBytes || stats.Records != count {
				t.Fatal("stats", stats)
			}
			for i, ref := range refs {
				b, _ := os.ReadFile(filepath.Join(dir, ref.Path))
				if !bytes.Equal(b, before[ref.Path]) {
					t.Fatal("source mutated")
				}
				if ref.Kind != SegmentHistory {
					b, _ = os.ReadFile(filepath.Join(dir, result[i].Path))
					if !bytes.Equal(b, before[ref.Path]) {
						t.Fatal("companion changed")
					}
				} else if stats.Converted {
					if recordColumnSelectedPages(t, filepath.Join(dir, result[i].Path)) == 0 {
						t.Fatal("no columns emitted")
					}
				}
			}
			if stats.Converted {
				inspection, e := InspectHistorySpaceFromManifest(dir, NewManifest(1, count, result), HistorySpaceInspectOptions{SampleSegments: 1, SampleIndexEntries: 32, SampleAccessorBlocks: 2, SampleHistoryBytes: 64 << 10})
				if e != nil || len(inspection.Segments) != 1 || inspection.Segments[0].HistoryLogical != stats.LogicalBytes || inspection.Segments[0].HistoryBlocks == 0 {
					t.Fatal("V5 space inspection", e)
				}
			}
			again, _, e := ReencodeHistoryRecordColumnsTrioContext(context.Background(), dir, result)
			if e != nil || !reflect.DeepEqual(again, result) {
				t.Fatal("idempotence", e)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, _, e = ReencodeHistoryRecordColumnsTrioContext(ctx, dir, refs); !errors.Is(e, context.Canceled) {
				t.Fatal("cancel", e)
			}
			bad := append([]SegmentRef(nil), refs...)
			bad[0].Checksum = badSnapshotChecksum()
			if _, _, e = ReencodeHistoryRecordColumnsTrioContext(context.Background(), dir, bad); e == nil {
				t.Fatal("bad source checksum")
			}
			if randomLarge {
				malformed := append([]SegmentRef(nil), refs...)
				for j, r := range malformed {
					if r.Kind == SegmentInverted {
						path := filepath.Join(dir, r.Path)
						b, _ := os.ReadFile(path)
						b[len(b)-1] ^= 1
						if e = os.WriteFile(path, b, 0600); e != nil {
							t.Fatal(e)
						}
						malformed[j].Size, malformed[j].Checksum, e = stateDomainChangeBinaryFileMetadata(path)
						if e != nil {
							t.Fatal(e)
						}
					}
				}
				if _, _, e = ReencodeHistoryRecordColumnsTrioContext(context.Background(), dir, malformed); e == nil {
					t.Fatal("fallback skipped strict source semantics")
				}
			}

		})
	}
}

func TestRecordColumnHistoryCompatibility(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "5")
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{{"read-paths", TestCompressedHistorySegmentProductionReadPaths}, {"full-check", TestCompressedHistorySegmentFullReadAndCheck}, {"compaction", TestCompactionMergesCompressedSources}, {"inspect", TestInspectHistorySpaceProfilesCompressedV6Trio}} {
		t.Run(tc.name, tc.run)
	}
}

func TestRecordColumnCompactionAndActualRestore(t *testing.T) {
	t.Setenv("GTRON_HISTORY_COMPRESSION_FORMAT", "5")
	dir := t.TempDir()
	var refs []SegmentRef
	var changes []*rawdb.StateDomainChange
	for _, from := range []uint64{1, 3001} {
		rows := v6StreamChanges(from, 3000, 1, 80)
		part := writeV6StateDomainHistorySegmentForTest(t, dir, from, from+2999, rows)
		next, s, e := ReencodeHistoryRecordColumnsTrioContext(context.Background(), dir, part)
		if e != nil || !s.Converted {
			t.Fatal("fixture", e, s)
		}
		refs = append(refs, next...)
		changes = append(changes, rows...)
	}
	if e := PublishManifest(dir, NewManifest(1, 6000, refs)); e != nil {
		t.Fatal(e)
	}
	merged, e := CompactHistoryDomain(dir, SegmentDatasetStateDomainChange, CompactionConfig{})
	if e != nil || !merged.Merged {
		t.Fatal("merge", e)
	}
	expectedDir := t.TempDir()
	expectedRefs := writeV6StateDomainHistorySegmentForTest(t, expectedDir, 1, 6000, changes)
	if e = PublishManifest(expectedDir, NewManifest(1, 6000, expectedRefs)); e != nil {
		t.Fatal(e)
	}
	want, e := os.ReadFile(filepath.Join(expectedDir, expectedRefs[0].Path))
	if e != nil {
		t.Fatal(e)
	}
	for _, ref := range merged.Segments {
		if ref.Kind != SegmentHistory {
			continue
		}
		if recordColumnSelectedPages(t, filepath.Join(dir, ref.Path)) == 0 {
			t.Fatal("streamed merge lost annotations")
		}
		r, n, _, e := openHistorySegmentForRead(dir, ref)
		if e != nil {
			t.Fatal(e)
		}
		got, e := io.ReadAll(io.NewSectionReader(r, 0, int64(n)))
		r.Close()
		if e != nil || !bytes.Equal(got, want) {
			t.Fatal("dictionary-remapped canonical stream", e)
		}
	}
	actual, expected := rawdb.NewMemoryDatabase(), rawdb.NewMemoryDatabase()
	defer actual.Close()
	defer expected.Close()
	for i, root := range []string{dir, expectedDir} {
		mgr, e := OpenManager(root)
		if e != nil {
			t.Fatal(e)
		}
		db := []ethdb.KeyValueStore{actual, expected}[i]
		stats, e := mgr.RestoreStateDomainHistory(db, 1, 6000)
		if e != nil || stats.ChangesRestored != 6000 || stats.TxRangesRestored != 6000 {
			t.Fatal("actual history restore", stats, e)
		}
	}
	var sums [][32]byte
	for _, db := range []ethdb.KeyValueStore{actual, expected} {
		digest := sha256.New()
		it := db.NewIterator(nil, nil)
		for it.Next() {
			digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(it.Key()))))
			digest.Write(it.Key())
			digest.Write(binary.BigEndian.AppendUint64(nil, uint64(len(it.Value()))))
			digest.Write(it.Value())
		}
		if e = it.Error(); e != nil {
			t.Fatal(e)
		}
		it.Release()
		var sum [32]byte
		copy(sum[:], digest.Sum(nil))
		sums = append(sums, sum)
	}
	if sums[0] != sums[1] {
		t.Fatal("restored physical rows differ from raw independent oracle")
	}
}

func FuzzRecordColumnStoredPage(f *testing.F) {
	raw, starts := recordColumnFixture()
	enc, _, e := cbCodec()
	if e != nil {
		f.Fatal(e)
	}
	transformed, e := encodeHistoryRecordColumns(raw, starts, 0)
	if e != nil {
		f.Fatal(e)
	}
	f.Add(append([]byte{0}, enc.EncodeAll(raw, nil)...), uint32(len(raw)))
	f.Add(append([]byte{1}, enc.EncodeAll(transformed, nil)...), uint32(len(raw)))
	f.Fuzz(func(t *testing.T, p []byte, n uint32) {
		if len(p) > historyRecordColumnStoredMax {
			return
		}
		out, scratch, e := decodeRecordColumnStoredPage(p, uint64(n), nil, nil)
		if cap(scratch) > historyRecordColumnPageMax+historyRecordColumnHeader {
			t.Fatal("scratch exceeded bound")
		}
		if e == nil && (len(out) != int(n) || len(out) > historyRecordColumnPageMax) {
			t.Fatal("invalid success")
		}
	})
}
