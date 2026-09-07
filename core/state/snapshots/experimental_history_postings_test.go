package snapshots

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func experimentalPostingFixture(count int) ([]ExperimentalHistoryPosting, ExperimentalHistoryRecordSource) {
	data := []byte("prefix")
	rows := make([]ExperimentalHistoryPosting, count)
	for i := range rows {
		rows[i] = ExperimentalHistoryPosting{TxNum: uint64(1_700_000_000 + i/3), Offset: uint64(len(data)), RecordOrdinal: uint64(i)}
		payload := bytes.Repeat([]byte{byte(i)}, 17+i%41)
		data = binary.BigEndian.AppendUint32(data, uint32(len(payload)))
		data = append(data, payload...)
	}
	return rows, ExperimentalHistoryRecordSource{Reader: bytes.NewReader(data), LogicalSize: uint64(len(data)), MaxScanRecords: 10_000_000}
}

func TestExperimentalPostingsExactLowerBoundScanAndEmptyKey(t *testing.T) {
	ctx := context.Background()
	global, source := experimentalPostingFixture(2300)
	encodedLocator, _, err := BuildExperimentalHistoryLocator(ctx, source, global[0].Offset, uint64(len(global)), 64)
	if err != nil {
		t.Fatal(err)
	}
	locator, err := OpenExperimentalHistoryLocator(encodedLocator)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{0, 1, 2, 127, 128, 129, 257, 2300} {
		for _, indirect := range []bool{false, true} {
			t.Run(fmt.Sprintf("count_%d/indirect_%t", count, indirect), func(t *testing.T) {
				rows := global[:count]
				// Empty byte key is valid and bound to the list checksum.
				blob, err := EncodeExperimentalHistoryPostings(ctx, nil, 1_700_000_000, rows, indirect)
				if err != nil {
					t.Fatal(err)
				}
				r, err := OpenExperimentalHistoryPostings(ctx, []byte{}, 1_700_000_000, uint64(count), blob)
				if err != nil {
					t.Fatal(err)
				}
				var got []ExperimentalHistoryPosting
				stats, err := r.Scan(ctx, locator, source, func(p ExperimentalHistoryPosting) error { got = append(got, p); return nil })
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(rows) || len(rows) > 0 && !reflect.DeepEqual(got, rows) {
					t.Fatal("scan mismatch")
				}
				if indirect && stats.RecordsSkipped > uint64(count+64) {
					t.Fatalf("sequential scan repeated prefix work: %+v", stats)
				}
				queries := []uint64{0, 1_699_999_999, 1_700_000_000, math.MaxUint64}
				for _, row := range rows {
					queries = append(queries, row.TxNum, row.TxNum+1)
				}
				for _, target := range queries {
					pos := sort.Search(len(rows), func(i int) bool { return rows[i].TxNum >= target })
					p, found, stats, err := r.LowerBound(ctx, target, locator, source)
					if err != nil || found != (pos < len(rows)) || found && p != rows[pos] {
						t.Fatalf("lower bound %d: %+v %t %v", target, p, found, err)
					}
					if stats.FramesDecoded > 2 || indirect && stats.RecordsSkipped >= 64 {
						t.Fatalf("unbounded lookup: %+v", stats)
					}
					p, found, _, err = r.Exact(ctx, target, locator, source)
					want := pos < len(rows) && rows[pos].TxNum == target
					if err != nil || found != want || found && p != rows[pos] {
						t.Fatalf("exact %d: %+v %t %v", target, p, found, err)
					}
				}
			})
		}
	}
}

func TestExperimentalPostingsDuplicateTxAcrossManyFrames(t *testing.T) {
	rows, _ := experimentalPostingFixture(600)
	for i := range rows {
		rows[i].TxNum = 42
		if i == 0 {
			rows[i].TxNum = 41
		}
	}
	blob, err := EncodeExperimentalHistoryPostings(context.Background(), []byte("k"), 0, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenExperimentalHistoryPostings(context.Background(), []byte("k"), 0, uint64(len(rows)), blob)
	if err != nil {
		t.Fatal(err)
	}
	p, ok, stats, err := r.Exact(context.Background(), 42, nil, ExperimentalHistoryRecordSource{})
	if err != nil || !ok || p != rows[1] || stats.FramesDecoded > 2 {
		t.Fatalf("duplicate boundary: %+v %t %+v %v", p, ok, stats, err)
	}
}

func TestExperimentalPostingsLargeIntegersAndRejections(t *testing.T) {
	ctx := context.Background()
	rows := []ExperimentalHistoryPosting{{TxNum: 0, Offset: 1, RecordOrdinal: 0}, {TxNum: 1 << 63, Offset: 1 << 63, RecordOrdinal: 1 << 63}, {TxNum: math.MaxUint64, Offset: math.MaxUint64, RecordOrdinal: math.MaxUint64}}
	blob, err := EncodeExperimentalHistoryPostings(ctx, []byte("large"), 0, rows, false)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenExperimentalHistoryPostings(ctx, []byte("large"), 0, 3, blob)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		got, ok, _, err := r.Exact(ctx, row.TxNum, nil, ExperimentalHistoryRecordSource{})
		if err != nil || !ok || got != row {
			t.Fatal(got, ok, err)
		}
	}
	for _, bad := range [][]ExperimentalHistoryPosting{
		{{TxNum: 0}}, // fromTx underflow
		{{TxNum: 4, Offset: 7}, {TxNum: 3, Offset: 8, RecordOrdinal: 1}},
		{{TxNum: 3, Offset: 7}, {TxNum: 3, Offset: 7, RecordOrdinal: 1}},
		{{TxNum: 3, Offset: 7}, {TxNum: 3, Offset: 8}},
	} {
		if _, err := EncodeExperimentalHistoryPostings(ctx, nil, 1, bad, false); err == nil {
			t.Fatalf("accepted bad postings: %+v", bad)
		}
	}
	for _, bounds := range []struct {
		key         []byte
		from, count uint64
	}{{[]byte("wrong"), 0, 3}, {[]byte("large"), 1, 3}, {[]byte("large"), 0, 2}} {
		if _, err := OpenExperimentalHistoryPostings(ctx, bounds.key, bounds.from, bounds.count, blob); err == nil {
			t.Fatal("accepted wrong external metadata")
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := EncodeExperimentalHistoryPostings(ctx, nil, 0, rows, false); err == nil {
		t.Fatal("encode ignored cancellation")
	}
	if _, _, _, err := r.Exact(ctx, 0, nil, ExperimentalHistoryRecordSource{}); err == nil {
		t.Fatal("query ignored cancellation")
	}
}

func TestExperimentalPostingsCorruptionTruncationAndDecoderLimits(t *testing.T) {
	rows, _ := experimentalPostingFixture(300)
	for _, indirect := range []bool{false, true} {
		blob, err := EncodeExperimentalHistoryPostings(context.Background(), nil, 0, rows, indirect)
		if err != nil {
			t.Fatal(err)
		}
		for n := 0; n < len(blob); n++ {
			if _, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 300, blob[:n]); err == nil {
				t.Fatalf("accepted truncation %d", n)
			}
		}
		for pos := range blob {
			bad := append([]byte(nil), blob...)
			bad[pos] ^= 0x80
			if _, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 300, bad); err == nil {
				t.Fatalf("accepted byte corruption %d", pos)
			}
		}
		bad := append([]byte(nil), blob...)
		bad = append(bad, 0)
		binary.BigEndian.PutUint32(bad[len(bad)-4:], experimentalPostingChecksum(nil, 0, 300, bad[:len(bad)-4]))
		if _, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 300, bad); err == nil {
			t.Fatal("accepted CRC-correct trailing data")
		}
	}
	for _, bad := range [][]byte{{2, 65}, {2, 0}, {2, 1, 0xff}, {3}, {0, 0x80, 0}, {1, 0x80}} {
		pos := 0
		if _, err := experimentalDecodeColumn(bad, &pos, 1); err == nil {
			t.Fatalf("accepted invalid column: %x", bad)
		}
	}
	// Recomputed checksum cannot make duplicate ordinal deltas valid.
	bad := []byte{0xc2, 0, 0, 1, 0, 0, 1}
	bad = binary.BigEndian.AppendUint32(bad, experimentalPostingChecksum(nil, 0, 2, bad))
	if _, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 2, bad); err == nil {
		t.Fatal("accepted duplicate ordinal")
	}
}

type experimentalVirtualFrames map[uint64]uint32

func (v experimentalVirtualFrames) ReadAt(out []byte, off int64) (int, error) {
	length, ok := v[uint64(off)]
	if !ok || len(out) != 4 || off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	binary.BigEndian.PutUint32(out, length)
	return 4, nil
}

func TestExperimentalLocatorLargeOffsetsBudgetAndCorruption(t *testing.T) {
	ctx := context.Background()
	frames := make(experimentalVirtualFrames)
	offsets := make([]uint64, 260)
	offset := uint64(1 << 40)
	for i := range offsets {
		offsets[i] = offset
		length := uint32(math.MaxUint32 - uint32(i))
		frames[offset] = length
		offset += uint64(length) + 4
	}
	source := ExperimentalHistoryRecordSource{Reader: frames, LogicalSize: offset, MaxScanRecords: 260}
	blob, stats, err := BuildExperimentalHistoryLocator(ctx, source, offsets[0], 260, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(blob) != 48+8*5 || stats.RecordsSkipped != 260 {
		t.Fatal("incorrect real locator size", len(blob), stats)
	}
	r, err := OpenExperimentalHistoryLocator(blob)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range offsets {
		var stats ExperimentalHistoryLookupStats
		got, err := r.resolve(ctx, uint64(i), source, &stats)
		if err != nil || got != want || stats.RecordsSkipped >= 64 {
			t.Fatal(i, got, want, stats, err)
		}
	}
	for n := 0; n < len(blob); n++ {
		if _, err := OpenExperimentalHistoryLocator(blob[:n]); err == nil {
			t.Fatal("accepted truncated locator")
		}
	}
	bad := append([]byte(nil), blob...)
	bad[len(bad)-1] ^= 1
	if _, err := OpenExperimentalHistoryLocator(bad); err == nil {
		t.Fatal("accepted corrupt locator")
	}
	source.MaxScanRecords = 1
	if _, _, err := BuildExperimentalHistoryLocator(ctx, source, offsets[0], 260, 64); err == nil {
		t.Fatal("build ignored bound")
	}
	var s ExperimentalHistoryLookupStats
	if _, err := r.resolve(ctx, 63, source, &s); err == nil {
		t.Fatal("lookup ignored scan bound")
	}
	if _, err := r.resolve(ctx, 260, source, &s); err == nil {
		t.Fatal("out-of-range ordinal accepted")
	}
	source.MaxScanRecords = 260
	frames[offsets[3]] = 0
	if _, err := r.resolve(ctx, 4, source, &s); err == nil {
		t.Fatal("invalid record frame accepted")
	}
}

func TestExperimentalColumnRandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for trial := 0; trial < 1000; trial++ {
		n := rng.Intn(1025)
		values := make([]uint64, n)
		width := uint(rng.Intn(65))
		mask := uint64(math.MaxUint64)
		if width < 64 {
			mask = (1 << width) - 1
		}
		for i := range values {
			values[i] = rng.Uint64() & mask
		}
		blob := experimentalEncodeColumn(values)
		pos := 0
		got, err := experimentalDecodeColumn(blob, &pos, n)
		if err != nil || pos != len(blob) || !reflect.DeepEqual(got, values) {
			t.Fatalf("trial %d: %v", trial, err)
		}
	}
}

func TestExperimentalTxIndexDenseAndSparse(t *testing.T) {
	global, source := experimentalPostingFixture(900)
	var rows []ExperimentalHistoryIndexEntry
	for i := 0; i < len(global); i += 3 {
		rows = append(rows, ExperimentalHistoryIndexEntry{TxNum: global[i].TxNum, Offset: global[i].Offset, RecordOrdinal: uint64(i), Count: 3})
	}
	for _, sparse := range []bool{false, true} {
		blob, err := EncodeExperimentalHistoryIndex(context.Background(), rows, ExperimentalHistoryIndexOptions{RestartEntries: 64, SparseOffsets: sparse})
		if err != nil {
			t.Fatal(err)
		}
		r, err := OpenExperimentalHistoryIndex(context.Background(), blob)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range rows {
			got, found, _, err := r.Exact(context.Background(), want.TxNum, source)
			if err != nil || !found || got != want {
				t.Fatal(got, want, found, err)
			}
		}
		var got []ExperimentalHistoryIndexEntry
		if _, err := r.Scan(context.Background(), source, func(row ExperimentalHistoryIndexEntry) error { got = append(got, row); return nil }); err != nil || !reflect.DeepEqual(got, rows) {
			t.Fatal("tx scan", err)
		}
		for n := 0; n < len(blob); n++ {
			if _, err := OpenExperimentalHistoryIndex(context.Background(), blob[:n]); err == nil {
				t.Fatal("accepted truncated tx index")
			}
		}
	}
}

func experimentalBuildRealV7Fixture(t testing.TB) (string, SegmentRef, SegmentRef) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, make([]byte, common.AccountIDLength)...))
	const records = 8192
	for i := 0; i < records; i++ {
		tx := uint64(1_700_000_000 + i/4)
		hash := common.Hash{}
		binary.BigEndian.PutUint64(hash[24:], tx)
		if i%4 == 0 {
			if err := rawdb.WriteStateTxRange(db, uint64(i/4+1), hash, tx, tx); err != nil {
				t.Fatal(err)
			}
		}
		key := []byte(fmt.Sprintf("slot/%04d", i%97))
		if i%97 == 0 {
			key = nil
		}
		change := &rawdb.StateDomainChange{BlockNum: uint64(i/4 + 1), BlockHash: hash, TxNum: tx, Seq: uint64(i%4 + 1), FlatDomain: rawdb.StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage, Key: key, PrevExists: true, Prev: bytes.Repeat([]byte{byte(i % 19)}, 33+i%211)}
		if err := rawdb.WriteStateDomainChange(db, change); err != nil {
			t.Fatal(err)
		}
	}
	dir := t.TempDir()
	refs, err := BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1_700_000_000, 1_700_002_047, "history/state-domain-change-experiment.seg")
	if err != nil {
		t.Fatal(err)
	}
	var history, accessor SegmentRef
	for _, ref := range refs {
		if ref.Kind == SegmentHistory {
			history = ref
		}
		if ref.Kind == SegmentAccessor {
			accessor = ref
		}
	}
	return dir, history, accessor
}

func TestExperimentalPostingsAgainstProductionV7Oracle(t *testing.T) {
	dir, history, accessor := experimentalBuildRealV7Fixture(t)
	report, err := ExperimentHistoryPostings(context.Background(), dir, history, accessor, ExperimentalHistoryPostingBenchOptions{MaxKeys: 1000, LocatorStride: 64, QuerySamplesPerKey: 8})
	if err != nil {
		t.Fatalf("%v report=%+v", err, report)
	}
	if !report.OracleEqual || !report.AllKeysMeasured || report.SourceRecords != 8192 || report.SelectedPostings != 8192 || report.SelectedKeys != 97 || report.V7PostingBytes != report.SourcePostingSectionBytes {
		t.Fatalf("incomplete oracle: %+v", report)
	}
	if report.IndirectPointStats.RecordsSkipped >= 64*report.PointQueries {
		t.Fatal("sparse query exceeded restart bound")
	}
	data, _ := json.Marshal(report)
	t.Log(string(data))
}

// Run explicitly on a stopped node's immutable snapshot files. Reads .seg/.kv
// only; all experimental bytes are in memory. No manifest or DB writes occur.
// GTRON_EXPERIMENT_HISTORY selects an exact active history ref path. Optional
// GTRON_EXPERIMENT_OPTIONS contains a JSON ExperimentalHistoryPostingBenchOptions.
func TestExperimentalHistoryPostingsFromEnv(t *testing.T) {
	dir, path := os.Getenv("GTRON_EXPERIMENT_SNAPSHOT_DIR"), os.Getenv("GTRON_EXPERIMENT_HISTORY")
	if dir == "" && path == "" {
		t.Skip("explicit read-only source environment not supplied")
	}
	if dir == "" || path == "" {
		t.Fatal("both GTRON_EXPERIMENT_SNAPSHOT_DIR and GTRON_EXPERIMENT_HISTORY are required")
	}
	manifest, err := LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var history, accessor SegmentRef
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentHistory && ref.Path == path {
			history = ref
		}
	}
	for _, ref := range manifest.Segments {
		if ref.Kind == SegmentAccessor && ref.Dataset == history.Dataset && ref.FromTxNum == history.FromTxNum && ref.ToTxNum == history.ToTxNum {
			accessor = ref
		}
	}
	if history.Path == "" || accessor.Path == "" {
		t.Fatal("active source history/accessor pair not found")
	}
	var opts ExperimentalHistoryPostingBenchOptions
	if raw := os.Getenv("GTRON_EXPERIMENT_OPTIONS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &opts); err != nil {
			t.Fatal(err)
		}
	}
	report, err := ExperimentHistoryPostings(context.Background(), dir, history, accessor, opts)
	data, _ := json.Marshal(report)
	t.Log(string(data))
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkExperimentalPostingLookup(b *testing.B) {
	rows, source := experimentalPostingFixture(8192)
	var sample []ExperimentalHistoryPosting
	for i := 0; i < len(rows); i += 7 {
		sample = append(sample, rows[i])
	}
	blob, _, err := BuildExperimentalHistoryLocator(context.Background(), source, rows[0].Offset, uint64(len(rows)), 64)
	if err != nil {
		b.Fatal(err)
	}
	locator, err := OpenExperimentalHistoryLocator(blob)
	if err != nil {
		b.Fatal(err)
	}
	for _, indirect := range []bool{false, true} {
		b.Run(strconv.FormatBool(indirect), func(b *testing.B) {
			data, err := EncodeExperimentalHistoryPostings(context.Background(), nil, 0, sample, indirect)
			if err != nil {
				b.Fatal(err)
			}
			index, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, uint64(len(sample)), data)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				target := sample[(i*1009)%len(sample)].TxNum
				if _, ok, _, err := index.LowerBound(context.Background(), target, locator, source); err != nil || !ok {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data))/float64(len(sample)), "B/posting")
		})
	}
}

func FuzzExperimentalPostingsOpen(f *testing.F) {
	rows, _ := experimentalPostingFixture(4)
	blob, _ := EncodeExperimentalHistoryPostings(context.Background(), nil, 0, rows, false)
	f.Add(blob, uint64(4))
	f.Add([]byte{}, uint64(math.MaxUint64))
	f.Fuzz(func(t *testing.T, data []byte, count uint64) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		// Reach semantic decoding instead of spending every mutation on the
		// outer CRC rejection. This deliberately models a CRC-valid bad file.
		if len(data) >= 5 {
			data = append([]byte(nil), data...)
			data[0] = 0xc0 | (data[0] & 3)
			binary.BigEndian.PutUint32(data[len(data)-4:], experimentalPostingChecksum(nil, 0, count, data[:len(data)-4]))
		}
		r, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, count, data)
		if err == nil {
			_, _, _, _ = r.LowerBound(context.Background(), 0, nil, ExperimentalHistoryRecordSource{})
		}
	})
}

func TestExperimentalPostingsForgedMetadataAndSparseBounds(t *testing.T) {
	rows, source := experimentalPostingFixture(65)
	locatorBlob, _, err := BuildExperimentalHistoryLocator(context.Background(), source, rows[0].Offset, 65, 64)
	if err != nil {
		t.Fatal(err)
	}
	locator, _ := OpenExperimentalHistoryLocator(locatorBlob)
	// Last sampled page has one actual record: no phantom ordinal 65 is valid.
	badRows := []ExperimentalHistoryPosting{rows[64], {TxNum: rows[64].TxNum + 1, Offset: source.LogicalSize, RecordOrdinal: 65}}
	blob, err := EncodeExperimentalHistoryPostings(context.Background(), nil, 0, badRows, true)
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 2, blob)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.Exact(context.Background(), badRows[1].TxNum, locator, source); err == nil {
		t.Fatal("point accepted phantom ordinal")
	}
	if _, err := r.Scan(context.Background(), locator, source, func(ExperimentalHistoryPosting) error { return nil }); err == nil {
		t.Fatal("scan accepted phantom ordinal")
	}
	// A CRC-valid oversized count must be rejected before allocation.
	minimal := []byte{0xc0}
	minimal = binary.BigEndian.AppendUint32(minimal, experimentalPostingChecksum(nil, 0, math.MaxUint64, minimal))
	if _, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, math.MaxUint64, minimal); err == nil {
		t.Fatal("accepted forged huge count")
	}
}

func TestExperimentalSparseRejectsIncompleteSelectedFrame(t *testing.T) {
	ctx := context.Background()
	rows, source := experimentalPostingFixture(1)
	data, _, err := BuildExperimentalHistoryLocator(ctx, source, rows[0].Offset, 1, 64)
	if err != nil {
		t.Fatal(err)
	}
	for _, forge := range []string{"end-minus-one", "phantom-second-record"} {
		bad := append([]byte(nil), data...)
		target := uint64(0)
		if forge == "end-minus-one" {
			binary.BigEndian.PutUint64(bad[44:52], source.LogicalSize-1)
		} else {
			binary.BigEndian.PutUint64(bad[8:16], 2)
			target = 1
		}
		binary.BigEndian.PutUint32(bad[len(bad)-4:], crc32.ChecksumIEEE(bad[:len(bad)-4]))
		locator, err := OpenExperimentalHistoryLocator(bad)
		if err != nil {
			t.Fatal(err)
		}
		var stats ExperimentalHistoryLookupStats
		if _, err := locator.resolve(ctx, target, source, &stats); err == nil {
			t.Fatalf("accepted %s", forge)
		}
	}
	for _, offset := range []uint64{rows[0].Offset, source.LogicalSize - 1, math.MaxUint64} {
		entry := ExperimentalHistoryIndexEntry{TxNum: 1, Offset: offset, Count: 1}
		blob, err := EncodeExperimentalHistoryIndex(ctx, []ExperimentalHistoryIndexEntry{entry}, ExperimentalHistoryIndexOptions{SparseOffsets: true})
		if err != nil {
			t.Fatal(err)
		}
		index, err := OpenExperimentalHistoryIndex(ctx, blob)
		if err != nil {
			t.Fatal(err)
		}
		badSource := source
		if offset == rows[0].Offset {
			badSource.Reader = nil
		}
		if _, found, _, err := index.Exact(ctx, 1, badSource); err == nil || found {
			t.Fatal("restart-zero accepted missing/invalid source")
		}
		if _, err := index.Scan(ctx, badSource, func(ExperimentalHistoryIndexEntry) error { return nil }); err == nil {
			t.Fatal("scan accepted invalid restart")
		}
	}
}

func TestExperimentalPostingSingleFrameSizeBeforeNarrowing(t *testing.T) {
	for _, test := range []struct {
		count, size uint64
		valid       bool
	}{
		{0, 0, true}, {0, 1, false}, {1, 30, true}, {128, 128 * 30, true},
		{1, (1 << 32) + 10, false}, {128, (1 << 32) + 3840, false}, {128, math.MaxUint64, false},
	} {
		if got := experimentalSinglePostingBodyFits(test.count, test.size); got != test.valid {
			t.Fatalf("count=%d size=%d got=%t", test.count, test.size, got)
		}
	}
	// Exercise the allocation-free gate with an ordinary-sized invalid input.
	bad := make([]byte, 5000)
	bad[0] = 0xc0
	if _, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 128, bad); err == nil {
		t.Fatal("accepted oversized single frame")
	}
}

func TestExperimentalLocatorValidationReadsShareScanBudget(t *testing.T) {
	rows, source := experimentalPostingFixture(3)
	data, _, err := BuildExperimentalHistoryLocator(context.Background(), source, rows[0].Offset, 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	locator, err := OpenExperimentalHistoryLocator(data)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := EncodeExperimentalHistoryPostings(context.Background(), nil, 0, rows, true)
	if err != nil {
		t.Fatal(err)
	}
	index, err := OpenExperimentalHistoryPostings(context.Background(), nil, 0, 3, blob)
	if err != nil {
		t.Fatal(err)
	}
	source.MaxScanRecords = 1
	visits := 0
	stats, err := index.Scan(context.Background(), locator, source, func(ExperimentalHistoryPosting) error { visits++; return nil })
	if err == nil || visits != 1 || stats.RecordFramesValidated != 1 || stats.HeaderBytesRead != 4 || stats.RecordsSkipped != 0 {
		t.Fatalf("target validation escaped read budget: visits=%d stats=%+v err=%v", visits, stats, err)
	}
}

func TestExperimentalSparseTxCountCannotExceedSource(t *testing.T) {
	rows, source := experimentalPostingFixture(1)
	entry := ExperimentalHistoryIndexEntry{TxNum: 1, Offset: rows[0].Offset, Count: math.MaxUint64}
	data, err := EncodeExperimentalHistoryIndex(context.Background(), []ExperimentalHistoryIndexEntry{entry}, ExperimentalHistoryIndexOptions{SparseOffsets: true})
	if err != nil {
		t.Fatal(err)
	}
	index, err := OpenExperimentalHistoryIndex(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _, err := index.Exact(context.Background(), 1, source); err == nil || found {
		t.Fatal("accepted physically impossible tx count")
	}
	if _, err := index.Scan(context.Background(), source, func(ExperimentalHistoryIndexEntry) error { return nil }); err == nil {
		t.Fatal("scan accepted impossible tx count")
	}
}
