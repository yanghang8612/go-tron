package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

type historyPrevTestReader struct {
	values    map[string][]byte
	reads     [][]byte
	err       error
	afterRead func()
}

func TestInspectStateHistoryPrevSharedMaterializationAndAccounting(t *testing.T) {
	f, _ := newHistoryReadViewFixture(t)
	writeHistoryReadViewSharedFixture(t, f.KeyValueStore, 10)
	physical, err := f.KeyValueStore.Get(stateChangeSetKey(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	_, rawLen, count, _, err := sharedStateHistoryPackHeader(physical, 10)
	if err != nil {
		t.Fatal(err)
	}
	chunkBytes := uint64(0)
	prefix := stateHistoryChunkBucketPrefix(stateHistoryChunkBucket(10))
	it := f.KeyValueStore.NewIterator(prefix, nil)
	for it.Next() {
		chunkBytes += uint64(len(it.Value()))
	}
	iterErr := it.Error()
	it.Release()
	if iterErr != nil {
		t.Fatal(iterErr)
	}
	// Delete the source immediately after snapshot capture. Inspect must retain
	// both the reference pack and all chunks, and open only that one snapshot.
	f.afterCapture = func() {
		if err := f.KeyValueStore.Delete(stateChangeSetKey(10, 0)); err != nil {
			t.Fatal(err)
		}
		if err := f.KeyValueStore.DeleteRange(prefix, prefixUpperBound(prefix)); err != nil {
			t.Fatal(err)
		}
	}
	o := historyPrevTestOptions(10, 10)
	exports := 0
	o.OnCompletePack = func(sample HistoryPrevPackSample, data []byte) error {
		codec, size, err := InspectStateHistoryPackEncoding(data)
		if err != nil || codec != "raw" || size != uint64(rawLen) || isStateHistorySharedPack(data) {
			t.Fatalf("export is not self-contained raw: %q/%d %v", codec, size, err)
		}
		if sample.Codec != "shared3" || sample.EncodedBytes != uint64(len(physical)) || sample.ExportCodec != "raw" || sample.ExportBytes != uint64(rawLen) {
			t.Fatalf("physical/export metadata mixed: %+v", sample)
		}
		rows, err := decodePersistedStateDomainChangeBlock(data, 10)
		if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Prev, []byte{10}) {
			t.Fatalf("exported rows=%v err=%v", rows, err)
		}
		exports++
		return nil
	}
	report, err := InspectStateHistoryPrev(context.Background(), f, o)
	if err != nil || !report.Complete || exports != 1 || f.opened != 1 || f.closed != 1 {
		t.Fatalf("shared inspect/export=%d snapshots=%d/%d report=%+v err=%v", exports, f.opened, f.closed, report, err)
	}
	if report.EncodedBytesRead != uint64(len(physical)) || report.ChunkReadBytes != chunkBytes || report.ChunkReads != uint64(count) || report.DecodedBytesReserved != uint64(rawLen) || report.ExportBytesAttempted != uint64(rawLen) || report.ExportBytesAccepted != uint64(rawLen) {
		t.Fatalf("byte ledgers mixed: %+v", report)
	}
}

func TestInspectStateHistoryPrevSharedBudgetsBeforeReadsAndExport(t *testing.T) {
	for _, mode := range []string{"decoded", "chunk", "export", "unpinned", "missing"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := newHistoryReadViewFixture(t)
			writeHistoryReadViewSharedFixture(t, f.KeyValueStore, 10)
			pack, err := f.KeyValueStore.Get(stateChangeSetKey(10, 0))
			if err != nil {
				t.Fatal(err)
			}
			_, size, _, _, err := sharedStateHistoryPackHeader(pack, 10)
			if err != nil {
				t.Fatal(err)
			}
			o := historyPrevTestOptions(10, 11)
			exported := 0
			o.OnCompletePack = func(HistoryPrevPackSample, []byte) error { exported++; return nil }
			wantReason := "decode_error"
			switch mode {
			case "decoded":
				o.MaxDecodedBytes, wantReason = uint64(size-1), "decoded_budget"
			case "chunk":
				o.MaxChunkReadBytes, wantReason = 1, "chunk_read_budget"
			case "export":
				o.MaxExportBytes, wantReason = uint64(size-1), "export_budget"
			case "unpinned":
				f.factoryErr = pointread.ErrKeyValueSnapshotUnsupported
			case "missing":
				prefix := stateHistoryChunkBucketPrefix(stateHistoryChunkBucket(10))
				if err := f.KeyValueStore.DeleteRange(prefix, prefixUpperBound(prefix)); err != nil {
					t.Fatal(err)
				}
			}
			report, err := InspectStateHistoryPrev(context.Background(), f, o)
			if !errors.Is(err, ErrHistoryPrevInspectionPartial) || report.Complete || report.StopReason != wantReason || exported != 0 || report.ExportBytesAccepted != 0 || report.Samples[1].Status != "not_visited" {
				t.Fatalf("partial mode=%s export=%d report=%+v err=%v", mode, exported, report, err)
			}
			if mode == "decoded" && (report.DecodedBytesReserved != 0 || report.ChunkReads != 0) {
				t.Fatal("decoded budget checked after allocation or chunk I/O")
			}
			if mode == "unpinned" && (!errors.Is(err, ErrStateHistoryReadViewUnpinned) || report.ChunkReads != 0) {
				t.Fatal("unpinned source performed chunk I/O")
			}
			if mode == "chunk" && (report.ChunkReads != 1 || report.ChunkReadBytes <= 1 || report.Rows != 0) {
				t.Fatal("rejected chunk not charged or decoded beyond budget")
			}
		})
	}
}

type historyPrevCancelChunkView struct {
	StateHistoryReadView
	prefix []byte
	cancel context.CancelFunc
}

func (v historyPrevCancelChunkView) GetWithPresence(key []byte) ([]byte, bool, error) {
	value, exists, err := readPresentValue(v.StateHistoryReadView, key, "cancellation fixture")
	if bytes.HasPrefix(key, v.prefix) {
		v.cancel()
	}
	return value, exists, err
}

func TestInspectStateHistoryPrevSharedCancellationAfterChunkRead(t *testing.T) {
	f, _ := newHistoryReadViewFixture(t)
	writeHistoryReadViewSharedFixture(t, f.KeyValueStore, 10)
	view, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := historyPrevCancelChunkView{StateHistoryReadView: view, prefix: stateHistoryChunkBucketPrefix(stateHistoryChunkBucket(10)), cancel: cancel}
	report, err := InspectStateHistoryPrev(ctx, source, historyPrevTestOptions(10, 11))
	if !errors.Is(err, context.Canceled) || report.StopReason != "context" || report.ChunkReads != 1 || report.ChunkReadBytes == 0 || report.Rows != 0 || report.Samples[1].Status != "not_visited" {
		t.Fatalf("chunk cancellation accounting report=%+v err=%v", report, err)
	}
	if f.closed != 0 {
		t.Fatal("inspection closed its caller-owned snapshot")
	}
}

func TestInspectStateHistoryPrevSharedRepeatedChunksChargeEveryLookup(t *testing.T) {
	const block = uint64(20)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{historyPrevTestRow(block, 1, 40<<10)})
	digest := sha256.Sum256(raw)
	count := (len(raw) + historychunk.MinSize - 1) / historychunk.MinSize
	pack := append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), stateDomainChangeBlockSharedVersion)
	pack = binary.AppendUvarint(pack, block)
	pack = binary.AppendUvarint(pack, uint64(len(raw)))
	pack = append(pack, digest[:]...)
	pack = binary.AppendUvarint(pack, uint64(count))
	values := make(map[string][]byte)
	var expectedReadBytes uint64
	for start := 0; start < len(raw); start += historychunk.MinSize {
		chunk := raw[start:min(start+historychunk.MinSize, len(raw))]
		hash := sha256.Sum256(chunk)
		encoded := encodeStateHistorySharedChunk(chunk)
		values[string(stateHistoryChunkKey(stateHistoryChunkBucket(block), hash))] = encoded
		expectedReadBytes += uint64(len(encoded))
		pack = binary.AppendUvarint(pack, uint64(len(chunk)))
		pack = append(pack, hash[:]...)
	}
	if len(values) >= count {
		t.Fatal("fixture has no repeated chunks")
	}
	values[string(stateChangeSetKey(block, 0))] = pack
	source := &historyPrevTestReader{values: values}
	// This immutable test source needs only Get; inspection never iterates it.
	view := &stateHistoryReadView{reader: source, pinned: true}
	report, err := InspectStateHistoryPrev(context.Background(), view, historyPrevTestOptions(block, block))
	if err != nil || !report.Complete || report.ChunkReads != uint64(count) || report.ChunkReadBytes != expectedReadBytes || len(source.reads) != count+1 {
		t.Fatalf("repeat lookup accounting reads=%d report=%+v err=%v", len(source.reads), report, err)
	}
}

func (r *historyPrevTestReader) Has([]byte) (bool, error) {
	panic("inspection should use coupled presence read")
}
func (r *historyPrevTestReader) Get([]byte) ([]byte, error) {
	panic("inspection should use coupled presence read")
}
func (r *historyPrevTestReader) GetWithPresence(key []byte) ([]byte, bool, error) {
	r.reads = append(r.reads, bytes.Clone(key))
	if r.afterRead != nil {
		r.afterRead()
	}
	v, exists := r.values[string(key)]
	return bytes.Clone(v), exists, r.err
}

func historyPrevTestOptions(from, to uint64) HistoryPrevInspectOptions {
	o := DefaultHistoryPrevInspectOptions()
	o.FromBlock = from
	o.ToBlock = to
	return o
}
func historyPrevTestRow(block, seq uint64, n int) *StateDomainChange {
	return &StateDomainChange{BlockNum: block, Seq: seq, TxNum: 100 + seq, FlatDomain: StateFlatDomainKVLatest,
		Owner: common.Address{0x41, 1}, Generation: 2, Domain: kvdomains.SystemDelegation, Key: []byte("drax-0-test"), PrevExists: true, Prev: bytes.Repeat([]byte{0xab}, n)}
}

func TestInspectStateHistoryPrevCodecsDomainsAndMissing(t *testing.T) {
	rows := []*StateDomainChange{historyPrevTestRow(10, 1, 0), historyPrevTestRow(10, 2, 32), historyPrevTestRow(10, 3, 20<<10)}
	rows[0].PrevExists = false
	rows[1].Domain = kvdomains.KVDomain(0xfefe) // Preserve unregistered numeric IDs.
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	snappyPack := append(append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), stateDomainChangeBlockSnappyVersion), snappy.Encode(nil, raw)...)
	chunks, ok := encodeStateChangeChunks(raw)
	if !ok {
		t.Fatal("compressible test fixture did not produce chunks")
	}
	reader := &historyPrevTestReader{values: map[string][]byte{
		string(stateChangeSetKey(10, 0)): raw, string(stateChangeSetKey(12, 0)): snappyPack, string(stateChangeSetKey(13, 0)): chunks,
		string(stateChangeSetKey(11, 1)): {0xff}, // seq>0 is explicitly outside this diagnostic.
	}}
	report, err := InspectStateHistoryPrev(context.Background(), reader, historyPrevTestOptions(10, 13))
	if err != nil || !report.Complete || report.StopReason != "complete" {
		t.Fatalf("report incomplete: %+v %v", report, err)
	}
	if report.PacksComplete != 3 || report.MissingPacks != 1 || report.Rows != 9 || report.PrevBytes != 3*(32+20<<10) {
		t.Fatalf("incorrect sample totals: %+v", report)
	}
	if len(reader.reads) != 4 || len(report.Samples) != 4 {
		t.Fatal("sampling replaced a missing block or performed additional reads")
	}
	for i, want := range []string{"raw", "unknown", "snappy1", "chunks2"} {
		if report.Samples[i].Codec != want {
			t.Fatalf("codec %d = %q", i, report.Samples[i].Codec)
		}
	}
	if report.Samples[1].Status != "missing" {
		t.Fatal("missing seq=0 was silently replaced with legacy or nearby rows")
	}
	if len(report.Domains) != 2 || report.Domains[1].KVDomain != 0xfefe || report.Domains[1].KVName != "" {
		t.Fatal("unknown domain lost")
	}
	if report.Domains[0].PrevExists != 3 || report.Histogram.Rows[0] != 3 || report.Histogram.Rows[1] != 3 || report.Histogram.Rows[6] != 3 {
		t.Fatalf("histogram/presence mismatch: %+v", report)
	}
	if report.LargeKeysTracked != 1 || len(report.LargeKeys) != 1 || report.LargeKeys[0].LargeVersions != 3 || report.LargeKeys[0].LargePrevBytes != 60<<10 {
		t.Fatalf("cross-pack large key totals: %+v", report.LargeKeys)
	}
	if report.MaxPrevBytes != 20<<10 || report.LargestRows[0].KeyPrefixHex == "" {
		t.Fatal("largest row identity missing")
	}
}

func TestInspectStateHistoryPrevSamplingReproducibleAndBounded(t *testing.T) {
	o := historyPrevTestOptions(100, 10099)
	o.Samples = 256
	a, b := historyPrevSamples(o), historyPrevSamples(o)
	if !reflect.DeepEqual(a, b) || len(a) != 256 {
		t.Fatal("sampling is not reproducible")
	}
	o.Seed++
	if reflect.DeepEqual(a, historyPrevSamples(o)) {
		t.Fatal("seed does not affect selections")
	}
	for i, s := range a {
		if s.Block < s.StratumFrom || s.Block > s.StratumTo || (i > 0 && a[i-1].StratumTo+1 != s.StratumFrom) {
			t.Fatalf("bad stratum: %+v", s)
		}
	}
	if a[0].StratumFrom != 100 || a[len(a)-1].StratumTo != 10099 {
		t.Fatal("strata do not cover inclusive range")
	}
	o = historyPrevTestOptions(^uint64(0)-2, ^uint64(0))
	for i, s := range historyPrevSamples(o) {
		if s.Block != o.FromBlock+uint64(i) {
			t.Fatal("near-max range overflow")
		}
	}
	o.FromBlock = 0
	if o.Validate() == nil {
		t.Fatal("full uint64 range overflow accepted")
	}
}

func TestInspectStateHistoryPrevBudgetsAndReadFailures(t *testing.T) {
	rows := []*StateDomainChange{historyPrevTestRow(1, 1, 512), historyPrevTestRow(1, 2, 512)}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	for _, tc := range []struct {
		name                     string
		configure                func(*HistoryPrevInspectOptions)
		reason                   string
		rows, accepted, reserved uint64
	}{
		{"encoded", func(o *HistoryPrevInspectOptions) { o.MaxEncodedBytes = uint64(len(raw) - 1) }, "encoded_budget", 0, 0, 0},
		{"decoded", func(o *HistoryPrevInspectOptions) { o.MaxDecodedBytes = uint64(len(raw) - 1) }, "decoded_budget", 0, uint64(len(raw)), 0},
		{"rows", func(o *HistoryPrevInspectOptions) { o.MaxRows = 1 }, "rows_budget", 1, uint64(len(raw)), uint64(len(raw))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := historyPrevTestOptions(1, 2)
			tc.configure(&o)
			r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): raw}}
			got, err := InspectStateHistoryPrev(context.Background(), r, o)
			if !errors.Is(err, ErrHistoryPrevInspectionPartial) || got.Complete || got.StopReason != tc.reason {
				t.Fatalf("partial contract: %+v %v", got, err)
			}
			if got.Rows != tc.rows || got.EncodedBytesAccepted != tc.accepted || got.DecodedBytesReserved != tc.reserved || got.EncodedBytesRead != uint64(len(raw)) {
				t.Fatalf("budget accounting: %+v", got)
			}
			if len(r.reads) != 1 || got.Samples[1].Status != "not_visited" {
				t.Fatal("continued reading after a limit")
			}
		})
	}
	t.Run("exact_budget_does_not_probe_next", func(t *testing.T) {
		o := historyPrevTestOptions(1, 2)
		o.MaxEncodedBytes = uint64(len(raw))
		r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): raw}}
		got, err := InspectStateHistoryPrev(context.Background(), r, o)
		if err == nil || got.PacksComplete != 1 || got.StopReason != "encoded_budget" || len(r.reads) != 1 {
			t.Fatalf("extra read at budget: %+v %v", got, err)
		}
	})
	t.Run("read_error", func(t *testing.T) {
		want := errors.New("injected point read failure")
		got, err := InspectStateHistoryPrev(context.Background(), &historyPrevTestReader{err: want}, historyPrevTestOptions(1, 2))
		if !errors.Is(err, want) || got.Complete || got.StopReason != "read_error" || got.MissingPacks != 0 {
			t.Fatalf("error treated as empty: %+v %v", got, err)
		}
	})
}

func TestInspectStateHistoryPrevDecodePreflightAndCorruption(t *testing.T) {
	header := append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), stateDomainChangeBlockChunksVersion)
	oversize := binary.AppendUvarint(bytes.Clone(header), stateDomainChangeBlockMaxDecodedBytes+1)
	badBody := binary.AppendUvarint(bytes.Clone(header), 1024)
	for _, tc := range []struct {
		name     string
		value    []byte
		reserved uint64
	}{
		{"bad_raw", []byte{0xff}, 1}, {"empty_value", []byte{}, 0},
		{"oversize_before_allocation", oversize, 0}, {"valid_length_bad_chunk_body", badBody, 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): tc.value}}
			got, err := InspectStateHistoryPrev(context.Background(), r, historyPrevTestOptions(1, 2))
			if err == nil || got.Complete || got.StopReason != "decode_error" || got.DecodedBytesReserved != tc.reserved || len(r.reads) != 1 {
				t.Fatalf("bad decode contract: %+v %v", got, err)
			}
		})
	}
	t.Run("advertised_length_exceeds_remaining", func(t *testing.T) {
		o := historyPrevTestOptions(1, 1)
		o.MaxDecodedBytes = 100
		r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): badBody}}
		got, err := InspectStateHistoryPrev(context.Background(), r, o)
		if err == nil || got.StopReason != "decoded_budget" || got.DecodedBytesReserved != 0 {
			t.Fatalf("decoded before budgeting: %+v %v", got, err)
		}
	})
}

func TestInspectStateHistoryPrevCancellationAndDuration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &historyPrevTestReader{}
	got, err := InspectStateHistoryPrev(ctx, r, historyPrevTestOptions(1, 2))
	if !errors.Is(err, context.Canceled) || got.Complete || got.StopReason != "context" || len(r.reads) != 0 {
		t.Fatalf("pre-read cancel: %+v %v", got, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	r = &historyPrevTestReader{afterRead: cancel}
	got, err = InspectStateHistoryPrev(ctx, r, historyPrevTestOptions(1, 1))
	if !errors.Is(err, context.Canceled) || got.Complete || got.StopReason != "context" {
		t.Fatalf("cancel during final missing read: %+v %v", got, err)
	}
	o := historyPrevTestOptions(1, 2)
	o.MaxDuration = time.Nanosecond
	r = &historyPrevTestReader{}
	got, err = InspectStateHistoryPrev(context.Background(), r, o)
	if err == nil || got.StopReason != "duration_budget" || len(r.reads) != 0 {
		t.Fatalf("duration budget: %+v %v", got, err)
	}
}

func TestInspectStateHistoryPrevChunkGateExactIdentityAndLimits(t *testing.T) {
	newGate := func() historyPrevChunkGate {
		return historyPrevChunkGate{seen: make(map[historyPrevGateIdentity]uint64)}
	}
	row := historyPrevTestRow(1, 1, historychunk.MaxSize)
	g := newGate()
	g.observe(row)
	for _, mutate := range []func(*StateDomainChange){
		func(c *StateDomainChange) { c.Owner[2]++ }, func(c *StateDomainChange) { c.Generation++ },
		func(c *StateDomainChange) { c.Domain++ }, func(c *StateDomainChange) { c.FlatDomain++ }, func(c *StateDomainChange) { c.Key = []byte("other") },
	} {
		copyRow := *row
		mutate(&copyRow)
		g.observe(&copyRow)
	}
	if g.repeated {
		t.Fatal("gate ignored full identity")
	}
	g.observe(row)
	if g.result(stateChangeBlockChunkMinRawBytes, true) != "eligible_size_and_repeated_large_identity" || g.result(stateChangeBlockChunkMinRawBytes-1, true) != "ineligible_raw_below_256KiB" {
		t.Fatal("gate size/repeat mismatch")
	}
	g = newGate()
	row.Key = make([]byte, historyPrevGateKeyBytes+1)
	g.observe(row)
	if g.result(stateChangeBlockChunkMinRawBytes, true) != "unknown_identity_budget" || len(g.seen) != 0 {
		t.Fatal("gate identity cap not respected")
	}
	if g.result(stateChangeBlockChunkMinRawBytes, false) != "unknown_incomplete_pack" {
		t.Fatal("partial pack described as exact negative")
	}
}

func TestInspectStateHistoryPrevExportHookOnlyAfterFullDecode(t *testing.T) {
	row := historyPrevTestRow(1, 1, 32)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, []*StateDomainChange{row})
	// The second pack has one valid row and a malformed later row. Valid-prefix
	// aggregates are retained, but its encoded payload must not be exported.
	var validBlock struct {
		Version  uint8
		FirstSeq uint64
		Rows     []rlp.RawValue
	}
	if err := rlp.DecodeBytes(raw, &validBlock); err != nil {
		t.Fatal(err)
	}
	validBlock.Rows = append(validBlock.Rows, rlp.RawValue{0xc0})
	bad, err := rlp.EncodeToBytes(&validBlock)
	if err != nil {
		t.Fatal(err)
	}
	r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): raw, string(stateChangeSetKey(2, 0)): bad}}
	o := historyPrevTestOptions(1, 2)
	var exported []uint64
	o.OnCompletePack = func(sample HistoryPrevPackSample, encoded []byte) error {
		if sample.Status != "complete" || !bytes.Equal(encoded, raw) {
			t.Fatal("unvalidated or transformed pack exported")
		}
		exported = append(exported, sample.Block)
		return nil
	}
	got, err := InspectStateHistoryPrev(context.Background(), r, o)
	if err == nil || got.Complete || got.StopReason != "decode_error" || got.Rows != 2 || !reflect.DeepEqual(exported, []uint64{1}) {
		t.Fatalf("partial export: %+v %v %v", got, err, exported)
	}
	want := errors.New("injected export failure")
	o.OnCompletePack = func(HistoryPrevPackSample, []byte) error { return want }
	got, err = InspectStateHistoryPrev(context.Background(), r, o)
	if !errors.Is(err, want) || got.Complete || got.StopReason != "export_error" || got.Samples[1].Status != "not_visited" {
		t.Fatalf("export failure ignored: %+v %v", got, err)
	}
}

func TestInspectStateHistoryPrevLargeKeyCapAndTopAreExplicit(t *testing.T) {
	// Shared fixture values keep source allocation bounded. Its ~64MiB decoded
	// pack tests the actual borrowed path and saturates the fixed 4096-key table.
	prev := bytes.Repeat([]byte{0x13}, historyPrevLargeThreshold)
	rows := make([]*StateDomainChange, historyPrevMaxGroups+2)
	for i := range rows {
		rows[i] = historyPrevTestRow(1, uint64(i+1), 0)
		rows[i].Prev = prev
		rows[i].Generation = uint64(i)
	}
	// A late repeated admitted key still accumulates after the table saturates.
	rows[len(rows)-1].Generation = 0
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	encoded, _ := encodeStateDomainChangeBlockStorage(raw)
	r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): encoded}}
	got, err := InspectStateHistoryPrev(context.Background(), r, historyPrevTestOptions(1, 1))
	if err != nil || !got.Complete || got.LargeKeysTracked != historyPrevMaxGroups || len(got.LargeKeys) != historyPrevTopCount || len(got.LargestRows) != historyPrevTopCount {
		t.Fatalf("bounded top: %v complete=%v tracked=%d top=%d", err, got.Complete, got.LargeKeysTracked, len(got.LargeKeys))
	}
	if got.LargeKeyOverflowRows != 1 || got.LargeKeyOverflowBytes != historyPrevLargeThreshold || got.LargeKeys[0].Generation != 0 || got.LargeKeys[0].LargeVersions != 2 {
		t.Fatalf("overflow/repeat accounting: %+v", got.LargeKeys[0])
	}
}

func TestInspectStateHistoryPrevDomainCapKeepsUnknownIDsBounded(t *testing.T) {
	rows := make([]*StateDomainChange, historyPrevMaxGroups+1)
	for i := range rows {
		rows[i] = historyPrevTestRow(1, uint64(i+1), 0)
		rows[i].Domain = kvdomains.KVDomain(i)
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	r := &historyPrevTestReader{values: map[string][]byte{string(stateChangeSetKey(1, 0)): raw}}
	got, err := InspectStateHistoryPrev(context.Background(), r, historyPrevTestOptions(1, 1))
	if err == nil || got.Complete || got.StopReason != "domain_groups_budget" || len(got.Domains) != historyPrevMaxGroups || got.Rows != historyPrevMaxGroups {
		t.Fatalf("unbounded distinct domains: reason=%s groups=%d rows=%d err=%v", got.StopReason, len(got.Domains), got.Rows, err)
	}
}
