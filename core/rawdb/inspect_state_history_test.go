package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/golang/snappy"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

type historyPrevTestReader struct {
	values    map[string][]byte
	reads     [][]byte
	err       error
	afterRead func()
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
	if g.result(2<<20, true) != "eligible_size_and_repeated_large_identity" || g.result(1<<20, true) != "ineligible_raw_below_2MiB" {
		t.Fatal("gate size/repeat mismatch")
	}
	g = newGate()
	row.Key = make([]byte, historyPrevGateKeyBytes+1)
	g.observe(row)
	if g.result(2<<20, true) != "unknown_identity_budget" || len(g.seen) != 0 {
		t.Fatal("gate identity cap not respected")
	}
	if g.result(2<<20, false) != "unknown_incomplete_pack" {
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
