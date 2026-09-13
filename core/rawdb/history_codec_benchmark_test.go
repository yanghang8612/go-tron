package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func newHistoryBenchmarkTest(t *testing.T, options HistoryCodecBenchmarkOptions) *HistoryCodecBenchmark {
	t.Helper()
	b, err := NewHistoryCodecBenchmark(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}

func TestHistoryCodecBenchmarkProductionFormatsAndRoundTrip(t *testing.T) {
	repeatedRows := chunkHistoryRows(512<<10, 3)
	repeatedRaw := encodeBorrowedStateDomainChangeTestBlock(t, repeatedRows)
	cdc, useful := encodeStateChangeChunks(repeatedRaw)
	if !useful {
		t.Fatal("fixture must produce CDC")
	}
	smallRows := chunkHistoryRows(8192, 2)
	for _, row := range smallRows {
		row.Prev = bytes.Repeat([]byte{7}, 8192)
	}
	smallRaw := encodeBorrowedStateDomainChangeTestBlock(t, smallRows)
	snappy, compressed := encodeStateDomainChangeBlockStorage(smallRaw)
	if !compressed {
		t.Fatal("fixture must produce Snappy")
	}
	flag := stateChangeBlockChunkEncoding.Load()
	for _, tc := range []struct {
		name    string
		encoded []byte
		raw     []byte
		rows    []*StateDomainChange
	}{
		{"raw_rlp", repeatedRaw, repeatedRaw, repeatedRows},
		{"snappy_v1", snappy, smallRaw, smallRows},
		{"cdc_v2", cdc, repeatedRaw, repeatedRows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newHistoryBenchmarkTest(t, HistoryCodecBenchmarkOptions{})
			before := bytes.Clone(tc.encoded)
			r, err := b.BenchmarkPack(context.Background(), 42, tc.encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Completed || r.Rows != uint64(len(tc.rows)) || r.DecodedBytes != uint64(len(tc.raw)) || r.RawSHA256 != historyBenchmarkDigest(tc.raw) || r.EncodedSHA256 != historyBenchmarkDigest(tc.encoded) {
				t.Fatalf("source contract mismatch: %+v", r)
			}
			if !bytes.Equal(before, tc.encoded) || stateChangeBlockChunkEncoding.Load() != flag {
				t.Fatal("benchmark mutated source or production flag")
			}
			if len(r.Candidates) != 4 || r.Candidates[0].StorageFormat != tc.name || r.Candidates[0].StoredBytes != uint64(len(tc.encoded)) || r.Candidates[0].Encode != nil {
				t.Fatalf("candidate contract mismatch: %+v", r.Candidates)
			}
			for i, c := range r.Candidates {
				if !c.ByteExact || c.StoredBytes == 0 || c.Decode.WallNS < 0 || c.Decode.ProcessCPUNS < 0 {
					t.Fatalf("candidate failed: %+v", c)
				}
				if i > 0 && (c.Encode == nil || c.Encode.WallNS < 0 || c.Encode.ProcessCPUNS < 0) {
					t.Fatalf("missing encode measurement: %+v", c)
				}
				if c.ProductionReadable != (c.Name != "zstd_default") {
					t.Fatalf("experimental format labeled production-readable: %+v", c)
				}
			}
			if r.ZstdWindowBytes != 8<<20 {
				t.Fatal("zstd window changed")
			}
			stats := b.Stats()
			if stats.Attempts != 1 || stats.Completed != 1 || stats.ChargedEncodedBytes != uint64(len(tc.encoded)) || stats.ChargedDecodedBytes != uint64(len(tc.raw)) {
				t.Fatalf("budget accounting mismatch: %+v", stats)
			}
		})
	}
}

func TestHistoryCodecBenchmarkForcedCDCGateAndIndependentPacks(t *testing.T) {
	b := newHistoryBenchmarkTest(t, HistoryCodecBenchmarkOptions{})
	// Duplicate large values fit below the existing 2 MiB gate. The candidate
	// must really invoke CDC, without changing the process-wide writer flag.
	rows := chunkHistoryRows(512<<10, 3)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	r, err := b.BenchmarkPack(context.Background(), 42, raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.ProductionCDCGate || !r.HasRepeatedLargeKey || !r.ForcedCDCUsefulAgainstRaw || !r.ForcedCDCWouldReplaceSnappy || r.Candidates[2].StoredBytes >= r.Candidates[1].StoredBytes {
		t.Fatalf("missed below-gate opportunity: %+v", r)
	}
	t.Logf("synthetic below-gate pack: raw=%d snappy=%d forced_cdc=%d zstd=%d (bytes only, no I/O claim)", len(raw), r.Candidates[1].StoredBytes, r.Candidates[2].StoredBytes, r.Candidates[3].StoredBytes)
	// A single random large image repeated in separate packs does not become
	// a cross-pack dictionary. Its forced CDC result explicitly falls back.
	one := encodeBorrowedStateDomainChangeTestBlock(t, chunkHistoryRows(2<<20, 1))
	var sizes [2][4]uint64
	for i := range sizes {
		r, err := b.BenchmarkPack(context.Background(), uint64(50+i), one)
		if err != nil {
			t.Fatal(err)
		}
		if r.HasRepeatedLargeKey || r.ProductionCDCGate || r.ForcedCDCUsefulAgainstRaw || r.ForcedCDCWouldReplaceSnappy || r.Candidates[2].StorageFormat != "raw_rlp" || r.Candidates[2].StoredBytes != uint64(len(one)) {
			t.Fatalf("single-version result hid raw fallback: %+v", r)
		}
		for j, c := range r.Candidates {
			sizes[i][j] = c.StoredBytes
		}
	}
	if sizes[0] != sizes[1] {
		t.Fatalf("independent pack result depends on prior history: %v", sizes)
	}
	t.Logf("synthetic single-version independent packs: existing/snappy/cdc/zstd=%v; repeated calls=%v", sizes[0], sizes[1])
}

func TestHistoryCodecBenchmarkGateMatchesProductionIdentity(t *testing.T) {
	for _, variant := range []string{"same", "owner", "generation", "domain", "key", "absent"} {
		t.Run(variant, func(t *testing.T) {
			rows := chunkHistoryRows(128<<10, 2)
			switch variant {
			case "owner":
				rows[1].Owner[1]++
			case "generation":
				rows[1].Generation++
			case "domain":
				rows[1].Domain = kvdomains.SystemReward
			case "key":
				rows[1].Key = []byte("another")
			case "absent":
				rows[1].PrevExists = false
			}
			raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
			var sample HistoryCodecBenchmarkSample
			if err := inspectHistoryBenchmarkPack(context.Background(), raw, 42, 100, &sample); err != nil {
				t.Fatal(err)
			}
			if got, want := sample.HasRepeatedLargeKey, stateChangeBlockHasLargeVersions(rows); got != want {
				t.Fatalf("gate diverged from production: got %v want %v", got, want)
			}
		})
	}
}

func TestHistoryCodecBenchmarkBudgetAndCorruption(t *testing.T) {
	rows := chunkHistoryRows(4096, 2)
	for _, row := range rows {
		row.Prev = bytes.Repeat([]byte{1}, 4096)
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	stored, _ := encodeStateDomainChangeBlockStorage(raw)
	for _, tc := range []struct {
		name string
		opts HistoryCodecBenchmarkOptions
	}{
		{"single_decoded", HistoryCodecBenchmarkOptions{MaxDecodedBytes: uint64(len(raw) - 1)}},
		{"total_decoded", HistoryCodecBenchmarkOptions{MaxTotalDecodedBytes: uint64(len(raw) - 1)}},
		{"total_encoded", HistoryCodecBenchmarkOptions{MaxTotalEncodedBytes: uint64(len(stored) - 1)}},
		{"rows", HistoryCodecBenchmarkOptions{MaxRowsPerPack: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newHistoryBenchmarkTest(t, tc.opts)
			r, err := b.BenchmarkPack(context.Background(), 42, stored)
			if !errors.Is(err, ErrHistoryCodecBenchmarkLimit) || r.Completed || b.Stats().Completed != 0 {
				t.Fatalf("limit not enforced: result=%+v stats=%+v err=%v", r, b.Stats(), err)
			}
		})
	}
	for _, limit := range []string{"samples", "encoded", "decoded"} {
		t.Run("cumulative_"+limit, func(t *testing.T) {
			opts := HistoryCodecBenchmarkOptions{}
			switch limit {
			case "samples":
				opts.MaxSamples = 1
			case "encoded":
				opts.MaxTotalEncodedBytes = uint64(len(stored))
			case "decoded":
				opts.MaxTotalDecodedBytes = uint64(len(raw))
			}
			b := newHistoryBenchmarkTest(t, opts)
			if _, err := b.BenchmarkPack(context.Background(), 42, stored); err != nil {
				t.Fatal(err)
			}
			if r, err := b.BenchmarkPack(context.Background(), 43, stored); !errors.Is(err, ErrHistoryCodecBenchmarkLimit) || r.Completed || b.Stats().Completed != 1 {
				t.Fatalf("cumulative cap not enforced: %+v %v", r, err)
			}
		})
	}
	cdc, useful := encodeStateChangeChunks(raw)
	if !useful {
		t.Fatal("fixture must compress")
	}
	corruptCDC := bytes.Clone(cdc)
	_, n := binary.Uvarint(corruptCDC[len(stateDomainChangeBlockEnvelopeMagic)+1:])
	corruptCDC[len(stateDomainChangeBlockEnvelopeMagic)+1+n] ^= 1
	huge := append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), stateDomainChangeBlockSnappyVersion)
	huge = binary.AppendUvarint(huge, 128<<20+1)
	badRLP := append(bytes.Clone(raw), 0)
	unknown := append(bytes.Clone(stateDomainChangeBlockEnvelopeMagic[:]), 255)
	for i, data := range [][]byte{corruptCDC, huge, badRLP, unknown, {0xc0}} {
		b := newHistoryBenchmarkTest(t, HistoryCodecBenchmarkOptions{})
		if r, err := b.BenchmarkPack(context.Background(), 42, data); err == nil || r.Completed || b.Stats().Completed != 0 {
			t.Fatalf("accepted malformed %d: %+v %v", i, r, err)
		}
		if b.Stats().Attempts != 1 || b.Stats().ChargedEncodedBytes != uint64(len(data)) {
			t.Fatalf("failed attempt erased input work: %+v", b.Stats())
		}
	}
}

type historyBenchmarkCancelContext struct {
	context.Context
	calls atomic.Int32
	after int32
}

func (c *historyBenchmarkCancelContext) Err() error {
	if c.calls.Add(1) >= c.after {
		return context.Canceled
	}
	return nil
}

func TestHistoryCodecBenchmarkCancellationAndClose(t *testing.T) {
	rows := chunkHistoryRows(4096, 4)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	b := newHistoryBenchmarkTest(t, HistoryCodecBenchmarkOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.BenchmarkPack(ctx, 42, raw); !errors.Is(err, context.Canceled) || b.Stats().Attempts != 0 {
		t.Fatalf("pre-canceled call performed work: %+v %v", b.Stats(), err)
	}
	ctx = &historyBenchmarkCancelContext{Context: context.Background(), after: 4}
	if r, err := b.BenchmarkPack(ctx, 42, raw); !errors.Is(err, context.Canceled) || r.Completed || b.Stats().ChargedDecodedBytes != uint64(len(raw)) || b.Stats().Completed != 0 {
		t.Fatalf("cancellation during validation lost budget: %+v %+v %v", r, b.Stats(), err)
	}
	if _, err := b.BenchmarkPack(context.Background(), 42, raw); err != nil {
		t.Fatalf("canceled call leaked lock: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.BenchmarkPack(context.Background(), 42, raw); !errors.Is(err, ErrHistoryCodecBenchmarkClosed) {
		t.Fatalf("closed benchmark accepted work: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryCodecBenchmarkPreservesTemporalAndPresenceFields(t *testing.T) {
	rows := chunkHistoryRows(32, 4)
	rows[0].PrevExists, rows[0].Prev = false, nil
	rows[1].Prev = []byte{} // present empty is different from absent
	rows[2].Generation++
	rows[2].Prev = []byte{0, 1, 0xff, 0, 7} // opaque content remains byte-exact
	rows[3].TxNum = rows[2].TxNum           // same-ordinal ordering is retained
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	b := newHistoryBenchmarkTest(t, HistoryCodecBenchmarkOptions{})
	if _, err := b.BenchmarkPack(context.Background(), 42, raw); err != nil {
		t.Fatal(err)
	}
	got, err := decodePersistedStateDomainChangeBlock(raw, 42)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if rows[i].PrevExists != got[i].PrevExists || rows[i].Generation != got[i].Generation || rows[i].TxNum != got[i].TxNum || rows[i].Seq != got[i].Seq || !bytes.Equal(rows[i].Prev, got[i].Prev) {
			t.Fatalf("history semantics changed at %d", i)
		}
	}
	// This is deliberately not accepted as an ordinary sequence-zero row.
	legacy, err := rlp.EncodeToBytes(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.BenchmarkPack(context.Background(), 42, legacy); err == nil {
		t.Fatal("accepted non-pack export")
	}
}

func TestHistoryCodecBenchmarkOptionLimits(t *testing.T) {
	for _, options := range []HistoryCodecBenchmarkOptions{
		{MaxDecodedBytes: 128<<20 + 1}, {MaxTotalDecodedBytes: 1<<30 + 1}, {MaxTotalEncodedBytes: 1<<30 + 1}, {MaxSamples: 257}, {MaxRowsPerPack: 1000001},
	} {
		if b, err := NewHistoryCodecBenchmark(options); err == nil {
			_ = b.Close()
			t.Fatalf("accepted invalid options: %+v", options)
		}
	}
	b := newHistoryBenchmarkTest(t, HistoryCodecBenchmarkOptions{})
	if !reflect.DeepEqual(b.Stats(), HistoryCodecBenchmarkStats{}) {
		t.Fatalf("nonzero initial stats: %+v", b.Stats())
	}
}
