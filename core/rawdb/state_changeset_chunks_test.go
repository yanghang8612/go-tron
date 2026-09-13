package rawdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand"
	"reflect"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

func chunkHistoryRows(size, versions int) []*StateDomainChange {
	rng := rand.New(rand.NewSource(20260907))
	base := make([]byte, size)
	_, _ = rng.Read(base)
	rows := make([]*StateDomainChange, versions)
	for i := range rows {
		// Repeated random address-like contents with a small insertion and a
		// changed header; alignment shifts so fixed-size dedup cannot win.
		prev := append([]byte{byte(i), 0x41}, base[:size/2]...)
		prev = append(prev, bytes.Repeat([]byte{byte(i)}, 21*i)...)
		prev = append(prev, base[size/2:]...)
		rows[i] = &StateDomainChange{BlockNum: 42, Seq: uint64(i + 1), TxNum: 500 + uint64(i), FlatDomain: StateFlatDomainKVLatest,
			Owner: common.Address{0x41, 3}, Generation: 7, Domain: kvdomains.SystemDelegation, Key: []byte("drax-0-fixture"), PrevExists: true, Prev: prev}
	}
	return rows
}

func TestStateChangeChunksProductionReadersAndUnwind(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(2<<20, 4)
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateDomainChangeBlockRows(db, rows); err != nil {
		t.Fatal(err)
	}
	stored, err := db.Get(stateChangeSetKey(42, 0))
	if err != nil {
		t.Fatal(err)
	}
	if stored[len(stateDomainChangeBlockEnvelopeMagic)] != stateDomainChangeBlockChunksVersion {
		t.Fatal("production writer did not use chunk pack")
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	if len(stored)*100 > len(raw)*35 {
		t.Fatalf("insufficient fixture savings: stored %d raw %d", len(stored), len(raw))
	}
	decoded, err := decodeStateDomainChangeBlockStorage(stored)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("raw RLP changed: %v", err)
	}
	owned, err := decodePersistedStateDomainChangeBlock(stored, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rows, owned) {
		t.Fatal("owning decode changed rows")
	}
	i := 0
	_, err = iteratePersistedStateDomainChangeBlockBorrowed(stored, 42, func(row *StateDomainChange) (bool, error) {
		if !reflect.DeepEqual(row, rows[i]) {
			t.Fatalf("borrowed row %d mismatch", i)
		}
		i++
		return true, nil
	})
	if err != nil || i != len(rows) {
		t.Fatalf("borrowed: %d %v", i, err)
	}
	for _, row := range rows {
		got, ok, err := ReadStateDomainChange(db, 42, row.Seq)
		if err != nil || !ok || !bytes.Equal(got.Prev, row.Prev) {
			t.Fatalf("point seq %d: %v %v", row.Seq, ok, err)
		}
	}
	first := rows[0]
	if err := WriteStateKVLatest(db, first.Owner, first.Generation, first.Domain, first.Key, []byte("tip")); err != nil {
		t.Fatal(err)
	}
	if _, err := CollectStateUnwind(db, 42, 41); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadStateKVLatest(db, first.Owner, first.Generation, first.Domain, first.Key)
	if err != nil || !ok || !bytes.Equal(got, first.Prev) {
		t.Fatalf("unwind: %v %v", ok, err)
	}
	t.Logf("synthetic 4-version block: raw=%d stored=%d saved=%.2f%%", len(raw), len(stored), 100*(1-float64(len(stored))/float64(len(raw))))
}

func TestStateChangeChunksGatePreservesOrdinaryPath(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(512<<10, 2)
	for _, variant := range []string{"single", "flat", "generation", "owner", "domain", "key", "absent", "small_prev"} {
		a, b := *rows[0], *rows[1]
		changes := []*StateDomainChange{&a, &b}
		switch variant {
		case "single":
			changes = changes[:1]
		case "flat":
			b.FlatDomain = StateFlatDomainKVGeneration
		case "generation":
			b.Generation++
		case "owner":
			b.Owner[1]++
		case "domain":
			b.Domain = kvdomains.SystemReward
		case "key":
			b.Key = []byte("other")
		case "absent":
			b.PrevExists = false
			b.Prev = nil
		case "small_prev":
			b.Prev = b.Prev[:historychunk.MaxSize-1]
		}
		if stateChangeBlockHasLargeVersions(changes) {
			t.Fatalf("gate crossed %s", variant)
		}
		raw := encodeBorrowedStateDomainChangeTestBlock(t, changes)
		baseline, _ := encodeStateDomainChangeBlockStorage(raw)
		selected, _ := encodeStateDomainChangeBlockStorageForChanges(raw, changes)
		if !bytes.Equal(selected, baseline) {
			t.Fatalf("ordinary baseline changed for %s", variant)
		}
	}
	if !stateChangeBlockHasLargeVersions(rows) {
		t.Fatal("repeated large values rejected")
	}
}

func TestStateChangeChunksProductionUsesSmallRepeatedPack(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(512<<10, 3)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	if len(raw) >= 2<<20 || len(raw) < 2*historychunk.MaxSize {
		t.Fatalf("fixture does not exercise the formerly excluded range: %d", len(raw))
	}
	baseline, _ := encodeStateDomainChangeBlockStorage(raw)
	want, useful := encodeStateChangeChunks(raw)
	if !useful || len(want)*8 >= len(baseline)*7 {
		t.Fatal("fixture must improve on baseline by more than 12.5%")
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := WriteStateDomainChangeBlockRows(db, rows); err != nil {
		t.Fatal(err)
	}
	stored, err := db.Get(stateChangeSetKey(42, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, want) {
		t.Fatalf("production skipped small repeated pack: raw=%d baseline=%d want CDC=%d got=%d", len(raw), len(baseline), len(want), len(stored))
	}
	decoded, err := decodeStateDomainChangeBlockStorage(stored)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("existing storage decoder: %v", err)
	}
	owned, err := decodePersistedStateDomainChangeBlock(stored, 42)
	if err != nil || !reflect.DeepEqual(owned, rows) {
		t.Fatalf("existing owning reader: %v", err)
	}
	for _, row := range rows {
		got, exists, err := ReadStateDomainChange(db, 42, row.Seq)
		if err != nil || !exists || !reflect.DeepEqual(got, row) {
			t.Fatalf("existing point reader seq=%d: %v", row.Seq, err)
		}
	}
	o := DefaultHistoryPrevInspectOptions()
	o.FromBlock = 42
	o.ToBlock = 42
	report, err := InspectStateHistoryPrev(context.Background(), db, o)
	if err != nil || !report.Complete || report.Samples[0].Codec != "chunks2" || report.Samples[0].ChunkGate != "eligible_size_and_repeated_large_identity" || report.Samples[0].LargeIdentityMaxVersions != 3 || !report.Samples[0].LargeIdentityTrackingComplete {
		t.Fatalf("inspector policy diverged: %+v %v", report.Samples, err)
	}
	stateChangeBlockChunkEncoding.Store(false)
	disabled, _ := encodeStateDomainChangeBlockStorageForChanges(raw, rows)
	if !bytes.Equal(disabled, baseline) {
		t.Fatal("disabled flag changed baseline path")
	}
	t.Logf("synthetic small repeated pack: raw=%d baseline=%d production_cdc=%d", len(raw), len(baseline), len(stored))
}

func TestStateChangeChunksSmallRepeatedKeyWithoutSavingsKeepsBaseline(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	rows := chunkHistoryRows(512<<10, 2)
	rng := rand.New(rand.NewSource(20260913))
	if _, err := rng.Read(rows[1].Prev); err != nil {
		t.Fatal(err)
	}
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	if len(raw) >= 2<<20 || !stateChangeBlockHasLargeVersions(rows) {
		t.Fatal("fixture must exercise a newly admitted, repeated full key")
	}
	baseline, _ := encodeStateDomainChangeBlockStorage(raw)
	if candidate, useful := encodeStateChangeChunks(raw); useful && len(candidate)*8 < len(baseline)*7 {
		t.Fatal("independent random values unexpectedly provide a CDC benefit")
	}
	selected, _ := encodeStateDomainChangeBlockStorageForChanges(raw, rows)
	if !bytes.Equal(selected, baseline) {
		t.Fatal("new gate bypassed the existing compression saving requirement")
	}
}

func TestStateChangeChunksSmallMetricsDescribeEncodingAttempts(t *testing.T) {
	prior := stateChangeBlockChunkEncoding.Swap(true)
	t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(prior) })
	snapshot := func() [4]int64 {
		return [4]int64{
			stateChangeSmallChunkAttemptsCounter.Snapshot().Count(), stateChangeSmallChunkSelectedCounter.Snapshot().Count(),
			stateChangeSmallChunkSavedBytesCounter.Snapshot().Count(), stateChangeSmallChunkWorkNanosCounter.Snapshot().Count(),
		}
	}
	for _, name := range []string{"gain", "no_gain", "different_identity", "flag_off", "formerly_eligible"} {
		t.Run(name, func(t *testing.T) {
			rows := chunkHistoryRows(512<<10, 3)
			switch name {
			case "no_gain":
				rng := rand.New(rand.NewSource(20260913))
				for _, row := range rows {
					if _, err := rng.Read(row.Prev); err != nil {
						t.Fatal(err)
					}
				}
			case "different_identity":
				for i, row := range rows {
					row.Generation += uint64(i)
				}
			case "flag_off":
				stateChangeBlockChunkEncoding.Store(false)
				t.Cleanup(func() { stateChangeBlockChunkEncoding.Store(true) })
			case "formerly_eligible":
				rows = chunkHistoryRows(1<<20, 3)
			}
			raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
			baseline, _ := encodeStateDomainChangeBlockStorage(raw)
			before := snapshot()
			selected, _ := encodeStateDomainChangeBlockStorageForChanges(raw, rows)
			after := snapshot()
			var delta [4]int64
			for i := range delta {
				delta[i] = after[i] - before[i]
			}
			if name == "gain" {
				if delta[0] != 1 || delta[1] != 1 || delta[2] != int64(len(baseline)-len(selected)) || delta[2] <= 0 || delta[3] <= 0 {
					t.Fatalf("gain metrics: %v", delta)
				}
			} else if name == "no_gain" {
				if delta[0] != 1 || delta[1] != 0 || delta[2] != 0 || delta[3] <= 0 || !bytes.Equal(selected, baseline) {
					t.Fatalf("no-gain metrics: %v", delta)
				}
			} else if delta != [4]int64{} {
				t.Fatalf("unrelated encoding paid small-chunk observation: %v", delta)
			}
		})
	}
	// Encoding is observed even when Put fails. The existing block-pack write
	// counter, in contrast, must not advance for this failed physical write.
	rows := chunkHistoryRows(512<<10, 3)
	before := snapshot()
	blocksBefore := stateChangeBlockPackBlocksCounter.Snapshot().Count()
	want := errors.New("injected changeset Put failure")
	if err := WriteStateDomainChangeBlockRows(smallChunkFailingWriter{err: want}, rows); !errors.Is(err, want) {
		t.Fatalf("Put failure: %v", err)
	}
	after := snapshot()
	if after[0]-before[0] != 1 || after[1]-before[1] != 1 || after[2] <= before[2] || after[3] <= before[3] || stateChangeBlockPackBlocksCounter.Snapshot().Count() != blocksBefore {
		t.Fatal("encoding counters were confused with successful writes")
	}
}

type smallChunkFailingWriter struct{ err error }

func (w smallChunkFailingWriter) Put([]byte, []byte) error { return w.err }
func (w smallChunkFailingWriter) Delete([]byte) error      { return errors.New("unexpected delete") }

func TestStateChangeChunksRejectMalformedAndCorruption(t *testing.T) {
	full := bytes.Repeat([]byte{9}, historychunk.MinSize)
	header := func(size uint64) []byte {
		b := binary.AppendUvarint(nil, size)
		return binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(full))
	}
	anchor := append(binary.AppendUvarint([]byte{0}, uint64(len(full))), full...)
	ref := binary.AppendUvarint(binary.AppendUvarint([]byte{2}, uint64(len(full))), 0)
	bad := [][]byte{
		{}, {0xff}, header(stateDomainChangeBlockMaxDecodedBytes + 1),
		append(header(uint64(len(full))), ref...), // forward reference
		append(header(1), 0, 0),                   // zero size
		append(header(1), 3, 1),                   // unknown kind
		append(header(1), 1, 1, 1, 0xff),          // corrupt snappy
		append(header(uint64(len(full))), anchor[:len(anchor)-1]...),
		append(append(header(uint64(len(full))), anchor...), 0), // trailing
	}
	// Third chunk tries to reference the second (which itself references 0).
	chain := append(header(uint64(3*len(full))), anchor...)
	chain = append(chain, ref...)
	chain = append(chain, binary.AppendUvarint(binary.AppendUvarint([]byte{2}, uint64(len(full))), 1)...)
	bad = append(bad, chain)
	for i, payload := range bad {
		if _, err := decodeStateChangeChunks(nil, payload); err == nil {
			t.Fatalf("accepted malformed %d", i)
		}
	}
	rows := chunkHistoryRows(1<<20, 3)
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	encoded, ok := encodeStateChangeChunks(raw)
	if !ok {
		t.Fatal("fixture did not compress")
	}
	for _, offset := range []int{len(stateDomainChangeBlockEnvelopeMagic) + 5, len(encoded) / 2, len(encoded) - 1} {
		mutated := bytes.Clone(encoded)
		mutated[offset] ^= 0x80
		if _, err := decodeStateDomainChangeBlockStorage(mutated); err == nil {
			t.Fatalf("accepted corruption at %d", offset)
		}
	}
}

func BenchmarkStateChangeChunkBlock(b *testing.B) {
	rows := chunkHistoryRows(2<<20, 4)
	raw := encodeBorrowedStateDomainChangeTestBlock(b, rows)
	chunked, ok := encodeStateChangeChunks(raw)
	if !ok {
		b.Fatal("fixture not compressed")
	}
	baseline, _ := encodeStateDomainChangeBlockStorage(raw)
	for _, name := range []string{"snappy-encode", "chunks-encode", "snappy-decode", "chunks-decode"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			stored := baseline
			if name == "chunks-encode" || name == "chunks-decode" {
				stored = chunked
			}
			b.ReportMetric(float64(len(stored)), "stored-B")
			for i := 0; i < b.N; i++ {
				switch name {
				case "snappy-encode":
					encodeStateDomainChangeBlockStorage(raw)
				case "chunks-encode":
					encodeStateChangeChunks(raw)
				default:
					if _, err := decodeStateDomainChangeBlockStorage(stored); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
