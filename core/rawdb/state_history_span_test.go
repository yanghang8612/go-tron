package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
)

func spanTestDB(t *testing.T, rows []*StateDomainChange, mode string) (ethdb.KeyValueStore, []byte) {
	t.Helper()
	db, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	spanPutPack(t, db, raw, mode)
	if err := WriteStateTxRange(db, 1, common.Hash{1}, 10, 99); err != nil {
		t.Fatal(err)
	}
	return db, raw
}
func spanPutPack(t *testing.T, db ethdb.KeyValueStore, raw []byte, mode string) {
	t.Helper()
	var encoded []byte
	switch mode {
	case "raw":
		encoded = raw
	case "snappy":
		encoded, _ = encodeStateDomainChangeBlockStorage(raw)
	case "chunks":
		encoded, _ = encodeStateChangeChunksWithWorkers(raw, 1)
	default:
		if mode == "nested-snappy" {
			raw, _ = encodeStateDomainChangeBlockStorage(raw)
		}
		var chunks map[string][]byte
		encoded, chunks = ownedReadBenchmarkPack(raw, 1, mode != "shared-raw")
		for k, v := range chunks {
			if err := db.Put([]byte(k), v); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Put(stateChangeSetKey(1, 0), encoded); err != nil {
		t.Fatal(err)
	}
}
func spanView(t *testing.T, db any) StateHistoryReadView {
	t.Helper()
	view, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = release() })
	return view
}
func spanRows(t *testing.T, b *StateHistorySpanBlock, mutate bool) []*StateDomainChange {
	t.Helper()
	var rows []*StateDomainChange
	cont, err := b.IterateRows(func(row *StateHistorySpanRow) (bool, error) {
		if row.Change.Prev != nil || row.Change.Next != nil || row.Change.NextExists {
			t.Fatal("materialized transient/past value in metadata")
		}
		c := cloneStateDomainChange(&row.Change)
		for _, span := range row.PrevSpans {
			meta, err := b.Chunk(int(span.ChunkIndex))
			if err != nil {
				t.Fatal(err)
			}
			chunk := make([]byte, meta.Length)
			n, err := b.CopyChunk(int(span.ChunkIndex), chunk)
			if err != nil || n != len(chunk) || sha256.Sum256(chunk) != meta.Digest {
				t.Fatal("unauthenticated chunk", err)
			}
			c.Prev = append(c.Prev, chunk[span.Offset:span.Offset+span.Length]...)
		}
		if uint64(len(c.Prev)) != row.PrevLength {
			t.Fatal("Prev length differs")
		}
		rows = append(rows, c)
		if mutate {
			for i := range row.Change.Key {
				row.Change.Key[i] ^= 255
			}
			for i := range row.PrevSpans {
				row.PrevSpans[i] = StateHistorySpan{^uint32(0), 0, 0}
			}
			row.Change.Seq = 0
		}
		return true, nil
	})
	if err != nil || !cont {
		t.Fatal("span rows", err)
	}
	return rows
}
func spanOracle(t *testing.T, view StateHistoryReadView, from, to uint64) []*StateDomainChange {
	t.Helper()
	var result []*StateDomainChange
	// This unchanged owning reader uses reflection RLP decoding and the existing
	// positive-sequence overwrite implementation, independently of span offsets.
	err := IterateStateDomainChangesByBlockTxRange(view, 1, 1, from, to, func(row *StateDomainChange) (bool, error) {
		c := cloneStateDomainChange(row)
		result = append(result, c)
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.SliceStable(result, func(i, j int) bool { return frozenSpanColdOrder(result[i], result[j]) < 0 })
	for _, c := range result {
		c.Next, c.NextExists = nil, false
	}
	return result
}

func TestStateHistorySpanRLPBoundariesAndOwnershipOracle(t *testing.T) {
	var rows []*StateDomainChange
	for i, size := range []int{0, 1, 55, 56, 255, 256, (128 << 10) - 1, 128 << 10, (128 << 10) + 1, 300 << 10} {
		row := borrowedStateDomainChangeTestRow(1, uint64(i+1), 10+uint64(i/2))
		row.Prev = bytes.Repeat([]byte{byte(11 + i)}, size)
		row.Key = append([]byte("same-prefix:"), bytes.Repeat([]byte{byte(11 + i)}, min(size, 100))...)
		row.PrevExists = i != 0
		rows = append(rows, row)
	}
	empty := borrowedStateDomainChangeTestRow(1, uint64(len(rows)+1), 19)
	empty.Prev = nil
	rows = append(rows, empty)
	for _, mode := range []string{"shared-raw", "shared", "raw", "snappy", "chunks", "nested-snappy"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := spanTestDB(t, rows, mode)
			view := spanView(t, db)
			want := spanOracle(t, view, 10, 19)
			observed := &pipelineTestView{StateHistoryReadView: view}
			var held *StateHistorySpanBlock
			var copied []byte
			err := IterateStateHistorySpanBlocks(context.Background(), observed, 1, 1, 10, 19, func(b *StateHistorySpanBlock) (bool, error) {
				held = b
				if b.Info().Rows != uint64(len(want)) {
					t.Fatal("row count", b.Info())
				}
				if b.Info().Fallback != (mode != "shared" && mode != "shared-raw") {
					t.Fatal("fallback identity", b.Info())
				}
				got := spanRows(t, b, true)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("oracle differs mode=%s", mode)
				}
				before := fmt.Sprint(observed.trace)
				if !reflect.DeepEqual(spanRows(t, b, false), want) || fmt.Sprint(observed.trace) != before {
					t.Fatal("repeat metadata iteration reread source or mutable alias")
				}
				meta, _ := b.Chunk(0)
				copied = make([]byte, meta.Length)
				_, _ = b.CopyChunk(0, copied)
				original := bytes.Clone(copied)
				copied[0] ^= 255
				_, _ = b.CopyChunk(0, copied)
				if !bytes.Equal(copied, original) {
					t.Fatal("CopyChunk exposed private bytes")
				}
				if n, err := b.CopyChunk(0, copied[:len(copied)-1]); n != 0 || !errors.Is(err, io.ErrShortBuffer) {
					t.Fatal("short copy", n, err)
				}
				if _, err := b.Chunk(-1); err == nil {
					t.Fatal("negative index")
				}
				return true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := held.CopyChunk(0, copied); !errors.Is(err, ErrStateHistorySpanExpired) {
				t.Fatal("expired block readable", err)
			}
			if held.ChunkCount() != 0 || held.Info().Rows != 0 || held.raw != nil || held.pooled != nil || held.ownRows != nil {
				t.Fatal("block retained payload after callback")
			}
			if ok, err := view.Has(stateChangeSetKey(1, 0)); err != nil || !ok {
				t.Fatal("caller snapshot closed")
			}
			if mode == "shared" || mode == "shared-raw" {
				for _, trace := range observed.trace {
					if !reflect.DeepEqual(trace, []string{"Has", "Get"}) {
						t.Fatal("source chunk was not read exactly once", trace)
					}
				}
			}
		})
	}
}

func TestStateHistorySpanRepairAndLegacyOracle(t *testing.T) {
	for _, mode := range []string{"shared", "raw", "snappy", "standalone", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			rows := []*StateDomainChange{borrowedStateDomainChangeTestRow(1, 3, 10), borrowedStateDomainChangeTestRow(1, 4, 11), borrowedStateDomainChangeTestRow(1, 5, 12)}
			rows[0].Prev = bytes.Repeat([]byte{7}, 180<<10)
			db, _ := spanTestDB(t, rows, mode)
			if mode == "standalone" || mode == "legacy" {
				row := borrowedStateDomainChangeTestRow(1, 0, 10)
				row.BlockHash = common.Hash{1}
				row.NextExists = true
				row.Next = []byte("transient")
				var encoded []byte
				if mode == "legacy" {
					encoded, _ = rlp.EncodeToBytes(row)
				} else {
					encoded, _ = encodePersistedStateDomainChange(row)
				}
				if err := db.Put(stateChangeSetKey(1, 0), encoded); err != nil {
					t.Fatal(err)
				}
			}
			for _, seq := range []uint64{2, 4, 6, 9} {
				row := borrowedStateDomainChangeTestRow(1, seq, 10+seq/2)
				if seq == 2 {
					row.TxNum = 10
				}
				if seq == 4 {
					row.TxNum = 11
				}
				if seq == 6 {
					row.TxNum = 13
				}
				if seq == 9 {
					// A late physical repair of an earlier transaction must
					// receive the old cold ETL order, not owning Seq order.
					row.TxNum = 10
				}
				row.Prev = bytes.Repeat([]byte{byte(seq)}, 140<<10)
				encoded, _ := encodePersistedStateDomainChange(row)
				if err := db.Put(stateChangeSetKey(1, seq), encoded); err != nil {
					t.Fatal(err)
				}
			}
			view := spanView(t, db)
			want := spanOracle(t, view, 10, 99)
			err := IterateStateHistorySpanBlocks(context.Background(), view, 1, 1, 10, 99, func(b *StateHistorySpanBlock) (bool, error) {
				if !b.Info().Fallback {
					t.Fatal("repair not flagged")
				}
				got := spanRows(t, b, true)
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(spanRows(t, b, false), want) {
					t.Fatalf("repair order/overwrite differs\ngot=%+v\nwant=%+v", got, want)
				}
				return true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStateHistorySpanCorruptionBeforeAnyBlockCallback(t *testing.T) {
	for _, mode := range []string{"missing", "bad-chunk", "chunk-hash", "bad-digest", "bad-header", "bad-rlp", "nested"} {
		t.Run(mode, func(t *testing.T) {
			db, _ := pipelineFixture(t, mode)
			view := spanView(t, db)
			called := 0
			err := IterateStateHistorySpanBlocks(context.Background(), view, 1, 1, 0, 99, func(*StateHistorySpanBlock) (bool, error) { called++; return true, nil })
			if err == nil || called != 0 {
				t.Fatal("corrupt block exposed", err, called)
			}
		})
	}
	t.Run("authenticated malformed final row", func(t *testing.T) {
		row := borrowedStateDomainChangeTestRow(1, 1, 10)
		db, raw := spanTestDB(t, []*StateDomainChange{row}, "shared")
		var block struct {
			Version  uint8
			FirstSeq uint64
			Rows     []rlp.RawValue
		}
		if err := rlp.DecodeBytes(raw, &block); err != nil {
			t.Fatal(err)
		}
		block.Rows = append(block.Rows, rlp.RawValue{0xc0})
		raw, _ = rlp.EncodeToBytes(block)
		spanPutPack(t, db, raw, "shared")
		called := 0
		err := IterateStateHistorySpanBlocks(context.Background(), spanView(t, db), 1, 1, 10, 10, func(*StateHistorySpanBlock) (bool, error) { called++; return true, nil })
		if err == nil || called != 0 {
			t.Fatal("late malformed fields escaped prevalidation", err, called)
		}
	})
}

func TestStateHistorySpanBindingEmptyFilterAndCancellation(t *testing.T) {
	db, _ := pipelineFixture(t, "shared", "shared")
	_ = WriteStateTxRange(db, 1, common.Hash{1}, 10, 19)
	_ = WriteStateTxRange(db, 2, common.Hash{2}, 20, 29)
	view := spanView(t, db)
	failure := errors.New("callback failure")
	var held *StateHistorySpanBlock
	err := IterateStateHistorySpanBlocks(context.Background(), view, 1, 2, 11, 28, func(b *StateHistorySpanBlock) (bool, error) { held = b; return false, failure })
	if !errors.Is(err, failure) || held.active {
		t.Fatal("callback cleanup", err)
	}
	for _, rowCallback := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		err := IterateStateHistorySpanBlocks(ctx, view, 1, 1, 0, 99, func(b *StateHistorySpanBlock) (bool, error) {
			if rowCallback {
				_, err := b.IterateRows(func(*StateHistorySpanRow) (bool, error) { cancel(); return true, nil })
				if !errors.Is(err, context.Canceled) {
					t.Fatal("last row cancellation lost", err)
				}
			} else {
				cancel()
			}
			return true, nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatal("last block cancellation lost", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	observed := &pipelineTestView{StateHistoryReadView: view, hook: func(op string, _ []byte) error {
		if op == "Has" {
			cancel()
		}
		return nil
	}}
	err = IterateStateHistorySpanBlocks(ctx, observed, 1, 2, 0, 99, func(*StateHistorySpanBlock) (bool, error) { t.Fatal("canceled block delivered"); return true, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, trace := range observed.trace {
		if len(trace) != 1 || trace[0] != "Has" {
			t.Fatal("Get after cancellation", trace)
		}
	}
	if err := IterateStateHistorySpanBlocks(context.Background(), &stateHistoryReadView{reader: view, iteratee: view}, 1, 1, 0, 99, func(*StateHistorySpanBlock) (bool, error) { return true, nil }); !errors.Is(err, ErrStateHistoryReadViewUnpinned) {
		t.Fatal("unpinned", err)
	}
	// Live mutations after pinning cannot replace the range, pack or chunks seen.
	_ = db.Delete(stateChangeSetKey(1, 0))
	_ = WriteStateTxRange(db, 1, common.Hash{9}, 90, 99)
	blocks := 0
	err = IterateStateHistorySpanBlocks(context.Background(), view, 1, 2, 11, 28, func(b *StateHistorySpanBlock) (bool, error) {
		blocks++
		if b.Info().BlockNum == 1 && b.Info().Rows != 0 {
			t.Fatal("filter ignored")
		}
		if b.Info().BlockNum == 2 && b.Info().Rows != 1 {
			t.Fatal("matching row lost")
		}
		return true, nil
	})
	if err != nil || blocks != 2 {
		t.Fatal("snapshot/filters", err, blocks)
	}
	// A fresh view now has an empty first block, but the discontinuous tx ranges
	// cannot attest complete coverage and must fail rather than manufacture it.
	err = IterateStateHistorySpanBlocks(context.Background(), spanView(t, db), 1, 2, 0, 99, func(*StateHistorySpanBlock) (bool, error) { return true, nil })
	if err == nil {
		t.Fatal("discontinuous ranges accepted")
	}
}

func TestStateHistorySpanMalformedBindingAndBudget(t *testing.T) {
	for _, which := range []string{"row tx", "physical range", "empty owner", "seq overflow", "negative field", "out-of-order"} {
		t.Run(which, func(t *testing.T) {
			rows := []*StateDomainChange{borrowedStateDomainChangeTestRow(1, 1, 10), borrowedStateDomainChangeTestRow(1, 2, 11)}
			if which == "row tx" {
				rows[1].TxNum = 100
			}
			if which == "out-of-order" {
				rows[1].TxNum = 9
			}
			db, raw := spanTestDB(t, rows, "shared")
			if which == "physical range" {
				encoded, _ := rlp.EncodeToBytes(StateTxRange{BlockNum: 2, BlockHash: common.Hash{1}, BeginTxNum: 10, EndTxNum: 99})
				_ = db.Put(stateTxRangeKey(1), encoded)
			}
			if which == "seq overflow" || which == "empty owner" || which == "negative field" {
				var block struct {
					Version  uint8
					FirstSeq uint64
					Rows     []rlp.RawValue
				}
				_ = rlp.DecodeBytes(raw, &block)
				if which == "seq overflow" {
					block.FirstSeq = ^uint64(0)
				} else {
					var fields []rlp.RawValue
					_ = rlp.DecodeBytes(block.Rows[1], &fields)
					if which == "empty owner" {
						fields[2] = []byte{0x80}
					} else {
						fields[1] = []byte{0x82, 1, 0}
					}
					block.Rows[1], _ = rlp.EncodeToBytes(fields)
				}
				raw, _ = rlp.EncodeToBytes(block)
				spanPutPack(t, db, raw, "shared")
			}
			calls := 0
			err := IterateStateHistorySpanBlocks(context.Background(), spanView(t, db), 1, 1, 10, 10, func(*StateHistorySpanBlock) (bool, error) { calls++; return true, nil })
			if err == nil || calls != 0 {
				t.Fatal("invalid bound/field escaped", err, calls)
			}
		})
	}
	b := &StateHistorySpanBlock{ctx: context.Background(), active: true, repairBytes: stateDomainChangeBlockMaxDecodedBytes}
	if err := b.add(nil, []byte{1}, 1); !errors.Is(err, ErrStateHistorySpanBudget) {
		t.Fatal("sum budget", err)
	}
	b.repairBytes = 0
	b.repairCount = stateHistorySpanRepairRows
	if err := b.add(nil, []byte{1}, 1); !errors.Is(err, ErrStateHistorySpanBudget) {
		t.Fatal("count budget", err)
	}
	db := NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })
	_ = WriteStateTxRange(db, ^uint64(0), common.Hash{1}, 7, 7)
	pinned := &stateHistoryReadView{reader: db, iteratee: db, pinned: true}
	calls := 0
	if err := IterateStateHistorySpanBlocks(context.Background(), pinned, ^uint64(0), ^uint64(0), 7, 7, func(b *StateHistorySpanBlock) (bool, error) {
		calls++
		if b.Info().Rows != 0 || b.ChunkCount() != 0 {
			t.Fatal("empty block fabricated rows")
		}
		return true, nil
	}); err != nil || calls != 1 {
		t.Fatal("terminal block", err, calls)
	}
}

type spanPresenceView struct {
	StateHistoryReadView
	calls int
}

func (v *spanPresenceView) Has([]byte) (bool, error) {
	return false, errors.New("unexpected separate Has")
}
func (v *spanPresenceView) Get([]byte) ([]byte, error) {
	return nil, errors.New("unexpected separate Get")
}
func (v *spanPresenceView) GetWithPresence(k []byte) ([]byte, bool, error) {
	v.calls++
	return readPresentValue(v.StateHistoryReadView, k, "presence fixture")
}

func TestStateHistorySpanPresenceCacheAndSourceErrors(t *testing.T) {
	db, keys := pipelineFixture(t, "shared")
	base := spanView(t, db)
	coupled := &spanPresenceView{StateHistoryReadView: base}
	if err := IterateStateHistorySpanBlocks(context.Background(), coupled, 1, 1, 10, 10, func(b *StateHistorySpanBlock) (bool, error) {
		if len(spanRows(t, b, false)) != 1 {
			t.Fatal("coupled source rows lost")
		}
		return true, nil
	}); err != nil || coupled.calls != 1 {
		t.Fatal("presence route changed", err, coupled.calls)
	}
	for _, op := range []string{"Has", "Get"} {
		failure := errors.New("original " + op + " failure")
		makeView := func() *pipelineTestView {
			return &pipelineTestView{StateHistoryReadView: base, hook: func(got string, _ []byte) error {
				if op == got {
					return failure
				}
				return nil
			}}
		}
		old, newer := makeView(), makeView()
		pack, _ := base.Get(stateChangeSetKey(1, 0))
		_, want := materializeStateHistorySharedPack(pack, 1, []ethdb.KeyValueReader{old})
		got := IterateStateHistorySpanBlocks(context.Background(), newer, 1, 1, 10, 10, func(*StateHistorySpanBlock) (bool, error) { t.Fatal("failing read delivered block"); return true, nil })
		if !errors.Is(got, failure) || fmt.Sprint(got) != fmt.Sprint(want) || !reflect.DeepEqual(old.trace, newer.trace) {
			t.Fatal("source failure/order changed", got, want, old.trace, newer.trace)
		}
	}
	cached, cache, observed := chunkCacheTestView(t, db, StateHistoryChunkCachePayloadBudget, StateHistoryChunkCacheEntryLimit)
	for run := 0; run < 2; run++ {
		if err := IterateStateHistorySpanBlocks(context.Background(), cached, 1, 1, 10, 10, func(b *StateHistorySpanBlock) (bool, error) {
			spanRows(t, b, true)
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if cache.Stats().Hits != 1 || len(observed.trace[string(keys[0])]) != 2 {
		t.Fatal("cache capability lost", cache.Stats(), observed.trace)
	}
	if cache.Stats().Closed {
		t.Fatal("borrower closed caller cache")
	}
}

// Frozen from 61708c9de685efa2be727167f8d2b3e3970a0099
// core/state/snapshots/history_binary.go: only function/type qualifiers renamed.
func frozenSpanColdOrder(a, b *StateDomainChange) int {
	if a.TxNum != b.TxNum {
		return frozenSpanUint64(a.TxNum, b.TxNum)
	}
	if a.Seq != b.Seq {
		return frozenSpanUint64(a.Seq, b.Seq)
	}
	if a.BlockNum != b.BlockNum {
		return frozenSpanUint64(a.BlockNum, b.BlockNum)
	}
	if cmp := bytes.Compare(a.BlockHash[:], b.BlockHash[:]); cmp != 0 {
		return cmp
	}
	if a.FlatDomain != b.FlatDomain {
		return frozenSpanUint8(uint8(a.FlatDomain), uint8(b.FlatDomain))
	}
	if cmp := bytes.Compare(a.Owner[:], b.Owner[:]); cmp != 0 {
		return cmp
	}
	if a.Generation != b.Generation {
		return frozenSpanUint64(a.Generation, b.Generation)
	}
	if a.Domain != b.Domain {
		return frozenSpanUint16(uint16(a.Domain), uint16(b.Domain))
	}
	if cmp := bytes.Compare(a.Key, b.Key); cmp != 0 {
		return cmp
	}
	if a.PrevExists != b.PrevExists {
		return frozenSpanBool(a.PrevExists, b.PrevExists)
	}
	if cmp := bytes.Compare(a.Prev, b.Prev); cmp != 0 {
		return cmp
	}
	if a.NextExists != b.NextExists {
		return frozenSpanBool(a.NextExists, b.NextExists)
	}
	return bytes.Compare(a.Next, b.Next)
}

func frozenSpanUint64(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func frozenSpanUint16(a, b uint16) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func frozenSpanUint8(a, b uint8) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func frozenSpanBool(a, b bool) int {
	switch {
	case !a && b:
		return -1
	case a && !b:
		return 1
	default:
		return 0
	}
}

func TestStateHistorySpanColdOrderOracle(t *testing.T) {
	mutations := []struct {
		name   string
		change func(*StateDomainChange)
	}{
		{"tx", func(c *StateDomainChange) { c.TxNum++ }},
		{"seq", func(c *StateDomainChange) { c.Seq++ }},
		{"block", func(c *StateDomainChange) { c.BlockNum++ }},
		{"hash", func(c *StateDomainChange) { c.BlockHash[0]++ }},
		{"flat", func(c *StateDomainChange) { c.FlatDomain++ }},
		{"owner", func(c *StateDomainChange) { c.Owner[0]++ }},
		{"generation", func(c *StateDomainChange) { c.Generation++ }},
		{"domain", func(c *StateDomainChange) { c.Domain++ }},
		{"key", func(c *StateDomainChange) { c.Key = append(c.Key, 1) }},
		{"prev exists", func(c *StateDomainChange) { c.PrevExists = !c.PrevExists }},
		{"prev", func(c *StateDomainChange) { c.Prev = append(c.Prev, 1) }},
		{"next exists", func(c *StateDomainChange) { c.NextExists = !c.NextExists }},
		{"next", func(c *StateDomainChange) { c.Next = append(c.Next, 1) }},
	}
	base := borrowedStateDomainChangeTestRow(1, 1, 10)
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			b := cloneStateDomainChange(base)
			test.change(b)
			for _, pair := range [][2]*StateDomainChange{{base, b}, {b, base}, {b, b}} {
				got, want := compareStateHistorySpanRows(pair[0], pair[1]), frozenSpanColdOrder(pair[0], pair[1])
				if got != want {
					t.Fatalf("order got %d want %d", got, want)
				}
			}
		})
	}
}
