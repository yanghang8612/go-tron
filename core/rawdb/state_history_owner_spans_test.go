package rawdb

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func ownerSpansRows() []*StateDomainChange {
	rows := make([]*StateDomainChange, 180)
	rng := rand.New(rand.NewSource(522))
	for i := range rows {
		owner := common.Address{0x41, 0x22}
		if i%7 == 0 {
			owner[0] = 0 // Same AccountID, distinct full owner.
		} else if i%3 == 0 {
			owner[1] = 0x33
		}
		seed := make([]byte, 24)
		rng.Read(seed)
		prev := bytes.Repeat(seed, 8)
		rows[i] = &StateDomainChange{BlockNum: 9, Seq: 5 + uint64(i), TxNum: 100 + uint64(i/3), Owner: owner,
			FlatDomain: StateFlatDomainKVLatest, Generation: 3, Domain: kvdomains.ContractStorage,
			Key: []byte{byte(i % 11)}, PrevExists: i%2 == 0, Prev: prev}
	}
	return rows
}

func ownerSpansFixture(t *testing.T, codec string) (*historyViewTestFactory, []*StateDomainChange) {
	t.Helper()
	f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
	t.Cleanup(func() { f.Close() })
	rows := ownerSpansRows()
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	pack := raw
	if codec != "raw" {
		pack, _ = encodeStateDomainChangeBlockStorage(raw)
	}
	if err := f.Put(stateChangeSetKey(9, 0), pack); err != nil {
		t.Fatal(err)
	}
	if codec == "shared" {
		writeHistoryReadViewSharedFixture(t, f.KeyValueStore, 9)
	}
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	built, err := BuildStateHistoryOwnerSpans(context.Background(), v, f, 9)
	if codec == "raw" {
		index, e := buildHistoryOwnerSpans(raw, pack, 9, nil)
		if e != nil {
			t.Fatal(e)
		}
		err, built = f.Put(stateChangeOwnerSpansKey(9), index), len(index) != 0
	}
	release()
	if err != nil || !built {
		t.Fatal("fixture index not admitted", built, err)
	}
	return f, rows
}

func TestHistoryOwnerSpansCodecAndSelectedOracle(t *testing.T) {
	for _, codec := range []string{"raw", "snappy", "shared"} {
		t.Run(codec, func(t *testing.T) {
			f, input := ownerSpansFixture(t, codec)
			for _, repair := range []*StateDomainChange{
				{BlockNum: 9, Seq: input[1].Seq, TxNum: 1, Owner: input[0].Owner, FlatDomain: StateFlatDomainKVLatest, Domain: kvdomains.ContractStorage, Prev: []byte("moved owner, old TxNum")},
				{BlockNum: 9, Seq: input[0].Seq, TxNum: 2, Owner: input[1].Owner, FlatDomain: StateFlatDomainKVLatest, Domain: kvdomains.ContractStorage, Prev: []byte("reverse repair")},
				{BlockNum: 9, Seq: 300, TxNum: 3, Owner: input[1].Owner, FlatDomain: StateFlatDomainKVLatest, Domain: kvdomains.ContractStorage, Prev: []byte("after packed sequence")},
			} {
				if err := WriteStateDomainChangeRow(f, repair); err != nil {
					t.Fatal(err)
				}
			}
			v, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if err := CheckStateHistoryOwnerSpans(context.Background(), v, 9); err != nil {
				t.Fatal(err)
			}
			for _, owner := range []common.Address{input[0].Owner, input[1].Owner, input[3].Owner, {0x41, 0x99}} {
				var want, got []*StateDomainChange
				if err := IterateStateDomainChangesByBlockRange(v, 9, 9, func(row *StateDomainChange) (bool, error) {
					if row.Owner == owner {
						want = append(want, cloneStateDomainChange(row))
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := iterateStateHistoryOwnerTail(context.Background(), v, 9, 9, owner, func(row *StateDomainChange) (bool, error) {
					got = append(got, cloneStateDomainChange(row))
					return true, nil
				}); err != nil || !reflect.DeepEqual(got, want) {
					t.Fatal("indexed owner differs from full repaired oracle", owner, err)
				}
			}
			pack, _ := v.Get(stateChangeSetKey(9, 0))
			var scratch StateDomainChange
			calls := 0
			used, err := iterateHistoryOwnerSpans(context.Background(), v, pack, 9, func(o common.Address) bool { return o == input[0].Owner }, &scratch, func(row *StateDomainChange) (bool, error) { calls++; return true, nil })
			if err != nil || !used || calls == 0 || calls >= len(input) {
				t.Fatal("selected path did not skip unrelated rows", used, calls, err)
			}
		})
	}
}

func TestHistoryOwnerSpansStaleCorruptMissingAndPinned(t *testing.T) {
	for _, mode := range []string{"missing", "truncated", "checksum", "stale", "unmarked", "retired-sha"} {
		t.Run(mode, func(t *testing.T) {
			f, input := ownerSpansFixture(t, "snappy")
			index, _ := f.Get(stateChangeOwnerSpansKey(9))
			switch mode {
			case "missing":
				f.Delete(stateChangeOwnerSpansKey(9))
			case "truncated":
				f.Put(stateChangeOwnerSpansKey(9), index[:len(index)/2])
			case "checksum":
				index[20] ^= 1
				f.Put(stateChangeOwnerSpansKey(9), index)
			case "retired-sha":
				index[7] = '1'
				f.Put(stateChangeOwnerSpansKey(9), index)
			case "stale":
				input[0].Prev = []byte("replacement pack")
				if err := WriteStateDomainChangeBlockRows(f, input); err != nil {
					t.Fatal(err)
				}
			}
			v, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			var db StateKVHistoryReader = v
			if mode == "unmarked" {
				db = f.KeyValueStore
			}
			pack, _ := db.Get(stateChangeSetKey(9, 0))
			var scratch StateDomainChange
			calls := 0
			used, err := iterateHistoryOwnerSpans(context.Background(), db, pack, 9, func(o common.Address) bool { return o == input[0].Owner }, &scratch, func(*StateDomainChange) (bool, error) { calls++; return true, nil })
			if err != nil || used || calls != 0 {
				t.Fatal("optional miss published partial data", used, calls, err)
			}
		})
	}
}

func TestHistoryOwnerSpansCanonicalFailureAndCancellation(t *testing.T) {
	f, rows := ownerSpansFixture(t, "shared")
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	pack, _ := v.Get(stateChangeSetKey(9, 0))
	var scratch StateDomainChange
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	used, err := iterateHistoryOwnerSpans(ctx, v, pack, 9, func(o common.Address) bool { return o == rows[0].Owner }, &scratch, func(*StateDomainChange) (bool, error) { calls++; cancel(); return true, nil })
	if !used || !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatal(used, calls, err)
	}
	// Even an absent owner cannot bypass missing/corrupt shared source chunks.
	it := f.NewIterator(stateHistorySharedChunkPrefix, nil)
	if !it.Next() {
		t.Fatal("shared fixture chunk missing")
	}
	key := bytes.Clone(it.Key())
	it.Release()
	f.Delete(key)
	v2, release2, _ := AcquireStateHistoryReadView(f)
	defer release2()
	used, err = iterateHistoryOwnerSpans(context.Background(), v2, pack, 9, func(common.Address) bool { return false }, &scratch, func(*StateDomainChange) (bool, error) { t.Fatal("corrupt source callback"); return true, nil })
	if !used || err == nil {
		t.Fatal("index hid missing shared source", used, err)
	}
}

func TestHistoryOwnerSpansWriterAtomicMVCCAndRetirement(t *testing.T) {
	db := sharedHistoryTestDB(t)
	rows := ownerSpansRows()
	w := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(w, rows); err != nil {
		t.Fatal(err)
	}
	if present, _ := db.Has(stateChangeOwnerSpansKey(9)); present {
		t.Fatal("index escaped block scope")
	}
	if err := w.batch.Write(); err != nil {
		t.Fatal(err)
	}
	v, release, err := AcquireStateHistoryReadView(db)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if present, _ := v.Has(stateChangeOwnerSpansKey(9)); !present {
		t.Fatal("atomic writer did not publish index")
	}
	if err := CheckStateHistoryOwnerSpans(context.Background(), v, 9); err != nil {
		t.Fatal(err)
	}
	if err := DeleteStateDomainChangeBlocks(db, []uint64{9}); err != nil {
		t.Fatal(err)
	}
	if present, _ := db.Has(stateChangeOwnerSpansKey(9)); present {
		t.Fatal("index survived canonical history deletion")
	}
	if err := CheckStateHistoryOwnerSpans(context.Background(), v, 9); err != nil {
		t.Fatal("pinned old source/index invalidated by retirement", err)
	}
	failure := errors.New("owner index publication failed")
	w2 := &sharedHistoryFaultScope{sharedHistoryTestBatch: newSharedHistoryTestBatch(db), failKey: stateChangeOwnerSpansKey(9), failure: failure}
	if err := WriteStateDomainChangeBlockRows(w2, rows); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	w2.batch.Reset()
	if present, _ := db.Has(stateChangeSetKey(9, 0)); present {
		t.Fatal("failed atomic scope published canonical source")
	}
}

func TestHistoryOwnerSpansBoundsAndWriteEquivalent(t *testing.T) {
	rows := ownerSpansRows()
	raw := encodeBorrowedStateDomainChangeTestBlock(t, rows)
	fromRows, err := buildHistoryOwnerSpans(raw, raw, 9, rows)
	if err != nil {
		t.Fatal(err)
	}
	fromDecode, err := buildHistoryOwnerSpans(raw, raw, 9, nil)
	if err != nil || !bytes.Equal(fromRows, fromDecode) || len(fromRows) == 0 {
		t.Fatal("writer index differs from strict row rebuild", err)
	}
	for n := 0; n < len(fromRows); n++ {
		if _, err := decodeHistoryOwnerSpans(fromRows[:n], 9); err == nil {
			t.Fatalf("truncation at %d accepted", n)
		}
	}
	if _, err := decodeHistoryOwnerSpans(fromRows, 10); err == nil {
		t.Fatal("wrong physical block accepted")
	}
	if out, err := buildHistoryOwnerSpans(raw[:100], raw[:100], 9, rows); err != nil || out != nil {
		t.Fatal("small pack should not allocate an index", err)
	}
	rng := rand.New(rand.NewSource(562))
	for n := 0; n < 300; n++ {
		data := make([]byte, rng.Intn(200))
		rng.Read(data)
		if _, err := decodeHistoryOwnerSpans(data, 9); err == nil {
			t.Fatal("random index accepted")
		}
	}
}

func TestHistoryOwnerSpansDenseFallbackAndPruneSelection(t *testing.T) {
	f, rows := ownerSpansFixture(t, "snappy")
	v, release, err := AcquireStateHistoryReadView(f)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	pack, _ := v.Get(stateChangeSetKey(9, 0))
	var scratch StateDomainChange
	calls := 0
	used, err := iterateHistoryOwnerSpans(context.Background(), v, pack, 9, func(o common.Address) bool { return o == rows[1].Owner }, &scratch, func(*StateDomainChange) (bool, error) { calls++; return true, nil })
	if used || err != nil || calls != 0 {
		t.Fatal("dense selection must fall back without callback", used, calls, err)
	}
	for _, mode := range []string{"single", "bulk", "range"} {
		t.Run(mode, func(t *testing.T) {
			db, base := newRangePruneTestStore(t)
			seedRangePruneRows(t, base, []uint64{9, 10, 11, 12, 13}, 4)
			for _, block := range []uint64{9, 11, 13} {
				if err := base.Put(stateChangeOwnerSpansKey(block), []byte("optional")); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "single" {
				for _, block := range []uint64{9, 11} {
					if err := DeleteStateDomainChanges(db, block); err != nil {
						t.Fatal(err)
					}
				}
			} else if mode == "bulk" {
				if err := DeleteStateDomainChangeBlocks(db, []uint64{9, 11}); err != nil {
					t.Fatal(err)
				}
			} else if _, err := DeleteStateDomainChangeBlocksWithOptions(db, []uint64{9, 10, 11}, StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2}); err != nil {
				t.Fatal(err)
			}
			if err := db.batch.Write(); err != nil {
				t.Fatal(err)
			}
			for _, block := range []uint64{9, 11, 13} {
				present, err := base.Has(stateChangeOwnerSpansKey(block))
				if err != nil || present != (block == 13) {
					t.Fatal("auxiliary retirement crossed selection", block, present, err)
				}
			}
		})
	}
}

func TestHistoryOwnerSpansPostingRetirementBoundedAndAtomic(t *testing.T) {
	for _, coarse := range []bool{false, true} {
		t.Run(map[bool]string{false: "points", true: "coarse"}[coarse], func(t *testing.T) {
			f := &historyViewTestFactory{KeyValueStore: NewMemoryDatabase()}
			defer f.Close()
			for block := uint64(1); block <= 600; block++ {
				if err := f.Put(stateChangeOwnerSpansKey(block), []byte("derived")); err != nil {
					t.Fatal(err)
				}
			}
			canonical := stateChangeSetKey(1, 0)
			f.Put(canonical, []byte("untouched"))
			old, release, err := AcquireStateHistoryReadView(f)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			batch := f.NewBatch()
			defer batch.Close()
			if err := RetireStateHistoryOwnerSpans(f, batch, 590, coarse); err != nil {
				t.Fatal(err)
			}
			if present, _ := f.Has(stateChangeOwnerSpansKey(1)); !present {
				t.Fatal("retirement escaped batch")
			}
			if err := WriteStageProgressWithHash(batch, StageStateHistoryIndex, 590, common.Hash{5}); err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			through := uint64(512)
			if coarse {
				through = 590
			}
			for block := uint64(1); block <= 600; block++ {
				present, e := f.Has(stateChangeOwnerSpansKey(block))
				if e != nil || present != (block > through) {
					t.Fatal("retirement crossed bound", block, present, e)
				}
			}
			if present, _ := old.Has(stateChangeOwnerSpansKey(1)); !present {
				t.Fatal("old pinned index disappeared")
			}
			if got, _ := f.Get(canonical); !bytes.Equal(got, []byte("untouched")) {
				t.Fatal("canonical rows modified")
			}
			// A late overlay flush is caught without any new watermark advance.
			if err := f.Put(stateChangeOwnerSpansKey(1), []byte("late")); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
			if err := RetireStateHistoryOwnerSpans(f, batch, 590, false); err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			if present, _ := f.Has(stateChangeOwnerSpansKey(1)); present {
				t.Fatal("late covered index leaked")
			}
			row, ok, err := ReadStageProgressRow(f, StageStateHistoryIndex)
			if err != nil || !ok || row.BlockNum != 590 {
				t.Fatal("cleanup advanced or rewound posting watermark", row, err)
			}
		})
	}
}
