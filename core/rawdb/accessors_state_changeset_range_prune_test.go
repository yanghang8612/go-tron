package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/pointread"
)

type rangePruneTestStore struct {
	StateKVLatestStore
	batch        ethdb.Batch
	ranges       [][2][]byte
	pointCalls   int
	valueCalls   int
	iterators    int
	releases     int
	getCalls     int
	deleteErr    error
	rangeErr     error
	iteratorErr  error
	getErr       error
	forbidValues bool
}

func (s *rangePruneTestStore) Get(key []byte) ([]byte, error) {
	s.getCalls++
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.StateKVLatestStore.Get(key)
}

func (s *rangePruneTestStore) Put(key, value []byte) error { return s.batch.Put(key, value) }
func (s *rangePruneTestStore) Delete(key []byte) error {
	s.pointCalls++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.batch.Delete(key)
}
func (s *rangePruneTestStore) DeleteRange(start, end []byte) error {
	s.ranges = append(s.ranges, [2][]byte{bytes.Clone(start), bytes.Clone(end)})
	if s.rangeErr != nil {
		return s.rangeErr
	}
	return s.batch.DeleteRange(start, end)
}
func (s *rangePruneTestStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	s.iterators++
	return &rangePruneTestIterator{Iterator: s.StateKVLatestStore.NewIterator(prefix, start), store: s}
}

type rangePruneTestIterator struct {
	ethdb.Iterator
	store *rangePruneTestStore
}

func (it *rangePruneTestIterator) Value() []byte {
	if it.store.forbidValues {
		panic("legacy indexed point path unexpectedly read a value")
	}
	it.store.valueCalls++
	return it.Iterator.Value()
}
func (it *rangePruneTestIterator) Error() error {
	if it.store.iteratorErr != nil {
		return it.store.iteratorErr
	}
	return it.Iterator.Error()
}
func (it *rangePruneTestIterator) Release() {
	it.store.releases++
	it.Iterator.Release()
}

func newRangePruneTestStore(t *testing.T) (*rangePruneTestStore, ethdb.Database) {
	t.Helper()
	base := ethrawdb.NewMemoryDatabase()
	batch := base.NewBatch()
	t.Cleanup(func() { batch.Close(); _ = base.Close() })
	return &rangePruneTestStore{StateKVLatestStore: base, batch: batch}, base
}

func seedRangePruneRows(t testing.TB, db ethdb.KeyValueWriter, blocks []uint64, valueSize int) uint64 {
	t.Helper()
	var size uint64
	for _, block := range blocks {
		key := stateChangeSetKey(block, 0)
		// Indexed pruning must not attempt to decode an already verified pack.
		value := bytes.Repeat([]byte{0xfd}, valueSize)
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
		size += uint64(len(key) + len(value))
	}
	if err := WriteStageProgressWithHash(db, StageStateHistoryIndex, ^uint64(0), common.Hash{1}); err != nil {
		t.Fatal(err)
	}
	return size
}

func rangePruneBlocks(first, last uint64) []uint64 {
	var blocks []uint64
	for block := first; ; block++ {
		blocks = append(blocks, block)
		if block == last {
			return blocks
		}
	}
}

func TestDeleteStateDomainChangeRangeSchemaHolesAndBatch(t *testing.T) {
	db, base := newRangePruneTestStore(t)
	seedRangePruneRows(t, base, []uint64{1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12}, 10)
	repair := stateChangeSetKey(6, 1)
	malformed := [][]byte{append(stateChangeSetKey(2, 0), 0), stateChangeSetBlockPrefix(7), append(stateChangeSetKey(12, 0), 0)}
	for _, key := range append(malformed, repair) {
		if err := base.Put(key, []byte("keep or point")); err != nil {
			t.Fatal(err)
		}
	}
	selected := []uint64{1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12}
	got, err := DeleteStateDomainChangeBlocksWithOptions(db, selected, StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2})
	if err != nil || got.RangeRuns != 3 || got.RangeRows != 8 || got.PointRows != 3 || db.valueCalls != 11 {
		t.Fatalf("stats=%+v err=%v values=%d ranges=%x", got, err, db.valueCalls, db.ranges)
	}
	if got.RangeBytes+got.PointBytes != 10*uint64(len(stateChangeSetKey(1, 0))+10)+uint64(len(repair)+len("keep or point")) {
		t.Fatalf("logical bytes do not account for selected rows: %+v", got)
	}
	// Submitting ranges must not mutate the underlying database before Write.
	assertPostingChunkExists(t, base, stateChangeSetKey(1, 0), true)
	if db.releases != db.iterators || db.iterators != 1 {
		t.Fatalf("iterator lifecycle = %d/%d", db.releases, db.iterators)
	}
	if db.getCalls != 1 {
		t.Fatalf("indexed pruning added point reads beyond its watermark: %d", db.getCalls)
	}
	if err := db.batch.Write(); err != nil {
		t.Fatal(err)
	}
	for _, block := range selected {
		assertPostingChunkExists(t, base, stateChangeSetKey(block, 0), false)
	}
	assertPostingChunkExists(t, base, repair, false)
	assertPostingChunkExists(t, base, stateChangeSetKey(4, 0), true)
	for _, key := range malformed {
		assertPostingChunkExists(t, base, key, true)
	}
}

func TestDeleteStateDomainChangeRangeBoundsAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    StateDomainChangeDeleteOptions
		noRange bool
		runs    uint64
		rows    uint64
	}{
		{name: "zero-disabled"},
		{name: "capability-hidden", opts: StateDomainChangeDeleteOptions{EnableRangeDelete: true}, noRange: true},
		{name: "default-minimum", opts: StateDomainChangeDeleteOptions{EnableRangeDelete: true}, runs: 1, rows: 70},
		{name: "block-cap-tail-points", opts: StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 20, MaxRangeBlocks: 32}, runs: 2, rows: 64},
		{name: "byte-cap-one-row-overshoot", opts: StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2, MaxRangeBytes: 50}, runs: 35, rows: 70},
		{name: "fat-row-below-minimum", opts: StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2, MaxRangeBytes: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, base := newRangePruneTestStore(t)
			blocks := rangePruneBlocks(1, 70)
			logical := seedRangePruneRows(t, base, blocks, 10)
			var store StateKVLatestStore = db
			if tc.noRange {
				store = struct{ StateKVLatestStore }{db}
			}
			got, err := DeleteStateDomainChangeBlocksWithOptions(store, blocks, tc.opts)
			if err != nil || got.RangeRuns != tc.runs || got.RangeRows != tc.rows || got.PointRows != 70-tc.rows || got.RangeBytes+got.PointBytes != logical {
				t.Fatalf("stats=%+v err=%v", got, err)
			}
			if err := db.batch.Write(); err != nil {
				t.Fatal(err)
			}
			for _, block := range blocks {
				assertPostingChunkExists(t, base, stateChangeSetKey(block, 0), false)
			}
		})
	}
	// The unchanged API does not read indexed values or create range tombstones.
	db, base := newRangePruneTestStore(t)
	seedRangePruneRows(t, base, []uint64{1, 2}, 10)
	db.forbidValues = true
	if err := DeleteStateDomainChangeBlocks(db, []uint64{1, 2}); err != nil || len(db.ranges) != 0 || db.pointCalls != 2 {
		t.Fatalf("legacy path changed: err=%v ranges=%v points=%d", err, db.ranges, db.pointCalls)
	}
}

func TestDeleteStateDomainChangeRangeSparseAndMaxHeight(t *testing.T) {
	for _, blocks := range [][]uint64{{1, 1 << 60}, {^uint64(0) - 1, ^uint64(0)}} {
		db, base := newRangePruneTestStore(t)
		seedRangePruneRows(t, base, blocks, 1)
		last := blocks[len(blocks)-1]
		extension := append(stateChangeSetKey(last, 0), 0)
		if err := base.Put(extension, []byte("preserved")); err != nil {
			t.Fatal(err)
		}
		got, err := DeleteStateDomainChangeBlocksWithOptions(db, blocks, StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2})
		if err != nil {
			t.Fatal(err)
		}
		if last == ^uint64(0) && got.RangeRows != 2 || last != ^uint64(0) && (got.PointRows != 2 || db.iterators != 2) {
			t.Fatalf("sparse/max boundary stats=%+v iterators=%d", got, db.iterators)
		}
		if db.getCalls != 1 {
			t.Fatalf("sparse selection repeated watermark reads: %d", db.getCalls)
		}
		if err := db.batch.Write(); err != nil {
			t.Fatal(err)
		}
		for _, block := range blocks {
			assertPostingChunkExists(t, base, stateChangeSetKey(block, 0), false)
		}
		assertPostingChunkExists(t, base, extension, true)
	}
}

func TestDeleteStateDomainChangeRangeFailuresDiscardStats(t *testing.T) {
	boom := errors.New("injected range prune error")
	for _, failure := range []string{"point", "range", "iterator", "watermark", "selection", "limits"} {
		t.Run(failure, func(t *testing.T) {
			db, base := newRangePruneTestStore(t)
			blocks := rangePruneBlocks(1, 5)
			seedRangePruneRows(t, base, blocks, 8)
			opts := StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2, MaxRangeBlocks: 2}
			switch failure {
			case "point":
				db.deleteErr = boom // Two accepted ranges precede the failing tail.
			case "range":
				db.rangeErr = boom
			case "iterator":
				db.iteratorErr = boom
			case "watermark":
				db.getErr = boom
			case "selection":
				blocks = []uint64{2, 1}
			case "limits":
				opts.MinRangeBlocks = 3
			}
			got, err := DeleteStateDomainChangeBlocksWithOptions(db, blocks, opts)
			if err == nil || got != (StateDomainChangeDeleteStats{}) || db.iterators != db.releases {
				t.Fatalf("failed operation stats=%+v err=%v lifecycle=%d/%d", got, err, db.iterators, db.releases)
			}
			if failure == "selection" || failure == "limits" {
				if db.getCalls != 0 || db.iterators != 0 {
					t.Fatal("invalid arguments accessed database")
				}
			}
			db.batch.Reset()
			for _, block := range rangePruneBlocks(1, 5) {
				assertPostingChunkExists(t, base, stateChangeSetKey(block, 0), true)
			}
			db.deleteErr, db.rangeErr, db.iteratorErr, db.getErr = nil, nil, nil, nil
			if _, err := DeleteStateDomainChangeBlocksWithOptions(db, rangePruneBlocks(1, 5), StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2}); err != nil {
				t.Fatal(err)
			}
			if err := db.batch.Write(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDeleteStateDomainChangeRangeUnindexedPostingSemantics(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		t.Run(fmt.Sprint(sparse), func(t *testing.T) {
			db, base := newRangePruneTestStore(t)
			blocks := []uint64{1, 2, 3}
			if sparse {
				blocks = []uint64{1, 100, 10000}
			}
			var changes []*StateDomainChange
			for i, block := range blocks {
				change := &StateDomainChange{BlockNum: block, TxNum: block, Seq: 1, FlatDomain: StateFlatDomainAccountLatest, Owner: common.Address{common.AddressPrefixMainnet, byte(i + 1)}}
				var err error
				if i == 1 {
					change.Seq = 0 // legacy singleton at seq zero
					err = WriteStateDomainChangeRow(base, change)
				} else {
					err = WriteStateDomainChangeBlockRows(base, []*StateDomainChange{change})
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := WriteStateDomainChangePostingIndex(base, change); err != nil {
					t.Fatal(err)
				}
				changes = append(changes, change)
			}
			if err := WriteStageProgressWithHash(base, StageStateHistoryIndex, 1, common.Hash{1}); err != nil {
				t.Fatal(err)
			}
			got, err := DeleteStateDomainChangeBlocksWithOptions(db, blocks, StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 2})
			if err != nil || got.PointRows != 3 || got.RangeRows != 0 {
				t.Fatalf("stats=%+v err=%v", got, err)
			}
			if err := db.batch.Write(); err != nil {
				t.Fatal(err)
			}
			for i, change := range changes {
				assertPostingChunkExists(t, base, stateChangeSetKey(change.BlockNum, 0), false)
				posting := stateChangePostingKey(stateChangePostingHash(mustStateDomainChangeLatestKey(t, change)), change.BlockNum)
				assertPostingChunkExists(t, base, posting, i == 0)
			}
		})
	}
}

func TestDeleteStateDomainChangeRangePebbleSnapshotAndReopen(t *testing.T) {
	path := t.TempDir()
	base, err := NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = base.Close() }()
	blocks := rangePruneBlocks(1, 70)
	seedRangePruneRows(t, base, blocks, 2048)
	keepKey := append(stateChangeSetKey(35, 0), 0)
	if err := base.Put(keepKey, []byte("malformed preserved")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := base.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	batch := base.NewBatch()
	db := &rangePruneTestStore{StateKVLatestStore: base, batch: batch}
	got, err := DeleteStateDomainChangeBlocksWithOptions(db, blocks, StateDomainChangeDeleteOptions{EnableRangeDelete: true, MinRangeBlocks: 32})
	if err != nil || got.RangeRuns != 2 || got.RangeRows != 70 {
		t.Fatalf("stats=%+v err=%v", got, err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	for _, block := range blocks {
		key := stateChangeSetKey(block, 0)
		assertPostingChunkExists(t, base, key, false)
		old, err := snapshot.Get(key)
		if err != nil || !bytes.Equal(old, bytes.Repeat([]byte{0xfd}, 2048)) {
			t.Fatalf("snapshot lost block %d: len=%d err=%v", block, len(old), err)
		}
	}
	// Read iterators see only the preserved malformed key, snapshots see all rows.
	for _, tc := range []struct {
		db   ethdb.Iteratee
		want int
	}{{base, 1}, {snapshot, 71}} {
		it := tc.db.NewIterator(stateChangeSetPrefix, nil)
		count := 0
		for it.Next() {
			count++
		}
		err := it.Error()
		it.Release()
		if count != tc.want || err != nil {
			t.Fatalf("iterator rows=%d want=%d err=%v", count, tc.want, err)
		}
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	base, err = NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	assertPostingChunkExists(t, base, keepKey, true)
	assertPostingChunkExists(t, base, stateChangeSetKey(70, 0), false)
}
