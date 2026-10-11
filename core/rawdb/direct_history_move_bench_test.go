package rawdb

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
)

type directMoveMeasuredStore struct {
	ethdb.KeyValueStore
	deletes, ranges, deleteBytes, syncs uint64
}

func (s *directMoveMeasuredStore) NewBatch() ethdb.Batch {
	return &directMoveMeasuredBatch{Batch: s.KeyValueStore.NewBatch(), store: s}
}
func (s *directMoveMeasuredStore) SyncKeyValue() error {
	s.syncs++
	return s.KeyValueStore.SyncKeyValue()
}

type directMoveMeasuredBatch struct {
	ethdb.Batch
	store *directMoveMeasuredStore
}

func (b *directMoveMeasuredBatch) Delete(key []byte) error {
	b.store.deletes++
	b.store.deleteBytes += uint64(len(key))
	return b.Batch.Delete(key)
}
func (b *directMoveMeasuredBatch) DeleteRange(start, end []byte) error {
	b.store.ranges++
	b.store.deleteBytes += uint64(len(start) + len(end))
	return b.Batch.DeleteRange(start, end)
}

// Actual Pebble stores, identical logical input and page size in every run.
// Setup and the complete post-move byte comparison are outside the timer.
func BenchmarkDirectHistoryMove(b *testing.B) {
	const rows, valueSize = 1 << 18, 128
	b.ReportAllocs()
	b.SetBytes(int64(rows * (len(stateChangeSetKey(1, 1)) + valueSize)))
	var deletes, ranges, deleteBytes, syncs uint64
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		dir := b.TempDir()
		hotDB, err := NewPebbleDB(filepath.Join(dir, "hot"), 16, 16)
		if err != nil {
			b.Fatal(err)
		}
		historyDB, err := NewPebbleDB(filepath.Join(dir, "history"), 16, 16)
		if err != nil {
			b.Fatal(err)
		}
		hot := &directMoveMeasuredStore{KeyValueStore: hotDB}
		history := &directMoveMeasuredStore{KeyValueStore: historyDB}
		seed := hot.NewBatch()
		value := make([]byte, valueSize)
		for i := 0; i < rows; i++ {
			binary.BigEndian.PutUint64(value, uint64(i))
			if err = seed.Put(stateChangeSetKey(uint64(i+1), 1), value); err != nil {
				b.Fatal(err)
			}
		}
		if err = seed.Write(); err != nil {
			b.Fatal(err)
		}
		seed.Close()
		if err = hot.Compact(stateChangeSetPrefix, prefixUpperBound(stateChangeSetPrefix)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		err = MoveDirectHistory(context.Background(), hot, nil, history, 1<<20, nil)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		deletes += hot.deletes
		ranges += hot.ranges
		deleteBytes += hot.deleteBytes
		syncs += hot.syncs + history.syncs
		it := history.NewIterator(stateChangeSetPrefix, nil)
		count := 0
		for it.Next() {
			if binary.BigEndian.Uint64(it.Value()) != uint64(count) {
				b.Fatal("moved bytes differ")
			}
			count++
		}
		err = it.Error()
		it.Release()
		if err != nil || count != rows {
			b.Fatal("moved coverage differs", count, err)
		}
		it = hot.NewIterator(stateChangeSetPrefix, nil)
		if it.Next() {
			b.Fatal("source not retired")
		}
		it.Release()
		if err = hot.Close(); err != nil {
			b.Fatal(err)
		}
		if err = history.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(deletes)/float64(b.N), "point-deletes/op")
	b.ReportMetric(float64(ranges)/float64(b.N), "range-deletes/op")
	b.ReportMetric(float64(deleteBytes)/float64(b.N), "delete-key-B/op")
	b.ReportMetric(float64(syncs)/float64(b.N), "durability-syncs/op")
}
