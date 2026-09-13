package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/internal/historychunk"
)

// This adapter models the explicit atomic publication contract. Neither an
// ordinary DB nor a bare batch silently gains that capability in production.
type sharedHistoryTestBatch struct {
	ethdb.KeyValueStore
	batch        ethdb.Batch
	pending      map[string][]byte
	puts, failAt int
}

func newSharedHistoryTestBatch(db ethdb.KeyValueStore) *sharedHistoryTestBatch {
	return &sharedHistoryTestBatch{KeyValueStore: db, batch: db.NewBatch(), pending: make(map[string][]byte)}
}
func (b *sharedHistoryTestBatch) StateHistoryChunkWritesAtomic() bool { return true }
func (b *sharedHistoryTestBatch) Get(key []byte) ([]byte, error) {
	if value, ok := b.pending[string(key)]; ok {
		return bytes.Clone(value), nil
	}
	return b.KeyValueStore.Get(key)
}
func (b *sharedHistoryTestBatch) Has(key []byte) (bool, error) {
	if _, ok := b.pending[string(key)]; ok {
		return true, nil
	}
	return b.KeyValueStore.Has(key)
}
func (b *sharedHistoryTestBatch) Put(key, value []byte) error {
	b.puts++
	if b.failAt != 0 && b.puts == b.failAt {
		return errors.New("injected chunk publication failure")
	}
	if err := b.batch.Put(key, value); err != nil {
		return err
	}
	b.pending[string(key)] = bytes.Clone(value)
	return nil
}

func enableSharedHistoryTest(t *testing.T) {
	t.Helper()
	old := stateHistoryCrossBlockDedup.Swap(true)
	t.Cleanup(func() { stateHistoryCrossBlockDedup.Store(old) })
}

func sharedHistoryTestDB(t *testing.T) ethdb.KeyValueStore {
	t.Helper()
	db, err := NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func sharedHistoryKVBytes(t *testing.T, db ethdb.Iteratee, prefix []byte) int {
	t.Helper()
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	total := 0
	for it.Next() {
		total += len(it.Key()) + len(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return total
}

func TestSharedHistoryCrossBlockPersistsAndReopens(t *testing.T) {
	enableSharedHistoryTest(t)
	path := t.TempDir()
	db, err := NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	rows := chunkHistoryRows(1<<20, 4)
	var baseline int
	var firstCost int
	for i, row := range rows {
		row.BlockNum, row.Seq, row.TxNum = 100+uint64(i), 1, 1000+uint64(i)
		input := []*StateDomainChange{row}
		raw := encodeBorrowedStateDomainChangeTestBlock(t, input)
		old, _ := encodeStateDomainChangeBlockStorageForChanges(raw, input)
		baseline += len(stateChangeSetKey(row.BlockNum, 0)) + len(old)
		tx := newSharedHistoryTestBatch(db)
		if err := WriteStateDomainChangeBlockRows(tx, input); err != nil {
			t.Fatal(err)
		}
		if ok, _ := db.Has(stateChangeSetKey(row.BlockNum, 0)); ok {
			t.Fatal("pack escaped atomic batch")
		}
		if err := tx.batch.Write(); err != nil {
			t.Fatal(err)
		}
		packed, err := db.Get(stateChangeSetKey(row.BlockNum, 0))
		if err != nil || !isStateHistorySharedPack(packed) {
			t.Fatalf("shared pack missing: %v", err)
		}
		decoded, err := decodeStateHistorySharedPack(db, packed, row.BlockNum)
		if err != nil || !bytes.Equal(decoded, raw) {
			t.Fatalf("raw bytes changed: %v", err)
		}
		if i == 0 {
			firstCost = sharedHistoryKVBytes(t, db, stateHistorySharedChunkPrefix)
		}
	}
	physical := sharedHistoryKVBytes(t, db, stateHistorySharedChunkPrefix) + sharedHistoryKVBytes(t, db, stateHistorySharedBucketPrefix) + sharedHistoryKVBytes(t, db, stateChangeSetPrefix)
	if physical*100 > baseline*45 {
		t.Fatalf("cross-block fixture did not materially share: %d vs %d", physical, baseline)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = NewPebbleDB(path, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		got, ok, err := ReadStateDomainChange(db, row.BlockNum, 1)
		if err != nil || !ok || !reflect.DeepEqual(got, row) {
			t.Fatalf("reopened history mismatch: %v %v", ok, err)
		}
	}
	t.Logf("same raw history, real Pebble reopen: baseline=%d physicalKV=%d firstSeed=%d", baseline, physical, firstCost)
}

func TestSharedHistoryBucketIsolationRetirementAndFallback(t *testing.T) {
	enableSharedHistoryTest(t)
	db := sharedHistoryTestDB(t)
	row := chunkHistoryRows(512<<10, 1)[0]
	for _, block := range []uint64{1023, 1024} {
		row.BlockNum = block
		tx := newSharedHistoryTestBatch(db)
		if err := WriteStateDomainChangeBlockRows(tx, []*StateDomainChange{row}); err != nil {
			t.Fatal(err)
		}
		if err := tx.batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	if sharedHistoryKVBytes(t, db, stateHistoryChunkBucketPrefix(0)) == 0 || sharedHistoryKVBytes(t, db, stateHistoryChunkBucketPrefix(1)) == 0 {
		t.Fatal("bucket seed missing")
	}
	// Removing a different bucket must never affect this pack's references.
	it := db.NewIterator(stateHistoryChunkBucketPrefix(0), nil)
	for it.Next() {
		if err := db.Delete(bytes.Clone(it.Key())); err != nil {
			t.Fatal(err)
		}
	}
	it.Release()
	if _, ok, err := ReadStateDomainChange(db, 1024, 1); err != nil || !ok {
		t.Fatalf("cross-bucket dependency: %v", err)
	}
	if err := db.Put(stateHistoryChunkBucketKey(0), []byte{1, 1}); err != nil {
		t.Fatal(err)
	}
	row.BlockNum = 1000
	tx := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(tx, []*StateDomainChange{row}); err != nil {
		t.Fatal(err)
	}
	if err := tx.batch.Write(); err != nil {
		t.Fatal(err)
	}
	pack, _ := db.Get(stateChangeSetKey(1000, 0))
	if isStateHistorySharedPack(pack) || sharedHistoryKVBytes(t, db, stateHistoryChunkBucketPrefix(0)) != 0 {
		t.Fatal("retired bucket reopened")
	}
	// An ordinary writer is not an atomic scope, even on an MVCC backend.
	row.BlockNum = 1200
	if err := WriteStateDomainChangeBlockRows(db, []*StateDomainChange{row}); err != nil {
		t.Fatal(err)
	}
	pack, _ = db.Get(stateChangeSetKey(1200, 0))
	if isStateHistorySharedPack(pack) {
		t.Fatal("ordinary writer implicitly opted in")
	}
}

func TestSharedHistoryPlanFailureDoesNotPublish(t *testing.T) {
	enableSharedHistoryTest(t)
	db := sharedHistoryTestDB(t)
	rows := chunkHistoryRows(512<<10, 1)
	for _, failure := range []int{1, 3} {
		tx := newSharedHistoryTestBatch(db)
		tx.failAt = failure
		if err := WriteStateDomainChangeBlockRows(tx, rows); err == nil {
			t.Fatal("expected injected failure")
		}
		// Discard the entire batch exactly as a failed canonical layer is discarded.
		tx.batch.Reset()
		if sharedHistoryKVBytes(t, db, stateHistorySharedChunkPrefix) != 0 || sharedHistoryKVBytes(t, db, stateChangeSetPrefix) != 0 {
			t.Fatal("failed plan published state")
		}
	}
	tx := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(tx, rows); err != nil {
		t.Fatal(err)
	}
	if err := tx.batch.Write(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadStateDomainChange(db, 42, 1); err != nil || !ok {
		t.Fatalf("retry did not recover: %v", err)
	}
}

func TestSharedHistoryCorruptionIsNeverMissing(t *testing.T) {
	enableSharedHistoryTest(t)
	db := sharedHistoryTestDB(t)
	rows := chunkHistoryRows(512<<10, 1)
	tx := newSharedHistoryTestBatch(db)
	if err := WriteStateDomainChangeBlockRows(tx, rows); err != nil {
		t.Fatal(err)
	}
	if err := tx.batch.Write(); err != nil {
		t.Fatal(err)
	}
	pack, _ := db.Get(stateChangeSetKey(42, 0))
	if _, err := decodeStateHistorySharedPack(db, pack, 43); err == nil {
		t.Fatal("accepted wrong physical block")
	}
	if _, err := decodeStateHistorySharedPack(db, append(bytes.Clone(pack), 0), 42); err == nil {
		t.Fatal("accepted trailing byte")
	}
	it := db.NewIterator(stateHistoryChunkBucketPrefix(0), nil)
	if !it.Next() {
		t.Fatal("no chunk")
	}
	key, value := bytes.Clone(it.Key()), bytes.Clone(it.Value())
	it.Release()
	if err := db.Delete(key); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadStateDomainChange(db, 42, 1); err == nil || ok {
		t.Fatalf("missing chunk became missing history: %v %v", ok, err)
	}
	if err := IterateStateDomainChangesContext(context.Background(), db, 42, func(*StateDomainChange) (bool, error) {
		t.Fatal("callback before complete verification")
		return true, nil
	}); err == nil {
		t.Fatal("iterator hid corrupt pack")
	}
	value[len(value)-1] ^= 1
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadStateDomainChange(db, 42, 1); err == nil {
		t.Fatal("corrupt chunk accepted")
	}
}

func TestSharedHistoryChunkBounds(t *testing.T) {
	raw := bytes.Repeat([]byte{7}, historychunk.MaxSize)
	hash := sha256.Sum256(raw)
	good := encodeStateHistorySharedChunk(raw)
	if got, err := decodeStateHistorySharedChunk(good, len(raw), hash); err != nil || !bytes.Equal(got, raw) {
		t.Fatal(err)
	}
	for _, data := range [][]byte{nil, {1, 2, 1, 9}, {1, 0, 0}, append(bytes.Clone(good), 0)} {
		if _, err := decodeStateHistorySharedChunk(data, len(raw), hash); err == nil {
			t.Fatal("accepted malformed chunk")
		}
	}
	if _, err := decodeStateHistorySharedChunk(good, historychunk.MaxSize+1, hash); err == nil {
		t.Fatal("accepted oversized chunk")
	}
}
