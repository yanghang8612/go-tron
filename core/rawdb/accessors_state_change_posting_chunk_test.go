package rawdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
)

func postingChunkTestLimits() StateChangePostingPruneLimits {
	return StateChangePostingPruneLimits{MaxScannedRows: 100, MaxScannedBytes: 1 << 20, MaxDeleteBytes: 1 << 20, MaxDuration: time.Minute}
}

// Chosen digests preserve readable physical ordering without relying on the
// accidental order of SHA-256 outputs. All keys use the production key helper.
func putPostingChunkFrame(t *testing.T, db ethdb.KeyValueWriter, ordinal byte, blocks ...uint64) ([]byte, uint64) {
	t.Helper()
	var hash [sha256.Size]byte
	hash[0] = ordinal
	key := stateChangePostingKey(hash, blocks[0])
	value, err := encodeStateChangePosting(blocks)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
	return key, uint64(len(key) + len(value))
}

func TestPruneStateChangePostingChunkWholeFramesAndExclusiveResume(t *testing.T) {
	store := ethrawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = store.Close() })
	db := &postingChunkTestDB{KeyValueStore: store}
	stale, staleBytes := putPostingChunkFrame(t, store, 1, 1, 2, 4)
	mixed, mixedBytes := putPostingChunkFrame(t, store, 2, 3, 5)
	live, liveBytes := putPostingChunkFrame(t, store, 3, 6, 7)
	boundary, boundaryBytes := putPostingChunkFrame(t, store, 4, 4)
	directory := stateChangeKeyDirectoryKey([]byte("untouched-directory"))
	if err := store.Put(directory, []byte("directory marker")); err != nil {
		t.Fatal(err)
	}
	limits := postingChunkTestLimits()
	limits.MaxScannedRows = 2
	first, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 4, nil, limits)
	if err != nil || first.Complete || !bytes.Equal(first.NextCursor, mixed) || first.RowsScanned != 2 || first.RowsDeleted != 1 || first.BytesScanned != staleBytes+mixedBytes || first.BytesDeleted != staleBytes {
		t.Fatalf("first chunk = %+v, %v", first, err)
	}
	assertPostingChunkExists(t, store, stale, false)
	assertPostingChunkExists(t, store, mixed, true)
	second, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 4, first.NextCursor, limits)
	if err != nil || second.RowsScanned != 2 || second.RowsDeleted != 1 || !bytes.Equal(second.NextCursor, boundary) || second.BytesScanned != liveBytes+boundaryBytes || second.BytesDeleted != boundaryBytes {
		t.Fatalf("second chunk = %+v, %v", second, err)
	}
	assertPostingChunkExists(t, store, boundary, false)
	assertPostingChunkExists(t, store, live, true)
	// A deleted resume row and an exactly-full final chunk need no sentinel
	// mutation: a subsequent empty iterator confirms completion.
	last, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 4, second.NextCursor, limits)
	if err != nil || !last.Complete || last.RowsScanned != 0 || last.RowsDeleted != 0 || !bytes.Equal(last.NextCursor, boundary) {
		t.Fatalf("final chunk = %+v, %v", last, err)
	}
	value, err := store.Get(directory)
	if err != nil || string(value) != "directory marker" {
		t.Fatalf("directory changed: %q, %v", value, err)
	}
	if db.iterators != 3 || db.releases != 3 || db.writes != 3 || db.pointReads != 0 {
		t.Fatalf("resource work = %+v", db)
	}
}

func TestPruneStateChangePostingChunkBoundsMakeProgress(t *testing.T) {
	for _, bound := range []string{"rows", "scan-bytes", "delete-bytes", "duration"} {
		t.Run(bound, func(t *testing.T) {
			store := ethrawdb.NewMemoryDatabase()
			t.Cleanup(func() { _ = store.Close() })
			db := &postingChunkTestDB{KeyValueStore: store}
			key, size := putPostingChunkFrame(t, store, 1, 1, 3)
			next, _ := putPostingChunkFrame(t, store, 2, 2)
			limits := postingChunkTestLimits()
			switch bound {
			case "rows":
				limits.MaxScannedRows = 1
			case "scan-bytes":
				limits.MaxScannedBytes = 1
			case "delete-bytes":
				limits.MaxDeleteBytes = 1
			case "duration":
				limits.MaxDuration = time.Nanosecond
			}
			got, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 3, nil, limits)
			if err != nil || got.Complete || !got.StoppedByBudget || got.RowsScanned != 1 || got.RowsDeleted != 1 || got.BytesScanned != size || got.BytesDeleted != size || !bytes.Equal(got.NextCursor, key) {
				t.Fatalf("bounded chunk = %+v, %v", got, err)
			}
			assertPostingChunkExists(t, store, key, false)
			assertPostingChunkExists(t, store, next, true)
			if db.releases != 1 || db.nextCalls != 1 {
				t.Fatalf("iterator read past bound: next=%d releases=%d", db.nextCalls, db.releases)
			}
		})
	}
}

func TestPruneStateChangePostingChunkFailuresDoNotAdvance(t *testing.T) {
	boom := errors.New("injected posting chunk failure")
	for _, failure := range []string{"delete", "iterator", "write", "write-ambiguous", "cancel-next", "cancel-after-scan"} {
		t.Run(failure, func(t *testing.T) {
			store := ethrawdb.NewMemoryDatabase()
			t.Cleanup(func() { _ = store.Close() })
			db := &postingChunkTestDB{KeyValueStore: store}
			resume, _ := putPostingChunkFrame(t, store, 1, 10)
			stale, _ := putPostingChunkFrame(t, store, 2, 1, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch failure {
			case "delete":
				db.deleteErr = boom
			case "iterator":
				db.iteratorErr = boom
			case "write", "write-ambiguous":
				db.writeErr = boom
				db.applyBeforeWriteError = failure == "write-ambiguous"
			case "cancel-next":
				db.onNext = cancel
			case "cancel-after-scan":
				db.onRelease = cancel
			}
			got, err := PruneStaleStateChangePostingChunkContext(ctx, db, 3, resume, postingChunkTestLimits())
			wantErr := boom
			if failure == "cancel-next" || failure == "cancel-after-scan" {
				wantErr = context.Canceled
			}
			if !errors.Is(err, wantErr) || got.Complete || got.StoppedByBudget || !bytes.Equal(got.NextCursor, resume) || got.RowsDeleted != 0 || got.BytesDeleted != 0 {
				t.Fatalf("failed chunk = %+v, %v, want %v and unchanged cursor", got, err, wantErr)
			}
			if db.releases != 1 || db.closes != 1 {
				t.Fatalf("leaked resource: releases=%d closes=%d", db.releases, db.closes)
			}
			assertPostingChunkExists(t, store, stale, failure != "write-ambiguous")
			// Retrying the unconfirmed cursor is safe even if the failed write
			// actually applied its entire atomic batch.
			db.deleteErr, db.iteratorErr, db.writeErr, db.onNext, db.onRelease = nil, nil, nil, nil, nil
			retry, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 3, resume, postingChunkTestLimits())
			if err != nil || !retry.Complete {
				t.Fatalf("retry = %+v, %v", retry, err)
			}
			assertPostingChunkExists(t, store, stale, false)
		})
	}
}

func TestPruneStateChangePostingChunkRejectsMalformedAndInvalidCursor(t *testing.T) {
	for _, malformed := range []string{"key-length", "version", "zero-delta", "overflow", "trailing", "oversized"} {
		t.Run(malformed, func(t *testing.T) {
			store := ethrawdb.NewMemoryDatabase()
			t.Cleanup(func() { _ = store.Close() })
			db := &postingChunkTestDB{KeyValueStore: store}
			stale, _ := putPostingChunkFrame(t, store, 1, 1)
			bad, _ := putPostingChunkFrame(t, store, 2, 2)
			value := []byte{stateChangePostingValueVersion, 1}
			switch malformed {
			case "key-length":
				if err := store.Delete(bad); err != nil {
					t.Fatal(err)
				}
				bad = append(bad, 0)
			case "version":
				value[0] = 0xff
			case "zero-delta":
				value = []byte{stateChangePostingValueVersion, 2, 0}
			case "overflow":
				value = []byte{stateChangePostingValueVersion, 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1}
			case "trailing":
				value = append(value, 0)
			case "oversized":
				value = make([]byte, maxStateChangePostingEncodedBytes+1)
			}
			if err := store.Put(bad, value); err != nil {
				t.Fatal(err)
			}
			got, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 3, nil, postingChunkTestLimits())
			if err == nil || len(got.NextCursor) != 0 || got.Complete || got.RowsDeleted != 0 || got.BytesDeleted != 0 || db.writes != 0 || db.releases != 1 {
				t.Fatalf("malformed chunk = %+v, %v, writes=%d releases=%d", got, err, db.writes, db.releases)
			}
			assertPostingChunkExists(t, store, stale, true)
			assertPostingChunkExists(t, store, bad, true)
		})
	}
	store := ethrawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = store.Close() })
	db := &postingChunkTestDB{KeyValueStore: store}
	for _, cursor := range [][]byte{{1}, stateChangeKeyDirectoryKey([]byte("wrong family")), append(stateChangePostingKey([sha256.Size]byte{}, 1), 0)} {
		if got, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 1, cursor, postingChunkTestLimits()); err == nil || !bytes.Equal(got.NextCursor, cursor) || db.iterators != 0 {
			t.Fatalf("invalid cursor = %+v, %v, iterators=%d", got, err, db.iterators)
		}
	}
}

func TestPruneStateChangePostingChunkRejectsUnboundedWorkAndPreCancellation(t *testing.T) {
	store := ethrawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = store.Close() })
	db := &postingChunkTestDB{KeyValueStore: store}
	resume, _ := putPostingChunkFrame(t, store, 1, 1)
	for _, missing := range []string{"rows", "scan-bytes", "delete-bytes", "duration", "negative-duration"} {
		limits := postingChunkTestLimits()
		switch missing {
		case "rows":
			limits.MaxScannedRows = 0
		case "scan-bytes":
			limits.MaxScannedBytes = 0
		case "delete-bytes":
			limits.MaxDeleteBytes = 0
		case "duration":
			limits.MaxDuration = 0
		case "negative-duration":
			limits.MaxDuration = -time.Second
		}
		got, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 1, resume, limits)
		if err == nil || db.iterators != 0 || !bytes.Equal(got.NextCursor, resume) {
			t.Fatalf("unbounded %s = %+v, %v", missing, got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := PruneStaleStateChangePostingChunkContext(ctx, db, 1, resume, postingChunkTestLimits())
	if !errors.Is(err, context.Canceled) || db.iterators != 0 || !bytes.Equal(got.NextCursor, resume) {
		t.Fatalf("pre-canceled = %+v, %v", got, err)
	}
}

func TestPruneStateChangePostingChunkPebblePreservesReadsAcrossCompaction(t *testing.T) {
	tune := DefaultPebbleOptions()
	tune.MemTableSizeBytes = 1 << 20
	tune.MaxConcurrentCompactions = 1
	db, err := NewPebbleDBWithOptions(t.TempDir(), 16, 16, tune)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	owner := common.Address{common.AddressPrefixMainnet, 0x69}
	frames := []struct {
		logical  string
		from, to uint64
	}{
		{"slot/stale", 1, 256},
		{"slot/mixed", 250, 300},
		{"slot/live", 257, 300},
		{"slot/boundary", 256, 256},
	}
	postingKeys := make(map[string][]byte)
	for index, frame := range frames {
		latest := StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, []byte(frame.logical))
		blocks := make([]uint64, 0, frame.to-frame.from+1)
		for number := frame.from; number <= frame.to; number++ {
			blocks = append(blocks, number)
			// Rows at/below 256 have already been authoritatively pruned.
			// Keep their immutable posting candidates to exercise stale reads.
			if number <= 256 {
				continue
			}
			change := &StateDomainChange{BlockNum: number, TxNum: number, Seq: uint64(index + 1),
				FlatDomain: StateFlatDomainKVLatest, Owner: owner, Domain: kvdomains.ContractStorage,
				Key: []byte(frame.logical), PrevExists: true, Prev: []byte(fmt.Sprintf("%s-before-%d", frame.logical, number))}
			if err := WriteStateDomainChangeRow(db, change); err != nil {
				t.Fatal(err)
			}
		}
		value, err := encodeStateChangePosting(blocks)
		if err != nil {
			t.Fatal(err)
		}
		key := stateChangePostingKey(stateChangePostingHash(latest), blocks[0])
		postingKeys[frame.logical] = key
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
		if err := db.Put(stateChangeKeyDirectoryKey(latest), nil); err != nil {
			t.Fatal(err)
		}
		if err := WriteStateKVLatest(db, owner, 0, kvdomains.ContractStorage, []byte(frame.logical), []byte("current/"+frame.logical)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	readViews := func() map[string]any {
		t.Helper()
		views := make(map[string]any)
		for _, frame := range frames {
			views[frame.logical+"/blocks"] = collectPostingTestBlocks(t, db, owner, []byte(frame.logical))
			value, ok, err := ReadStateKVAsOf(db, owner, 0, kvdomains.ContractStorage, []byte(frame.logical), 256, 300)
			if err != nil || !ok {
				t.Fatalf("as-of %s: %q, %v, %v", frame.logical, value, ok, err)
			}
			views[frame.logical+"/as-of"] = string(value)
		}
		var prefixBlocks []uint64
		if err := IterateStateDomainChangeBlocksByPrefix(db, owner, 0, kvdomains.ContractStorage, []byte("slot/"), func(number uint64) (bool, error) {
			prefixBlocks = append(prefixBlocks, number)
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(prefixBlocks)
		views["prefix/blocks"] = prefixBlocks
		return views
	}
	want := readViews()
	if want["slot/mixed/as-of"] != "slot/mixed-before-257" || len(want["slot/mixed/blocks"].([]uint64)) != 44 || len(want["slot/stale/blocks"].([]uint64)) != 0 || len(want["prefix/blocks"].([]uint64)) != 44 {
		t.Fatalf("invalid real-Pebble read fixture: %v", want)
	}
	limits := postingChunkTestLimits()
	limits.MaxScannedRows = 1
	var cursor []byte
	var scanned, deleted uint64
	complete := false
	for chunk := 0; chunk < 6; chunk++ {
		result, err := PruneStaleStateChangePostingChunkContext(context.Background(), db, 256, cursor, limits)
		if err != nil {
			t.Fatal(err)
		}
		if result.RowsScanned > 1 {
			t.Fatalf("row budget escaped: %+v", result)
		}
		scanned += result.RowsScanned
		deleted += result.RowsDeleted
		cursor = result.NextCursor
		if got := readViews(); !reflect.DeepEqual(got, want) {
			t.Fatalf("read views changed during chunk %d: got %v want %v", chunk, got, want)
		}
		if result.Complete {
			complete = true
			break
		}
	}
	if !complete || scanned != 4 || deleted != 2 {
		t.Fatalf("chunk totals: complete=%v scanned=%d deleted=%d", complete, scanned, deleted)
	}
	for _, frame := range frames {
		assertPostingChunkExists(t, db, postingKeys[frame.logical], frame.to > 256)
		latest := StateKVLatestCommitmentKey(owner, 0, kvdomains.ContractStorage, []byte(frame.logical))
		assertPostingChunkExists(t, db, stateChangeKeyDirectoryKey(latest), true)
	}
	if err := db.Compact(nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := readViews(); !reflect.DeepEqual(got, want) {
		t.Fatalf("compaction changed read views: got %v want %v", got, want)
	}
}

func assertPostingChunkExists(t *testing.T, db ethdb.KeyValueReader, key []byte, want bool) {
	t.Helper()
	if got, err := db.Has(key); err != nil || got != want {
		t.Fatalf("key %x exists=%v err=%v, want %v", key, got, err, want)
	}
}

type postingChunkTestDB struct {
	ethdb.KeyValueStore
	iterators, releases, nextCalls, writes, closes, pointReads int
	deleteErr, iteratorErr, writeErr                           error
	applyBeforeWriteError                                      bool
	onNext, onRelease                                          func()
}

func (db *postingChunkTestDB) Get(key []byte) ([]byte, error) {
	db.pointReads++
	return db.KeyValueStore.Get(key)
}

func (db *postingChunkTestDB) Has(key []byte) (bool, error) {
	db.pointReads++
	return db.KeyValueStore.Has(key)
}

func (db *postingChunkTestDB) NewIterator(prefix, start []byte) ethdb.Iterator {
	if !bytes.Equal(prefix, stateChangePostingPrefix) {
		panic(fmt.Sprintf("unexpected non-posting iterator %x", prefix))
	}
	db.iterators++
	return &postingChunkTestIterator{Iterator: db.KeyValueStore.NewIterator(prefix, start), db: db}
}

func (db *postingChunkTestDB) NewBatch() ethdb.Batch {
	return &postingChunkTestBatch{Batch: db.KeyValueStore.NewBatch(), db: db}
}

type postingChunkTestIterator struct {
	ethdb.Iterator
	db *postingChunkTestDB
}

func (it *postingChunkTestIterator) Next() bool {
	it.db.nextCalls++
	if it.db.onNext != nil {
		it.db.onNext()
	}
	return it.Iterator.Next()
}

func (it *postingChunkTestIterator) Error() error {
	if it.db.iteratorErr != nil {
		return it.db.iteratorErr
	}
	return it.Iterator.Error()
}

func (it *postingChunkTestIterator) Release() {
	it.db.releases++
	it.Iterator.Release()
	if it.db.onRelease != nil {
		it.db.onRelease()
	}
}

type postingChunkTestBatch struct {
	ethdb.Batch
	db *postingChunkTestDB
}

func (b *postingChunkTestBatch) Delete(key []byte) error {
	if b.db.deleteErr != nil {
		return b.db.deleteErr
	}
	return b.Batch.Delete(key)
}

func (b *postingChunkTestBatch) Write() error {
	if b.db.iterators != b.db.releases {
		panic("batch write while iterator still live")
	}
	b.db.writes++
	if b.db.writeErr != nil && !b.db.applyBeforeWriteError {
		return b.db.writeErr
	}
	if err := b.Batch.Write(); err != nil {
		return err
	}
	return b.db.writeErr
}

func (b *postingChunkTestBatch) Close() {
	b.db.closes++
	b.Batch.Close()
}
