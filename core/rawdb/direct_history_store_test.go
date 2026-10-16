package rawdb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func directTestPut(t *testing.T, db ethdb.KeyValueWriter, key, value []byte) {
	t.Helper()
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
}
func directTestRead(t *testing.T, db ethdb.KeyValueReader, key, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
	}
}
func directTestAbsent(t *testing.T, db ethdb.KeyValueReader, key []byte) {
	t.Helper()
	present, err := db.Has(key)
	if err != nil || present {
		t.Fatalf("Has(%q)=%v,%v", key, present, err)
	}
}
func directTestKeys(t *testing.T, source ethdb.Iteratee, prefix, start []byte) []string {
	t.Helper()
	it := source.NewIterator(prefix, start)
	defer it.Release()
	var keys []string
	for it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestDirectHistoryPartition(t *testing.T) {
	hot, history := NewMemoryDatabase(), NewMemoryDatabase()
	db := NewDirectHistoryStore(hot, history)
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Close()
	for _, r := range DirectHistoryRanges() {
		key := bytes.Clone(r.Start)
		if !IsDirectHistoryKey(key) {
			t.Fatalf("partition start %q is not history", key)
		}
		if err := batch.Put(key, []byte("history")); err != nil {
			t.Fatal(err)
		}
	}
	hotKeys := [][]byte{headBlockKey, stateAccountLatestPrefix, stateKVLatestPrefix, stateCodePrefix, stageProgressKey(StageFinish), append(stageProgressKey(StageStateHistoryIndex), 'x')}
	for _, key := range hotKeys {
		directTestPut(t, batch, key, []byte("hot"))
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	for _, r := range DirectHistoryRanges() {
		directTestRead(t, history, r.Start, []byte("history"))
		directTestAbsent(t, hot, r.Start)
		directTestRead(t, db, r.Start, []byte("history"))
	}
	for _, key := range hotKeys {
		directTestRead(t, hot, key, []byte("hot"))
		directTestAbsent(t, history, key)
	}
	// Stale rows in the wrong physical store are never used as a fallback.
	key := append(bytes.Clone(stateChangeSetPrefix), 'z')
	directTestPut(t, hot, key, []byte("stale"))
	directTestAbsent(t, db, key)
	if err := db.Delete(DirectHistoryRanges()[0].Start); err != nil {
		t.Fatal(err)
	}
	directTestAbsent(t, history, DirectHistoryRanges()[0].Start)
}

func TestDirectHistoryMergedIteratorPrefixStart(t *testing.T) {
	db := NewDirectHistoryStore(NewMemoryDatabase(), NewMemoryDatabase())
	defer db.Close()
	keys := []string{"a", "stage-progress-v1-Finish", "stage-progress-v1-StateHistoryIndex", "stage-progress-v1-StateHistoryIndex-x", "state-account-latest-v1-a", "state-change-posting-v3-a", "state-changeset-v2-a", "state-changeset-v2-z", "state-tx-range-v1-a", "z"}
	for _, key := range keys {
		directTestPut(t, db, []byte(key), []byte(key))
	}
	directTestPut(t, db.hot, []byte("state-changeset-v2-stale"), []byte("stale"))
	directTestPut(t, db.history, []byte("unknown"), []byte("stale"))
	for _, tc := range []struct{ prefix, start string }{{"", ""}, {"", "state"}, {"stage-progress-v1-", "StateHistoryIndex"}, {"state-", "change"}, {"state-changeset-v2-", "b"}, {"z", ""}} {
		t.Run(tc.prefix+"/"+tc.start, func(t *testing.T) {
			var want []string
			for _, key := range keys {
				if bytes.HasPrefix([]byte(key), []byte(tc.prefix)) && key >= tc.prefix+tc.start {
					want = append(want, key)
				}
			}
			got := directTestKeys(t, db, []byte(tc.prefix), []byte(tc.start))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("keys=%q want=%q", got, want)
			}
			key, value, present, err := db.SeekPrefix([]byte(tc.prefix), []byte(tc.start))
			if err != nil || present != (len(want) > 0) || present && (string(key) != want[0] || string(value) != want[0]) {
				t.Fatalf("seek=%q,%q,%v,%v", key, value, present, err)
			}
		})
	}
}

func TestDirectHistoryDeleteRangeAndBatchReplace(t *testing.T) {
	db := NewDirectHistoryStore(NewMemoryDatabase(), NewMemoryDatabase())
	defer db.Close()
	keys := []string{"a", "stage-progress-v1-Finish", "stage-progress-v1-StateHistoryIndex", "stage-progress-v1-StateHistoryIndex-x", "state-account-latest-v1-a", "state-change-posting-v3-a", "state-changeset-v2-a", "state-tx-range-v1-a", "z"}
	for _, key := range keys {
		directTestPut(t, db, []byte(key), []byte("old"))
	}
	batch := db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange([]byte("stage-progress-v1-StateHistoryIndex"), []byte("state-tx")); err != nil {
		t.Fatal(err)
	}
	directTestPut(t, batch, []byte("state-changeset-v2-a"), []byte("replacement"))
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	want := []string{"a", "stage-progress-v1-Finish", "state-changeset-v2-a", "state-tx-range-v1-a", "z"}
	if got := directTestKeys(t, db, nil, nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("keys=%q want=%q", got, want)
	}
	directTestRead(t, db, []byte("state-changeset-v2-a"), []byte("replacement"))
	// Exact singleton range must preserve the similarly named hot stage row.
	directTestPut(t, db, stageProgressKey(StageStateHistoryIndex), []byte("index"))
	sibling := append(stageProgressKey(StageStateHistoryIndex), 'x')
	directTestPut(t, db, sibling, []byte("sibling"))
	key := stageProgressKey(StageStateHistoryIndex)
	if err := db.DeleteRange(key, append(bytes.Clone(key), 0)); err != nil {
		t.Fatal(err)
	}
	directTestAbsent(t, db, key)
	directTestRead(t, db, sibling, []byte("sibling"))
	if err := db.DeleteRange(nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := directTestKeys(t, db, nil, nil); len(got) != 0 {
		t.Fatalf("remaining keys %q", got)
	}
}

type directRecordingDB struct {
	ethdb.KeyValueStore
	name                       string
	events                     *[]string
	writeErr, syncErr          error
	writeEntered, writeRelease chan struct{}
}

func (d *directRecordingDB) NewBatch() ethdb.Batch {
	return &directRecordingBatch{Batch: d.KeyValueStore.NewBatch(), db: d}
}
func (d *directRecordingDB) NewBatchWithSize(size int) ethdb.Batch {
	return &directRecordingBatch{Batch: d.KeyValueStore.NewBatchWithSize(size), db: d}
}
func (d *directRecordingDB) SyncKeyValue() error {
	*d.events = append(*d.events, d.name+".Sync")
	return d.syncErr
}
func (d *directRecordingDB) NewKeyValueSnapshot() (pointread.KeyValueSnapshot, error) {
	return d.KeyValueStore.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
}

type directRecordingBatch struct {
	ethdb.Batch
	db *directRecordingDB
}

func (b *directRecordingBatch) Write() error {
	*b.db.events = append(*b.db.events, b.db.name+".Write")
	if b.db.writeErr != nil {
		return b.db.writeErr
	}
	if err := b.Batch.Write(); err != nil {
		return err
	}
	if b.db.writeEntered != nil {
		close(b.db.writeEntered)
		<-b.db.writeRelease
	}
	return nil
}
func TestDirectHistoryWriteDurabilityOrder(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprintf("split=%t", split), func(t *testing.T) {
			testDirectHistoryWriteDurabilityOrder(t, split)
		})
	}
}

func testDirectHistoryWriteDurabilityOrder(t *testing.T, split bool) {
	sentinel := errors.New("injected")
	for _, failure := range []string{"", "write", "sync"} {
		t.Run(failure, func(t *testing.T) {
			var events []string
			hot := &directRecordingDB{KeyValueStore: NewMemoryDatabase(), name: "hot", events: &events}
			history := &directRecordingDB{KeyValueStore: NewMemoryDatabase(), name: "history", events: &events}
			if failure == "write" {
				history.writeErr = sentinel
			}
			if failure == "sync" {
				history.syncErr = sentinel
			}
			db := NewDirectHistoryStore(hot, history)
			defer db.Close()
			var batch ethdb.Batch
			if split {
				batch = db.NewSplitBatchWithSize(128, 128)
			} else {
				batch = db.NewBatch()
			}
			defer batch.Close()
			directTestPut(t, batch, headBlockKey, []byte("new"))
			directTestPut(t, batch, stateChangeSetPrefix, []byte("new"))
			err := batch.Write()
			want := []string{"history.Write"}
			if failure != "write" {
				want = append(want, "history.Sync")
			}
			if failure == "" {
				want = append(want, "hot.Write")
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, sentinel) {
				t.Fatalf("error=%v", err)
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events=%v want=%v", events, want)
			}
			if failure != "" {
				directTestAbsent(t, hot, headBlockKey)
			}
		})
	}
}

func directTestPebble(t *testing.T, path string) ethdb.KeyValueStore {
	t.Helper()
	options := DefaultPebbleOptions()
	options.MemTableSizeBytes = 1 << 20
	db, err := NewPebbleDBWithOptions(path, 16, 16, options)
	if err != nil {
		t.Fatal(err)
	}
	return db
}
func TestDirectHistorySnapshotCoherenceDuringMixedWrite(t *testing.T) {
	var events []string
	hot := directTestPebble(t, filepath.Join(t.TempDir(), "hot"))
	history := &directRecordingDB{KeyValueStore: directTestPebble(t, filepath.Join(t.TempDir(), "history")), name: "history", events: &events, writeEntered: make(chan struct{}), writeRelease: make(chan struct{})}
	db := NewDirectHistoryStore(hot, history)
	defer db.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(history.writeRelease) }) }
	defer release()
	directTestPut(t, db, headBlockKey, []byte("old"))
	directTestPut(t, db, stateChangeSetPrefix, []byte("old"))
	before, err := db.NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	batch := db.NewBatch()
	defer batch.Close()
	directTestPut(t, batch, headBlockKey, []byte("new"))
	directTestPut(t, batch, stateChangeSetPrefix, []byte("new"))
	written := make(chan error, 1)
	go func() { written <- batch.Write() }()
	<-history.writeEntered
	// The history write is visible in its engine, while hot is still old.
	directTestRead(t, history, stateChangeSetPrefix, []byte("new"))
	directTestRead(t, hot, headBlockKey, []byte("old"))
	type snapshotResult struct {
		snapshot pointread.KeyValueSnapshot
		err      error
	}
	taken := make(chan snapshotResult, 1)
	started := make(chan struct{})
	go func() { close(started); s, e := db.NewKeyValueSnapshot(); taken <- snapshotResult{s, e} }()
	<-started
	select {
	case result := <-taken:
		if result.snapshot != nil {
			result.snapshot.Close()
		}
		t.Fatal("snapshot crossed a half-written batch")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	result := <-taken
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.snapshot.Close()
	for _, key := range [][]byte{headBlockKey, stateChangeSetPrefix} {
		directTestRead(t, before, key, []byte("old"))
		directTestRead(t, result.snapshot, key, []byte("new"))
	}
	if got := directTestKeys(t, result.snapshot, nil, nil); len(got) != 2 {
		t.Fatalf("snapshot keys=%q", got)
	}
}

func TestDirectHistoryPebbleReorgAndPointSnapshots(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprintf("split=%t", split), func(t *testing.T) {
			testDirectHistoryPebbleReorgAndPointSnapshots(t, split)
		})
	}
}

func testDirectHistoryPebbleReorgAndPointSnapshots(t *testing.T, split bool) {
	dir := t.TempDir()
	hotPath, historyPath := filepath.Join(dir, "hot"), filepath.Join(dir, "history")
	db := NewDirectHistoryStore(directTestPebble(t, hotPath), directTestPebble(t, historyPath))
	key := append(bytes.Clone(stateChangeSetPrefix), 'a')
	directTestPut(t, db, headBlockKey, []byte("old"))
	directTestPut(t, db, key, []byte("old"))
	snapshot, err := db.NewPointReadSnapshotWithCapacity(2)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := snapshot.NewCursor(nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := db.NewPointReadView()
	if err != nil {
		t.Fatal(err)
	}
	var batch ethdb.Batch
	if split {
		batch = db.NewSplitBatchWithSize(128, 128)
	} else {
		batch = db.NewBatchWithSize(128)
	}
	strings := batch.(interface {
		PutString(string, []byte) error
		DeleteString(string) error
	})
	if err := strings.DeleteString(string(key)); err != nil {
		t.Fatal(err)
	}
	if err := strings.PutString(string(key), []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	directTestPut(t, batch, headBlockKey, []byte("replacement"))
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	for _, k := range [][]byte{headBlockKey, key} {
		found, err := cursor.View(k, func(v []byte) error {
			if string(v) != "old" {
				t.Errorf("snapshot value=%q", v)
			}
			return nil
		})
		if err != nil || !found {
			t.Fatalf("cursor read=%v,%v", found, err)
		}
		if err := view.Get(k, func(v []byte) error {
			if string(v) != "replacement" {
				t.Errorf("live view=%q", v)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = NewDirectHistoryStore(directTestPebble(t, hotPath), directTestPebble(t, historyPath))
	defer db.Close()
	directTestRead(t, db, key, []byte("replacement"))
	directTestRead(t, db, headBlockKey, []byte("replacement"))
	if err := db.Compact([]byte("state-"), []byte("state.")); err != nil {
		t.Fatal(err)
	}
}

func TestDirectHistorySchemaConversionHelpers(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	if initialized, err := DirectHistoryLayoutInitialized(db); err != nil || initialized {
		t.Fatalf("empty initialized=%v,%v", initialized, err)
	}
	if err := MarkDirectHistoryLayout(db); err != nil {
		t.Fatal(err)
	}
	if initialized, err := DirectHistoryLayoutInitialized(db); err != nil || !initialized {
		t.Fatalf("initialized=%v,%v", initialized, err)
	}
	directTestPut(t, db, directHistoryLayoutKey, []byte("unknown"))
	if _, err := DirectHistoryLayoutInitialized(db); err == nil {
		t.Fatal("accepted unknown marker")
	}
	key := historyStagingPayloadKey(7, stateTxRangeKey(9))
	decoded, err := DecodeLegacyHistoryPayloadKey(7, key)
	if err != nil || !bytes.Equal(decoded, stateTxRangeKey(9)) {
		t.Fatalf("decode=%q,%v", decoded, err)
	}
	for _, bad := range [][]byte{key, historyStagingPayloadKey(8, headBlockKey), LegacyHistoryPayloadPrefix(8)} {
		if _, err := DecodeLegacyHistoryPayloadKey(8, bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	ranges := DirectHistoryRanges()
	ranges[0].Start[0] ^= 0xff
	if !IsDirectHistoryKey(DirectHistoryRanges()[0].Start) {
		t.Fatal("ranges alias schema")
	}
}

type directSizedBatchDB struct {
	ethdb.KeyValueStore
	sizes   []int
	unsized int
}

func (d *directSizedBatchDB) NewBatch() ethdb.Batch {
	d.unsized++
	return d.KeyValueStore.NewBatch()
}
func (d *directSizedBatchDB) NewBatchWithSize(size int) ethdb.Batch {
	d.sizes = append(d.sizes, size)
	return d.KeyValueStore.NewBatchWithSize(size)
}
func TestDirectHistorySplitBatchSizeHints(t *testing.T) {
	for _, sizes := range [][2]int{{12345, 67890}, {12345, 0}, {0, 67890}, {0, 0}} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			hot := &directSizedBatchDB{KeyValueStore: NewMemoryDatabase()}
			history := &directSizedBatchDB{KeyValueStore: NewMemoryDatabase()}
			db := NewDirectHistoryStore(hot, history)
			defer db.Close()
			batch := db.NewSplitBatchWithSize(sizes[0], sizes[1])
			defer batch.Close()
			for side, engine := range []*directSizedBatchDB{hot, history} {
				if sizes[side] > 0 {
					if !reflect.DeepEqual(engine.sizes, []int{sizes[side]}) || engine.unsized != 0 {
						t.Fatalf("side=%d sizes=%v unsized=%d", side, engine.sizes, engine.unsized)
					}
				} else if len(engine.sizes) != 0 || engine.unsized != 1 {
					t.Fatalf("empty side=%d sizes=%v unsized=%d", side, engine.sizes, engine.unsized)
				}
			}
			// Hints do not restrict routing, replay, or reuse, even if an initially
			// empty side later receives records. Put still copies caller-owned bytes.
			value := []byte("first")
			directTestPut(t, batch, headBlockKey, value)
			directTestPut(t, batch, stateChangeSetPrefix, value)
			value[0] = 'X'
			if batch.ValueSize() == 0 {
				t.Fatal("empty logical size")
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			directTestRead(t, hot, headBlockKey, []byte("first"))
			directTestRead(t, history, stateChangeSetPrefix, []byte("first"))
			replay := NewMemoryDatabase()
			defer replay.Close()
			if err := batch.Replay(replay); err != nil {
				t.Fatal(err)
			}
			directTestRead(t, replay, headBlockKey, []byte("first"))
			directTestRead(t, replay, stateChangeSetPrefix, []byte("first"))
			batch.Reset()
			if batch.ValueSize() != 0 {
				t.Fatal("reset logical size")
			}
			if err := batch.Delete(headBlockKey); err != nil {
				t.Fatal(err)
			}
			if err := batch.Delete(stateChangeSetPrefix); err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			directTestAbsent(t, hot, headBlockKey)
			directTestAbsent(t, history, stateChangeSetPrefix)
		})
	}
}
