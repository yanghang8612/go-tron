package rawdb

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/pointread"
)

func seedSharedGCBucket(t *testing.T, db ethdb.KeyValueWriter, bucket uint64) []byte {
	t.Helper()
	key := append(stateHistoryChunkBucketPrefix(bucket), bytes.Repeat([]byte{0x2a}, 32)...)
	if err := db.Put(key, []byte("immutable chunk")); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(stateHistoryChunkBucketKey(bucket), []byte{1, 0}); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestStateHistoryChunkGCRetirementAtomicAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, err := NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	chunk := seedSharedGCBucket(t, db, 3)
	other := seedSharedGCBucket(t, db, 4)
	pack := stateChangeSetKey(3*StateHistoryChunkBucketBlocks+7, 0)
	if err := db.Put(pack, []byte("old pack")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.(pointread.KeyValueSnapshotter).NewKeyValueSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(pack); err != nil {
		t.Fatal(err)
	}
	got, err := RetireStateHistoryChunkBucket(context.Background(), db, 3)
	if err != nil || !got.Retired {
		t.Fatalf("retire = %+v %v", got, err)
	}
	if value, err := snapshot.Get(chunk); err != nil || string(value) != "immutable chunk" {
		t.Fatalf("old snapshot chunk = %q %v", value, err)
	}
	if value, err := snapshot.Get(pack); err != nil || string(value) != "old pack" {
		t.Fatalf("old snapshot pack = %q %v", value, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if ok, err := db.Has(chunk); err != nil || ok {
		t.Fatalf("live chunk survived %v %v", ok, err)
	}
	if ok, err := db.Has(other); err != nil || !ok {
		t.Fatalf("neighbor chunk lost %v %v", ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = NewPebbleDB(dir, 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	got, err = RetireStateHistoryChunkBucket(context.Background(), db, 3)
	if err != nil || !got.AlreadyRetired || got.Retired {
		t.Fatalf("reopen retry = %+v %v", got, err)
	}
	if ok, err := db.Has(chunk); err != nil || ok {
		t.Fatalf("reopened chunk survived %v %v", ok, err)
	}
}

func TestStateHistoryChunkGCKeepsAnyPhysicalHistory(t *testing.T) {
	first := uint64(2) * StateHistoryChunkBucketBlocks
	for _, key := range [][]byte{stateChangeSetKey(first, 0), stateChangeSetKey(first+1, 9), stateChangeSetBlockPrefix(first), append(stateChangeSetKey(first+1023, 0), 7)} {
		t.Run(string(key), func(t *testing.T) {
			db := NewMemoryDatabase()
			defer db.Close()
			chunk := seedSharedGCBucket(t, db, 2)
			if err := db.Put(key, []byte{9}); err != nil {
				t.Fatal(err)
			}
			got, err := RetireStateHistoryChunkBucket(context.Background(), db, 2)
			if err != nil || !got.NonEmpty || got.Retired {
				t.Fatalf("nonempty = %+v %v", got, err)
			}
			if ok, _ := db.Has(chunk); !ok {
				t.Fatal("deleted referenced chunk")
			}
		})
	}
}

func TestStateHistoryChunkGCScanBoundsAndRevisit(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	for bucket := uint64(1); bucket <= 7; bucket++ {
		seedSharedGCBucket(t, db, bucket)
	}
	if err := db.Put(stateHistoryChunkBucketKey(2), []byte{1, 1}); err != nil {
		t.Fatal(err)
	}
	first, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, 8*1024-1, 3, 4)
	if err != nil || first.Scanned != 3 || len(first.Buckets) != 2 || first.Buckets[0] != 1 || first.Buckets[1] != 3 || first.Complete {
		t.Fatalf("first = %+v %v", first, err)
	}
	second, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, first.Next, 8*1024-1, 64, 4)
	if err != nil || len(second.Buckets) != 4 || second.Buckets[0] != 4 || second.Buckets[3] != 7 {
		t.Fatalf("second = %+v %v", second, err)
	}
	end, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, second.Next, 8*1024-1, 64, 4)
	if err != nil || !end.Complete || len(end.Next) != 0 {
		t.Fatalf("end = %+v %v", end, err)
	}
	retry, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, end.Next, 8*1024-1, 64, 4)
	if err != nil || retry.Buckets[0] != 1 {
		t.Fatalf("busy bucket not revisited %+v %v", retry, err)
	}
	tail, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, 2046, 64, 4)
	if err != nil || !tail.Complete || len(tail.Buckets) != 0 {
		t.Fatalf("partial bucket = %+v %v", tail, err)
	}
}

type sharedGCFailStore struct {
	ethdb.KeyValueStore
	phase  string
	cancel context.CancelFunc
}

func (s *sharedGCFailStore) NewBatch() ethdb.Batch {
	return &sharedGCFailBatch{Batch: s.KeyValueStore.NewBatch(), store: s}
}
func (s *sharedGCFailStore) NewBatchWithSize(int) ethdb.Batch { return s.NewBatch() }

type sharedGCFailBatch struct {
	ethdb.Batch
	store *sharedGCFailStore
}

type sharedGCPinnedStore struct{ ethdb.KeyValueStore }

func (sharedGCPinnedStore) IsPinnedKeyValueView() bool { return true }

func TestStateHistoryChunkGCRejectsPinnedBatchWriter(t *testing.T) {
	db := NewMemoryDatabase()
	defer db.Close()
	chunk := seedSharedGCBucket(t, db, 1)
	if _, err := RetireStateHistoryChunkBucket(context.Background(), sharedGCPinnedStore{db}, 1); err == nil {
		t.Fatal("accepted pinned read view with unrelated current batch writer")
	}
	if ok, _ := db.Has(chunk); !ok {
		t.Fatal("rejected pinned view deleted chunks")
	}
}

var errSharedGCTest = errors.New("injected GC batch failure")

func (b *sharedGCFailBatch) DeleteRange(start, end []byte) error {
	if b.store.phase == "range" {
		return errSharedGCTest
	}
	return b.Batch.DeleteRange(start, end)
}
func (b *sharedGCFailBatch) Put(key, value []byte) error {
	if b.store.phase == "put" {
		return errSharedGCTest
	}
	if b.store.cancel != nil {
		b.store.cancel()
	}
	return b.Batch.Put(key, value)
}
func (b *sharedGCFailBatch) Write() error {
	if b.store.phase == "write" {
		return errSharedGCTest
	}
	if err := b.Batch.Write(); err != nil {
		return err
	}
	if b.store.phase == "ambiguous" {
		return errSharedGCTest
	}
	return nil
}

func TestStateHistoryChunkGCFailureAndCancellation(t *testing.T) {
	for _, phase := range []string{"range", "put", "write", "ambiguous", "cancel"} {
		t.Run(phase, func(t *testing.T) {
			base := NewMemoryDatabase()
			defer base.Close()
			chunk := seedSharedGCBucket(t, base, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &sharedGCFailStore{KeyValueStore: base, phase: phase}
			if phase == "cancel" {
				store.cancel = cancel
			}
			got, err := RetireStateHistoryChunkBucket(ctx, store, 1)
			if err == nil || got.Retired {
				t.Fatalf("failure = %+v %v", got, err)
			}
			exists, _ := base.Has(chunk)
			meta, _ := base.Get(stateHistoryChunkBucketKey(1))
			if phase == "ambiguous" {
				if exists || !bytes.Equal(meta, []byte{1, 1}) {
					t.Fatal("ambiguous batch was not atomic")
				}
			} else if !exists || !bytes.Equal(meta, []byte{1, 0}) {
				t.Fatal("uncommitted retirement changed data")
			}
			retry, err := RetireStateHistoryChunkBucket(context.Background(), base, 1)
			if err != nil || (!retry.Retired && !retry.AlreadyRetired) {
				t.Fatalf("retry = %+v %v", retry, err)
			}
		})
	}
}

func TestStateHistoryChunkGCRejectsInvalidMetadataAndArguments(t *testing.T) {
	for _, value := range [][]byte{nil, {1}, {2, 0}, {1, 2}, {1, 0, 0}} {
		db := NewMemoryDatabase()
		if err := db.Put(stateHistoryChunkBucketKey(1), value); err != nil {
			t.Fatal(err)
		}
		if _, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, 4096, 64, 4); err == nil {
			t.Fatalf("accepted metadata %x", value)
		}
		if _, err := RetireStateHistoryChunkBucket(context.Background(), db, 1); err == nil {
			t.Fatalf("retired metadata %x", value)
		}
		_ = db.Close()
	}
	if _, _, err := StateHistoryChunkBucketBounds(^uint64(0)); err == nil {
		t.Fatal("accepted overflowing bucket")
	}
	db := NewMemoryDatabase()
	defer db.Close()
	if _, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, []byte{1}, 4096, 64, 4); err == nil {
		t.Fatal("accepted bad cursor")
	}
	if _, err := ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, 4096, 0, 4); err == nil {
		t.Fatal("accepted unbounded scan")
	}
	if _, err := RetireStateHistoryChunkBucket(context.Background(), db, 1); err == nil {
		t.Fatal("retired absent metadata")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RetireStateHistoryChunkBucket(ctx, db, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
}

func TestResetMutableStateSharedReferenceGraphAtomic(t *testing.T) {
	for _, phase := range []string{"success", "range", "write", "ambiguous"} {
		t.Run(phase, func(t *testing.T) {
			base := NewMemoryDatabase()
			defer base.Close()
			chunk := seedSharedGCBucket(t, base, 1)
			pack := stateChangeSetKey(1024, 0)
			if err := base.Put(pack, []byte("shared pack")); err != nil {
				t.Fatal(err)
			}
			if err := base.Put(stateHistoryChunkBucketKey(2), []byte{1, 1}); err != nil {
				t.Fatal(err)
			}
			if err := WriteHistoryPruneMode(base, "snap"); err != nil {
				t.Fatal(err)
			}
			store := &sharedGCFailStore{KeyValueStore: base, phase: phase}
			err := ResetMutableState(store)
			if (err == nil) != (phase == "success") {
				t.Fatalf("reset %s = %v", phase, err)
			}
			wantPresent := phase == "range" || phase == "write"
			for _, key := range [][]byte{pack, chunk, stateHistoryChunkBucketKey(1), stateHistoryChunkBucketKey(2)} {
				if present, err := base.Has(key); err != nil || present != wantPresent {
					t.Fatalf("reference graph split at %x: %v %v", key, present, err)
				}
			}
			if mode, present, err := ReadHistoryPruneMode(base); err != nil || !present || mode != "snap" {
				t.Fatalf("capability metadata changed: %s %v %v", mode, present, err)
			}
		})
	}
}
