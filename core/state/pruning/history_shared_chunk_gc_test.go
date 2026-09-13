package pruning

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// This scope is private to a fixture's one complete pack; Put queues into one
// atomic Pebble batch and reads see committed prior data. No test writes schema
// prefixes or manufactures a shared pack/meta: the production writer does it.
type sharedGCFixtureWriter struct {
	ethdb.KeyValueStore
	batch ethdb.Batch
}

func (w sharedGCFixtureWriter) Put(key, value []byte) error       { return w.batch.Put(key, value) }
func (sharedGCFixtureWriter) StateHistoryChunkWritesAtomic() bool { return true }

func newSharedGCWorker(t *testing.T) (Worker, ethdb.KeyValueStore) {
	t.Helper()
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	const first, last = uint64(1024), uint64(2047)
	for block := first; block <= last; block++ {
		change, _, _ := writeSnapPruningChange(t, db, block, block*10, block*10+2)
		change.BlockHash = common.Hash{1, byte(block)}
		if err := rawdb.WriteStateTxRange(db, block, change.BlockHash, block*10, block*10+2); err != nil {
			t.Fatal(err)
		}
		if block == first {
			rng := rand.New(rand.NewPCG(19, 29))
			piece := make([]byte, 128<<10)
			for i := range piece {
				piece[i] = byte(rng.Uint32())
			}
			change.Prev = bytes.Repeat(piece, 8)
			batch := db.NewBatch()
			scope := sharedGCFixtureWriter{KeyValueStore: db, batch: batch}
			rawdb.SetStateHistoryCrossBlockDedup(true)
			err := rawdb.WriteStateDomainChangeBlockRows(scope, []*rawdb.StateDomainChange{change})
			rawdb.SetStateHistoryCrossBlockDedup(false)
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
			if closer, ok := batch.(interface{ Close() }); ok {
				closer.Close()
			}
		}
	}
	page, err := rawdb.ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, last, 64, 4)
	if err != nil || len(page.Buckets) != 1 || page.Buckets[0] != 1 {
		t.Fatalf("production writer did not seed shared bucket: %+v %v", page, err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageStateHistoryIndex, last); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, first*10, last*10+2, "history/state-domain-change-shared-gc.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(first*10, last*10+2, refs)); err != nil {
		t.Fatal(err)
	}
	return Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir, HistorySharedChunkGC: true, historyChunkGC: &historyChunkGCState{}}, db
}

func TestWorkerSharedChunkGCAfterFlushAndBusyRetry(t *testing.T) {
	w, db := newSharedGCWorker(t)
	calls := 0
	w.HistoryRangeGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
		calls++
		if through != 2047 {
			t.Fatalf("guard boundary = %d", through)
		}
		// GC must see the committed pack deletion even when the old hot
		// pruner took its unguarded point path and its cursor has moved on.
		rows := 0
		if err := rawdb.IterateStateDomainChanges(db, 1024, func(*rawdb.StateDomainChange) (bool, error) { rows++; return true, nil }); err != nil {
			t.Fatal(err)
		}
		if rows != 0 {
			t.Fatal("GC invoked before pack-delete batch flush")
		}
		if calls == 1 {
			return false, nil
		}
		return true, work()
	}
	first, err := w.PruneTo(2050)
	if err != nil || first.HistoryChunkGC.Busy != 1 || first.HistoryChunkGC.Retired != 0 {
		t.Fatalf("first = %+v %v", first, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1024); err != nil || !ok {
		t.Fatalf("Snap removed coverage metadata %v %v", ok, err)
	}
	second, err := w.PruneTo(2050)
	if err != nil || second.HistoryChunkGC.Retired != 1 || second.DeletedDomainChangeBlocks != 0 {
		t.Fatalf("retry = %+v %v", second, err)
	}
	third, err := w.PruneTo(2050)
	if err != nil || third.HistoryChunkGC.Candidates != 0 || calls != 2 {
		t.Fatalf("retired revisit = %+v calls=%d %v", third, calls, err)
	}
}

func TestWorkerSharedChunkGCRetainsMissingProofAndRepairs(t *testing.T) {
	w, db := newSharedGCWorker(t)
	w.HistoryRangeGuard = func(_ context.Context, _ uint64, work func() error) (bool, error) { return true, work() }
	coverage, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	covered, err := w.historyChunkBucketCovered(context.Background(), coverage, 2050, 1024, 2047)
	if err != nil || !covered {
		t.Fatalf("complete bucket proof = %v %v", covered, err)
	}
	if err := rawdb.DeleteStateTxRange(db, 1536); err != nil {
		t.Fatal(err)
	}
	got := w.pruneHistorySharedChunks(context.Background(), coverage, 2050)
	if got.NotCovered != 1 || got.Retired != 0 || got.Errors != 0 {
		t.Fatalf("missing proof = %+v", got)
	}
	if err := rawdb.WriteStateTxRange(db, 1536, common.Hash{1}, 15360, 15362); err != nil {
		t.Fatal(err)
	}
	// Complete proof is still insufficient while any pack/repair remains.
	got = w.pruneHistorySharedChunks(context.Background(), coverage, 2050)
	if got.NonEmpty != 1 || got.Retired != 0 {
		t.Fatalf("live pack = %+v", got)
	}
}

func TestWorkerSharedChunkGCErrorDoesNotBlockNormalPrune(t *testing.T) {
	w, _ := newSharedGCWorker(t)
	w.HistoryRangeGuard = func(context.Context, uint64, func() error) (bool, error) { return false, errors.New("proof changed") }
	stats, err := w.PruneTo(2050)
	if err != nil || stats.DeletedDomainChangeBlocks != 1024 || stats.HistoryChunkGC.Errors != 1 || stats.HistoryChunkGC.LastError != "proof changed" {
		t.Fatalf("isolated GC failure = %+v %v", stats, err)
	}
	w.HistoryRangeGuard = func(_ context.Context, _ uint64, work func() error) (bool, error) { return true, work() }
	retry, err := w.PruneTo(2050)
	if err != nil || retry.HistoryChunkGC.Retired != 1 {
		t.Fatalf("error retry = %+v %v", retry, err)
	}
}

func TestWorkerSharedChunkGCRequiresLiveSafetyConfiguration(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	defer db.Close()
	for _, worker := range []Worker{
		{DB: db, Policy: FullPolicy(2, 1), HistorySharedChunkGC: true},
		{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: t.TempDir(), HistorySharedChunkGC: true},
	} {
		if _, err := worker.PruneTo(2050); err == nil {
			t.Fatal("accepted missing live GC proof")
		}
	}
}
