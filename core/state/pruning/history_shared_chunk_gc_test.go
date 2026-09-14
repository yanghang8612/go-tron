package pruning

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

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
	return newSharedGCWorkerBuckets(t, 1)
}

func newSharedGCWorkerBuckets(t *testing.T, buckets uint64) (Worker, ethdb.KeyValueStore) {
	t.Helper()
	db, err := rawdb.NewPebbleDB(t.TempDir(), 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	first, last := uint64(1024), (buckets+1)*1024-1
	for block := first; block <= last; block++ {
		change, _, _ := writeSnapPruningChange(t, db, block, block*10, block*10+2)
		change.BlockHash = common.Hash{1, byte(block)}
		if err := rawdb.WriteStateTxRange(db, block, change.BlockHash, block*10, block*10+2); err != nil {
			t.Fatal(err)
		}
		if block%rawdb.StateHistoryChunkBucketBlocks == 0 {
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
	if err != nil || len(page.Buckets) != int(min(buckets, 4)) || page.Buckets[0] != 1 {
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

func TestWorkerSharedChunkGCQueuedBudgetAndMetrics(t *testing.T) {
	for _, mode := range []string{"accepted", "index-busy", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			w, _ := newSharedGCWorkerBuckets(t, 5)
			var queued, tried []uint64
			w.HistoryRangeGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
				tried = append(tried, through)
				return true, work()
			}
			if mode != "disabled" {
				w.HistoryRangeQueuedGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
					queued = append(queued, through)
					if mode == "index-busy" {
						return false, nil
					}
					return true, work()
				}
			}
			stats, err := w.PruneTo(6146)
			if err != nil {
				t.Fatal(err)
			}
			got := stats.HistoryChunkGC
			wantTried, wantQueued, wantRetired := []uint64{3071, 4095, 5119}, []uint64{2047}, uint64(4)
			if mode == "disabled" {
				wantTried, wantQueued = []uint64{2047, 3071, 4095, 5119}, nil
			} else if mode == "index-busy" {
				wantRetired = 3
			}
			if !slices.Equal(tried, wantTried) || !slices.Equal(queued, wantQueued) || got.Candidates != 4 || got.Retired != wantRetired || got.Errors != 0 {
				t.Fatalf("queued=%v tried=%v stats=%+v", queued, tried, got)
			}
			if got.QueuedAttempts != uint64(len(wantQueued)) || got.QueuedErrors != 0 || got.QueuedWorkMaxNanos > got.QueuedWorkNanos {
				t.Fatalf("queue accounting=%+v", got)
			}
			if mode == "accepted" && (got.QueuedAdmitted != 1 || got.QueuedBusy != 0 || got.QueuedWorkNanos == 0) {
				t.Fatalf("admitted accounting=%+v", got)
			}
			if mode == "index-busy" && (got.Busy != 1 || got.QueuedBusy != 1 || got.QueuedAdmitted != 0 || got.QueuedWorkNanos != 0) {
				t.Fatalf("busy accounting=%+v", got)
			}
			// The independent page limit remains four. A later pass grants a
			// fresh, single queue allowance to the fifth bucket.
			queued, tried = nil, nil
			second, err := w.PruneTo(6146)
			if err != nil || second.HistoryChunkGC.Candidates != 1 || second.HistoryChunkGC.QueuedAttempts != uint64(len(wantQueued)) {
				t.Fatalf("next page=%+v err=%v", second.HistoryChunkGC, err)
			}
			if mode != "disabled" && !slices.Equal(queued, []uint64{6143}) {
				t.Fatalf("next queue=%v", queued)
			}
			total := w.historyChunkGC.stats()
			if total.QueuedAttempts != got.QueuedAttempts+second.HistoryChunkGC.QueuedAttempts || total.QueuedWorkNanos != got.QueuedWorkNanos+second.HistoryChunkGC.QueuedWorkNanos || total.QueuedWorkMaxNanos != max(got.QueuedWorkMaxNanos, second.HistoryChunkGC.QueuedWorkMaxNanos) {
				t.Fatalf("cumulative queue accounting=%+v", total)
			}
			gauges := newHistoryChunkGCMetrics(t.Name() + "/")
			updateHistoryChunkGCMetrics(gauges, true, total)
			for name, want := range map[string]uint64{"queued_attempts": total.QueuedAttempts, "queued_admitted": total.QueuedAdmitted, "queued_busy": total.QueuedBusy, "queued_errors": total.QueuedErrors, "queued_work_total_ns": total.QueuedWorkNanos, "queued_work_max_ns": total.QueuedWorkMaxNanos} {
				if got := gauges[name].Snapshot().Value(); got != int64(want) {
					t.Fatalf("%s=%d want=%d", name, got, want)
				}
			}
		})
	}
}

func TestWorkerSharedChunkGCQueueRequiresEntireCoverage(t *testing.T) {
	w, db := newSharedGCWorkerBuckets(t, 2)
	coverage, err := w.newSnapshotStateDomainChangeCoverageGate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.DeleteStateTxRange(db, 1536); err != nil {
		t.Fatal(err)
	}
	var queued []uint64
	w.HistoryRangeGuard = func(context.Context, uint64, func() error) (bool, error) {
		t.Fatal("missing coverage consumed queue allowance")
		return false, nil
	}
	w.HistoryRangeQueuedGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
		queued = append(queued, through)
		return true, work()
	}
	got := w.pruneHistorySharedChunks(context.Background(), coverage, 3074)
	if !slices.Equal(queued, []uint64{3071}) || got.NotCovered != 1 || got.NonEmpty != 1 || got.QueuedAttempts != 1 || got.Retired != 0 || got.Errors != 0 {
		t.Fatalf("covered admission=%v stats=%+v", queued, got)
	}
}

func TestWorkerSharedChunkGCQueueContentionAndCancellation(t *testing.T) {
	for _, mode := range []string{"handoff", "cancel", "proof-changed"} {
		t.Run(mode, func(t *testing.T) {
			w, db := newSharedGCWorker(t)
			var chain sync.Mutex
			chain.Lock()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(chain.Unlock) }
			defer release()
			entered := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			proofErr := errors.New("canonical proof changed while waiting")
			proofChanged := false // written under chain; observed after handoff
			w.HistoryRangeGuard = func(context.Context, uint64, func() error) (bool, error) {
				t.Error("queued candidate fell back to opportunistic guard")
				return false, nil
			}
			w.HistoryRangeQueuedGuard = func(ctx context.Context, through uint64, work func() error) (bool, error) {
				close(entered)
				chain.Lock()
				defer chain.Unlock()
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if proofChanged {
					return false, proofErr
				}
				return true, work()
			}
			type outcome struct {
				stats Stats
				err   error
			}
			done := make(chan outcome, 1)
			go func() {
				stats, err := w.PruneToContext(ctx, 2050)
				done <- outcome{stats, err}
			}()
			joined := false
			defer func() {
				cancel()
				release()
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("queued GC goroutine did not finish")
					}
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("GC did not queue")
			}
			select {
			case result := <-done:
				joined = true
				t.Fatalf("GC escaped held writer mutex: %+v", result)
			default:
			}
			// Normal hot deletions are committed before GC waits, but chunks
			// are still available until the protected retirement can run.
			page, err := rawdb.ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, 2050, 64, 4)
			if err != nil || len(page.Buckets) != 1 {
				t.Fatalf("retired before handoff: %+v %v", page, err)
			}
			if mode == "cancel" {
				cancel()
			} else if mode == "proof-changed" {
				proofChanged = true
			}
			release()
			var result outcome
			select {
			case result = <-done:
				joined = true
			case <-time.After(5 * time.Second):
				t.Fatal("GC did not finish after handoff")
			}
			got := w.historyChunkGC.stats()
			if mode == "handoff" {
				if result.err != nil || got.Retired != 1 || got.QueuedAdmitted != 1 || got.QueuedErrors != 0 {
					t.Fatalf("handoff result=%+v GC=%+v", result, got)
				}
			} else {
				if got.Retired != 0 || got.QueuedAdmitted != 0 || got.QueuedErrors != 1 || got.QueuedWorkNanos != 0 || len(w.historyChunkGC.cursor) != 0 {
					t.Fatalf("failed handoff published retirement/cursor: %+v", got)
				}
				if mode == "cancel" && (!errors.Is(result.err, context.Canceled) || result.stats != (Stats{})) {
					t.Fatalf("cancellation published pass: %+v", result)
				}
				if mode == "proof-changed" && (result.err != nil || result.stats.HistoryChunkGC.LastError != proofErr.Error()) {
					t.Fatalf("optional GC error blocked normal prune: %+v", result)
				}
				w.HistoryRangeQueuedGuard = func(_ context.Context, _ uint64, work func() error) (bool, error) { return true, work() }
				retry, err := w.PruneTo(2050)
				if err != nil || retry.HistoryChunkGC.Retired != 1 {
					t.Fatalf("failed handoff retry=%+v %v", retry, err)
				}
			}
			if !chain.TryLock() {
				t.Fatal("GC leaked the writer mutex")
			}
			chain.Unlock()
		})
	}
}

type sharedGCQueuedFailStore struct {
	ethdb.KeyValueStore
	err error
}

func (s *sharedGCQueuedFailStore) NewBatch() ethdb.Batch {
	return &sharedGCQueuedFailBatch{Batch: s.KeyValueStore.NewBatch(), store: s}
}

type sharedGCQueuedFailBatch struct {
	ethdb.Batch
	store *sharedGCQueuedFailStore
}

func (b *sharedGCQueuedFailBatch) Close() {
	if closer, ok := b.Batch.(interface{ Close() }); ok {
		closer.Close()
	}
}

func (b *sharedGCQueuedFailBatch) Write() error {
	if b.store.err != nil {
		return b.store.err
	}
	return b.Batch.Write()
}

func TestWorkerSharedChunkGCQueuedRetirementFailureIsRetryable(t *testing.T) {
	w, db := newSharedGCWorker(t)
	store := &sharedGCQueuedFailStore{KeyValueStore: db}
	w.DB = store
	injected := errors.New("retirement batch failed")
	w.HistoryRangeGuard = func(context.Context, uint64, func() error) (bool, error) {
		t.Fatal("failed queued retirement fell back")
		return false, nil
	}
	w.HistoryRangeQueuedGuard = func(_ context.Context, _ uint64, work func() error) (bool, error) {
		// Inject only in the retirement callback, after ordinary history
		// deletes were flushed; later progress writes remain available.
		store.err = injected
		defer func() { store.err = nil }()
		return true, work()
	}
	first, err := w.PruneTo(2050)
	got := first.HistoryChunkGC
	if err != nil || first.DeletedDomainChangeBlocks != 1024 || got.Retired != 0 || got.Errors != 1 || got.QueuedAttempts != 1 || got.QueuedAdmitted != 1 || got.QueuedErrors != 1 || got.QueuedWorkNanos == 0 || len(w.historyChunkGC.cursor) != 0 {
		t.Fatalf("retirement failure=%+v %v", first, err)
	}
	page, err := rawdb.ScanStateHistoryChunkGCBuckets(context.Background(), db, nil, 2050, 64, 4)
	if err != nil || len(page.Buckets) != 1 {
		t.Fatalf("failed batch published retired marker: %+v %v", page, err)
	}
	w.HistoryRangeQueuedGuard = func(_ context.Context, _ uint64, work func() error) (bool, error) { return true, work() }
	second, err := w.PruneTo(2050)
	if err != nil || second.DeletedDomainChangeBlocks != 0 || second.HistoryChunkGC.Retired != 1 || second.HistoryChunkGC.QueuedErrors != 0 {
		t.Fatalf("retry=%+v %v", second, err)
	}
}
