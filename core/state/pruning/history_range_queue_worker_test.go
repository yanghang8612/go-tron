package pruning

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func newHistoryRangeQueueWorker(t *testing.T, count uint64) (Worker, *pruneBatchCountingStore) {
	t.Helper()
	base := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = base.Close() })
	db := &pruneBatchCountingStore{KeyValueStore: base}
	dir := t.TempDir()
	for block := uint64(1); block <= count; block++ {
		writeSnapPruningChange(t, base, block, block*10, block*10+2)
	}
	if err := rawdb.WriteStageProgress(base, rawdb.StageStateHistoryIndex, count); err != nil {
		t.Fatal(err)
	}
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(base, dir, 10, count*10+2, "history/state-domain-change-queued-range.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, count*10+2, refs)); err != nil {
		t.Fatal(err)
	}
	return Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir, HistoryRangePrune: true}, db
}

func assertHistoryQueueBlockPresent(t *testing.T, db rawdb.StateKVLatestStore, block uint64, want bool) {
	t.Helper()
	if _, exists, err := rawdb.ReadStateDomainChange(db, block, 1); err != nil || exists != want {
		t.Fatalf("block %d exists=%v want=%v err=%v", block, exists, want, err)
	}
}

func TestWorkerHistoryRangeQueuedGuardBudgetAndBusy(t *testing.T) {
	for _, mode := range []string{"accepted", "index_busy", "try_only"} {
		t.Run(mode, func(t *testing.T) {
			worker, db := newHistoryRangeQueueWorker(t, 600)
			var queued, tried []uint64
			workAndVerify := func(through uint64, work func() error) (bool, error) {
				if err := work(); err != nil {
					return true, err
				}
				// The durable deletion must finish before either guard releases.
				assertHistoryQueueBlockPresent(t, db, through, false)
				return true, nil
			}
			worker.HistoryRangeGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
				tried = append(tried, through)
				return workAndVerify(through, work)
			}
			if mode != "try_only" {
				worker.HistoryRangeQueuedGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
					queued = append(queued, through)
					if mode == "index_busy" {
						return false, nil
					}
					return workAndVerify(through, work)
				}
			}
			stats, err := worker.PruneTo(602)
			if err != nil {
				t.Fatal(err)
			}
			wantQueued, wantTried := []uint64{64, 128, 192, 256}, []uint64{512, 600}
			wantRuns, wantRows, wantFallback := uint64(6), uint64(600), uint64(0)
			switch mode {
			case "index_busy":
				wantRuns, wantRows, wantFallback = 2, 344, 256
			case "try_only":
				wantQueued, wantTried, wantRuns = nil, []uint64{256, 512, 600}, 3
			}
			if !slices.Equal(queued, wantQueued) || !slices.Equal(tried, wantTried) {
				t.Fatalf("queued=%v tried=%v; want queued=%v tried=%v", queued, tried, wantQueued, wantTried)
			}
			if stats.DeletedDomainChangeBlocks != 600 || stats.HistoryDeletes.RangeRuns != wantRuns || stats.HistoryDeletes.RangeRows != wantRows || stats.HistoryRangeFallbackBlocks != wantFallback {
				t.Fatalf("stats=%+v", stats)
			}
			for _, block := range []uint64{1, 64, 65, 256, 257, 512, 513, 600} {
				assertHistoryQueueBlockPresent(t, db, block, false)
			}
			if db.directDeletes != 0 {
				t.Fatalf("direct deletes bypassed batches: %d", db.directDeletes)
			}
		})
	}
}

func TestWorkerHistoryRangeQueuedGuardFailuresDoNotPublish(t *testing.T) {
	for _, mode := range []string{"proof", "write", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			worker, db := newHistoryRangeQueueWorker(t, 192)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("queued range failed")
			wantErr := injected
			calls, tries := 0, 0
			worker.HistoryRangeGuard = func(context.Context, uint64, func() error) (bool, error) {
				tries++
				return false, nil
			}
			worker.HistoryRangeQueuedGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
				calls++
				if calls == 2 {
					switch mode {
					case "proof":
						return false, injected
					case "write":
						db.writeErr = injected
					case "cancel":
						cancel()
						wantErr = context.Canceled
						return false, ctx.Err()
					}
				}
				return true, work()
			}
			stats, err := worker.PruneToContext(ctx, 194)
			if !errors.Is(err, wantErr) || stats != (Stats{}) || calls != 2 || tries != 0 {
				t.Fatalf("stats=%+v queued=%d tried=%d err=%v want=%v", stats, calls, tries, err, wantErr)
			}
			// An earlier successful chunk is retry-safe, but the failed chunk
			// must neither fall back to points nor publish pass progress.
			assertHistoryQueueBlockPresent(t, db, 64, false)
			assertHistoryQueueBlockPresent(t, db, 65, true)
			assertHistoryQueueBlockPresent(t, db, 128, true)
			assertHistoryQueueBlockPresent(t, db, 192, true)
			for _, stage := range []rawdb.StageID{rawdb.StageSnapshotHotPrune, rawdb.StageSnapshotPrune} {
				if _, exists, err := rawdb.ReadStageProgress(db, stage); err != nil || exists {
					t.Fatalf("failed pass published %s: exists=%v err=%v", stage, exists, err)
				}
			}
			manifest, err := snapshots.LoadProductionManifest(worker.SnapshotDir)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Progress != nil && (manifest.Progress.HotPruneBlockNum != 0 || manifest.Progress.HotPruneTxNum != 0) {
				t.Fatalf("failed pass published manifest progress: %+v", manifest.Progress)
			}
		})
	}
}

func TestWorkerHistoryRangeQueuedGuardBudgetSurvivesCallbacks(t *testing.T) {
	worker, db := newHistoryRangeQueueWorker(t, 600)
	var queued, tried []uint64
	worker.HistoryRangeGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
		tried = append(tried, through)
		return true, work()
	}
	worker.HistoryRangeQueuedGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
		queued = append(queued, through)
		return true, work()
	}
	store, flush := newPruneBatchStoreWithRanges(db, maxPruneBatchValueSize, true)
	var stats Stats
	var deleted rawdb.StateDomainChangeDeleteStats
	deleter := worker.historyRangeBlockDeleter(context.Background(), flush, &stats, &deleted)
	if err := deleter(store, nil); err != nil {
		t.Fatal(err)
	}
	// The first short plan spends no queue allowance. Four later eligible
	// attempts exhaust the one pass budget, despite separate callback calls.
	first := uint64(1)
	for _, last := range []uint64{63, 191, 319, 600} {
		blocks := make([]uint64, last-first+1)
		for i := range blocks {
			blocks[i] = first + uint64(i)
		}
		if err := deleter(store, blocks); err != nil {
			t.Fatal(err)
		}
		assertHistoryQueueBlockPresent(t, db, last, false)
		first = last + 1
	}
	if !slices.Equal(queued, []uint64{127, 191, 255, 319}) || !slices.Equal(tried, []uint64{63, 575, 600}) {
		t.Fatalf("callback queue budget or short-tail behavior changed: queued=%v tried=%v", queued, tried)
	}
	if deleted.RangeRuns != 5 || deleted.RangeRows != 512 || deleted.PointRows != 88 {
		t.Fatalf("callback deletes=%+v", deleted)
	}
}

func TestWorkerHistoryRangeQueuedGuardStillRequiresTryGuard(t *testing.T) {
	worker, db := newHistoryRangeQueueWorker(t, 64)
	worker.HistoryRangeQueuedGuard = func(context.Context, uint64, func() error) (bool, error) {
		t.Fatal("queued guard ran without the required Try guard")
		return false, nil
	}
	if _, err := worker.PruneTo(66); err == nil {
		t.Fatal("queue-only worker silently lost its fallback guard")
	}
	assertHistoryQueueBlockPresent(t, db, 1, true)
}
