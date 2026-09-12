package pruning

import (
	"context"
	"errors"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type historyRangePruneTestChain struct{ *fakePruneChain }

func (c *historyRangePruneTestChain) TryWithStateDomainChangePruneGuard(ctx context.Context, through, head uint64, hash common.Hash, work func() error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if head != 66 || through != 64 || hash != (common.Hash{66}) {
		return false, errors.New("unexpected bound range proof")
	}
	return true, work()
}

func TestPrunerHistoryRangePruneMetrics(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	for block := uint64(1); block <= 64; block++ {
		writeSnapPruningChange(t, db, block, block*10, block*10+2)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 66, common.Hash{66}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageStateHistoryIndex, 64, common.Hash{64}); err != nil {
		t.Fatal(err)
	}
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 642, "history/state-domain-change-metrics.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 642, refs)); err != nil {
		t.Fatal(err)
	}
	chain := &historyRangePruneTestChain{&fakePruneChain{db: db, solidified: 66, canonicalHashes: map[uint64]common.Hash{66: {66}, 64: {64}}}}
	ns := "test/pruner/history-range-metrics/"
	p := NewPruner(chain, PrunerConfig{Policy: SnapPolicy(2, 1), SnapshotDir: dir, HistoryRangePrune: true, MetricsNamespace: ns})
	if _, err := p.PrunePass(); err != nil {
		t.Fatal(err)
	}
	stats := p.Stats()
	if !stats.HistoryRangePruneEnabled || stats.HistoryDeletes.RangeRuns != 1 || stats.HistoryDeletes.RangeRows != 64 || stats.Errors != 0 {
		t.Fatalf("pruner stats=%+v", stats)
	}
	assertPrunerGauge(t, ns+"history/delete/range/enabled", 1)
	assertPrunerGauge(t, ns+"history/delete/range/rows", 64)
	assertPrunerGauge(t, ns+"history/delete/range/logical_bytes", int64(stats.HistoryDeletes.RangeBytes))
	assertPrunerGauge(t, ns+"history/delete/range/fallback_blocks", 0)
	assertPrunerGauge(t, ns+"history/delete/range/work/total_ns", int64(stats.HistoryRangeGuardDuration))
	assertPrunerGauge(t, ns+"history/delete/range/work/max_ns", int64(stats.HistoryRangeGuardMaxDuration))
}

func TestHistoryRangePruneBatchCapabilityAndFailure(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = base.Close() })
	db := &pruneBatchCountingStore{KeyValueStore: base}
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := base.Put([]byte(key), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	type rangeWriter interface{ DeleteRange([]byte, []byte) error }
	plain, _ := newPruneBatchStoreWithRanges(db, 2, false)
	if _, ok := plain.(rangeWriter); ok {
		t.Fatal("default wrapper exposed range deletes")
	}
	store, flush := newPruneBatchStoreWithRanges(db, 2, true)
	ranges := store.(rangeWriter)
	if err := ranges.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	if ok, _ := base.Has([]byte("a")); !ok {
		t.Fatal("range delete bypassed batch")
	}
	if err := ranges.DeleteRange([]byte("b"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if db.batchWrites != 1 {
		t.Fatalf("bounded writes = %d, want 1", db.batchWrites)
	}
	if ok, _ := base.Has([]byte("a")); ok {
		t.Fatal("bounded flush did not delete first range")
	}
	if ok, _ := base.Has([]byte("b")); !ok {
		t.Fatal("second range flushed early")
	}
	injected := errors.New("range commit failed")
	db.writeErr = injected
	if err := flush(); !errors.Is(err, injected) {
		t.Fatalf("flush = %v", err)
	}
	if ok, _ := base.Has([]byte("b")); !ok {
		t.Fatal("failed batch changed data")
	}
	if db.directDeletes != 0 {
		t.Fatalf("direct deletes = %d", db.directDeletes)
	}
}

func TestWorkerHistoryRangePruneCoverageAndCommit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		enabled, fail bool
		covered       uint64
	}{{"enabled", true, false, 64}, {"default", false, false, 64}, {"coverage_gap", true, false, 63}, {"failed_commit", true, true, 64}} {
		t.Run(tc.name, func(t *testing.T) {
			base := rawdb.NewMemoryDatabase()
			t.Cleanup(func() { _ = base.Close() })
			db := &pruneBatchCountingStore{KeyValueStore: base}
			dir := t.TempDir()
			for block := uint64(1); block <= 66; block++ {
				writeSnapPruningChange(t, base, block, block*10, block*10+2)
			}
			if err := rawdb.WriteStageProgress(base, rawdb.StageStateHistoryIndex, 66); err != nil {
				t.Fatal(err)
			}
			refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(base, dir, 10, tc.covered*10+2, "history/state-domain-change-range.seg")
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, tc.covered*10+2, refs)); err != nil {
				t.Fatal(err)
			}
			worker := Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir, HistoryRangePrune: tc.enabled,
				HistoryRangeGuard: func(_ context.Context, _ uint64, work func() error) (bool, error) { return true, work() }}
			if tc.fail {
				db.writeErr = errors.New("commit failed")
			}
			stats, err := worker.PruneTo(66)
			if tc.fail {
				if err == nil || stats.HistoryDeletes != (rawdb.StateDomainChangeDeleteStats{}) {
					t.Fatalf("failed pass = %+v, %v", stats, err)
				}
				for _, stage := range []rawdb.StageID{rawdb.StageSnapshotHotPrune, rawdb.StageSnapshotPrune} {
					if _, exists, err := rawdb.ReadStageProgress(base, stage); err != nil || exists {
						t.Fatalf("failed pass published %s: %v, %v", stage, exists, err)
					}
				}
				if _, exists, err := rawdb.ReadStateDomainChange(base, 1, 1); err != nil || !exists {
					t.Fatalf("failed batch deleted history: %v, %v", exists, err)
				}
				db.writeErr = nil
				stats, err = worker.PruneTo(66)
			}
			if err != nil {
				t.Fatal(err)
			}
			if stats.DeletedDomainChangeBlocks != int(tc.covered) || stats.DeletedTxRanges != 0 {
				t.Fatalf("stats = %+v", stats)
			}
			if tc.enabled {
				if tc.covered >= 64 {
					if stats.HistoryDeletes.RangeRuns != 1 || stats.HistoryDeletes.RangeRows != 64 || stats.HistoryDeletes.RangeBytes == 0 || stats.HistoryDeletes.PointRows != 0 {
						t.Fatalf("range stats = %+v", stats.HistoryDeletes)
					}
				} else if stats.HistoryDeletes.RangeRuns != 0 || stats.HistoryDeletes.PointRows != tc.covered {
					t.Fatalf("short run stats = %+v", stats.HistoryDeletes)
				}
			} else if stats.HistoryDeletes != (rawdb.StateDomainChangeDeleteStats{}) {
				t.Fatalf("default stats changed: %+v", stats.HistoryDeletes)
			}
			for block := uint64(1); block <= 66; block++ {
				if _, exists, err := rawdb.ReadStateDomainChange(base, block, 1); err != nil || exists != (block > tc.covered) {
					t.Fatalf("block %d exists=%v err=%v", block, exists, err)
				}
				if _, exists, err := rawdb.ReadStateTxRange(base, block); err != nil || !exists {
					t.Fatalf("tx range %d lost: %v, %v", block, exists, err)
				}
			}
			if db.directDeletes != 0 {
				t.Fatalf("direct deletes = %d", db.directDeletes)
			}
		})
	}
}

func TestWorkerHistoryRangePruneGuardChunksAndFallback(t *testing.T) {
	for _, rejectProof := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy_fallback", true: "proof_failure"}[rejectProof], func(t *testing.T) {
			base := rawdb.NewMemoryDatabase()
			t.Cleanup(func() { _ = base.Close() })
			db := &pruneBatchCountingStore{KeyValueStore: base}
			dir := t.TempDir()
			for block := uint64(1); block <= 600; block++ {
				writeSnapPruningChange(t, base, block, block*10, block*10+2)
			}
			if err := rawdb.WriteStageProgress(base, rawdb.StageStateHistoryIndex, 600); err != nil {
				t.Fatal(err)
			}
			refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(base, dir, 10, 6002, "history/state-domain-change-chunks.seg")
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 6002, refs)); err != nil {
				t.Fatal(err)
			}
			calls := 0
			injected := errors.New("canonical proof changed")
			worker := Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir, HistoryRangePrune: true}
			worker.HistoryRangeGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
				calls++
				want := []uint64{256, 512, 600}[calls-1]
				if through != want {
					t.Fatalf("guard %d through=%d want=%d", calls, through, want)
				}
				if calls == 2 {
					if rejectProof {
						return false, injected
					}
					return false, nil
				}
				if err := work(); err != nil {
					return true, err
				}
				if _, exists, err := rawdb.ReadStateDomainChange(base, through, 1); err != nil || exists {
					t.Fatalf("range batch not flushed inside guard: %v %v", exists, err)
				}
				return true, nil
			}
			stats, err := worker.PruneTo(602)
			if rejectProof {
				if !errors.Is(err, injected) || stats.HistoryDeletes != (rawdb.StateDomainChangeDeleteStats{}) || calls != 2 {
					t.Fatalf("failed proof pass: stats=%+v calls=%d err=%v", stats, calls, err)
				}
				if _, exists, err := rawdb.ReadStageProgress(base, rawdb.StageSnapshotHotPrune); err != nil || exists {
					t.Fatalf("partial pass advanced progress: %v, %v", exists, err)
				}
				if _, exists, err := rawdb.ReadStateDomainChange(base, 257, 1); err != nil || !exists {
					t.Fatalf("proof failure fell back to deletes: %v, %v", exists, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if calls != 3 || stats.DeletedDomainChangeBlocks != 600 || stats.HistoryDeletes.RangeRuns != 2 || stats.HistoryDeletes.RangeRows != 344 || stats.HistoryRangeFallbackBlocks != 256 {
				t.Fatalf("chunked stats=%+v calls=%d", stats, calls)
			}
			if stats.HistoryRangeGuardDuration <= 0 || stats.HistoryRangeGuardMaxDuration > stats.HistoryRangeGuardDuration {
				t.Fatalf("guard work durations=%+v", stats)
			}
			if _, exists, err := rawdb.ReadStateDomainChange(base, 257, 1); err != nil || exists {
				t.Fatalf("busy fallback did not preserve old point behavior: %v, %v", exists, err)
			}
		})
	}
}
