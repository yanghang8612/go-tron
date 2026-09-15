package pruning

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Discover the metadata prefix through the existing accessor, rather than
// introducing schema bytes into this fixture. Delay only that scan after arming.
type gcDensityScanStore struct {
	ethdb.KeyValueStore
	prefix []byte
	armed  bool
	scans  int
}

func (d *gcDensityScanStore) NewIterator(prefix, start []byte) ethdb.Iterator {
	if d.prefix == nil {
		d.prefix = bytes.Clone(prefix)
	}
	if d.armed && bytes.Equal(prefix, d.prefix) {
		d.scans++
		time.Sleep(20 * time.Millisecond)
	}
	return d.KeyValueStore.NewIterator(prefix, start)
}

func TestWorkerIndependentGCTimerIncludesScanProofAndGuard(t *testing.T) {
	w, db := newSharedGCWorker(t)
	// Prime only ordinary hot-row pruning. The independent old bucket remains,
	// so its first proof below cannot be confused with current hot-row work.
	w.HistorySharedChunkGC = false
	first, err := w.PruneTo(2050)
	if err != nil || first.DeletedDomainChangeBlocks != 1024 || first.HistoryChunkGCDuration != 0 {
		t.Fatalf("prime: %+v %v", first, err)
	}
	delayed := &gcDensityScanStore{KeyValueStore: db}
	page, err := rawdb.ScanStateHistoryChunkGCBuckets(context.Background(), delayed, nil, 2050, 64, 4)
	if err != nil || len(page.Buckets) != 1 {
		t.Fatalf("prefix discovery: %+v %v", page, err)
	}
	delayed.armed = true
	w.DB, w.HistorySharedChunkGC = delayed, true
	w.coverageVerificationCache = newSnapshotCoverageVerificationCache("")
	var proofs uint64
	w.coverageVerificationCache.setStatsObserver(func(s snapshotCoverageVerificationCacheStats) {
		if s.ChecksumStarted > proofs {
			proofs = s.ChecksumStarted
			time.Sleep(20 * time.Millisecond)
		}
	})
	guards := 0
	w.HistoryRangeGuard = func(_ context.Context, through uint64, work func() error) (bool, error) {
		if through != 2047 {
			t.Fatal("changed bucket boundary")
		}
		guards++
		time.Sleep(20 * time.Millisecond)
		return true, work()
	}
	started := time.Now()
	got, err := w.PruneTo(2050)
	elapsed := time.Since(started)
	if err != nil || got.DeletedDomainChangeBlocks != 0 || got.HistoryChunkGC.Retired != 1 || delayed.scans != 1 || proofs != 1 || guards != 1 {
		t.Fatalf("GC exercise: %+v %v scans=%d proofs=%d guards=%d", got, err, delayed.scans, proofs, guards)
	}
	if got.HistoryChunkGCDuration < 60*time.Millisecond || got.HistoryChunkGCDuration+got.HistoryMetadataDuration > elapsed {
		t.Fatalf("timer omitted scan/proof/guard or overlaps metadata: %+v total=%s", got, elapsed)
	}
	// Retirement remains idempotent, and an empty later sweep still has a real
	// separately timed scan without manufacturing a retirement or guard call.
	next, err := w.PruneTo(2050)
	if err != nil || next.HistoryChunkGC.Candidates != 0 || guards != 1 || next.HistoryChunkGCDuration < 20*time.Millisecond {
		t.Fatalf("revisit: %+v %v", next, err)
	}
}

type gcDensityLifecycleChain struct{ *fakePruneChain }

func (c *gcDensityLifecycleChain) TryWithStateDomainChangePruneGuard(context.Context, uint64, uint64, common.Hash, func() error) (bool, error) {
	return false, errors.New("fixture has no shared buckets")
}

func TestLifecycleIndependentGCTimingReachesDensity(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 2, common.Hash{2}); err != nil {
		t.Fatal(err)
	}
	chain := &gcDensityLifecycleChain{&fakePruneChain{db: db, solidified: 2, canonicalHashes: map[uint64]common.Hash{2: {2}}}}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 1, HistoryCatchupMode: snapshots.HistoryCatchupThroughput, MetricsNamespace: "test/gc-density/lifecycle"},
		Pruner:   PrunerConfig{Policy: SnapPolicy(1, 1), Interval: time.Hour, SnapshotDir: dir, HistorySharedChunkGC: true},
	})
	got, err := lifecycle.OnePass()
	if err != nil || !got.Snapshot.Built || got.Prune.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("real lifecycle: %+v %v", got, err)
	}
	s := got.Snapshot
	if got.Prune.HistoryChunkGCDuration <= 0 || s.BeforeMergeHistoryGCDuration != got.Prune.HistoryChunkGCDuration || s.BeforeMergeHistoryGCDuration+s.BeforeMergeMetadataDuration > s.BeforeMergeDuration {
		t.Fatalf("GC timing lost or overlapping: %+v prune=%+v", s, got.Prune)
	}
	if s.HistoryMaintenanceDuration < s.BeforeMergeDuration || s.HistoryRecoveryCost < s.BeforeMergeDuration {
		t.Fatalf("GC excluded from complete wall accounting: %+v", s)
	}
	if got.Prune.HistoryChunkGC.Candidates != 0 {
		t.Fatal("fixture unexpectedly retired a bucket")
	}
}
