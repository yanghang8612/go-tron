package pruning

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

type historyRangeQueueTestChain struct {
	*historyRangePruneTestChain
	queuedCalls int
}

func (c *historyRangeQueueTestChain) WithStateDomainChangePruneGuard(ctx context.Context, through, head uint64, hash common.Hash, work func() error) (bool, error) {
	c.queuedCalls++
	return c.TryWithStateDomainChangePruneGuard(ctx, through, head, hash, work)
}

func TestPrunerHistoryRangeQueueWiringAndAdmission(t *testing.T) {
	for _, name := range []string{"default", "queued", "missing-queued-source", "missing-range-opt-in"} {
		t.Run(name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			t.Cleanup(func() { _ = db.Close() })
			dir := t.TempDir()
			for block := uint64(1); block <= 64; block++ {
				writeSnapPruningChange(t, db, block, block*10, block*10+2)
			}
			for stage, head := range map[rawdb.StageID]uint64{rawdb.StageFinish: 66, rawdb.StageStateHistoryIndex: 64} {
				if err := rawdb.WriteStageProgressWithHash(db, stage, head, common.Hash{byte(head)}); err != nil {
					t.Fatal(err)
				}
			}
			refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 642, "history/state-domain-change-queue.seg")
			if err != nil {
				t.Fatal(err)
			}
			if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 642, refs)); err != nil {
				t.Fatal(err)
			}
			chain := &historyRangeQueueTestChain{historyRangePruneTestChain: &historyRangePruneTestChain{&fakePruneChain{
				db: db, solidified: 66, canonicalHashes: map[uint64]common.Hash{66: {66}, 64: {64}},
			}}}
			var source ChainSource = chain
			if name == "missing-queued-source" {
				source = chain.historyRangePruneTestChain
			}
			ns := "test/pruner/history-range-queue/" + name + "/"
			p := NewPruner(source, PrunerConfig{Policy: SnapPolicy(2, 1), SnapshotDir: dir,
				HistoryRangePrune: name != "missing-range-opt-in", HistoryRangeQueue: name != "default", MetricsNamespace: ns})
			_, err = p.PrunePass()
			invalid := name == "missing-queued-source" || name == "missing-range-opt-in"
			if (err != nil) != invalid {
				t.Fatalf("pass err=%v, invalid=%v", err, invalid)
			}
			if invalid {
				if _, exists, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || !exists {
					t.Fatalf("rejected admission removed history: exists=%v err=%v", exists, err)
				}
				if _, exists, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || exists {
					t.Fatalf("rejected admission published progress: exists=%v err=%v", exists, err)
				}
				return
			}
			wantCalls := 0
			if name == "queued" {
				wantCalls = 1
			}
			if chain.queuedCalls != wantCalls || p.Stats().HistoryDeletes.RangeRows != 64 || p.Stats().HistoryDeletes.RangeRuns != 1 {
				t.Fatalf("queued=%d, stats=%+v", chain.queuedCalls, p.Stats())
			}
			assertPrunerGauge(t, ns+"history/delete/range/queue/enabled", int64(wantCalls))
		})
	}
}
