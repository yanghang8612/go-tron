package pruning

import (
	"context"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func TestPostingPrunePermissionRequiresSuccessfulDurableColdPrune(t *testing.T) {
	for _, scenario := range []string{"success", "missing_finish", "bad_finish_hash", "missing_cold", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			dir := t.TempDir()
			writeSnapPruningChange(t, db, 1, 1, 3)
			refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 3, "history/state-domain-change-1-3.seg")
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "missing_cold" {
				if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 3, refs)); err != nil {
					t.Fatal(err)
				}
			}
			proof := common.Hash{10}
			if scenario != "missing_finish" {
				rowHash := proof
				if scenario == "bad_finish_hash" {
					rowHash = common.Hash{99}
				}
				if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 10, rowHash); err != nil {
					t.Fatal(err)
				}
			}
			cfg := PrunerConfig{Policy: SnapPolicy(3, 2), SnapshotDir: dir, MetricsNamespace: "test/" + t.Name()}
			chain := &fakePruneChain{db: db, solidified: 10, canonicalHashes: map[uint64]common.Hash{10: proof}}
			p := NewPruner(chain, cfg)
			l := &SnapshotLifecycle{pruner: p}
			if l.PostingPruneBoundary() != (PostingPruneBoundary{}) {
				t.Fatal("manifest authorized work before successful pass")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			_, err = p.PrunePassContext(ctx)
			if scenario == "success" {
				if err != nil {
					t.Fatal(err)
				}
				want := PostingPruneBoundary{PrunedThrough: 1, ProofHead: 10, ProofHash: proof}
				if got := l.PostingPruneBoundary(); got != want {
					t.Fatalf("permission=%+v want=%+v", got, want)
				}
				manifest, err := snapshots.LoadProductionManifest(dir)
				if err != nil || manifest.Progress == nil || manifest.Progress.HotPruneBlockNum != 1 {
					t.Fatalf("permission without manifest progress: %+v %v", manifest, err)
				}
				restarted := &SnapshotLifecycle{pruner: NewPruner(chain, cfg)}
				if restarted.PostingPruneBoundary() != (PostingPruneBoundary{}) {
					t.Fatal("restart reused old permission")
				}
			} else if l.PostingPruneBoundary() != (PostingPruneBoundary{}) {
				t.Fatalf("%s granted posting prune permission (err=%v)", scenario, err)
			}
		})
	}
}
