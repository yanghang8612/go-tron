package pruning

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

// Real immutable files and the production prune verifier are used by both the
// failure tests and the ordering benchmark; no fake delay stands in for merge.
func coveredLifecycleFixture(t testing.TB, blocks, segmentBlocks uint64) (ethdb.KeyValueStore, string, []snapshots.SegmentRef) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { db.Close() })
	dir := t.TempDir()
	var refs []snapshots.SegmentRef
	for block := uint64(1); block <= blocks; block++ {
		hash := common.Hash{byte(block)}
		if err := rawdb.WriteStateTxRange(db, block, hash, 2*block-1, 2*block); err != nil {
			t.Fatal(err)
		}
		prev := make([]byte, 8<<10)
		for i := range prev {
			prev[i] = byte((i*31 + int(block)*17) ^ (i >> 4))
		}
		change := &rawdb.StateDomainChange{
			BlockNum: block, BlockHash: hash, TxNum: 2 * block, Seq: 1,
			FlatDomain: rawdb.StateFlatDomainKVLatest, Domain: kvdomains.SystemDynamicProperty,
			Owner: common.BytesToAddress([]byte{common.AddressPrefixMainnet, byte(block)}),
			Key:   []byte("ordered-prune"), PrevExists: true, Prev: prev,
			NextExists: true, Next: []byte("next"),
		}
		if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChangePostingIndex(db, change); err != nil {
			t.Fatal(err)
		}
		if block%segmentBlocks == 0 {
			from, to := 2*(block-segmentBlocks)+1, 2*block
			built, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, from, to, fmt.Sprintf("history/state-domain-change-%d-%d.seg", from, to))
			if err != nil {
				t.Fatal(err)
			}
			refs = append(refs, built...)
		}
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 2*blocks, refs)); err != nil {
		t.Fatal(err)
	}
	return db, dir, refs
}

func assertCoveredLifecycleRows(t testing.TB, db ethdb.KeyValueStore, dir string, blocks uint64, pruned bool) {
	t.Helper()
	for block := uint64(1); block <= blocks; block++ {
		if _, ok, err := rawdb.ReadStateDomainChange(db, block, 1); err != nil || ok == pruned {
			t.Fatalf("block %d hot row present=%v pruned=%v err=%v", block, ok, pruned, err)
		}
	}
	if !pruned {
		return
	}
	manifest, err := snapshots.LoadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	var records uint64
	for _, ref := range manifest.Segments {
		if ref.Kind == snapshots.SegmentHistory {
			if err := cfg.IterateHistoryRange(dir, manifest, ref, 1, 2*blocks, func(*rawdb.StateDomainChange) (bool, error) { records++; return true, nil }); err != nil {
				t.Fatal(err)
			}
		}
	}
	if records != blocks {
		t.Fatalf("cold records=%d want=%d", records, blocks)
	}
}

func TestSnapshotLifecyclePressureProbeFailureStillPrunesVerifiedCoverage(t *testing.T) {
	db, dir, refs := coveredLifecycleFixture(t, 2, 1)
	probeErr := errors.New("statfs unavailable")
	freezerCalled := false
	l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 3, syncRemaining: 100, syncRemainingOK: true}, SnapshotLifecycleConfig{
		Snapshot:                        snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, HistoryPressureProbe: func(context.Context) (snapshots.HistoryPressure, error) { return snapshots.HistoryPressure{}, probeErr }},
		Pruner:                          PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir},
		DeferPruneOnSyncHistoryDeferral: true,
		ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
			freezerCalled = true
			return snapshots.ChainFreezerSnapshotPassResult{}, nil
		},
	})
	defer l.cancel()
	out, err := l.OnePass()
	if !errors.Is(err, probeErr) || out.Snapshot.Built || out.Snapshot.Compaction.Merged || !out.Snapshot.HistorySpaceDeferred || out.Snapshot.HistoryRetryAfter <= 0 || freezerCalled || out.Prune.DeletedDomainChangeBlocks != 2 {
		t.Fatalf("probe failure out=%+v err=%v freezer=%v", out, err, freezerCalled)
	}
	assertCoveredLifecycleRows(t, db, dir, 2, true)
	manifest, err := snapshots.LoadManifest(dir)
	if err != nil || len(manifest.Segments) != len(refs) {
		t.Fatalf("probe failure allowed a merge: %+v %v", manifest, err)
	}
}

func TestSnapshotLifecycleEarlyPruneRetainsCompanionVerificationGate(t *testing.T) {
	db, dir, refs := coveredLifecycleFixture(t, 2, 1)
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentAccessor {
			if err := os.WriteFile(filepath.Join(dir, ref.Path), []byte("corrupt-accessor"), 0o644); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 3}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1},
		Pruner:   PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir},
	})
	defer l.cancel()
	out, err := l.OnePass()
	if err == nil || out.Snapshot.Compaction.Merged || out.Prune.DeletedDomainChangeBlocks != 0 {
		t.Fatalf("corrupt coverage must stop before prune/merge: %+v %v", out, err)
	}
	assertCoveredLifecycleRows(t, db, dir, 2, false)
}

func TestSnapshotLifecycleEarlyPruneSurvivesLaterFreezerFailure(t *testing.T) {
	db, dir, _ := coveredLifecycleFixture(t, 2, 2)
	freezerErr := errors.New("freezer unavailable")
	l := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 3}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1},
		Pruner:   PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir},
		ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
			return snapshots.ChainFreezerSnapshotPassResult{}, freezerErr
		},
	})
	defer l.cancel()
	out, err := l.OnePass()
	if !errors.Is(err, freezerErr) || out.Prune.DeletedDomainChangeBlocks != 2 {
		t.Fatalf("later failure lost completed prune: %+v %v", out, err)
	}
	assertCoveredLifecycleRows(t, db, dir, 2, true)
}

func BenchmarkCoveredHistoryPruneOrder(b *testing.B) {
	for _, tc := range []struct{ early, trusted bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		early := tc.early
		name := "merge-then-prune"
		if early {
			name = "prune-then-merge"
		}
		if tc.trusted {
			name += "/known-builder-refs"
		} else {
			name += "/uncached-existing-refs"
		}
		b.Run(name, func(b *testing.B) {
			var pruneReady time.Duration
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, dir, refs := coveredLifecycleFixture(b, 32, 16)
				chain := &fakePruneChain{db: db, solidified: 33}
				r := snapshots.NewRunner(snapshotChainSource{chain: chain}, snapshots.Config{Dir: dir, Enabled: true, HistoryWindow: 1, MaxCompactionPasses: 1, CompactMaxSteps: 2})
				p := NewPruner(chain, PrunerConfig{Policy: SnapPolicy(1, 1), SnapshotDir: dir})
				if tc.trusted {
					// Model refs produced by this process through the real builder.
					// A restarted process with no cache uses the other pair of cases.
					if err := p.RecordTrustedSnapshotSegments(refs); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				started := time.Now()
				prune := func(ctx context.Context, published snapshots.PassResult) error {
					if err := p.RecordTrustedSnapshotSegments(published.Segments); err != nil {
						return err
					}
					_, err := p.PrunePassContext(ctx)
					pruneReady += time.Since(started)
					return err
				}
				var out snapshots.PassResult
				var err error
				if early {
					out, err = r.OnePassWithMaintenanceContext(context.Background(), prune)
				} else {
					out, err = r.OnePassContext(context.Background())
					if err == nil {
						err = p.RecordTrustedSnapshotSegments(out.Compaction.Segments)
					}
					if err == nil {
						err = prune(context.Background(), out)
					}
				}
				if early && err == nil {
					err = p.RecordTrustedSnapshotSegments(out.Compaction.Segments)
				}
				b.StopTimer()
				if err != nil || !out.Compaction.Merged {
					b.Fatalf("real merge fixture: %+v %v", out, err)
				}
				assertCoveredLifecycleRows(b, db, dir, 32, true)
			}
			b.ReportMetric(float64(pruneReady.Nanoseconds())/float64(b.N), "prune-ready-ns/op")
		})
	}
}
