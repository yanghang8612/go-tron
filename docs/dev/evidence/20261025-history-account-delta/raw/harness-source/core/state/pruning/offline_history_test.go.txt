package pruning

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb/etl"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
)

func offlineHistoryFixture(t *testing.T) (ethdb.KeyValueStore, OfflineHistoryOptions) {
	t.Helper()
	db := rawdb.NewMemoryDatabase()
	t.Cleanup(func() { _ = db.Close() })
	for block := uint64(1); block <= 8; block++ {
		writeSnapPruningChange(t, db, block, (block-1)*3+1, block*3)
	}
	if err := rawdb.WriteHistoryPruneMode(db, "snap"); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 8, common.Hash{8}); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	chain := snapshots.ChainIdentity{ChainID: 1, GenesisHash: strings.Repeat("11", 32)}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifestForChain(0, 0, nil, chain)); err != nil {
		t.Fatal(err)
	}
	return db, OfflineHistoryOptions{
		SnapshotDir: dir, ExpectedChain: chain, Policy: SnapPolicy(2, 1), SolidifiedBlock: 8,
		MaxBlocks: 2, MaxTxNums: 20, MaxInputBytes: 1 << 20,
		ETL:           etl.Options{TempDir: filepath.Join(dir, "etl"), BufferLimit: 64},
		CanonicalHash: func(block uint64) (common.Hash, bool, error) { return common.Hash{byte(block)}, true, nil },
		Sync:          func() error { return nil }, BeforeWrite: func(OfflineHistoryPlan) error { return nil },
	}
}

func offlineHistoryManifest(t *testing.T, dir string) *snapshots.Manifest {
	t.Helper()
	m, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func offlineHistoryAssertHot(t *testing.T, db ethdb.KeyValueReader, block uint64, want bool) {
	t.Helper()
	_, ok, err := rawdb.ReadStateDomainChange(db, block, 1)
	if err != nil || ok != want {
		t.Fatalf("hot block %d: present=%t want=%t error=%v", block, ok, want, err)
	}
}

func TestOfflineHistorySingleBatchDurabilityAndWindow(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotPrune, 800); err != nil {
		t.Fatal(err)
	}
	const sentinel = "offline-history-unrelated-latest"
	if err := db.Put([]byte(sentinel), []byte("untouched")); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanOfflineHistoryContext(context.Background(), db, opts)
	if err != nil || plan.Blocks != 2 || !plan.BuildNeeded || plan.Input.Records != 2 || plan.ToTxNum != 6 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
	if m := offlineHistoryManifest(t, opts.SnapshotDir); len(m.Segments) != 0 || m.Progress != nil {
		t.Fatal("planning mutated the manifest")
	}
	if _, err := os.Stat(opts.ETL.TempDir); !os.IsNotExist(err) {
		t.Fatalf("planning created ETL scratch: %v", err)
	}
	var syncs int
	opts.Sync = func() error {
		syncs++
		m := offlineHistoryManifest(t, opts.SnapshotDir)
		offlineHistoryAssertHot(t, db, 1, false)
		if syncs == 1 && m.Progress.HotPruneBlockNum != 0 {
			t.Fatal("hot-prune cursor moved before durable deletes")
		}
		if syncs == 2 && m.Progress.HotPruneBlockNum != 2 {
			t.Fatal("second barrier did not follow cursor publication")
		}
		return nil
	}
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err != nil || !result.Built || !result.ProgressDurable || result.ComparedRecords != 2 || syncs != 2 {
		t.Fatalf("result=%+v error=%v syncs=%d", result, err, syncs)
	}
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	if m.Generation != 3 || len(m.Segments) != 3 || m.Progress.HotPruneTxNum != 6 {
		t.Fatalf("manifest=%+v", m)
	}
	if got, _, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune); err != nil || got != 800 {
		t.Fatalf("unrelated prune stage=%d error=%v", got, err)
	}
	if got, err := db.Get([]byte(sentinel)); err != nil || string(got) != "untouched" {
		t.Fatalf("unrelated key changed: %q %v", got, err)
	}
	opts.Sync = func() error { return nil }
	for pass := 0; pass < 2; pass++ {
		if r, err := OfflineHistoryPassContext(context.Background(), db, opts); err != nil || r.DeletedBlocks != 2 {
			t.Fatalf("pass=%d result=%+v error=%v", pass, r, err)
		}
	}
	if r, err := OfflineHistoryPassContext(context.Background(), db, opts); err != nil || r.Plan.Blocks != 0 {
		t.Fatalf("window no-op result=%+v error=%v", r, err)
	}
	m = offlineHistoryManifest(t, opts.SnapshotDir)
	if len(m.Segments) != 9 || len(m.Retired) != 0 || m.Progress.HotPruneBlockNum != 6 {
		t.Fatalf("pass merged or crossed window: %+v", m)
	}
	for block := uint64(1); block <= 8; block++ {
		offlineHistoryAssertHot(t, db, block, block > 6)
		if _, ok, err := rawdb.ReadStateTxRange(db, block); err != nil || !ok {
			t.Fatalf("source tx range removed: block=%d error=%v", block, err)
		}
	}
}

func TestOfflineHistoryPublicationUncertaintyKeepsFilesAndRetries(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	injected := errors.New("directory fsync failed after rename")
	result, err := offlineHistoryPassContext(context.Background(), db, opts, func(dir string, m *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		return injected
	})
	if !errors.Is(err, injected) || !result.PublicationAttempted || result.Published || result.DeletedBlocks != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	for _, ref := range offlineHistoryManifest(t, opts.SnapshotDir).Segments {
		if _, err := os.Stat(filepath.Join(opts.SnapshotDir, ref.Path)); err != nil {
			t.Fatalf("uncertain publication lost active file: %v", err)
		}
	}
	offlineHistoryAssertHot(t, db, 1, true)
	retry, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err != nil || retry.Built || retry.Plan.BuildNeeded || !retry.ProgressDurable || retry.ComparedRecords != 2 {
		t.Fatalf("retry=%+v error=%v", retry, err)
	}
	if m := offlineHistoryManifest(t, opts.SnapshotDir); m.Generation != 3 || len(m.Segments) != 3 {
		t.Fatalf("retry rebuilt or lost generation: %+v", m)
	}
}

func TestOfflineHistoryDeleteSyncFailureRetriesCoveredMissingHot(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	injected := errors.New("WAL sync failed")
	opts.Sync = func() error { return injected }
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if !errors.Is(err, injected) || result.DeletesDurable || result.ProgressPublished || result.DeletedBlocks != 2 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if m := offlineHistoryManifest(t, opts.SnapshotDir); m.Progress.HotPruneBlockNum != 0 || len(m.Segments) != 3 {
		t.Fatalf("cursor advanced on failed sync: %+v", m)
	}
	offlineHistoryAssertHot(t, db, 1, false)
	opts.Sync = func() error { return nil }
	retry, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err != nil || retry.Built || retry.ComparedRecords != 0 || !retry.ProgressDurable {
		t.Fatalf("retry=%+v error=%v", retry, err)
	}
}

func TestOfflineHistoryRejectsValidColdWithDifferentHotPreimage(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	original, ok, err := rawdb.ReadStateDomainChange(db, 1, 1)
	if err != nil || !ok {
		t.Fatal(err)
	}
	changed := *original
	changed.Prev = []byte("different previous image")
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{&changed}); err != nil {
		t.Fatal(err)
	}
	_, err = offlineHistoryPassContext(context.Background(), db, opts, func(dir string, m *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		return errors.New("stop after valid cold publication")
	})
	if err == nil {
		t.Fatal("expected injected stop")
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{original}); err != nil {
		t.Fatal(err)
	}
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err == nil || !strings.Contains(err.Error(), "previous image mismatch") || result.DeletedBlocks != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	offlineHistoryAssertHot(t, db, 1, true)
	if m := offlineHistoryManifest(t, opts.SnapshotDir); m.Progress.HotPruneBlockNum != 0 {
		t.Fatal("mismatching previous image advanced cursor")
	}
}

func TestOfflineHistoryAdmissionAndBoundaryFailureAreReadOnly(t *testing.T) {
	for _, failure := range []string{"admission", "cancel", "tx-limit", "input-limit", "canonical", "range-gap", "cursor-gap"} {
		t.Run(failure, func(t *testing.T) {
			db, opts := offlineHistoryFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch failure {
			case "admission":
				opts.BeforeWrite = func(OfflineHistoryPlan) error { return errors.New("space denied") }
			case "cancel":
				opts.BeforeWrite = func(OfflineHistoryPlan) error { cancel(); return nil }
			case "tx-limit":
				opts.MaxTxNums = 2
			case "input-limit":
				opts.MaxInputBytes = 1
			case "canonical":
				opts.CanonicalHash = func(uint64) (common.Hash, bool, error) { return common.Hash{99}, true, nil }
			case "range-gap":
				if err := rawdb.WriteStateTxRange(db, 2, common.Hash{2}, 5, 6); err != nil {
					t.Fatal(err)
				}
			case "cursor-gap":
				if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 2, 3); err != nil {
					t.Fatal(err)
				}
			}
			result, err := OfflineHistoryPassContext(ctx, db, opts)
			if err == nil || result.Built || result.DeletedBlocks != 0 {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if m := offlineHistoryManifest(t, opts.SnapshotDir); len(m.Segments) != 0 || m.Generation != 1 {
				t.Fatalf("failed admission mutated manifest: %+v", m)
			}
			offlineHistoryAssertHot(t, db, 1, true)
		})
	}
}

func TestOfflineHistoryCancellationAfterPublishRetainsColdAndHot(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := offlineHistoryPassContext(ctx, db, opts, func(dir string, m *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || !result.Published || result.DeletedBlocks != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	offlineHistoryAssertHot(t, db, 1, true)
	if result, err := OfflineHistoryPassContext(context.Background(), db, opts); err != nil || result.Built || !result.ProgressDurable {
		t.Fatalf("retry=%+v error=%v", result, err)
	}
}

func TestOfflineHistoryCoveredBudgetIncludesWholeTrio(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	opts.MaxBlocks = 4
	_, err := offlineHistoryPassContext(context.Background(), db, opts, func(dir string, m *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		return errors.New("published; stop before deletes")
	})
	if err == nil {
		t.Fatal("expected injected stop")
	}
	opts.MaxBlocks = 1
	plan, err := PlanOfflineHistoryContext(context.Background(), db, opts)
	if err != nil || plan.BuildNeeded || plan.Input.Records != 1 || plan.Blocks != 1 || plan.VerificationBytes != (1<<20)+51*4 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
	if _, err := os.Stat(filepath.Join(opts.SnapshotDir, "etl")); err == nil {
		entries, err := os.ReadDir(filepath.Join(opts.SnapshotDir, "etl"))
		if err != nil || len(entries) != 0 {
			t.Fatalf("read-only budget left scratch: %v %v", entries, err)
		}
	}
}

func TestOfflineHistoryCorruptCoveredTrioNeverDeletesHot(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	_, err := offlineHistoryPassContext(context.Background(), db, opts, func(dir string, m *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		return errors.New("published; stop before deletes")
	})
	if err == nil {
		t.Fatal("expected injected stop")
	}
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	path := filepath.Join(opts.SnapshotDir, m.Segments[0].Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err == nil || result.DeletedBlocks != 0 || result.ProgressPublicationAttempted {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	offlineHistoryAssertHot(t, db, 1, true)
}

func TestOfflineHistoryProgressPublicationUncertaintyFollowsDurableDeletes(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	var calls, syncs int
	opts.Sync = func() error { syncs++; return nil }
	injected := errors.New("cursor rename succeeded; fsync failed")
	result, err := offlineHistoryPassContext(context.Background(), db, opts, func(dir string, m *snapshots.Manifest) error {
		calls++
		if calls == 2 && syncs != 1 {
			t.Fatal("cursor publication preceded delete barrier")
		}
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		if calls == 2 {
			return injected
		}
		return nil
	})
	if !errors.Is(err, injected) || !result.DeletesDurable || !result.ProgressPublicationAttempted || result.ProgressPublished {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	if m.Progress.HotPruneBlockNum != 2 || m.Generation != 3 {
		t.Fatalf("manifest=%+v", m)
	}
	for _, ref := range m.Segments {
		if _, err := os.Stat(filepath.Join(opts.SnapshotDir, ref.Path)); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := PlanOfflineHistoryContext(context.Background(), db, opts)
	if err != nil || plan.FromBlock != 3 || plan.FromTxNum != 7 {
		t.Fatalf("plan=%+v error=%v", plan, err)
	}
}

func TestOfflineHistoryPartialBlockDeletionRetryAndMonotonicStages(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	opts.Sync = func() error { return errors.New("simulated interrupted delete") }
	if _, err := OfflineHistoryPassContext(context.Background(), db, opts); err == nil {
		t.Fatal("expected failure")
	}
	// A database batch failure can persist an earlier block while a later
	// block remains. The retry must compare and remove that later block.
	writeSnapPruningChange(t, db, 2, 4, 6)
	m := offlineHistoryManifest(t, opts.SnapshotDir)
	m.Progress.AccessorBuildTxNum = 24
	if err := snapshots.PublishManifest(opts.SnapshotDir, m); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotAccessor, 24); err != nil {
		t.Fatal(err)
	}
	opts.Sync = func() error { return nil }
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err != nil || result.Built || result.ComparedRecords != 1 || !result.ProgressDurable {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	if accessor, _, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotAccessor); err != nil || accessor != 24 {
		t.Fatalf("accessor regressed: %d %v", accessor, err)
	}
	if history, _, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || history != 6 {
		t.Fatalf("history stage not repaired: %d %v", history, err)
	}
	if build, _, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || build != 2 {
		t.Fatalf("build stage not repaired: %d %v", build, err)
	}
}

func TestOfflineHistoryEmptyBlockRequiresMatchingColdRange(t *testing.T) {
	db, opts := offlineHistoryFixture(t)
	if err := rawdb.DeleteStateDomainChanges(db, 1); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{99}, 1, 3); err != nil {
		t.Fatal(err)
	}
	originalLookup := opts.CanonicalHash
	opts.CanonicalHash = func(block uint64) (common.Hash, bool, error) {
		if block == 1 {
			return common.Hash{99}, true, nil
		}
		return originalLookup(block)
	}
	_, err := offlineHistoryPassContext(context.Background(), db, opts, func(dir string, m *snapshots.Manifest) error {
		if err := snapshots.PublishManifest(dir, m); err != nil {
			return err
		}
		return errors.New("published; stop before deletes")
	})
	if err == nil {
		t.Fatal("expected injected stop")
	}
	if len(offlineHistoryManifest(t, opts.SnapshotDir).Segments) != 3 {
		t.Fatalf("failed to publish fixture: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{1}, 1, 3); err != nil {
		t.Fatal(err)
	}
	opts.CanonicalHash = originalLookup
	result, err := OfflineHistoryPassContext(context.Background(), db, opts)
	if err == nil || !strings.Contains(err.Error(), "cold block range mismatch") || result.DeletedBlocks != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	offlineHistoryAssertHot(t, db, 2, true)
}
