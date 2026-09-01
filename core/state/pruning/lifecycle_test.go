package pruning

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/maintenance"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// lifecycleFailStageDB intercepts both direct and batched snapshot-stage writes
// after the immutable manifest is published. This exercises lifecycle retry
// scheduling with a partially successful forced-busy pass.
type lifecycleFailStageDB struct {
	ethdb.KeyValueStore
	fail atomic.Bool
	err  error
}

func (db *lifecycleFailStageDB) Put(key, value []byte) error {
	if db.fail.Load() && bytes.Equal(key, []byte("stage-progress-v1-"+string(rawdb.StageSnapshotBuild))) {
		return db.err
	}
	return db.KeyValueStore.Put(key, value)
}

func (db *lifecycleFailStageDB) NewBatch() ethdb.Batch {
	return &lifecycleFailStageBatch{Batch: db.KeyValueStore.NewBatch(), db: db}
}

func (db *lifecycleFailStageDB) NewBatchWithSize(size int) ethdb.Batch {
	return &lifecycleFailStageBatch{Batch: db.KeyValueStore.NewBatchWithSize(size), db: db}
}

type lifecycleFailStageBatch struct {
	ethdb.Batch
	db *lifecycleFailStageDB
}

func (batch *lifecycleFailStageBatch) Put(key, value []byte) error {
	if batch.db.fail.Load() && bytes.Equal(key, []byte("stage-progress-v1-"+string(rawdb.StageSnapshotBuild))) {
		return batch.db.err
	}
	return batch.Batch.Put(key, value)
}

func TestSnapshotLifecycleBuildsVisibleHistoryBeforePruningHotRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)

	chain := &fakePruneChain{db: db, solidified: 2}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:           dir,
			Enabled:       true,
			Interval:      time.Hour,
			HistoryWindow: 1,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.Built || result.Snapshot.FromTxNum != 1 || result.Snapshot.ToTxNum != 12 {
		t.Fatalf("snapshot result = %+v, want visible history [1,12]", result.Snapshot)
	}
	if result.Prune.DeletedDomainChangeBlocks != 1 || result.Prune.DeletedTxRanges != 0 {
		t.Fatalf("prune result = %+v, want one covered hot change block pruned and tx range retained", result.Prune)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("hot domain change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range should remain hot in snap mode ok=%v err=%v", ok, err)
	}
	manifest, err := snapshots.LoadManifest(dir)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if manifest.Progress == nil || manifest.Progress.HistoryBuildTxNum != 12 || manifest.Progress.HotPruneTxNum != 12 || manifest.Progress.HotPruneBlockNum != 1 {
		t.Fatalf("manifest progress = %+v, want history/hot-prune at block 1 tx 12", manifest.Progress)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot history stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot hot-prune stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
}

func TestSnapshotLifecycleRunsStateChangeIndexSweepOnlyAfterCatchupIsIdle(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	chain := &fakePruneChain{db: db, solidified: 2, syncRemaining: 100, syncRemainingOK: true}
	var calls int
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir: dir, Enabled: true, Interval: time.Hour, HistoryWindow: 1,
		},
		Pruner: PrunerConfig{
			Policy: SnapPolicy(1, 1), Interval: time.Hour, SnapshotDir: dir,
		},
		StateChangeIndexPrune: func(context.Context) (*rawdb.StateChangePostingPruneResult, error) {
			calls++
			return &rawdb.StateChangePostingPruneResult{PostingRowsDeleted: 1}, nil
		},
	})
	deferred, err := lifecycle.OnePass()
	if err != nil {
		t.Fatal(err)
	}
	if !deferred.StateChangeIndexDeferred || deferred.StateChangeIndexPrune != nil || calls != 0 {
		t.Fatalf("active-sync sweep = result:%+v calls:%d, want deferred", deferred, calls)
	}
	chain.syncRemaining = 0
	chain.syncRemainingOK = false
	completed, err := lifecycle.OnePass()
	if err != nil {
		t.Fatal(err)
	}
	if completed.StateChangeIndexDeferred || completed.StateChangeIndexPrune == nil || calls != 1 {
		t.Fatalf("idle caught-up sweep = result:%+v calls:%d, want one run", completed, calls)
	}
}

func TestArchiveLifecycleBuildsColdHistoryBeforePruningDuplicateHotRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)

	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:           dir,
			Enabled:       true,
			Interval:      time.Hour,
			HistoryWindow: 1,
		},
		Pruner: PrunerConfig{
			Policy:      ArchiveColdPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("archive lifecycle pass: %v", err)
	}
	if !result.Snapshot.Built || result.Prune.DeletedDomainChangeBlocks != 1 || result.Prune.DeletedTxRanges != 0 {
		t.Fatalf("archive lifecycle result = %+v", result)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("archive duplicate hot change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("archive tx range missing after prune ok=%v err=%v", ok, err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open archive cold manager: %v", err)
	}
	var coldChanges int
	if err := mgr.IterateStateDomainChanges(10, 12, func(change *rawdb.StateDomainChange) (bool, error) {
		coldChanges++
		return true, nil
	}); err != nil {
		t.Fatalf("read archive cold history: %v", err)
	}
	if coldChanges != 1 {
		t.Fatalf("cold history changes = %d, want 1", coldChanges)
	}
}

func TestSnapshotLifecycleAutomaticallyDrainsColdBuildBacklog(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeSnapPruningChange(t, db, blockNum, blockNum*10, blockNum*10+2)
	}

	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 4}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:           dir,
			Enabled:       true,
			Interval:      time.Hour,
			HistoryWindow: 1,
			BatchBlocks:   1,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
		Interval: time.Hour,
	})
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Stop(); err != nil {
			t.Errorf("stop lifecycle: %v", err)
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil {
			t.Fatalf("read snapshot build stage: %v", err)
		} else if ok && progress == 3 && lifecycle.pruner.Stats().Passes == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild); err != nil || !ok || progress != 3 {
		t.Fatalf("snapshot build stage = %d ok=%v err=%v, want 3 without waiting for interval", progress, ok, err)
	}
	// StageSnapshotBuild is published inside the builder, before the ordered
	// prune half of the same lifecycle pass. Wait for both halves above: Stop is
	// allowed to cancel an in-flight prune scan during graceful shutdown.
	if err := lifecycle.Stop(); err != nil {
		t.Fatalf("stop lifecycle: %v", err)
	}
	if stats := lifecycle.builder.Snapshot(); stats.PassesCompleted != 3 || stats.SegmentsBuilt != 3 || stats.LastLagBlocks != 0 {
		t.Fatalf("builder stats = %+v, want three drained batches", stats)
	}
	if stats := lifecycle.pruner.Stats(); stats.Passes != 3 || stats.Errors != 0 {
		t.Fatalf("pruner stats = %+v, want one ordered prune per build", stats)
	}
}

func TestSnapshotLifecycleRateLimitedWakeRunsRemainingMaintenance(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	for blockNum := uint64(1); blockNum <= 3; blockNum++ {
		writeSnapPruningChange(t, db, blockNum, blockNum*10, blockNum*10+2)
	}
	chain := &fakePruneChain{db: db, solidified: 4, syncRemaining: 100, syncRemainingOK: true}
	chainFreezerBuilds := 0
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:                     dir,
			Enabled:                 true,
			HistoryWindow:           1,
			BatchBlocks:             2,
			CatchupBuildMinInterval: time.Hour,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			SnapshotDir: dir,
		},
		ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
			chainFreezerBuilds++
			return snapshots.ChainFreezerSnapshotPassResult{}, nil
		},
	})
	first, err := lifecycle.OnePass()
	if err != nil || !first.Snapshot.Built || !first.Snapshot.NeedsCatchup() {
		t.Fatalf("first lifecycle pass = %+v err=%v", first, err)
	}
	prunePasses := lifecycle.pruner.Stats().Passes
	second, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("rate-limited lifecycle pass: %v", err)
	}
	if !second.Snapshot.HistoryRateLimited || second.Snapshot.Built {
		t.Fatalf("rate-limited lifecycle pass = %+v", second)
	}
	if chainFreezerBuilds != 2 || lifecycle.pruner.Stats().Passes != prunePasses+1 {
		t.Fatalf("downstream maintenance skipped after rate limit: freezer=%d prune=%d/%d", chainFreezerBuilds, lifecycle.pruner.Stats().Passes, prunePasses)
	}
}

func TestSnapshotLifecycleAcceleratedCatchupPreservesOrderedMaintenance(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	for blockNum := uint64(1); blockNum <= 6; blockNum++ {
		writeSnapPruningChange(t, db, blockNum, blockNum*10, blockNum*10+2)
	}
	chain := &fakePruneChain{db: db, solidified: 7, syncRemaining: 100, syncRemainingOK: true}
	chainFreezerBuilds := 0
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:                         dir,
			Enabled:                     true,
			HistoryWindow:               1,
			BatchBlocks:                 2,
			CatchupBuildMinInterval:     time.Hour,
			CatchupUnthrottledLagBlocks: 2,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			SnapshotDir: dir,
		},
		ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
			chainFreezerBuilds++
			return snapshots.ChainFreezerSnapshotPassResult{}, nil
		},
	})

	first, err := lifecycle.OnePass()
	if err != nil || !first.Snapshot.Built || !first.Snapshot.HistoryAccelerated {
		t.Fatalf("first accelerated lifecycle pass = %+v err=%v", first, err)
	}
	second, err := lifecycle.OnePass()
	if err != nil || !second.Snapshot.Built || !second.Snapshot.HistoryAccelerated {
		t.Fatalf("second accelerated lifecycle pass = %+v err=%v", second, err)
	}
	if chainFreezerBuilds != 2 || lifecycle.pruner.Stats().Passes != 2 {
		t.Fatalf("ordered maintenance after acceleration: freezer=%d prune=%d", chainFreezerBuilds, lifecycle.pruner.Stats().Passes)
	}

	third, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("post-acceleration lifecycle pass: %v", err)
	}
	if !third.Snapshot.HistoryRateLimited || third.Snapshot.Built {
		t.Fatalf("post-acceleration lifecycle pass = %+v", third)
	}
	if chainFreezerBuilds != 3 || lifecycle.pruner.Stats().Passes != 3 {
		t.Fatalf("ordered maintenance after rate limit: freezer=%d prune=%d", chainFreezerBuilds, lifecycle.pruner.Stats().Passes)
	}
}

func TestSnapshotLifecycleRetriesAcceleratedCatchupAfterGateCooldown(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	for blockNum := uint64(1); blockNum <= 4; blockNum++ {
		writeSnapPruningChange(t, db, blockNum, blockNum*10, blockNum*10+2)
	}
	gate := maintenance.NewHeavyWorkGateWithCooldownAfter(2*time.Second, 0)
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{
		db:              db,
		solidified:      5,
		syncRemaining:   100,
		syncRemainingOK: true,
	}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:                         dir,
			Enabled:                     true,
			HistoryWindow:               1,
			BatchBlocks:                 2,
			CatchupBuildMinInterval:     time.Hour,
			CatchupUnthrottledLagBlocks: 1,
			HeavyWorkGate:               gate,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
		Interval: time.Hour,
	})
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Stop(); err != nil {
			t.Errorf("stop lifecycle: %v", err)
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild)
		if err != nil {
			t.Fatalf("read snapshot build stage: %v", err)
		}
		if ok && progress == 4 && lifecycle.builder.Snapshot().SegmentsBuilt == 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	progress, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotBuild)
	if err != nil || !ok || progress != 4 {
		t.Fatalf("snapshot build stage = %d ok=%v err=%v, want cooldown retry to reach 4", progress, ok, err)
	}
	stats := lifecycle.builder.Snapshot()
	if stats.SegmentsBuilt != 2 || stats.HistoryAcceleratedBuilds != 2 || stats.HistoryGateDeferred == 0 {
		t.Fatalf("builder cooldown retry stats = %+v", stats)
	}
}

func TestSnapshotLifecyclePreservesAndSchedulesFailedForcedBusyRetry(t *testing.T) {
	newLifecycle := func(t *testing.T) (*SnapshotLifecycle, *lifecycleFailStageDB) {
		t.Helper()
		base := rawdb.NewMemoryDatabase()
		dir := t.TempDir()
		for blockNum := uint64(1); blockNum <= 12; blockNum++ {
			writeSnapPruningChange(t, base, blockNum, blockNum, blockNum)
		}
		stageErr := errors.New("injected lifecycle snapshot stage failure")
		db := &lifecycleFailStageDB{KeyValueStore: base, err: stageErr}
		db.fail.Store(true)
		lifecycle := NewSnapshotLifecycle(&fakePruneChain{
			db: db, solidified: 13, syncRemaining: 1_000, syncRemainingOK: true,
		}, SnapshotLifecycleConfig{
			Snapshot: snapshots.Config{
				Dir:                           dir,
				Enabled:                       true,
				HistoryWindow:                 1,
				BatchBlocks:                   8,
				BatchTxNums:                   8,
				CatchupBuildMinInterval:       80 * time.Millisecond,
				CatchupUnthrottledLagBlocks:   1,
				CatchupHeavyWorkCooldown:      20 * time.Millisecond,
				DeferHistoryBuildWhileSyncing: true,
				MaxDeferredHistoryBlocks:      2,
				MaxBusyDeferredHistoryBlocks:  4,
				SyncBuildReady:                func() bool { return false },
			},
			Pruner: PrunerConfig{
				Policy:      SnapPolicy(1, 1),
				Interval:    time.Hour,
				SnapshotDir: dir,
			},
			Interval: time.Hour,
		})
		return lifecycle, db
	}

	// The synchronous API must not discard the builder's recovery metadata
	// merely because the ordered pass returns an error.
	direct, _ := newLifecycle(t)
	failed, err := direct.OnePass()
	if err == nil {
		t.Fatal("forced-busy lifecycle pass unexpectedly succeeded")
	}
	if !failed.Snapshot.HistoryForcedBusy || !failed.Snapshot.HistoryBuildAttempted ||
		failed.Snapshot.HistoryRetryDeadline.IsZero() || failed.Snapshot.HistoryRetryRemaining(time.Now()) <= 0 {
		t.Fatalf("failed lifecycle snapshot metadata = %+v", failed.Snapshot)
	}

	// The background loop must use that deadline on the error path. Its normal
	// interval is one hour, so progress within seconds proves the retry timer,
	// rather than the coarse maintenance ticker, woke the next pass.
	lifecycle, db := newLifecycle(t)
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Stop(); err != nil {
			t.Errorf("stop lifecycle: %v", err)
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for lifecycle.builder.Snapshot().PassErrors == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if lifecycle.builder.Snapshot().PassErrors == 0 {
		t.Fatal("timed out waiting for injected forced-busy failure")
	}
	db.fail.Store(false)
	for lifecycle.builder.Snapshot().SegmentsBuilt == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := lifecycle.builder.Snapshot(); stats.SegmentsBuilt == 0 || stats.ForcedBusyAttempts < 2 {
		t.Fatalf("lifecycle did not retry failed forced-busy work: %+v", stats)
	}
}

func TestSnapshotLifecycleStartPreparesLatestBuildWatermark(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 50, 50, 50)
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 50}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:               dir,
			Enabled:           true,
			Interval:          time.Hour,
			HistoryWindow:     100,
			LatestBuildBlocks: 10,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(100, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
		Interval: time.Hour,
	})
	completed := make(chan struct{}, 1)
	lifecycle.AddPassCompleteHook(func() {
		select {
		case completed <- struct{}{}:
		default:
		}
	})
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Stop(); err != nil {
			t.Errorf("stop lifecycle: %v", err)
		}
	})
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial lifecycle pass")
	}
	if err := lifecycle.Stop(); err != nil {
		t.Fatalf("stop lifecycle: %v", err)
	}
	if got := lifecycle.builder.Snapshot(); got.PassesCompleted != 1 || got.LatestDeferredSync != 0 {
		t.Fatalf("builder stats = %+v, want one cadence-gated pass", got)
	}
	if got := lifecycle.builder.Snapshot().LastLatestBuildBlock; got != 50 {
		t.Fatalf("prepared latest build block = %d, want 50", got)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotLatestBuild); err != nil || ok {
		t.Fatalf("latest build stage exists after prepared initial pass: ok=%v err=%v", ok, err)
	}
}

func TestSnapshotLifecycleForwardsActiveSyncToLatestBuilder(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 50, 50, 50)
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{
		db:              db,
		solidified:      50,
		syncRemaining:   1_000,
		syncRemainingOK: true,
	}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:                          dir,
			Enabled:                      true,
			HistoryWindow:                100,
			LatestBuildBlocks:            10,
			DeferLatestBuildWhileSyncing: true,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(100, 1),
			SnapshotDir: dir,
		},
	})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.LatestDeferred || result.Snapshot.LatestBuilt {
		t.Fatalf("snapshot result = %+v, want active-sync deferral", result.Snapshot)
	}
}

func TestSnapshotLifecyclePublishesCatalogAfterHotPrune(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	identity := snapshots.ChainIdentity{
		ChainID:     1,
		NetworkID:   1,
		GenesisHash: strings.Repeat("91", common.HashLength),
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x91}, ed25519.SeedSize))

	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:               dir,
			Enabled:           true,
			Interval:          time.Hour,
			HistoryWindow:     1,
			CatalogSigningKey: privateKey,
			CatalogChain:      &identity,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})
	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.CatalogPublished || result.Prune.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("lifecycle result = %+v, want signed catalog before hot prune", result)
	}
	if _, _, err := snapshots.VerifySignedSnapshotCatalog(dir, identity, []ed25519.PublicKey{privateKey.Public().(ed25519.PublicKey)}); err != nil {
		t.Fatalf("VerifySignedSnapshotCatalog: %v", err)
	}
}

func TestSnapshotLifecycleDoesNotPruneWhenCatalogIdentityMismatches(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	wrongIdentity := snapshots.ChainIdentity{
		ChainID:     2,
		NetworkID:   1,
		GenesisHash: strings.Repeat("92", common.HashLength),
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifestForChain(0, 0, nil, wrongIdentity)); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	wantIdentity := wrongIdentity
	wantIdentity.ChainID = 1
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x92}, ed25519.SeedSize))
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:               dir,
			Enabled:           true,
			Interval:          time.Hour,
			HistoryWindow:     1,
			CatalogSigningKey: privateKey,
			CatalogChain:      &wantIdentity,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})
	if _, err := lifecycle.OnePass(); err == nil || !strings.Contains(err.Error(), "chain identity mismatch") {
		t.Fatalf("lifecycle pass error = %v, want catalog identity mismatch", err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || !ok {
		t.Fatalf("hot domain change after failed catalog publish = ok %v err %v, want retained", ok, err)
	}
}

func TestSnapshotLifecycleBuildsEventLogsBeforePruningHotRows(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	addr := []byte{0x41, 0x21, 0x22, 0x23, 0x24}
	topic := common.Hash{0xbb}
	block, infos := lifecycleEventLogBlock(t, 1, []*corepb.TransactionInfo_Log{
		{Address: addr, Topics: [][]byte{topic[:]}, Data: []byte{0x01}},
	})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}

	chain := &fakePruneChain{db: db, solidified: 2}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:            dir,
			Enabled:        true,
			Interval:       time.Hour,
			HistoryWindow:  1,
			BuildEventLogs: true,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.EventLogBuilt || result.Snapshot.FromBlock != 1 || result.Snapshot.ToBlock != 1 {
		t.Fatalf("snapshot result = %+v, want event-log build over block 1", result.Snapshot)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("hot domain change survived ok=%v err=%v", ok, err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered = %v/%v, want true/nil", covered, err)
	}
	var rows []rawdb.EventLog
	if err := mgr.IterateEventLogs(1, 1, rawdb.EventLogFilter{
		Addresses: []common.Address{common.BytesToAddress(addr)},
		Topics:    [][]common.Hash{{topic}},
	}, func(row rawdb.EventLog) (bool, error) {
		rows = append(rows, row)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateEventLogs: %v", err)
	}
	if len(rows) != 1 || rows[0].BlockNum != 1 || !bytes.Equal(rows[0].Log.GetData(), []byte{0x01}) {
		t.Fatalf("event rows = %+v, want one cold event log", rows)
	}
}

func TestSnapshotLifecycleBuildsBalanceTracesBeforePruningHotRows(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	dir := t.TempDir()
	writeSnapPruningChange(t, db, 1, 10, 12)
	traceOwner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x77}, common.AccountIDLength)...))
	block, _ := lifecycleEventLogBlock(t, 1, nil)
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 1, lifecycleBlockBalanceTrace(block, 40_001)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, traceOwner.Bytes(), 1, 444); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}

	chain := &fakePruneChain{db: db, solidified: 2}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:                dir,
			Enabled:            true,
			Interval:           time.Hour,
			HistoryWindow:      1,
			BuildBalanceTraces: true,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
		BalanceTracePrune: func() (*snapshots.PruneHotBalanceTraceResult, error) {
			manifest, err := snapshots.LoadProductionManifest(dir)
			if err != nil {
				return nil, err
			}
			return snapshots.PruneHotBalanceTracesWithProgress(db, dir, manifest)
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.Snapshot.BalanceTraceBuilt || result.Snapshot.FromBlock != 1 || result.Snapshot.ToBlock != 1 {
		t.Fatalf("snapshot result = %+v, want balance-trace build over block 1", result.Snapshot)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("hot domain change survived ok=%v err=%v", ok, err)
	}
	if result.BalanceTracePrune == nil ||
		result.BalanceTracePrune.BlockTracesDeleted != 1 ||
		result.BalanceTracePrune.AccountTracesDeleted != 1 ||
		result.BalanceTracePrune.ColdTraceSegments != 1 {
		t.Fatalf("balance trace prune result = %+v, want one hot block/account trace deleted", result.BalanceTracePrune)
	}
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got != nil {
		t.Fatalf("hot BlockBalanceTrace survived = %+v, want nil", got)
	}
	if balance, ok := rawdb.ReadAccountTrace(db, traceOwner.Bytes(), 1); ok || balance != 0 {
		t.Fatalf("hot AccountTrace survived = %d/%v, want 0/false", balance, ok)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	db.SetBalanceTraceReader(mgr)
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got == nil || got.GetTimestamp() != 40_001 {
		t.Fatalf("cold ReadBlockBalanceTrace after prune = %+v, want timestamp 40001", got)
	}
	traceBlock, balance, ok, err := rawdb.ReadAccountTraceAtOrBefore(db, traceOwner.Bytes(), 1)
	if err != nil || !ok || traceBlock != 1 || balance != 444 {
		t.Fatalf("cold ReadAccountTraceAtOrBefore = block %d balance %d ok %v err %v, want 1/444/true/nil", traceBlock, balance, ok, err)
	}
}

func TestSnapshotLifecycleDefersDerivedBuildsUntilNearTip(t *testing.T) {
	db := rawdb.NewMemoryChainDB()
	dir := t.TempDir()
	change, _, _ := writeSnapPruningChange(t, db, 1, 10, 12)
	traceOwner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x78}, common.AccountIDLength)...))
	addr := []byte{0x41, 0x31, 0x32, 0x33, 0x34}
	block, infos := lifecycleEventLogBlock(t, 1, []*corepb.TransactionInfo_Log{{Address: addr, Data: []byte{0x02}}})
	if err := rawdb.WriteBlock(db, block); err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if err := rawdb.WriteTransactionInfosByBlock(db, 1, infos); err != nil {
		t.Fatalf("WriteTransactionInfosByBlock: %v", err)
	}
	if err := rawdb.WriteBlockBalanceTrace(db, 1, lifecycleBlockBalanceTrace(block, 40_002)); err != nil {
		t.Fatalf("WriteBlockBalanceTrace: %v", err)
	}
	if err := rawdb.WriteAccountTrace(db, traceOwner.Bytes(), 1, 445); err != nil {
		t.Fatalf("WriteAccountTrace: %v", err)
	}

	chain := &fakePruneChain{db: db, solidified: 2, syncRemaining: 100, syncRemainingOK: true}
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Snapshot: snapshots.Config{
			Dir:                           dir,
			Enabled:                       true,
			Interval:                      time.Hour,
			HistoryWindow:                 1,
			DeferHistoryBuildWhileSyncing: true,
			BuildBalanceTraces:            true,
			BuildEventLogs:                true,
		},
		Pruner: PrunerConfig{
			Policy:      SnapPolicy(1, 1),
			Interval:    time.Hour,
			SnapshotDir: dir,
		},
		BalanceTracePrune: func() (*snapshots.PruneHotBalanceTraceResult, error) {
			manifest, err := snapshots.LoadProductionManifest(dir)
			if err != nil {
				if os.IsNotExist(err) {
					return nil, nil
				}
				return nil, err
			}
			return snapshots.PruneHotBalanceTracesWithProgress(db, dir, manifest)
		},
	})

	deferred, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("far-behind lifecycle pass: %v", err)
	}
	if !deferred.Snapshot.HistoryDeferred || deferred.Snapshot.Built || deferred.Snapshot.EventLogBuilt || deferred.Snapshot.BalanceTraceBuilt {
		t.Fatalf("far-behind lifecycle result = %+v, want cold and derived builds deferred", deferred.Snapshot)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, change.Seq); err != nil || !ok {
		t.Fatalf("hot domain change during deferral = ok %v err %v, want retained", ok, err)
	}
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got == nil {
		t.Fatal("hot balance trace removed during deep-sync deferral")
	}

	chain.syncRemaining = 1
	completed, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("near-tip lifecycle pass: %v", err)
	}
	if !completed.Snapshot.Built || completed.Snapshot.HistoryDeferred || !completed.Snapshot.EventLogBuilt || !completed.Snapshot.BalanceTraceBuilt {
		t.Fatalf("near-tip lifecycle result = %+v, want cold and derived backlog built", completed.Snapshot)
	}
	if completed.BalanceTracePrune == nil || completed.BalanceTracePrune.BlockTracesDeleted != 1 || completed.BalanceTracePrune.AccountTracesDeleted != 1 {
		t.Fatalf("near-tip balance trace prune = %+v, want hot duplicates reclaimed", completed.BalanceTracePrune)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	covered, err := mgr.EventLogRangeCovered(1, 1)
	if err != nil || !covered {
		t.Fatalf("EventLogRangeCovered after near-tip build = %v/%v, want true/nil", covered, err)
	}
	db.SetBalanceTraceReader(mgr)
	if got := rawdb.ReadBlockBalanceTrace(db, 1); got == nil || got.GetTimestamp() != 40_002 {
		t.Fatalf("cold BlockBalanceTrace after near-tip build = %+v, want timestamp 40002", got)
	}
}

func TestSnapshotLifecyclePruneDeferralRequiresOptIn(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deferPrune    bool
		wantDeferred  bool
		wantPruneRuns int
	}{
		{name: "default preserves ordered prune", wantPruneRuns: 1},
		{name: "busy importer defers ordered prune", deferPrune: true, wantDeferred: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			dir := t.TempDir()
			for blockNum := uint64(1); blockNum <= 4; blockNum++ {
				writeSnapPruningChange(t, db, blockNum, blockNum, blockNum)
			}
			chain := &fakePruneChain{db: db, solidified: 4, syncRemaining: 1_000, syncRemainingOK: true}
			pruneRuns := 0
			lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
				Snapshot: snapshots.Config{
					Dir:                           dir,
					Enabled:                       true,
					Interval:                      time.Hour,
					HistoryWindow:                 1,
					DeferHistoryBuildWhileSyncing: true,
					MaxDeferredHistoryBlocks:      2,
					MaxBusyDeferredHistoryBlocks:  4,
					SyncBuildReady:                func() bool { return false },
				},
				Pruner: PrunerConfig{
					Policy:      SnapPolicy(1, 1),
					Interval:    time.Hour,
					SnapshotDir: dir,
				},
				ChainLookupPrune: func() (*snapshots.PruneHotChainLookupResult, error) {
					pruneRuns++
					return nil, nil
				},
				DeferPruneOnSyncHistoryDeferral: tc.deferPrune,
			})

			result, err := lifecycle.OnePass()
			if err != nil {
				t.Fatalf("OnePass: %v", err)
			}
			if !result.Snapshot.HistoryDeferred || result.Snapshot.Built {
				t.Fatalf("snapshot pass = %+v, want busy-importer history deferral", result.Snapshot)
			}
			if result.PruneDeferred != tc.wantDeferred || pruneRuns != tc.wantPruneRuns {
				t.Fatalf("PruneDeferred=%v pruneRuns=%d, want %v/%d", result.PruneDeferred, pruneRuns, tc.wantDeferred, tc.wantPruneRuns)
			}
		})
	}
}

func TestSnapshotLifecycleRunsChainLookupPruneAfterHotPrune(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeSnapPruningChange(t, db, 1, 10, 12)
	chain := &fakePruneChain{db: db, solidified: 5}
	sawHotPruneStage := false
	chainLookupRan := false
	sawChainLookupBeforeSectionBloom := false
	sectionBloomRan := false
	sawSectionBloomBeforeBalanceTrace := false
	balanceTraceRan := false
	sawBalanceTraceBeforeRetiredPrune := false
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:    FullPolicy(2, 1),
			Interval:  time.Hour,
			BatchSize: 10,
		},
		ChainLookupPrune: func() (*snapshots.PruneHotChainLookupResult, error) {
			got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune)
			if err != nil {
				return nil, err
			}
			sawHotPruneStage = ok && got == 5
			chainLookupRan = true
			return &snapshots.PruneHotChainLookupResult{
				HasRange:          true,
				FromBlock:         0,
				ToBlock:           1,
				ColdIndexSegments: 1,
			}, nil
		},
		SectionBloomPrune: func() (*snapshots.PruneHotSectionBloomResult, error) {
			sawChainLookupBeforeSectionBloom = chainLookupRan
			sectionBloomRan = true
			return &snapshots.PruneHotSectionBloomResult{
				HasRange:          true,
				FromSection:       0,
				ToSection:         1,
				ColdBloomSegments: 1,
				RowsDeleted:       2,
			}, nil
		},
		BalanceTracePrune: func() (*snapshots.PruneHotBalanceTraceResult, error) {
			sawSectionBloomBeforeBalanceTrace = sectionBloomRan
			balanceTraceRan = true
			return &snapshots.PruneHotBalanceTraceResult{
				HasRange:             true,
				FromBlock:            10,
				ToBlock:              12,
				ColdTraceSegments:    1,
				BlockTracesDeleted:   2,
				AccountTracesDeleted: 2,
			}, nil
		},
		RetiredPrune: func(context.Context, snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error) {
			sawBalanceTraceBeforeRetiredPrune = balanceTraceRan
			return &snapshots.PruneRetiredSegmentFilesResult{
				RetiredSegments: 3,
				FilesDeleted:    2,
				BytesDeleted:    100,
			}, nil
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !sawHotPruneStage {
		t.Fatal("chain lookup prune hook ran before state hot-prune stage advanced")
	}
	if result.ChainLookupPrune == nil || !result.ChainLookupPrune.HasRange || result.ChainLookupPrune.ToBlock != 1 {
		t.Fatalf("chain lookup prune result = %+v, want hook result", result.ChainLookupPrune)
	}
	if !sawChainLookupBeforeSectionBloom {
		t.Fatal("section bloom prune hook ran before chain lookup prune hook")
	}
	if result.SectionBloomPrune == nil || !result.SectionBloomPrune.HasRange || result.SectionBloomPrune.RowsDeleted != 2 {
		t.Fatalf("section bloom prune result = %+v, want hook result", result.SectionBloomPrune)
	}
	if !sawSectionBloomBeforeBalanceTrace {
		t.Fatal("balance trace prune hook ran before section bloom prune hook")
	}
	if result.BalanceTracePrune == nil || !result.BalanceTracePrune.HasRange || result.BalanceTracePrune.ToBlock != 12 {
		t.Fatalf("balance trace prune result = %+v, want hook result", result.BalanceTracePrune)
	}
	if !sawBalanceTraceBeforeRetiredPrune {
		t.Fatal("retired segment prune hook ran before balance trace prune hook")
	}
	if result.RetiredPrune == nil || result.RetiredPrune.FilesDeleted != 2 || result.RetiredPrune.BytesDeleted != 100 {
		t.Fatalf("retired segment prune result = %+v, want hook result", result.RetiredPrune)
	}
}

func TestSnapshotLifecycleBuildsChainFreezerBeforePruningLookups(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	chain := &fakePruneChain{db: db, solidified: 2}
	chainFreezerBuilt := false
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:   FullPolicy(2, 1),
			Interval: time.Hour,
		},
		ChainFreezerBuild: func() (snapshots.ChainFreezerSnapshotPassResult, error) {
			chainFreezerBuilt = true
			return snapshots.ChainFreezerSnapshotPassResult{
				Built:     true,
				FromBlock: 0,
				ToBlock:   1,
				ColdHead:  2,
			}, nil
		},
		ChainLookupPrune: func() (*snapshots.PruneHotChainLookupResult, error) {
			if !chainFreezerBuilt {
				t.Fatal("chain lookup pruning ran before chain-freezer cold coverage was built")
			}
			return &snapshots.PruneHotChainLookupResult{HasRange: true, FromBlock: 0, ToBlock: 1}, nil
		},
	})

	result, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if !result.ChainFreezerBuild.Built || result.ChainFreezerBuild.ToBlock != 1 {
		t.Fatalf("chain-freezer result = %+v, want published range through block 1", result.ChainFreezerBuild)
	}
	if result.ChainLookupPrune == nil || !result.ChainLookupPrune.HasRange {
		t.Fatalf("chain lookup result = %+v, want hook result", result.ChainLookupPrune)
	}
}

func TestSnapshotLifecycleRequestPassCoalesces(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	chain := &fakePruneChain{db: db, solidified: 2}
	entered := make(chan int, 3)
	release := make(chan struct{})
	var passes atomic.Int32
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:   FullPolicy(2, 1),
			Interval: time.Hour,
		},
		RetiredPrune: func(context.Context, snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error) {
			pass := int(passes.Add(1))
			entered <- pass
			if pass == 2 {
				<-release
			}
			return nil, nil
		},
	})
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	defer func() {
		if err := lifecycle.Stop(); err != nil {
			t.Fatalf("stop lifecycle: %v", err)
		}
	}()

	waitLifecyclePass(t, entered, 1)
	lifecycle.RequestPass()
	waitLifecyclePass(t, entered, 2)
	lifecycle.RequestPass()
	lifecycle.RequestPass()
	close(release)
	waitLifecyclePass(t, entered, 3)

	time.Sleep(50 * time.Millisecond)
	if got := passes.Load(); got != 3 {
		t.Fatalf("lifecycle passes = %d, want initial pass plus two coalesced requested passes", got)
	}
}

func TestSnapshotLifecycleDefersRetiredPruneWhileSyncing(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	chain := &fakePruneChain{
		db:              db,
		solidified:      2,
		syncRemaining:   1_000,
		syncRemainingOK: true,
	}
	var calls int
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:   FullPolicy(2, 1),
			Interval: time.Hour,
		},
		DeferRetiredPruneWhileSyncing: true,
		RetiredPrune: func(context.Context, snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error) {
			calls++
			return &snapshots.PruneRetiredSegmentFilesResult{FilesDeleted: 1}, nil
		},
	})

	deferred, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("syncing lifecycle pass: %v", err)
	}
	if !deferred.RetiredDeferred || deferred.RetiredPrune != nil || calls != 0 {
		t.Fatalf("syncing retired prune = result:%+v calls:%d, want deferred", deferred, calls)
	}

	chain.syncRemaining = 0
	chain.syncRemainingOK = false
	completed, err := lifecycle.OnePass()
	if err != nil {
		t.Fatalf("completed-sync lifecycle pass: %v", err)
	}
	if completed.RetiredDeferred || completed.RetiredPrune == nil || completed.RetiredPrune.FilesDeleted != 1 || calls != 1 {
		t.Fatalf("completed-sync retired prune = result:%+v calls:%d, want one execution", completed, calls)
	}
}

func TestSnapshotLifecycleCancelsRetiredPruneWhenSyncBecomesActive(t *testing.T) {
	chain := &atomicSyncPruneChain{db: rawdb.NewMemoryDatabase(), solidified: 2}
	entered := make(chan struct{})
	var sawVerifier atomic.Bool
	lifecycle := NewSnapshotLifecycle(chain, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:   FullPolicy(2, 1),
			Interval: time.Hour,
		},
		DeferRetiredPruneWhileSyncing: true,
		RetiredPrune: func(ctx context.Context, verifyActive snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error) {
			sawVerifier.Store(verifyActive != nil)
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	result := make(chan SnapshotLifecyclePass, 1)
	errResult := make(chan error, 1)
	go func() {
		pass, err := lifecycle.OnePass()
		result <- pass
		errResult <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("retired prune did not start")
	}
	chain.syncRemaining.Store(80_000_000)
	chain.syncActive.Store(true)
	select {
	case err := <-errResult:
		if err != nil {
			t.Fatalf("catch-up-deferred lifecycle pass: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retired prune was not canceled after sync became active")
	}
	pass := <-result
	if !pass.RetiredDeferred || pass.RetiredPrune != nil {
		t.Fatalf("retired pass after dynamic catch-up = %+v, want deferred", pass)
	}
	if !sawVerifier.Load() {
		t.Fatal("retired prune did not receive active-manifest verifier")
	}
	if got := lifecycle.pruner.Stats(); got.RetiredVerificationCanceled != 1 {
		t.Fatalf("retired verification stats = %+v, want one catch-up cancellation", got)
	}
}

func TestSnapshotLifecycleStopCancelsRetiredPrune(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	entered := make(chan struct{})
	exited := make(chan struct{})
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:   FullPolicy(2, 1),
			Interval: time.Hour,
		},
		RetiredPrune: func(ctx context.Context, _ snapshots.ActiveManifestVerifier) (*snapshots.PruneRetiredSegmentFilesResult, error) {
			close(entered)
			<-ctx.Done()
			close(exited)
			return nil, ctx.Err()
		},
	})
	if err := lifecycle.Start(); err != nil {
		t.Fatalf("start lifecycle: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for retired prune")
	}
	stopped := make(chan error, 1)
	go func() { stopped <- lifecycle.Stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop lifecycle: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lifecycle Stop did not cancel retired prune")
	}
	select {
	case <-exited:
	default:
		t.Fatal("retired prune did not observe lifecycle cancellation")
	}
}

func TestSnapshotLifecyclePassCompleteHookRunsAfterSuccess(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	lifecycle := NewSnapshotLifecycle(&fakePruneChain{db: db, solidified: 2}, SnapshotLifecycleConfig{
		Pruner: PrunerConfig{
			Policy:   FullPolicy(2, 1),
			Interval: time.Hour,
		},
	})
	completed := 0
	lifecycle.AddPassCompleteHook(func() { completed++ })
	if _, err := lifecycle.OnePass(); err != nil {
		t.Fatalf("lifecycle pass: %v", err)
	}
	if completed != 1 {
		t.Fatalf("completed hooks = %d, want 1", completed)
	}
}

func TestSnapshotLifecyclePassLogContextReportsOnlyChangedMaintenance(t *testing.T) {
	ctx, changed := snapshotLifecyclePassLogContext(SnapshotLifecyclePass{}, 0, time.Second)
	if changed || len(ctx) != 2 {
		t.Fatalf("no-op context = %+v, changed=%t", ctx, changed)
	}

	pass := SnapshotLifecyclePass{
		Snapshot: snapshots.PassResult{LatestBuilt: true, LatestDuration: 2 * time.Second},
		ChainFreezerBuild: snapshots.ChainFreezerSnapshotPassResult{
			Built: true, FromBlock: 10, ToBlock: 19, EventLogBuilt: true, EventLogFromBlock: 10, EventLogToBlock: 19,
		},
		ChainLookupPrune: &snapshots.PruneHotChainLookupResult{
			HasRange: true, FromBlock: 10, ToBlock: 19, TxIndexesDeleted: 7,
		},
	}
	ctx, changed = snapshotLifecyclePassLogContext(pass, 19, 3*time.Second)
	if !changed {
		t.Fatal("changed maintenance was not reported")
	}
	fields := make(map[string]any, len(ctx)/2)
	for i := 0; i+1 < len(ctx); i += 2 {
		key, ok := ctx[i].(string)
		if !ok {
			t.Fatalf("field key %d = %T, want string", i, ctx[i])
		}
		fields[key] = ctx[i+1]
	}
	for key, want := range map[string]any{
		"latestSnapshotBuilt":           true,
		"latestSnapshotBlock":           uint64(19),
		"chainFreezerFromBlock":         uint64(10),
		"chainFreezerToBlock":           uint64(19),
		"chainFreezerEventLogBuilt":     true,
		"chainFreezerEventLogFromBlock": uint64(10),
		"chainFreezerEventLogToBlock":   uint64(19),
		"chainLookupTxIndexesDeleted":   uint64(7),
	} {
		if got := fields[key]; got != want {
			t.Errorf("field %s = %#v, want %#v", key, got, want)
		}
	}
}

func waitLifecyclePass(t *testing.T, entered <-chan int, want int) {
	t.Helper()
	select {
	case got := <-entered:
		if got != want {
			t.Fatalf("lifecycle pass = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for lifecycle pass %d", want)
	}
}

func lifecycleEventLogBlock(t *testing.T, number uint64, logs []*corepb.TransactionInfo_Log) (*coretypes.Block, []*corepb.TransactionInfo) {
	t.Helper()
	txPB := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp:  int64(10_000 + number),
			Expiration: int64(20_000 + number),
			Data:       []byte{byte(number)},
		},
	}
	tx := coretypes.NewTransactionFromPB(txPB)
	info := &corepb.TransactionInfo{
		Id:             append([]byte(nil), tx.Hash().Bytes()...),
		BlockNumber:    int64(number),
		BlockTimeStamp: int64(30_000 + number),
		Log:            logs,
	}
	block := coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(30_000 + number),
			},
		},
		Transactions: []*corepb.Transaction{txPB},
	})
	return block, []*corepb.TransactionInfo{info}
}

func lifecycleBlockBalanceTrace(block *coretypes.Block, timestamp int64) *contractpb.BlockBalanceTrace {
	if block == nil {
		return &contractpb.BlockBalanceTrace{Timestamp: timestamp}
	}
	return &contractpb.BlockBalanceTrace{
		BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
			Hash:   append([]byte(nil), block.Hash().Bytes()...),
			Number: int64(block.Number()),
		},
		Timestamp: timestamp,
	}
}
