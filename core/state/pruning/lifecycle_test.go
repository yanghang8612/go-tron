package pruning

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

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
	if manifest.Progress == nil || manifest.Progress.HistoryBuildTxNum != 12 || manifest.Progress.HotPruneTxNum != 12 {
		t.Fatalf("manifest progress = %+v, want history/hot-prune at 12", manifest.Progress)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHistory); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot history stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot hot-prune stage = %d ok=%v err=%v, want 12", got, ok, err)
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
