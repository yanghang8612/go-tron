package pruning

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statepkg "github.com/tronprotocol/go-tron/core/state"
	statedomains "github.com/tronprotocol/go-tron/core/state/domains"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
	coretypes "github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func TestWorkerPrunesDomainHistoryAndCheckpoints(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x33}, common.AccountIDLength)...))
	hash1 := common.Hash{0x01}
	hash4 := common.Hash{0x04}
	key := []byte("k")

	for _, blockNum := range []uint64{1, 4} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum:   blockNum,
			BlockHash:  common.Hash{byte(blockNum)},
			TxNum:      blockNum,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 0,
			Domain:     kvdomains.SystemDynamicProperty,
			Key:        key,
			PrevExists: true,
			Prev:       []byte("prev"),
			NextExists: true,
			Next:       []byte("next"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{BlockNum: 1, BlockHash: hash1, Root: hash1, Scheme: rawdb.LatestDomainCommitmentScheme}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{BlockNum: 4, BlockHash: hash4, Root: hash4, Scheme: rawdb.LatestDomainCommitmentScheme}); err != nil {
		t.Fatal(err)
	}

	stats, err := Run(db, FullPolicy(3, 2), 5)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if stats.DeletedTxRanges != 1 || stats.DeletedDomainChangeBlocks != 1 || stats.DeletedCommitmentCheckpoints != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || ok {
		t.Fatalf("block 1 range survived ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 4); err != nil || !ok {
		t.Fatalf("block 4 range missing ok:%v err:%v", ok, err)
	}
	var touched []uint64
	if err := rawdb.IterateStateDomainChangeBlocks(db, owner, 0, kvdomains.SystemDynamicProperty, key, func(blockNum uint64) (bool, error) {
		touched = append(touched, blockNum)
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(touched) != 1 || touched[0] != 4 {
		t.Fatalf("inverse blocks = %v, want [4]", touched)
	}
	if _, ok, err := rawdb.ReadStateCommitmentCheckpoint(db, 1); err != nil || ok {
		t.Fatalf("block 1 checkpoint survived ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateCommitmentCheckpoint(db, 4); err != nil || !ok {
		t.Fatalf("block 4 checkpoint missing ok:%v err:%v", ok, err)
	}
	report, err := Check(db, FullPolicy(3, 2), 5, "")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(report.Warnings) != 0 || report.RetainedTxRanges != 1 || report.RetainedDomainChanges != 1 || report.CommitmentCheckpoints != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestWorkerPrunesDomainHistoryForBlocksAndMinimalModes(t *testing.T) {
	for _, policy := range []Policy{
		BlocksPolicy(3, 2),
		MinimalPolicy(3, 2),
	} {
		t.Run(string(policy.Mode), func(t *testing.T) {
			db := rawdb.NewMemoryDatabase()
			oldChange, _, _ := writeSnapPruningChange(t, db, 1, 10, 12)
			recentChange, _, _ := writeSnapPruningChange(t, db, 4, 40, 42)

			pre, err := Check(db, policy, 5, "")
			if err != nil {
				t.Fatalf("pre-prune check: %v", err)
			}
			if !hasWarning(pre.Warnings, "state tx range for prunable block 1 is still present") {
				t.Fatalf("pre-prune warnings = %v, want stale tx-range warning", pre.Warnings)
			}

			stats, err := Worker{DB: db, Policy: policy}.PruneTo(5)
			if err != nil {
				t.Fatalf("prune %s: %v", policy.Mode, err)
			}
			if stats.DeletedTxRanges != 1 || stats.DeletedDomainChangeBlocks != 1 {
				t.Fatalf("%s stats = %+v, want one history range and change block deleted", policy.Mode, stats)
			}
			if _, ok, err := rawdb.ReadStateTxRange(db, oldChange.BlockNum); err != nil || ok {
				t.Fatalf("%s old tx range ok=%v err=%v, want deleted", policy.Mode, ok, err)
			}
			if _, ok, err := rawdb.ReadStateDomainChange(db, oldChange.BlockNum, oldChange.Seq); err != nil || ok {
				t.Fatalf("%s old domain change ok=%v err=%v, want deleted", policy.Mode, ok, err)
			}
			if _, ok, err := rawdb.ReadStateTxRange(db, recentChange.BlockNum); err != nil || !ok {
				t.Fatalf("%s recent tx range ok=%v err=%v, want retained", policy.Mode, ok, err)
			}
			if _, ok, err := rawdb.ReadStateDomainChange(db, recentChange.BlockNum, recentChange.Seq); err != nil || !ok {
				t.Fatalf("%s recent domain change ok=%v err=%v, want retained", policy.Mode, ok, err)
			}
			post, err := Check(db, policy, 5, "")
			if err != nil {
				t.Fatalf("post-prune check: %v", err)
			}
			if len(post.Warnings) != 0 || post.RetainedTxRanges != 1 || post.RetainedDomainChanges != 1 {
				t.Fatalf("%s post-prune report = %+v", policy.Mode, post)
			}
		})
	}
}

func TestWorkerBatchesHotPruneDeletes(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	db := &pruneBatchCountingStore{KeyValueStore: base}
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x71}, common.AccountIDLength)...))

	for _, blockNum := range []uint64{1, 2} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
		for seq := uint64(1); seq <= 2; seq++ {
			if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
				BlockNum:   blockNum,
				BlockHash:  common.Hash{byte(blockNum)},
				TxNum:      blockNum,
				Seq:        seq,
				FlatDomain: rawdb.StateFlatDomainKVLatest,
				Owner:      owner,
				Domain:     kvdomains.SystemDynamicProperty,
				Key:        []byte{byte(seq)},
				PrevExists: true,
				Prev:       []byte("prev"),
				NextExists: true,
				Next:       []byte("next"),
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := rawdb.WriteStateCommitmentCheckpoint(db, &rawdb.StateCommitmentCheckpoint{
			BlockNum:  blockNum,
			BlockHash: common.Hash{byte(blockNum)},
			Root:      common.Hash{byte(blockNum)},
			Scheme:    rawdb.LatestDomainCommitmentScheme,
		}); err != nil {
			t.Fatal(err)
		}
	}
	db.batchWrites = 0
	db.directDeletes = 0

	stats, err := Worker{DB: db, Policy: FullPolicy(2, 1)}.PruneTo(5)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if stats.DeletedTxRanges != 2 || stats.DeletedDomainChangeBlocks != 2 || stats.DeletedCommitmentCheckpoints != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if db.directDeletes != 0 {
		t.Fatalf("direct prune deletes = %d, want 0", db.directDeletes)
	}
	if db.batchWrites != 2 {
		t.Fatalf("prune batch writes = %d, want 2 (history and checkpoints)", db.batchWrites)
	}
}

func TestPruneBatchStoreFlushesAtConfiguredLimit(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	keys := [][]byte{[]byte("aaaaaa"), []byte("bbbbbb"), []byte("cccccc")}
	for _, key := range keys {
		if err := base.Put(key, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	db := &pruneBatchCountingStore{KeyValueStore: base}
	store, flush := newPruneBatchStoreWithLimit(db, 10)
	for _, key := range keys {
		if err := store.Delete(key); err != nil {
			t.Fatalf("delete %q: %v", key, err)
		}
	}
	if err := flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if db.directDeletes != 0 {
		t.Fatalf("direct deletes = %d, want 0", db.directDeletes)
	}
	if db.batchWrites != len(keys) {
		t.Fatalf("batch writes = %d, want %d", db.batchWrites, len(keys))
	}
	for _, key := range keys {
		has, err := base.Has(key)
		if err != nil || has {
			t.Fatalf("key %q remains=%t err=%v", key, has, err)
		}
	}
}

func TestWorkerDoesNotAdvancePruneStagesWhenHistoryBatchFails(t *testing.T) {
	base := rawdb.NewMemoryDatabase()
	db := &pruneBatchCountingStore{KeyValueStore: base, writeErr: errors.New("injected batch failure")}
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x72}, common.AccountIDLength)...))
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemDynamicProperty,
		Key:        []byte("key"),
		PrevExists: true,
		Prev:       []byte("prev"),
		NextExists: true,
		Next:       []byte("next"),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := (Worker{DB: db, Policy: FullPolicy(2, 1)}).PruneTo(5); err == nil || !strings.Contains(err.Error(), "injected batch failure") {
		t.Fatalf("prune error = %v, want injected batch failure", err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range after failed batch ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || !ok {
		t.Fatalf("domain change after failed batch ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || ok {
		t.Fatalf("snapshot/hot-prune stage advanced after failed batch ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune); err != nil || ok {
		t.Fatalf("snapshot/prune stage advanced after failed batch ok=%v err=%v", ok, err)
	}
}

func TestWorkerSnapPreservesHotChangesWithoutCompleteSnapshotCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 11, "history/state-domain-change-10-11.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 11, refs)); err != nil {
		t.Fatal(err)
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(3, 2), SnapshotDir: dir}.PruneTo(5)
	if err != nil {
		t.Fatalf("snap prune: %v", err)
	}
	if stats.DeletedDomainChangeBlocks != 0 || stats.DeletedTxRanges != 0 {
		t.Fatalf("stats = %+v, want no hot pruning", stats)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range not retained ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || !ok {
		t.Fatalf("domain change not retained ok:%v err:%v", ok, err)
	}
	report, err := Check(db, SnapPolicy(3, 2), 5, dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(report.Warnings) != 0 || report.RetainedTxRanges != 1 || report.RetainedDomainChanges != 1 {
		t.Fatalf("report = %+v", report)
	}
}

func TestWorkerSnapPrunesHotChangesWithSnapshotCoverageAndKeepsTxRange(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(3, 2), SnapshotDir: dir}.PruneTo(5)
	if err != nil {
		t.Fatalf("snap prune: %v", err)
	}
	if stats.DeletedDomainChangeBlocks != 1 || stats.DeletedTxRanges != 0 {
		t.Fatalf("stats = %+v, want one hot change block and no tx range deletes", stats)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || !ok || got != 12 {
		t.Fatalf("snapshot hot-prune stage progress = %d ok=%v err=%v, want 12", got, ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range not retained ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("domain change survived ok:%v err:%v", ok, err)
	}
	report, err := Check(db, SnapPolicy(3, 2), 5, dir)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if len(report.Warnings) != 0 || report.RetainedTxRanges != 1 || report.RetainedDomainChanges != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestWorkerSnapResumesHotHistoryPruneAfterPersistedBlockCursor(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	for _, blockNum := range []uint64{1, 2} {
		writeSnapPruningChange(t, db, blockNum, blockNum*10, blockNum*10+2)
	}

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 22, "history/state-domain-change-10-22.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 22, refs)); err != nil {
		t.Fatal(err)
	}

	worker := Worker{DB: db, Policy: SnapPolicy(1, 1), SnapshotDir: dir, MaxBlocks: 1}
	first, err := worker.PruneTo(5)
	if err != nil {
		t.Fatalf("first snap prune: %v", err)
	}
	if first.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("first stats = %+v, want one deleted change block", first)
	}
	manifest, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Progress == nil || manifest.Progress.HotPruneBlockNum != 1 || manifest.Progress.HotPruneTxNum != 12 {
		t.Fatalf("first progress = %+v, want block 1 tx 12", manifest.Progress)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || ok {
		t.Fatalf("block 1 change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 2, 1); err != nil || !ok {
		t.Fatalf("block 2 change missing before resume ok=%v err=%v", ok, err)
	}

	second, err := (Worker{DB: db, Policy: SnapPolicy(1, 1), SnapshotDir: dir, MaxBlocks: 1}).PruneTo(5)
	if err != nil {
		t.Fatalf("resumed snap prune: %v", err)
	}
	if second.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("second stats = %+v, want next deleted change block", second)
	}
	manifest, err = snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Progress == nil || manifest.Progress.HotPruneBlockNum != 2 || manifest.Progress.HotPruneTxNum != 22 {
		t.Fatalf("second progress = %+v, want block 2 tx 22", manifest.Progress)
	}
	for _, blockNum := range []uint64{1, 2} {
		if _, ok, err := rawdb.ReadStateTxRange(db, blockNum); err != nil || !ok {
			t.Fatalf("snap tx range %d should remain ok=%v err=%v", blockNum, ok, err)
		}
	}
}

func TestWorkerArchivePrunesOnlyVerifiedColdDuplicateChanges(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	oldChange, _, _ := writeSnapPruningChange(t, db, 1, 10, 12)
	recentChange, _, _ := writeSnapPruningChange(t, db, 4, 40, 42)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}

	stats, err := Worker{DB: db, Policy: ArchiveColdPolicy(3, 2), SnapshotDir: dir}.PruneTo(5)
	if err != nil {
		t.Fatalf("archive cold prune: %v", err)
	}
	if stats.DeletedDomainChangeBlocks != 1 || stats.DeletedTxRanges != 0 {
		t.Fatalf("stats = %+v, want one duplicate change block and no tx range deletes", stats)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, oldChange.BlockNum, oldChange.Seq); err != nil || ok {
		t.Fatalf("covered old domain change survived ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, oldChange.BlockNum); err != nil || !ok {
		t.Fatalf("archive block/tx mapping was pruned ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, recentChange.BlockNum, recentChange.Seq); err != nil || !ok {
		t.Fatalf("recent hot domain change missing ok=%v err=%v", ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); err != nil || !ok || got != 12 {
		t.Fatalf("archive hot-prune stage = %d ok=%v err=%v, want 12", got, ok, err)
	}
	report, err := Check(db, ArchiveColdPolicy(3, 2), 5, dir)
	if err != nil {
		t.Fatalf("archive check after prune: %v", err)
	}
	if len(report.Warnings) != 0 || report.RetainedTxRanges != 2 || report.RetainedDomainChanges != 1 {
		t.Fatalf("archive post-prune report = %+v", report)
	}
}

func TestWorkerArchivePreservesHotChangesWhenColdCompanionIsCorrupt(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	change, _, _ := writeSnapPruningChange(t, db, 1, 10, 12)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentAccessor {
			corruptSnapshotFile(t, dir, ref)
			break
		}
	}

	stats, err := Worker{DB: db, Policy: ArchiveColdPolicy(3, 2), SnapshotDir: dir}.PruneTo(5)
	if err == nil || !strings.Contains(err.Error(), "accessor") {
		t.Fatalf("archive prune stats=%+v err=%v, want corrupt accessor error", stats, err)
	}
	if _, ok, readErr := rawdb.ReadStateDomainChange(db, change.BlockNum, change.Seq); readErr != nil || !ok {
		t.Fatalf("hot change deleted after failed cold verification ok=%v err=%v", ok, readErr)
	}
	if _, ok, readErr := rawdb.ReadStageProgress(db, rawdb.StageSnapshotHotPrune); readErr != nil || ok {
		t.Fatalf("hot-prune stage advanced after failed verification ok=%v err=%v", ok, readErr)
	}
}

func TestWorkerArchiveConcurrentColdReadsDuringHotPrune(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	change, _, _ := writeSnapPruningChange(t, db, 1, 10, 12)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}
	mgr, err := snapshots.OpenManager(dir)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			count := 0
			if err := mgr.IterateStateDomainChanges(10, 12, func(got *rawdb.StateDomainChange) (bool, error) {
				count++
				return true, nil
			}); err != nil {
				errCh <- err
				return
			}
			if count != 1 {
				errCh <- fmt.Errorf("cold change count %d, want 1", count)
				return
			}
		}
	}()
	close(start)
	if _, err := (Worker{DB: db, Policy: ArchiveColdPolicy(3, 2), SnapshotDir: dir}).PruneTo(5); err != nil {
		t.Fatalf("archive prune: %v", err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, change.BlockNum, change.Seq); err != nil || ok {
		t.Fatalf("duplicate hot change survived ok=%v err=%v", ok, err)
	}
}

func TestWorkerSnapRequiresReadableSnapshotCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentHistory {
			if err := os.Remove(filepath.Join(dir, ref.Path)); err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(3, 2), SnapshotDir: dir}.PruneTo(5)
	if err == nil {
		t.Fatalf("snap prune stats = %+v, want missing snapshot segment error", stats)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range after failed prune ok:%v err:%v, want retained", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || !ok {
		t.Fatalf("domain change after failed prune ok:%v err:%v, want retained", ok, err)
	}
}

func TestWorkerSnapRequiresReadableSnapshotCompanions(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)

	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}
	corruptedAccessor := false
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentAccessor {
			if err := os.WriteFile(filepath.Join(dir, ref.Path), []byte("corrupt-accessor"), 0o644); err != nil {
				t.Fatal(err)
			}
			corruptedAccessor = true
			break
		}
	}
	if !corruptedAccessor {
		t.Fatal("test fixture did not build a snapshot accessor companion")
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(3, 2), SnapshotDir: dir}.PruneTo(5)
	if err == nil || !strings.Contains(err.Error(), "accessor") {
		t.Fatalf("snap prune stats = %+v err = %v, want snapshot accessor error", stats, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("state tx range after failed companion prune ok:%v err:%v, want retained", ok, err)
	}
	if _, ok, err := rawdb.ReadStateDomainChange(db, 1, 1); err != nil || !ok {
		t.Fatalf("domain change after failed companion prune ok:%v err:%v, want retained", ok, err)
	}
}

func TestWorkerSnapshotCoverageContextCancelsVerification(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}

	ctx := &cancelAfterChecksContext{}
	// Pass the outer coverage and verification entry checks, then cancel from
	// the checksum reader inside companion verification.
	ctx.remaining.Store(4)
	_, err = (Worker{Policy: SnapPolicy(3, 2), SnapshotDir: dir}).snapshotStateDomainChangeCoverageContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("snapshot coverage error = %v, want context.Canceled", err)
	}
}

func TestWorkerCatchupCancellationStopsAtVerificationBoundary(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	oldChange, _, _ := writeSnapPruningChange(t, db, 1, 10, 12)
	_, _, _ = writeSnapPruningChange(t, db, 4, 40, 42)
	verificationCtx, cancelVerification := context.WithCancel(context.Background())
	worker := Worker{
		DB:                          db,
		Policy:                      FullPolicy(3, 2),
		coverageVerificationContext: verificationCtx,
		coverageVerificationDone:    cancelVerification,
	}
	stats, err := worker.PruneToContext(context.Background(), 5)
	if err != nil {
		t.Fatalf("prune after verification boundary: %v", err)
	}
	if !errors.Is(verificationCtx.Err(), context.Canceled) {
		t.Fatalf("verification context error = %v, want context.Canceled", verificationCtx.Err())
	}
	if stats.DeletedTxRanges != 1 || stats.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("prune stats after verification context closed = %+v", stats)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, oldChange.BlockNum); err != nil || ok {
		t.Fatalf("old tx range after verification context closed ok=%v err=%v, want deleted", ok, err)
	}
}

func TestWorkerSnapshotCoverageCacheReusesAndInvalidatesFileIdentity(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}

	cache := newSnapshotCoverageVerificationCache(dir)
	worker := Worker{Policy: SnapPolicy(3, 2), SnapshotDir: dir, coverageVerificationCache: cache}
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err != nil {
		t.Fatalf("initial coverage verification: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, snapshotCoverageVerificationCacheFile)); err != nil {
		t.Fatalf("persistent verification cache: %v", err)
	}
	// A cache hit performs the two outer checks plus the shared verifier entry
	// check, but no checksum or record reads. Without reuse, the next check in a
	// context reader would cancel.
	ctx := &cancelAfterChecksContext{}
	ctx.remaining.Store(3)
	if _, err := worker.snapshotStateDomainChangeCoverageContext(ctx); err != nil {
		t.Fatalf("cached coverage verification: %v", err)
	}

	var accessorPath string
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentAccessor {
			accessorPath = filepath.Join(dir, ref.Path)
			break
		}
	}
	if accessorPath == "" {
		t.Fatal("fixture has no accessor companion")
	}
	if err := os.WriteFile(accessorPath, []byte("changed accessor identity"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err == nil {
		t.Fatal("changed accessor identity reused cached verification")
	}
}

func TestWorkerSnapshotCoveragePersistentCacheRehashesAfterRestart(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}

	first := newSnapshotCoverageVerificationCache(dir)
	worker := Worker{Policy: SnapPolicy(3, 2), SnapshotDir: dir, coverageVerificationCache: first}
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err != nil {
		t.Fatalf("initial full verification: %v", err)
	}
	if got := first.Stats(); got.FullVerified != 1 || got.Entries != 1 {
		t.Fatalf("initial verification cache stats = %+v", got)
	}

	restarted := newSnapshotCoverageVerificationCache(dir)
	if err := restarted.LoadError(); err != nil {
		t.Fatalf("reload verification cache: %v", err)
	}
	worker.coverageVerificationCache = restarted
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err != nil {
		t.Fatalf("persistent checksum reuse: %v", err)
	}
	if got := restarted.Stats(); got.PersistentHits != 1 || got.FullVerified != 0 {
		t.Fatalf("restarted verification cache stats = %+v", got)
	}

	var accessor snapshots.SegmentRef
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentAccessor {
			accessor = ref
			break
		}
	}
	path := filepath.Join(dir, accessor.Path)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, info.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}

	// A fresh process has no timestamp shortcut. Even when size and mtime are
	// restored, the durable semantic record is accepted only after SHA-256.
	restarted = newSnapshotCoverageVerificationCache(dir)
	worker.coverageVerificationCache = restarted
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("tampered persistent-cache object error = %v, want checksum mismatch", err)
	}
}

func TestWorkerSnapshotCoverageMalformedPersistentCacheFallsBackToFullVerification(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}
	manifest, err := snapshots.LoadProductionManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	var history snapshots.SegmentRef
	for _, ref := range refs {
		if ref.Kind == snapshots.SegmentHistory {
			history = ref
			break
		}
	}
	key, err := snapshotHistoryVerificationKeyFor(dir, manifest, history)
	if err != nil {
		t.Fatal(err)
	}
	valid := snapshotHistoryVerificationRecordFor(key)
	invalid := valid
	invalid.Accessor.Checksum = "sha256:invalid"
	data, err := json.Marshal(snapshotCoverageVerificationDisk{
		Version: snapshotCoverageVerificationCacheVersion,
		Entries: []snapshotHistoryVerificationRecord{valid, invalid},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotCoverageVerificationCacheFile), data, 0o600); err != nil {
		t.Fatal(err)
	}

	cache := newSnapshotCoverageVerificationCache(dir)
	if cache.LoadError() == nil {
		t.Fatal("malformed persistent verification cache loaded without error")
	}
	if got := cache.Stats(); got.Entries != 0 {
		t.Fatalf("malformed persistent verification cache retained partial entries: %+v", got)
	}
	worker := Worker{Policy: SnapPolicy(3, 2), SnapshotDir: dir, coverageVerificationCache: cache}
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err != nil {
		t.Fatalf("full verification fallback: %v", err)
	}
	if got := cache.Stats(); got.FullVerified != 1 || got.PersistentHits != 0 || got.Entries != 1 {
		t.Fatalf("fallback verification cache stats = %+v", got)
	}
}

func TestPrunerRecordsTrustedLocalSnapshotForRestart(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 10, 12)
	refs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-10-12.seg")
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(10, 12, refs)); err != nil {
		t.Fatal(err)
	}
	pruner := NewPruner(&fakePruneChain{db: db, solidified: 1}, PrunerConfig{
		Policy:      SnapPolicy(3, 2),
		SnapshotDir: dir,
	})
	if err := pruner.RecordTrustedSnapshotSegments(refs); err != nil {
		t.Fatalf("record trusted snapshot: %v", err)
	}
	if got := pruner.coverageVerificationCache.Stats(); got.TrustedRecorded != 1 || got.Entries != 1 {
		t.Fatalf("trusted verification cache stats = %+v", got)
	}

	restarted := newSnapshotCoverageVerificationCache(dir)
	worker := Worker{Policy: SnapPolicy(3, 2), SnapshotDir: dir, coverageVerificationCache: restarted}
	if _, err := worker.snapshotStateDomainChangeCoverageContext(context.Background()); err != nil {
		t.Fatalf("restart trusted checksum reuse: %v", err)
	}
	if got := restarted.Stats(); got.PersistentHits != 1 || got.FullVerified != 0 {
		t.Fatalf("trusted restart verification cache stats = %+v", got)
	}
}

func TestRetiredPruneUsesPersistentStateHistoryVerification(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 1, 3)
	_, _, _ = writeSnapPruningChange(t, db, 10, 10, 12)
	retiredRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 3, "history/state-domain-change-retired.seg")
	if err != nil {
		t.Fatal(err)
	}
	activeRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-active.seg")
	if err != nil {
		t.Fatal(err)
	}
	manifest := snapshots.NewManifest(10, 12, activeRefs)
	manifest.Retired = retiredRefs
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	first := NewPruner(&fakePruneChain{db: db, solidified: 10}, PrunerConfig{Policy: SnapPolicy(3, 2), SnapshotDir: dir})
	if err := first.RecordTrustedSnapshotSegments(activeRefs); err != nil {
		t.Fatalf("record trusted active snapshot: %v", err)
	}

	restarted := NewPruner(&fakePruneChain{db: db, solidified: 10}, PrunerConfig{Policy: SnapPolicy(3, 2), SnapshotDir: dir})
	result, err := snapshots.PruneRetiredSegmentFilesContextWithVerifier(context.Background(), dir, restarted.verifyActiveSnapshotManifest)
	if err != nil {
		t.Fatalf("retired prune with persistent verification: %v", err)
	}
	if result.FilesDeleted != len(retiredRefs) {
		t.Fatalf("retired prune result = %+v, want %d deleted files", result, len(retiredRefs))
	}
	if got := restarted.Stats(); got.RetiredVerificationPersistentHits != 1 || got.RetiredVerificationFull != 0 {
		t.Fatalf("retired persistent verification stats = %+v", got)
	}
	for _, ref := range retiredRefs {
		if _, err := os.Stat(filepath.Join(dir, ref.Path)); !os.IsNotExist(err) {
			t.Fatalf("retired file %q still present or stat failed: %v", ref.Path, err)
		}
	}
}

func TestRetiredPruneReauthenticatesTrustedMemoryHitBeforeDelete(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	_, _, _ = writeSnapPruningChange(t, db, 1, 1, 3)
	_, _, _ = writeSnapPruningChange(t, db, 10, 10, 12)
	retiredRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 3, "history/state-domain-change-retired.seg")
	if err != nil {
		t.Fatal(err)
	}
	activeRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 10, 12, "history/state-domain-change-active.seg")
	if err != nil {
		t.Fatal(err)
	}
	manifest := snapshots.NewManifest(10, 12, activeRefs)
	manifest.Retired = retiredRefs
	if err := snapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}
	pruner := NewPruner(&fakePruneChain{db: db, solidified: 10}, PrunerConfig{Policy: SnapPolicy(3, 2), SnapshotDir: dir})
	if err := pruner.RecordTrustedSnapshotSegments(activeRefs); err != nil {
		t.Fatalf("record trusted active snapshot: %v", err)
	}
	var accessor snapshots.SegmentRef
	for _, ref := range activeRefs {
		if ref.Kind == snapshots.SegmentAccessor {
			accessor = ref
			break
		}
	}
	path := filepath.Join(dir, accessor.Path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = snapshots.PruneRetiredSegmentFilesContextWithVerifier(context.Background(), dir, pruner.verifyActiveSnapshotManifest)
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("tampered trusted active snapshot error = %v, want checksum mismatch", err)
	}
	for _, ref := range retiredRefs {
		if _, err := os.Stat(filepath.Join(dir, ref.Path)); err != nil {
			t.Fatalf("retired fallback %q removed after failed active gate: %v", ref.Path, err)
		}
	}
}

func TestCheckerValidatesSnapshotSegmentsAndCodeHashes(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x44}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	ref, err := snapshots.BuildLatestDomainSegmentFromDB(db, dir, kvdomains.SystemDynamicProperty, 1, 1, "latest/system-dp.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Domain:     kvdomains.SystemDynamicProperty,
		Key:        []byte("k"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("v"),
	}); err != nil {
		t.Fatal(err)
	}
	historyRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 1, 1, "history/state-domain-change-1-1.seg")
	if err != nil {
		t.Fatal(err)
	}
	refs := append([]snapshots.SegmentRef{ref}, historyRefs...)
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 1, refs)); err != nil {
		t.Fatal(err)
	}
	report, err := Check(db, ArchivePolicy(), 1, dir)
	if err != nil {
		t.Fatalf("check snapshots: %v", err)
	}
	if report.SnapshotSegments != 4 || report.LatestRows != 1 || report.KVLatestRows != 1 || !report.CommitmentRootPresent {
		t.Fatalf("report = %+v", report)
	}
	code := []byte{0xde, 0xad}
	hash := common.Keccak256(code)
	if err := CheckCodeHashes(db, []common.Hash{hash}); err == nil {
		t.Fatal("missing code hash accepted")
	}
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if err := CheckCodeHashes(db, []common.Hash{hash}); err != nil {
		t.Fatalf("code hash check: %v", err)
	}
}

func TestCheckerRequiresCommitmentRootForFlatLatest(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x45}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	_, err := Check(db, ArchivePolicy(), 1, "")
	if err == nil || !strings.Contains(err.Error(), "CommitmentDomain root") {
		t.Fatalf("check error = %v, want missing CommitmentDomain root", err)
	}
}

func TestCheckerCountsFlatLatestDatasets(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x46}, common.AccountIDLength)...))
	writeAccountLatestEnvelope(t, db, owner, common.Hash{})
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	report, err := Check(db, ArchivePolicy(), 1, "")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.LatestRows != 3 || report.AccountLatestRows != 1 || report.KVGenerationRows != 1 || report.KVLatestRows != 1 {
		t.Fatalf("latest counts = %+v", report)
	}
	if !report.CommitmentRootPresent || report.CommitmentDomainRows == 0 {
		t.Fatalf("commitment counts = %+v", report)
	}
}

func TestCheckerRejectsCorruptLatestCommitmentCheckpointPointer(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateCommitmentDomain(db, rawdb.LatestStateCommitmentCheckpointLogicalKey(), []byte("not-rlp")); err != nil {
		t.Fatal(err)
	}
	_, err := Check(db, ArchivePolicy(), 0, "")
	if err == nil || !strings.Contains(err.Error(), "latest commitment checkpoint pointer") {
		t.Fatalf("check error = %v, want corrupt latest commitment checkpoint pointer", err)
	}
}

func TestCheckerRequiresReferencedCodeHashCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x47}, common.AccountIDLength)...))
	code := []byte{0x60, 0x01, 0x60, 0x02}
	hash := common.Keccak256(code)
	writeAccountLatestEnvelope(t, db, owner, hash)
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	_, err := Check(db, ArchivePolicy(), 1, "")
	if err == nil || !strings.Contains(err.Error(), "missing code hash") {
		t.Fatalf("check error = %v, want missing code hash", err)
	}
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	report, err := Check(db, ArchivePolicy(), 1, "")
	if err != nil {
		t.Fatalf("check with hot code: %v", err)
	}
	if report.ReferencedCodeHashes != 1 {
		t.Fatalf("referenced code hashes = %d, want 1", report.ReferencedCodeHashes)
	}
}

func TestCheckerSurfacesUnreadableHotCodeWithoutSnapshotCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4d}, common.AccountIDLength)...))
	code := []byte{0x60, 0x11}
	hash := common.Keccak256(code)
	writeAccountLatestEnvelope(t, db, owner, hash)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	wrapped := &checkerCodeHidingStore{
		Store: db,
		hidden: map[common.Hash]struct{}{
			hash: {},
		},
	}
	_, err := Check(wrapped, ArchivePolicy(), 1, "")
	if err == nil || !strings.Contains(err.Error(), "hidden state code") {
		t.Fatalf("check error = %v, want hidden state code", err)
	}
}

func TestCheckerRequiresHistoricalCodeHashCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x49}, common.AccountIDLength)...))
	oldCode := []byte{0x60, 0x05}
	newCode := []byte{0x60, 0x06}
	oldHash := common.Keccak256(oldCode)
	newHash := common.Keccak256(newCode)
	writeAccountLatestEnvelope(t, db, owner, newHash)
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCode(db, newHash, newCode); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       accountLatestEnvelopeBytes(t, oldHash),
		NextExists: true,
		Next:       accountLatestEnvelopeBytes(t, newHash),
	}); err != nil {
		t.Fatal(err)
	}
	_, err := Check(db, ArchivePolicy(), 2, "")
	if err == nil || !strings.Contains(err.Error(), "missing code hash") {
		t.Fatalf("check error = %v, want missing historical code hash", err)
	}
	if err := rawdb.WriteStateCode(db, oldHash, oldCode); err != nil {
		t.Fatal(err)
	}
	report, err := Check(db, ArchivePolicy(), 2, "")
	if err != nil {
		t.Fatalf("check with hot historical code: %v", err)
	}
	if report.ReferencedCodeHashes != 2 {
		t.Fatalf("referenced code hashes = %d, want 2", report.ReferencedCodeHashes)
	}
}

func TestCheckerAcceptsReferencedCodeHashFromSnapshot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x48}, common.AccountIDLength)...))
	code := []byte{0x60, 0x03, 0x60, 0x04}
	hash := common.Keccak256(code)
	writeAccountLatestEnvelope(t, db, owner, hash)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 1, 1, "latest/code-1-1.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	refs := []snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 1, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	wrapped := &checkerCodeHidingStore{
		Store: db,
		hidden: map[common.Hash]struct{}{
			hash: {},
		},
	}
	report, err := Check(wrapped, ArchivePolicy(), 1, dir)
	if err != nil {
		t.Fatalf("check with cold code: %v", err)
	}
	if report.ReferencedCodeHashes != 1 || report.SnapshotSegments != 3 {
		t.Fatalf("report = %+v", report)
	}
}

func TestCheckerSurfacesCorruptColdCodeSnapshot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4e}, common.AccountIDLength)...))
	code := []byte{0x60, 0x12}
	hash := common.Keccak256(code)
	writeAccountLatestEnvelope(t, db, owner, hash)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 1, 1, "latest/code-1-1.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 1, []snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	corruptSnapshotFile(t, dir, codeRef)
	_, err = Check(db, ArchivePolicy(), 1, dir)
	if err == nil || !strings.Contains(err.Error(), "latest/code-1-1-") {
		t.Fatalf("check error = %v, want corrupt cold code snapshot", err)
	}
}

func TestCheckerAcceptsHistoricalCodeHashFromColdSnapshots(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4a}, common.AccountIDLength)...))
	oldCode := []byte{0x60, 0x07}
	newCode := []byte{0x60, 0x08}
	oldHash := common.Keccak256(oldCode)
	newHash := common.Keccak256(newCode)
	writeAccountLatestEnvelope(t, db, owner, newHash)
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCode(db, oldHash, oldCode); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateCode(db, newHash, newCode); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       accountLatestEnvelopeBytes(t, oldHash),
		NextExists: true,
		Next:       accountLatestEnvelopeBytes(t, newHash),
	}); err != nil {
		t.Fatal(err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 2, 2, "latest/code-2-2.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	historyRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 2, 2, "history/state-domain-change-2-2.seg")
	if err != nil {
		t.Fatalf("build history snapshot: %v", err)
	}
	refs := append([]snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef}, historyRefs...)
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(2, 2, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	if err := rawdb.DeleteStateDomainChanges(db, 2); err != nil {
		t.Fatal(err)
	}
	wrapped := &checkerCodeHidingStore{
		Store: db,
		hidden: map[common.Hash]struct{}{
			oldHash: {},
		},
	}
	report, err := Check(wrapped, ArchivePolicy(), 2, dir)
	if err != nil {
		t.Fatalf("check with cold historical code: %v", err)
	}
	if report.ReferencedCodeHashes != 2 || report.SnapshotSegments != 6 {
		t.Fatalf("report = %+v", report)
	}
}

func TestWorkerSnapPrunesHistoricalStateCodeCoveredByCodeDomain(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4b}, common.AccountIDLength)...))
	code := []byte{0x60, 0x09}
	hash := common.Keccak256(code)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       accountLatestEnvelopeBytes(t, hash),
	}); err != nil {
		t.Fatal(err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 2, 2, "latest/code-2-2.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	historyRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 2, 2, "history/state-domain-change-2-2.seg")
	if err != nil {
		t.Fatalf("build history snapshot: %v", err)
	}
	refs := append([]snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef}, historyRefs...)
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(2, 2, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir}.PruneTo(5)
	if err != nil {
		t.Fatalf("snap prune: %v", err)
	}
	if stats.DeletedStateCodeRows != 1 || stats.DeletedDomainChangeBlocks != 1 {
		t.Fatalf("stats = %+v, want one code row and one hot history block pruned", stats)
	}
	if got := rawdb.ReadStateCode(db, hash); got != nil {
		t.Fatalf("hot code survived: %x", got)
	}
	if _, err := Check(db, SnapPolicy(2, 1), 5, dir); err != nil {
		t.Fatalf("check after hot code prune: %v", err)
	}
}

func TestWorkerSnapKeepsCodeWhenSnapshotStartsAfterEarliestReference(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	code := []byte{0x60, 0x2a}
	hash := common.Keccak256(code)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	writeCodeHashHistoryBlockPack(t, db, 2, 2, 1, hash)
	writeCodeHashHistoryBlockPack(t, db, 5, 5, 1, hash)
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 5, 5, "latest/code-5-5.seg")
	if err != nil {
		t.Fatalf("build later code snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(5, 5, []snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef})); err != nil {
		t.Fatalf("publish later code snapshot: %v", err)
	}

	deleted, err := (Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir}).pruneStateCodeRows(db, 5)
	if err != nil {
		t.Fatalf("prune code with late snapshot: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted code rows = %d, want zero without earliest-reference coverage", deleted)
	}
	if got := rawdb.ReadStateCode(db, hash); !bytes.Equal(got, code) {
		t.Fatalf("hot code after late snapshot = %x, want %x", got, code)
	}
}

func TestWorkerSnapRejectsCorruptColdCodeSnapshot(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4f}, common.AccountIDLength)...))
	code := []byte{0x60, 0x13}
	hash := common.Keccak256(code)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       accountLatestEnvelopeBytes(t, hash),
	}); err != nil {
		t.Fatal(err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 2, 2, "latest/code-2-2.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	historyRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 2, 2, "history/state-domain-change-2-2.seg")
	if err != nil {
		t.Fatalf("build history snapshot: %v", err)
	}
	refs := append([]snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef}, historyRefs...)
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(2, 2, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	corruptSnapshotFile(t, dir, codeRef)

	stats, err := Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir}.PruneTo(5)
	if err == nil || !strings.Contains(err.Error(), "latest/code-2-2-") {
		t.Fatalf("snap prune stats=%+v err=%v, want corrupt cold code snapshot", stats, err)
	}
	if got := rawdb.ReadStateCode(db, hash); !bytes.Equal(got, code) {
		t.Fatalf("hot code after failed prune = %x, want %x", got, code)
	}
}

func TestWorkerSnapPrunesCurrentLatestStateCodeCoveredByCodeDomain(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4c}, common.AccountIDLength)...))
	code := []byte{0x60, 0x0a}
	hash := common.Keccak256(code)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	writeAccountLatestEnvelope(t, db, owner, hash)
	if _, err := statedomains.NewStagedCommitmentStore(db).Rebuild(); err != nil {
		t.Fatal(err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := snapshots.BuildCodeSegmentFilesFromDB(db, dir, 5, 5, "latest/code-5-5.seg")
	if err != nil {
		t.Fatalf("build code snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(5, 5, []snapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef})); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir}.PruneTo(5)
	if err != nil {
		t.Fatalf("snap prune: %v", err)
	}
	if stats.DeletedStateCodeRows != 1 {
		t.Fatalf("stats = %+v, want current latest code pruned", stats)
	}
	if got := rawdb.ReadStateCode(db, hash); got != nil {
		t.Fatalf("current hot code survived: %x", got)
	}
	if _, err := Check(db, SnapPolicy(2, 1), 5, dir); err != nil {
		t.Fatalf("check after current hot code prune: %v", err)
	}
}

// TestWorkerSnapPreservesHotCodeWithoutCodeDomainCoverage is the negative guard
// for the CodeDomain retention policy: snapshot coverage is the ONLY path that
// authorizes deleting a hot state-code row. It mirrors the historical positive
// case (TestWorkerSnapPrunesHistoricalStateCodeCoveredByCodeDomain) exactly —
// same referenced hash, same history coverage — but publishes a manifest with
// NO CodeDomain segment. With the hash not snapshot-backed, the worker must keep
// the hot code bytes. A regression that drops the codeHashAvailableInSnapshot
// gate (deleting code regardless of coverage) flips DeletedStateCodeRows to 1.
func TestWorkerSnapPreservesHotCodeWithoutCodeDomainCoverage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x4b}, common.AccountIDLength)...))
	code := []byte{0x60, 0x09}
	hash := common.Keccak256(code)
	if err := rawdb.WriteStateCode(db, hash, code); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, common.Hash{0x02}, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		BlockHash:  common.Hash{0x02},
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      owner,
		PrevExists: true,
		Prev:       accountLatestEnvelopeBytes(t, hash),
	}); err != nil {
		t.Fatal(err)
	}
	// Publish history coverage but DELIBERATELY omit the CodeDomain segment, so
	// the referenced code hash is not backed by any snapshot.
	historyRefs, err := snapshots.BuildStateDomainChangeHistorySegmentsFromDB(db, dir, 2, 2, "history/state-domain-change-2-2.seg")
	if err != nil {
		t.Fatalf("build history snapshot: %v", err)
	}
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(2, 2, historyRefs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}

	stats, err := Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir}.PruneTo(5)
	if err != nil {
		t.Fatalf("snap prune: %v", err)
	}
	if stats.DeletedStateCodeRows != 0 {
		t.Fatalf("stats = %+v, want zero code rows deleted without CodeDomain coverage", stats)
	}
	if got := rawdb.ReadStateCode(db, hash); got == nil {
		t.Fatal("hot code wrongly pruned despite no CodeDomain snapshot coverage")
	}
}

func TestWorkerSnapSkipsHistoryScanWithoutHotCodeRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	if err := snapshots.PublishManifest(dir, snapshots.NewManifest(1, 1, nil)); err != nil {
		t.Fatalf("publish empty manifest: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 1, common.Hash{0x01}, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   1,
		BlockHash:  common.Hash{0x01},
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		PrevExists: true,
		Prev:       []byte("invalid account envelope that must not be decoded"),
	}); err != nil {
		t.Fatal(err)
	}

	deleted, err := (Worker{DB: db, Policy: SnapPolicy(2, 1), SnapshotDir: dir}).pruneStateCodeRows(db, 1)
	if err != nil {
		t.Fatalf("empty CodeDomain prune: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted code rows = %d, want 0", deleted)
	}
}

func TestCheckerHistoryCodeHashCollectionHonorsContext(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x5a}, common.AccountIDLength)...))
	for txNum := uint64(1); txNum <= 3; txNum++ {
		if err := rawdb.WriteStateTxRange(db, txNum, common.Hash{byte(txNum)}, txNum, txNum); err != nil {
			t.Fatal(err)
		}
		if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
			BlockNum:   txNum,
			BlockHash:  common.Hash{byte(txNum)},
			TxNum:      txNum,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainAccountLatest,
			Owner:      owner,
			PrevExists: true,
			Prev:       accountLatestEnvelopeBytes(t, common.Hash{byte(txNum)}),
		}); err != nil {
			t.Fatal(err)
		}
	}

	ctx := &cancelAfterChecksContext{}
	ctx.remaining.Store(2)
	err := (Checker{DB: db}).collectHistoryCodeHashesContext(ctx, make(codeHashRefs))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("history collection error = %v, want context.Canceled", err)
	}
}

func TestCollectHotHistoryCodeHashesUsesBorrowedBlockPacks(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{0x42}
	writeCodeHashHistoryBlockPack(t, db, 7, 70, 4, hash)
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain history config")
	}
	cfg.IterateHotHistoryTxRangeChanges = func(ethdb.Iteratee, uint64, uint64, func(*rawdb.StateDomainChange) (bool, error)) error {
		return errors.New("owning history iterator unexpectedly used")
	}
	refs := make(codeHashRefs)
	err := collectHotHistoryCodeHashes(context.Background(), cfg, db, refs)
	if err != nil {
		t.Fatal(err)
	}
	if earliest, ok := refs[hash]; !ok || earliest != 70 {
		t.Fatalf("borrowed code hash earliest ref = %d/%v, want 70/true", earliest, ok)
	}
}

func TestCodeHashRefsKeepEarliestMeaningfulReference(t *testing.T) {
	hash := common.Hash{0x7a}
	refs := make(codeHashRefs)
	refs.add(hash, 90)
	refs.add(hash, 30)
	refs.add(hash, 60)
	refs.add(common.Hash{}, 1)
	refs.add(emptyCodeHash, 1)
	if earliest, ok := refs[hash]; !ok || earliest != 30 {
		t.Fatalf("earliest code reference = %d/%v, want 30/true", earliest, ok)
	}
	if len(refs) != 1 {
		t.Fatalf("meaningful code refs = %v, want only %x", refs, hash)
	}
}

func TestCollectStateDomainChangeCodeHashFiltersNonHotHashes(t *testing.T) {
	wantedHash := common.Hash{0x10}
	otherHash := common.Hash{0x20}
	refs := make(codeHashRefs)
	wanted := map[common.Hash]struct{}{wantedHash: {}}
	change := &rawdb.StateDomainChange{
		BlockNum: 10, TxNum: 61, Seq: 2,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		PrevExists: true, Prev: accountLatestEnvelopeBytes(t, otherHash),
	}
	if err := collectStateDomainChangeCodeHashesFiltered(refs, wanted, change); err != nil {
		t.Fatal(err)
	}
	if len(refs) != 0 {
		t.Fatalf("filtered refs = %+v, want empty", refs)
	}
	change.Prev = accountLatestEnvelopeBytes(t, wantedHash)
	if err := collectStateDomainChangeCodeHashesFiltered(refs, wanted, change); err != nil {
		t.Fatal(err)
	}
	if got := refs[wantedHash]; got != 61 || len(refs) != 1 {
		t.Fatalf("filtered refs = %+v, want wanted hash at tx 61", refs)
	}
}

func TestCollectHotHistoryCodeHashesFallsBackForStandaloneRows(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{0x24}
	if err := rawdb.WriteStateTxRange(db, 3, common.Hash{3}, 30, 30); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChangeRow(db, &rawdb.StateDomainChange{
		BlockNum: 3, TxNum: 30, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      common.Address{common.AddressPrefixMainnet, 3},
		PrevExists: true, Prev: accountLatestEnvelopeBytes(t, hash),
	}); err != nil {
		t.Fatal(err)
	}
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain history config")
	}
	owned := cfg.IterateHotHistoryTxRangeChanges
	ownedCalls := 0
	cfg.IterateHotHistoryTxRangeChanges = func(db ethdb.Iteratee, fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
		ownedCalls++
		return owned(db, fromTxNum, toTxNum, fn)
	}
	refs := make(codeHashRefs)
	err := collectHotHistoryCodeHashes(context.Background(), cfg, db, refs)
	if err != nil {
		t.Fatal(err)
	}
	if earliest, ok := refs[hash]; ownedCalls != 1 || !ok || earliest != 30 {
		t.Fatalf("legacy fallback calls=%d earliest=%d/%v, want 1 and 30/true", ownedCalls, earliest, ok)
	}
}

func TestCollectHotHistoryCodeHashesFallbackDiscardsShadowedPackedReference(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	packedHash := common.Hash{0x31}
	repairHash := common.Hash{0x32}
	writeCodeHashHistoryBlockPack(t, db, 7, 70, 4, packedHash)
	if err := rawdb.WriteStateDomainChangeRow(db, &rawdb.StateDomainChange{
		BlockNum: 7, TxNum: 70, Seq: 1,
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      common.Address{common.AddressPrefixMainnet, 7, 0},
		PrevExists: true, Prev: accountLatestEnvelopeBytes(t, repairHash),
	}); err != nil {
		t.Fatal(err)
	}
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		t.Fatal("missing state-domain history config")
	}
	refs := make(codeHashRefs)
	if err := collectHotHistoryCodeHashes(context.Background(), cfg, db, refs); err != nil {
		t.Fatal(err)
	}
	if packedEarliest, packedOK := refs[packedHash]; !packedOK || packedEarliest != 71 {
		t.Fatalf("packed earliest ref = %d/%v, want 71/true after repair shadow", packedEarliest, packedOK)
	}
	if repairEarliest, repairOK := refs[repairHash]; !repairOK || repairEarliest != 70 {
		t.Fatalf("repair earliest ref = %d/%v, want 70/true", repairEarliest, repairOK)
	}
}

var benchmarkCollectedCodeHashRefs int

func BenchmarkCollectHotHistoryCodeHashes(b *testing.B) {
	const (
		blocks       = 128
		rowsPerBlock = 64
	)
	db := rawdb.NewMemoryDatabase()
	hash := common.Hash{0x77}
	for block := uint64(1); block <= blocks; block++ {
		writeCodeHashHistoryBlockPack(b, db, block, block*rowsPerBlock, rowsPerBlock, hash)
	}
	cfg, ok := snapshots.DefaultDomainRegistry().Dataset(snapshots.SegmentDatasetStateDomainChange)
	if !ok {
		b.Fatal("missing state-domain history config")
	}
	for _, benchmark := range []struct {
		name     string
		borrowed bool
	}{
		{name: "owned", borrowed: false},
		{name: "borrowed", borrowed: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			benchCfg := cfg
			if !benchmark.borrowed {
				benchCfg.IterateHotHistoryBlockTxBorrowed = nil
			}
			warmRefs := make(codeHashRefs)
			warmRefs.add(hash, blocks*rowsPerBlock+1)
			if err := collectHotHistoryCodeHashes(context.Background(), benchCfg, db, warmRefs); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ReportMetric(blocks*rowsPerBlock, "changes/op")
			b.ResetTimer()
			var refs codeHashRefs
			for i := 0; i < b.N; i++ {
				refs = make(codeHashRefs)
				refs.add(hash, blocks*rowsPerBlock+1)
				if err := collectHotHistoryCodeHashes(context.Background(), benchCfg, db, refs); err != nil {
					b.Fatal(err)
				}
			}
			benchmarkCollectedCodeHashRefs = len(refs)
		})
	}
}

func writeCodeHashHistoryBlockPack(t testing.TB, db ethdb.KeyValueWriter, blockNum, firstTxNum, rows uint64, hash common.Hash) {
	t.Helper()
	if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, firstTxNum, firstTxNum+rows-1); err != nil {
		t.Fatal(err)
	}
	changes := make([]*rawdb.StateDomainChange, rows)
	prev := accountLatestEnvelopeBytes(t, hash)
	for i := range changes {
		changes[i] = &rawdb.StateDomainChange{
			BlockNum: blockNum, TxNum: firstTxNum + uint64(i), Seq: uint64(i + 1),
			FlatDomain: rawdb.StateFlatDomainAccountLatest,
			Owner:      common.Address{common.AddressPrefixMainnet, byte(blockNum), byte(i)},
			PrevExists: true, Prev: prev,
		}
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, changes); err != nil {
		t.Fatal(err)
	}
}

type cancelAfterChecksContext struct {
	remaining atomic.Int64
}

func (c *cancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterChecksContext) Done() <-chan struct{}       { return nil }
func (c *cancelAfterChecksContext) Value(any) any               { return nil }

func (c *cancelAfterChecksContext) Err() error {
	if c.remaining.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

func TestPrunerPassUsesSolidifiedBlockAndBatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for _, blockNum := range []uint64{1, 2, 3} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	chain := &fakePruneChain{db: db, solidified: 5}
	namespace := normalizePrunerMetricNamespace("test/state/prune/" + strings.ReplaceAll(t.Name(), "/", "_"))
	t.Cleanup(func() { unregisterPrunerMetricNamespace(namespace) })
	pruner := NewPruner(chain, PrunerConfig{
		Policy:           FullPolicy(2, 1),
		Interval:         time.Hour,
		BatchSize:        1,
		MetricsNamespace: namespace,
	})
	stats, err := pruner.PrunePass()
	if err != nil {
		t.Fatalf("prune pass: %v", err)
	}
	if stats.DeletedTxRanges != 1 {
		t.Fatalf("deleted tx ranges = %d, want 1", stats.DeletedTxRanges)
	}
	if got := pruner.Stats(); got.Passes != 1 || got.LastSolidifiedBlock != 5 {
		t.Fatalf("pruner stats = %+v", got)
	}
	assertPrunerGauge(t, namespace+"passes", 1)
	assertPrunerGauge(t, namespace+"errors", 0)
	assertPrunerGauge(t, namespace+"deleted/tx_ranges", 1)
	assertPrunerGauge(t, namespace+"last/solidified_block", 5)
	if got := prunerGaugeValue(t, namespace+"lastpass/duration"); got <= 0 {
		t.Fatalf("lastpass/duration = %d, want positive", got)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune); err != nil || !ok || got != 5 {
		t.Fatalf("snapshot/prune stage progress = %d ok=%v err=%v, want 5", got, ok, err)
	}
	remaining := 0
	if err := rawdb.IterateStateTxRanges(db, func(row *rawdb.StateTxRange) (bool, error) {
		remaining++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if remaining != 2 {
		t.Fatalf("remaining tx ranges = %d, want 2", remaining)
	}
	if err := pruner.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := pruner.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestPrunerCatchupDefaultsUseBoundedLargeBlockWindow(t *testing.T) {
	pruner := NewPruner(&fakePruneChain{db: rawdb.NewMemoryDatabase()}, PrunerConfig{
		Policy: FullPolicy(2, 1),
	})
	if pruner.cfg.BatchSize != 25_000 {
		t.Fatalf("default prune batch = %d, want 25000", pruner.cfg.BatchSize)
	}
	if pruner.cfg.Interval != time.Minute {
		t.Fatalf("default prune interval = %s, want 1m", pruner.cfg.Interval)
	}
}

func writeAccountLatestEnvelope(t *testing.T, db ethdb.KeyValueWriter, owner common.Address, codeHash common.Hash) {
	t.Helper()
	data := accountLatestEnvelopeBytes(t, codeHash)
	if err := rawdb.WriteStateAccountLatest(db, owner, data); err != nil {
		t.Fatal(err)
	}
}

func accountLatestEnvelopeBytes(t testing.TB, codeHash common.Hash) []byte {
	t.Helper()
	data, err := (&statepkg.StateAccountV2{
		Version:  statepkg.StateAccountVersion,
		CodeHash: codeHash,
	}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func corruptSnapshotFile(t *testing.T, dir string, ref snapshots.SegmentRef) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, ref.Path), []byte("corrupt snapshot segment"), 0o644); err != nil {
		t.Fatalf("corrupt snapshot file %s: %v", ref.Path, err)
	}
}

type checkerCodeHidingStore struct {
	Store
	hidden map[common.Hash]struct{}
}

type pruneBatchCountingStore struct {
	ethdb.KeyValueStore
	batchWrites   int
	directDeletes int
	writeErr      error
}

func (db *pruneBatchCountingStore) Delete(key []byte) error {
	db.directDeletes++
	return db.KeyValueStore.Delete(key)
}

func (db *pruneBatchCountingStore) NewBatch() ethdb.Batch {
	return &pruneCountingBatch{Batch: db.KeyValueStore.NewBatch(), writes: &db.batchWrites, writeErr: &db.writeErr}
}

func (db *pruneBatchCountingStore) NewBatchWithSize(size int) ethdb.Batch {
	return &pruneCountingBatch{Batch: db.KeyValueStore.NewBatchWithSize(size), writes: &db.batchWrites, writeErr: &db.writeErr}
}

type pruneCountingBatch struct {
	ethdb.Batch
	writes   *int
	writeErr *error
}

func (b *pruneCountingBatch) Write() error {
	*b.writes++
	if b.writeErr != nil && *b.writeErr != nil {
		return *b.writeErr
	}
	return b.Batch.Write()
}

func (db *checkerCodeHidingStore) Get(key []byte) ([]byte, error) {
	if hash, ok := rawdb.DecodeStateCodeKey(key); ok {
		if _, hide := db.hidden[hash]; hide {
			return nil, errors.New("hidden state code")
		}
	}
	return db.Store.Get(key)
}

func TestPrunerSkipsWhileSyncLagExceedsThreshold(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for _, blockNum := range []uint64{1, 2} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	chain := &fakePruneChain{db: db, solidified: 100, syncRemaining: 1_000, syncRemainingOK: true}
	pruner := NewPruner(chain, PrunerConfig{
		Policy:     FullPolicy(2, 1),
		Interval:   time.Hour,
		BatchSize:  10,
		MaxSyncLag: 100,
	})
	stats, err := pruner.PrunePass()
	if err != nil {
		t.Fatalf("prune pass: %v", err)
	}
	if stats.DeletedTxRanges != 0 || pruner.Stats().SkippedCatchup != 1 {
		t.Fatalf("stats after skip = %+v pruner=%+v", stats, pruner.Stats())
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("block 1 range pruned during catch-up ok:%v err:%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune); err != nil || ok {
		t.Fatalf("snapshot/prune stage progressed during catch-up ok=%v err=%v", ok, err)
	}

	chain.syncRemaining = 10
	stats, err = pruner.PrunePass()
	if err != nil {
		t.Fatalf("prune pass after catch-up: %v", err)
	}
	if stats.DeletedTxRanges != 2 {
		t.Fatalf("deleted tx ranges after catch-up = %d, want 2", stats.DeletedTxRanges)
	}
	if got := pruner.Stats(); got.Passes != 1 || got.SkippedCatchup != 1 {
		t.Fatalf("pruner stats after catch-up = %+v", got)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune); err != nil || !ok || got != 100 {
		t.Fatalf("snapshot/prune stage after catch-up = %d ok=%v err=%v, want 100", got, ok, err)
	}
}

func TestPrunerCatchupWatchCancelsAfterSyncBecomesActive(t *testing.T) {
	chain := &atomicSyncPruneChain{db: rawdb.NewMemoryDatabase(), solidified: 100}
	pruner := NewPruner(chain, PrunerConfig{
		Policy:     SnapPolicy(262_144, 128),
		Interval:   time.Hour,
		MaxSyncLag: 262_144,
	})
	ctx, stop := pruner.contextWithCatchupCancellation(context.Background())
	defer stop()
	if err := ctx.Err(); err != nil {
		t.Fatalf("catch-up watch started canceled: %v", err)
	}
	chain.syncRemaining.Store(80_000_000)
	chain.syncActive.Store(true)
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), errPruneDeferredForCatchup) {
			t.Fatalf("catch-up cancellation cause = %v", context.Cause(ctx))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("catch-up watch did not cancel after sync became active")
	}
}

func TestPrunerCapsHeadAtFinishStageProgress(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	finishHash := common.Hash{0x05}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 5, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	pruner := NewPruner(&fakePruneChain{db: db, solidified: 10, canonicalHashes: map[uint64]common.Hash{5: finishHash}}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	stats, err := pruner.PrunePass()
	if err != nil {
		t.Fatalf("prune pass: %v", err)
	}
	if stats.DeletedTxRanges != 3 {
		t.Fatalf("deleted tx ranges = %d, want 3", stats.DeletedTxRanges)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 3); err != nil || ok {
		t.Fatalf("block 3 range after prune ok=%v err=%v, want deleted", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 4); err != nil || !ok {
		t.Fatalf("block 4 range after prune ok=%v err=%v, want retained by finish-stage cap", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 8); err != nil || !ok {
		t.Fatalf("block 8 range after prune ok=%v err=%v, want retained above finish stage", ok, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotPrune); err != nil || !ok || got != 5 {
		t.Fatalf("snapshot/prune stage = %d ok=%v err=%v, want 5", got, ok, err)
	}
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != finishHash {
		t.Fatalf("snapshot/prune row = %+v ok=%v err=%v, want hash-bound finish stage", row, ok, err)
	}
}

func TestPrunerCapsHeadAtFinishStageProgressFromRawDBFallback(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	finishBlock := pruningTestBlock(5)
	if err := rawdb.WriteBlock(db, finishBlock); err != nil {
		t.Fatalf("write finish block: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, finishBlock.Number(), finishBlock.Hash()); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	pruner := NewPruner(&fallbackPruneChain{db: db, solidified: 10}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	stats, err := pruner.PrunePass()
	if err != nil {
		t.Fatalf("prune pass: %v", err)
	}
	if stats.DeletedTxRanges != 3 {
		t.Fatalf("deleted tx ranges = %d, want 3", stats.DeletedTxRanges)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 3); err != nil || ok {
		t.Fatalf("block 3 range after fallback prune ok=%v err=%v, want deleted", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 4); err != nil || !ok {
		t.Fatalf("block 4 range after fallback prune ok=%v err=%v, want retained by finish-stage cap", ok, err)
	}
}

func TestPrunerCapsHeadAtFinishStageProgressFromChainDBFallback(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	finishBlock := pruningTestBlock(5)
	finishRaw, err := finishBlock.Marshal()
	if err != nil {
		t.Fatalf("marshal finish block: %v", err)
	}
	ancient := newPruneFakeAncient()
	ancient.put(rawdb.AncientBlocksTable, finishBlock.Number(), finishRaw)
	if hotHash := rawdb.ReadBlockHashByNumber(db, finishBlock.Number()); hotHash != (common.Hash{}) {
		t.Fatalf("hot fallback unexpectedly resolved finish block hash %x", hotHash)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, finishBlock.Number(), finishBlock.Hash()); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	chainDB := rawdb.NewChainDB(db, ancient)
	pruner := NewPruner(&fallbackPruneChain{db: db, chainDB: chainDB, solidified: 10}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	stats, err := pruner.PrunePass()
	if err != nil {
		t.Fatalf("prune pass: %v", err)
	}
	if stats.DeletedTxRanges != 3 {
		t.Fatalf("deleted tx ranges = %d, want 3", stats.DeletedTxRanges)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 3); err != nil || ok {
		t.Fatalf("block 3 range after chain fallback prune ok=%v err=%v, want deleted", ok, err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 4); err != nil || !ok {
		t.Fatalf("block 4 range after chain fallback prune ok=%v err=%v, want retained by finish-stage cap", ok, err)
	}
}

func TestPrunerWritesHashBoundSnapshotPruneAtSolidifiedHead(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 8; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	headHash := common.Hash{0x08}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 8, headHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	pruner := NewPruner(&fakePruneChain{db: db, solidified: 8, canonicalHashes: map[uint64]common.Hash{8: headHash}}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	if _, err := pruner.PrunePass(); err != nil {
		t.Fatalf("prune pass: %v", err)
	}
	row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotPrune)
	if err != nil || !ok || row.BlockNum != 8 || !row.HasBlockHash || row.BlockHash != headHash {
		t.Fatalf("snapshot/prune row = %+v ok=%v err=%v, want block 8 hash-bound", row, ok, err)
	}
}

func TestSnapshotChainSourceCanonicalHashUsesChainDBFallback(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block := pruningTestBlock(7)
	blockRaw, err := block.Marshal()
	if err != nil {
		t.Fatalf("marshal block: %v", err)
	}
	ancient := newPruneFakeAncient()
	ancient.put(rawdb.AncientBlocksTable, block.Number(), blockRaw)
	source := snapshotChainSource{chain: &fallbackPruneChain{
		db:         db,
		chainDB:    rawdb.NewChainDB(db, ancient),
		solidified: 10,
	}}
	if hotHash := rawdb.ReadBlockHashByNumber(db, block.Number()); hotHash != (common.Hash{}) {
		t.Fatalf("hot fallback unexpectedly resolved block hash %x", hotHash)
	}
	hash, ok := source.CanonicalBlockHash(block.Number())
	if !ok || hash != block.Hash() {
		t.Fatalf("CanonicalBlockHash = %x/%v, want %x/true", hash, ok, block.Hash())
	}
}

func TestSnapshotChainSourceCanonicalHashStrictPropagatesLookupError(t *testing.T) {
	wantErr := errors.New("canonical snapshot boundary corrupt")
	source := snapshotChainSource{chain: &fakePruneChain{
		db:            rawdb.NewMemoryDatabase(),
		solidified:    10,
		canonicalErrs: map[uint64]error{7: wantErr},
	}}
	hash, ok, err := source.CanonicalBlockHashStrict(7)
	if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("CanonicalBlockHashStrict err = %v, want %q", err, wantErr)
	}
	if ok || hash != (common.Hash{}) {
		t.Fatalf("CanonicalBlockHashStrict = %x/%v, want zero/false on error", hash, ok)
	}
	hash, ok = source.CanonicalBlockHash(7)
	if ok || hash != (common.Hash{}) {
		t.Fatalf("CanonicalBlockHash = %x/%v, want legacy non-strict error collapse", hash, ok)
	}
}

func TestPrunerRejectsUnboundFinishStageProgress(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageFinish, 5); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	pruner := NewPruner(&fakePruneChain{db: db, solidified: 10, canonicalHashes: map[uint64]common.Hash{5: {0x05}}}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	if _, err := pruner.PrunePass(); err == nil || !strings.Contains(err.Error(), "finish stage 5 is not hash-bound") {
		t.Fatalf("prune pass error = %v, want unbound finish-stage rejection", err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("block 1 range pruned despite unbound finish stage ok=%v err=%v", ok, err)
	}
}

func TestPrunerRejectsFinishStageHashMismatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 5, common.Hash{0x05}); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	pruner := NewPruner(&fakePruneChain{db: db, solidified: 10, canonicalHashes: map[uint64]common.Hash{5: {0xaa}}}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	if _, err := pruner.PrunePass(); err == nil || !strings.Contains(err.Error(), "finish stage 5 hash") {
		t.Fatalf("prune pass error = %v, want finish-stage hash mismatch", err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("block 1 range pruned despite finish hash mismatch ok=%v err=%v", ok, err)
	}
}

func TestPrunerPropagatesFinishStageHashLookupError(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	finishHash := common.Hash{0x05}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageFinish, 5, finishHash); err != nil {
		t.Fatalf("write finish stage: %v", err)
	}
	pruner := NewPruner(&fakePruneChain{
		db:              db,
		solidified:      10,
		canonicalHashes: map[uint64]common.Hash{5: finishHash},
		canonicalErrs:   map[uint64]error{5: errors.New("canonical hash corrupt")},
	}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	if _, err := pruner.PrunePass(); err == nil || !strings.Contains(err.Error(), "finish stage 5 canonical hash lookup") || !strings.Contains(err.Error(), "canonical hash corrupt") {
		t.Fatalf("prune pass error = %v, want finish-stage hash lookup error", err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("block 1 range pruned despite finish hash lookup error ok=%v err=%v", ok, err)
	}
}

func TestPrunerPropagatesPruneHeadHashLookupError(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for blockNum := uint64(1); blockNum <= 10; blockNum++ {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	pruner := NewPruner(&fakePruneChain{
		db:            db,
		solidified:    10,
		canonicalErrs: map[uint64]error{10: errors.New("canonical head corrupt")},
	}, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 10,
	})
	if _, err := pruner.PrunePass(); err == nil || !strings.Contains(err.Error(), "prune head 10 canonical hash lookup") || !strings.Contains(err.Error(), "canonical head corrupt") {
		t.Fatalf("prune pass error = %v, want prune-head hash lookup error", err)
	}
	if _, ok, err := rawdb.ReadStateTxRange(db, 1); err != nil || !ok {
		t.Fatalf("block 1 range pruned despite prune-head hash lookup error ok=%v err=%v", ok, err)
	}
}

type fakePruneChain struct {
	db              ethdb.KeyValueStore
	solidified      int64
	syncRemaining   uint64
	syncRemainingOK bool
	canonicalHashes map[uint64]common.Hash
	canonicalErrs   map[uint64]error
}

type atomicSyncPruneChain struct {
	db            ethdb.KeyValueStore
	solidified    int64
	syncRemaining atomic.Uint64
	syncActive    atomic.Bool
}

func (f *atomicSyncPruneChain) DB() ethdb.KeyValueStore { return f.db }

func (f *atomicSyncPruneChain) LatestSolidifiedBlockNum() int64 { return f.solidified }

func (f *atomicSyncPruneChain) SyncRemainingBlocks() (uint64, bool) {
	return f.syncRemaining.Load(), f.syncActive.Load()
}

func (f *fakePruneChain) DB() ethdb.KeyValueStore { return f.db }

func (f *fakePruneChain) LatestSolidifiedBlockNum() int64 { return f.solidified }

func (f *fakePruneChain) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	hash, ok, err := f.CanonicalBlockHashStrict(blockNum)
	if err != nil {
		return common.Hash{}, false
	}
	return hash, ok
}

func (f *fakePruneChain) CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error) {
	if err := f.canonicalErrs[blockNum]; err != nil {
		return common.Hash{}, false, err
	}
	if hash, ok := f.canonicalHashes[blockNum]; ok {
		return hash, true, nil
	}
	if f.db != nil {
		row, ok, err := rawdb.ReadStateTxRange(f.db, blockNum)
		if err != nil || !ok || row.BlockHash == (common.Hash{}) {
			return common.Hash{}, ok, err
		}
		return row.BlockHash, true, nil
	}
	return common.Hash{}, false, nil
}

func (f *fakePruneChain) SyncRemainingBlocks() (uint64, bool) {
	return f.syncRemaining, f.syncRemainingOK
}

type fallbackPruneChain struct {
	db         ethdb.KeyValueStore
	chainDB    *rawdb.ChainDB
	solidified int64
}

func (f *fallbackPruneChain) DB() ethdb.KeyValueStore { return f.db }

func (f *fallbackPruneChain) ChainDB() *rawdb.ChainDB { return f.chainDB }

func (f *fallbackPruneChain) LatestSolidifiedBlockNum() int64 { return f.solidified }

type pruneFakeAncient struct {
	rows map[string]map[uint64][]byte
}

func newPruneFakeAncient() *pruneFakeAncient {
	return &pruneFakeAncient{rows: make(map[string]map[uint64][]byte)}
}

func (f *pruneFakeAncient) put(kind string, number uint64, data []byte) {
	table := f.rows[kind]
	if table == nil {
		table = make(map[uint64][]byte)
		f.rows[kind] = table
	}
	table[number] = data
}

func (f *pruneFakeAncient) Ancient(kind string, number uint64) ([]byte, error) {
	table := f.rows[kind]
	if table == nil {
		return nil, rawdb.ErrNotInAncient
	}
	data, ok := table[number]
	if !ok {
		return nil, rawdb.ErrNotInAncient
	}
	return data, nil
}

func (f *pruneFakeAncient) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if _, err := f.Ancient(kind, start); err != nil {
		return nil, err
	}
	table := f.rows[kind]
	var out [][]byte
	var total uint64
	for i := uint64(0); i < count; i++ {
		row, ok := table[start+i]
		if !ok {
			break
		}
		if maxBytes > 0 && len(out) > 0 && total+uint64(len(row)) > maxBytes {
			break
		}
		out = append(out, row)
		total += uint64(len(row))
	}
	return out, nil
}

func (f *pruneFakeAncient) AncientCount(kind string) (uint64, error) {
	table := f.rows[kind]
	var count uint64
	for {
		if _, ok := table[count]; !ok {
			return count, nil
		}
		count++
	}
}

func (f *pruneFakeAncient) HasAncient(kind string, number uint64) (bool, error) {
	table := f.rows[kind]
	if table == nil {
		return false, nil
	}
	_, ok := table[number]
	return ok, nil
}

func pruningTestBlock(number uint64) *coretypes.Block {
	return coretypes.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(number),
				Timestamp: int64(number) * 3000,
			},
		},
	})
}

func writeSnapPruningChange(t *testing.T, db ethdb.KeyValueWriter, blockNum, beginTxNum, endTxNum uint64) (*rawdb.StateDomainChange, common.Address, []byte) {
	t.Helper()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{byte(blockNum + 0x50)}, common.AccountIDLength)...))
	key := []byte("snap-key")
	blockHash := common.Hash{byte(blockNum)}
	if err := rawdb.WriteStateTxRange(db, blockNum, blockHash, beginTxNum, endTxNum); err != nil {
		t.Fatal(err)
	}
	change := &rawdb.StateDomainChange{
		BlockNum:   blockNum,
		BlockHash:  blockHash,
		TxNum:      beginTxNum + 1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 0,
		Domain:     kvdomains.SystemDynamicProperty,
		Key:        key,
		PrevExists: true,
		Prev:       []byte("prev"),
		NextExists: true,
		Next:       []byte("next"),
	}
	if err := rawdb.WriteStateDomainChangeBlockRows(db, []*rawdb.StateDomainChange{change}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteStateDomainChangePostingIndex(db, change); err != nil {
		t.Fatal(err)
	}
	return change, owner, key
}

func hasWarning(warnings []string, substr string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, substr) {
			return true
		}
	}
	return false
}

func assertPrunerGauge(t *testing.T, name string, want int64) {
	t.Helper()
	if got := prunerGaugeValue(t, name); got != want {
		t.Fatalf("gauge %s = %d, want %d", name, got, want)
	}
}

func prunerGaugeValue(t *testing.T, name string) int64 {
	t.Helper()
	gauge, ok := metrics.DefaultRegistry.Get(name).(*metrics.Gauge)
	if !ok {
		t.Fatalf("missing gauge %s", name)
	}
	return gauge.Snapshot().Value()
}

func unregisterPrunerMetricNamespace(namespace string) {
	for _, suffix := range []string{
		"passes",
		"errors",
		"skipped/catchup",
		"deleted/tx_ranges",
		"deleted/domain_change_blocks",
		"deleted/commitment_checkpoints",
		"deleted/state_code_rows",
		"last/solidified_block",
		"lastpass/duration",
	} {
		metrics.DefaultRegistry.Unregister(namespace + suffix)
	}
}
