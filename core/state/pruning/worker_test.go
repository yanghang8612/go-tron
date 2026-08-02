package pruning

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
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

func TestPrunerPassUsesSolidifiedBlockAndBatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	for _, blockNum := range []uint64{1, 2, 3} {
		if err := rawdb.WriteStateTxRange(db, blockNum, common.Hash{byte(blockNum)}, blockNum, blockNum); err != nil {
			t.Fatal(err)
		}
	}
	chain := &fakePruneChain{db: db, solidified: 5}
	pruner := NewPruner(chain, PrunerConfig{
		Policy:    FullPolicy(2, 1),
		Interval:  time.Hour,
		BatchSize: 1,
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

func writeAccountLatestEnvelope(t *testing.T, db ethdb.KeyValueWriter, owner common.Address, codeHash common.Hash) {
	t.Helper()
	data := accountLatestEnvelopeBytes(t, codeHash)
	if err := rawdb.WriteStateAccountLatest(db, owner, data); err != nil {
		t.Fatal(err)
	}
}

func accountLatestEnvelopeBytes(t *testing.T, codeHash common.Hash) []byte {
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
	if err := rawdb.WriteStateDomainChange(db, change); err != nil {
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
