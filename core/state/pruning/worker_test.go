package pruning

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statepkg "github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	"github.com/tronprotocol/go-tron/core/state/snapshots"
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

func TestCheckerValidatesSnapshotSegmentsAndCodeHashes(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	dir := t.TempDir()
	owner := common.BytesToAddress(append([]byte{common.AddressPrefixMainnet}, bytes.Repeat([]byte{0x44}, common.AccountIDLength)...))
	if err := rawdb.WriteStateKVLatest(db, owner, 0, kvdomains.SystemDynamicProperty, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
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
	if report.SnapshotSegments != 4 || report.LatestRows != 1 || report.KVLatestRows != 1 || !report.CommitmentRootPresent || report.CommitmentNodes == 0 {
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
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
		t.Fatal(err)
	}
	report, err := Check(db, ArchivePolicy(), 1, "")
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if report.LatestRows != 3 || report.AccountLatestRows != 1 || report.KVGenerationRows != 1 || report.KVLatestRows != 1 {
		t.Fatalf("latest counts = %+v", report)
	}
	if !report.CommitmentRootPresent || report.CommitmentNodes == 0 || report.CommitmentDomainRows == 0 {
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
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
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
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
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
	if _, err := rawdb.RebuildLatestDomainCommitment(db); err != nil {
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

type checkerCodeHidingStore struct {
	Store
	hidden map[common.Hash]struct{}
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

type fakePruneChain struct {
	db              ethdb.KeyValueStore
	solidified      int64
	syncRemaining   uint64
	syncRemainingOK bool
	canonicalHashes map[uint64]common.Hash
}

func (f *fakePruneChain) DB() ethdb.KeyValueStore { return f.db }

func (f *fakePruneChain) LatestSolidifiedBlockNum() int64 { return f.solidified }

func (f *fakePruneChain) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	hash, ok := f.canonicalHashes[blockNum]
	return hash, ok
}

func (f *fakePruneChain) SyncRemainingBlocks() (uint64, bool) {
	return f.syncRemaining, f.syncRemainingOK
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
