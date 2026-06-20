package snapshots

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

func writeChainTailPruneDependencyStage(t *testing.T, db ethdb.KeyValueWriter, stage rawdb.StageID, blockNum uint64) common.Hash {
	t.Helper()
	hash := common.Hash{0x7a, byte(blockNum >> 56), byte(blockNum >> 48), byte(blockNum >> 40), byte(blockNum >> 32), byte(blockNum >> 24), byte(blockNum >> 16), byte(blockNum >> 8), byte(blockNum)}
	if err := rawdb.WriteStateTxRange(db, blockNum, hash, blockNum, blockNum); err != nil {
		t.Fatalf("WriteStateTxRange %d: %v", blockNum, err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, stage, blockNum, hash); err != nil {
		t.Fatalf("WriteStageProgressWithHash %s: %v", stage, err)
	}
	return hash
}

func TestPlanChainFreezerTailPruneRequiresStages(t *testing.T) {
	input := ChainFreezerTailPrunePlanInput{
		AncientHead:              100,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        90,
		ChainLookupPruneBlock:    90,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       90,
		HasEventLogBuildBlock:    true,
	}
	if plan := PlanChainFreezerTailPrune(input); plan.CanPrune || plan.Reason != chainFreezerTailPruneReasonMissingFreezerStage {
		t.Fatalf("plan without freezer stage = %+v, want missing freezer stage", plan)
	}

	input.HasChainFreezerBlock = true
	input.HasChainLookupPruneBlock = false
	if plan := PlanChainFreezerTailPrune(input); plan.CanPrune || plan.Reason != chainFreezerTailPruneReasonMissingLookupStage {
		t.Fatalf("plan without lookup stage = %+v, want missing lookup stage", plan)
	}
}

func TestPlanChainFreezerTailPruneAllowsGenesisOnlyWithoutEventLogStage(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              10,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        0,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    0,
		HasChainLookupPruneBlock: true,
	})
	if !plan.CanPrune || plan.TargetTail != 1 {
		t.Fatalf("genesis-only plan = %+v, want target tail 1", plan)
	}
	if plan.CoverageBlock != 0 || plan.CoverageTail != 1 || plan.RetentionTail != 91 {
		t.Fatalf("genesis-only bounds = %+v, want coverage block/tail 0/1 and retention tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneCapsMissingEventLogStageAtGenesis(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              200,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        95,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    95,
		HasChainLookupPruneBlock: true,
	})
	if !plan.CanPrune || plan.TargetTail != 1 {
		t.Fatalf("plan without event-log stage = %+v, want genesis-only target tail 1", plan)
	}
	if plan.CoverageBlock != 0 || plan.CoverageTail != 1 || plan.RetentionTail != 91 {
		t.Fatalf("plan bounds without event-log stage = %+v, want coverage block/tail 0/1 and retention tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneCapsAtLookupCoverage(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              200,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        95,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    80,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       95,
		HasEventLogBuildBlock:    true,
	})
	if !plan.CanPrune || plan.TargetTail != 81 {
		t.Fatalf("plan = %+v, want target tail 81", plan)
	}
	if plan.CoverageBlock != 80 || plan.CoverageTail != 81 || plan.RetentionTail != 91 {
		t.Fatalf("plan bounds = %+v, want coverage block/tail 80/81 and retention tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneCapsAtEventLogCoverage(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              200,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        95,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    95,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       70,
		HasEventLogBuildBlock:    true,
	})
	if !plan.CanPrune || plan.TargetTail != 71 {
		t.Fatalf("plan = %+v, want target tail 71", plan)
	}
	if plan.CoverageBlock != 70 || plan.CoverageTail != 71 || plan.RetentionTail != 91 {
		t.Fatalf("plan bounds = %+v, want event-log coverage block/tail 70/71 and retention tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneCapsAtRetentionWindow(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              200,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        95,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    95,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       95,
		HasEventLogBuildBlock:    true,
	})
	if !plan.CanPrune || plan.TargetTail != 91 {
		t.Fatalf("plan = %+v, want target tail 91", plan)
	}
	if plan.CoverageTail != 96 || plan.RetentionTail != 91 {
		t.Fatalf("plan bounds = %+v, want coverage tail 96 and retention tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneCapsAtAncientHead(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              50,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        95,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    95,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       95,
		HasEventLogBuildBlock:    true,
	})
	if !plan.CanPrune || plan.TargetTail != 50 {
		t.Fatalf("plan = %+v, want ancient-head capped target tail 50", plan)
	}
}

func TestPlanChainFreezerTailPruneNoopsWhenCurrentTailSatisfiesLimits(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              91,
		AncientHead:              200,
		HeadBlock:                100,
		RetainBlocks:             10,
		ChainFreezerBlock:        95,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    95,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       95,
		HasEventLogBuildBlock:    true,
	})
	if plan.CanPrune || plan.TargetTail != 91 || plan.Reason != chainFreezerTailPruneReasonCurrentTailCovered {
		t.Fatalf("plan = %+v, want no-op at current tail", plan)
	}
}

func TestPlanChainFreezerTailPruneKeepsShortChains(t *testing.T) {
	plan := PlanChainFreezerTailPrune(ChainFreezerTailPrunePlanInput{
		CurrentTail:              0,
		AncientHead:              10,
		HeadBlock:                9,
		RetainBlocks:             10,
		ChainFreezerBlock:        9,
		HasChainFreezerBlock:     true,
		ChainLookupPruneBlock:    9,
		HasChainLookupPruneBlock: true,
		EventLogBuildBlock:       9,
		HasEventLogBuildBlock:    true,
	})
	if plan.CanPrune || plan.TargetTail != 0 || plan.RetentionTail != 0 {
		t.Fatalf("short-chain plan = %+v, want no-op retention tail 0", plan)
	}
}

func TestPlanChainFreezerTailPruneFromDB(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 95)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 95)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 95)
	plan, err := PlanChainFreezerTailPruneFromDB(db, 0, 200, 100, 10)
	if err != nil {
		t.Fatalf("PlanChainFreezerTailPruneFromDB: %v", err)
	}
	if !plan.CanPrune || plan.TargetTail != 91 {
		t.Fatalf("plan from db = %+v, want target tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneFromDBRejectsUnboundDependencyStage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 95)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 95)
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 95); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}

	_, err := PlanChainFreezerTailPruneFromDB(db, 0, 200, 100, 10)
	if err == nil || !strings.Contains(err.Error(), "SnapshotEventLogBuild") || !strings.Contains(err.Error(), "not hash-bound") {
		t.Fatalf("PlanChainFreezerTailPruneFromDB err = %v, want unbound event-log stage rejection", err)
	}
}

func TestPlanChainFreezerTailPruneFromDBRejectsDependencyStageHashMismatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 95)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 95)
	canonical := writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 95)
	other := canonical
	other[0] ^= 0xff
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotEventLogBuild, 95, other); err != nil {
		t.Fatalf("WriteStageProgressWithHash SnapshotEventLogBuild mismatch: %v", err)
	}

	_, err := PlanChainFreezerTailPruneFromDB(db, 0, 200, 100, 10)
	if err == nil || !strings.Contains(err.Error(), "SnapshotEventLogBuild") || !strings.Contains(err.Error(), "does not match canonical hash") {
		t.Fatalf("PlanChainFreezerTailPruneFromDB err = %v, want hash mismatch rejection", err)
	}
}

func TestPlanChainFreezerTailPruneFromDBCapsMissingEventLogStageAtGenesis(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 95)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 95)
	plan, err := PlanChainFreezerTailPruneFromDB(db, 0, 200, 100, 10)
	if err != nil {
		t.Fatalf("PlanChainFreezerTailPruneFromDB: %v", err)
	}
	if !plan.CanPrune || plan.TargetTail != 1 {
		t.Fatalf("plan without event-log build stage = %+v, want genesis-only target tail 1", plan)
	}
}

func TestApplyChainFreezerTailPruneFromDBAllowsGenesisOnlyWithoutEventLogStage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTailPruneBlockRows(t, f, 2)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 1)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 1)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 10, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if !result.Applied || result.OldTail != 0 || result.NewTail != 1 || result.Plan.TargetTail != 1 {
		t.Fatalf("apply result = %+v, want applied 0->1", result)
	}
	if tail, err := f.Tail(); err != nil || tail != 1 {
		t.Fatalf("freezer tail = %d/%v, want 1/nil", tail, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotChainFreezerTailPrune); err != nil || !ok || got != 0 {
		t.Fatalf("StageSnapshotChainFreezerTailPrune = %d ok=%v err=%v, want 0", got, ok, err)
	}
	block0, _, _ := chainFreezerBlockWithTx(t, 0)
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainFreezerTailPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block0.Hash() {
		t.Fatalf("StageSnapshotChainFreezerTailPrune row = %+v ok=%v err=%v, want hash %x", row, ok, err, block0.Hash())
	}
	if ok, err := f.HasAncient(rawdb.AncientBlocksTable, 0); err != nil || ok {
		t.Fatalf("HasAncient(0) = %v/%v, want false/nil", ok, err)
	}
	if ok, err := f.HasAncient(rawdb.AncientBlocksTable, 1); err != nil || !ok {
		t.Fatalf("HasAncient(1) = %v/%v, want true/nil", ok, err)
	}
}

func TestApplyChainFreezerTailPruneFromDBTruncatesTailWithColdCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTailPruneBlockRows(t, f, 10)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	eventRef := buildChainTailPruneEventLogSegment(t, snapshotDir, 1, 9)
	eventIndexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, indexRef, eventRef, eventIndexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if !result.Applied || result.OldTail != 0 || result.NewTail != 7 || result.Plan.TargetTail != 7 {
		t.Fatalf("apply result = %+v, want applied 0->7", result)
	}
	if tail, err := f.Tail(); err != nil || tail != 7 {
		t.Fatalf("freezer tail = %d/%v, want 7/nil", tail, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotChainFreezerTailPrune); err != nil || !ok || got != 6 {
		t.Fatalf("StageSnapshotChainFreezerTailPrune = %d ok=%v err=%v, want 6", got, ok, err)
	}
	block6, _, _ := chainFreezerBlockWithTx(t, 6)
	if row, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSnapshotChainFreezerTailPrune); err != nil || !ok || !row.HasBlockHash || row.BlockHash != block6.Hash() {
		t.Fatalf("StageSnapshotChainFreezerTailPrune row = %+v ok=%v err=%v, want hash %x", row, ok, err, block6.Hash())
	}
	if ok, err := f.HasAncient(rawdb.AncientBlocksTable, 6); err != nil || ok {
		t.Fatalf("HasAncient(6) = %v/%v, want false/nil", ok, err)
	}
	if ok, err := f.HasAncient(rawdb.AncientBlocksTable, 7); err != nil || !ok {
		t.Fatalf("HasAncient(7) = %v/%v, want true/nil", ok, err)
	}
}

func TestApplyChainFreezerTailPruneFromDBRejectsStageHashConflictBeforeTruncate(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTailPruneBlockRows(t, f, 2)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 1)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 1)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 1)
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSnapshotChainFreezerTailPrune, 0, common.Hash{0xee}); err != nil {
		t.Fatalf("WriteStageProgressWithHash SnapshotChainFreezerTailPrune: %v", err)
	}

	_, err = ApplyChainFreezerTailPruneFromDB(db, f, mgr, 10, 3)
	if err == nil || !strings.Contains(err.Error(), "does not match pruned block hash") {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB err = %v, want stage hash conflict", err)
	}
	if tail, err := f.Tail(); err != nil || tail != 0 {
		t.Fatalf("freezer tail after rejected prune = %d/%v, want 0/nil", tail, err)
	}
}

func TestApplyChainFreezerTailPruneRequiresEventLogColdCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTailPruneBlockRows(t, f, 10)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, indexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingEventLogCold {
		t.Fatalf("apply result = %+v, want no apply due to missing event-log coverage", result)
	}
	if tail, err := f.Tail(); err != nil || tail != 0 {
		t.Fatalf("freezer tail = %d/%v, want 0/nil", tail, err)
	}
}

func TestApplyChainFreezerTailPruneRequiresEventLogIndexCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTailPruneBlockRows(t, f, 10)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	eventRef := buildChainTailPruneEventLogSegment(t, snapshotDir, 1, 9)
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, indexRef, eventRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingEventLogCold {
		t.Fatalf("apply result = %+v, want no apply due to missing event-log-index coverage", result)
	}
}

func TestApplyChainFreezerTailPruneRequiresChainIndexColdCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTailPruneBlockRows(t, f, 10)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	eventRef := buildChainTailPruneEventLogSegment(t, snapshotDir, 1, 9)
	eventIndexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, eventRef, eventIndexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingChainIndex {
		t.Fatalf("apply result = %+v, want no apply due to missing chain-index coverage", result)
	}
}

func TestVerifyColdEventLogTailCoverageSkipsGenesis(t *testing.T) {
	if err := verifyColdEventLogTailCoverage(rawdb.NoopAncient{}, 0, 1); err != nil {
		t.Fatalf("verifyColdEventLogTailCoverage genesis-only = %v, want nil", err)
	}
}

func TestApplyChainFreezerTailPruneFromDBRequiresColdCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTestRows(t, f, 0, 9)

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, rawdb.NoopAncient{}, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingColdCoverage {
		t.Fatalf("apply result = %+v, want no apply due to missing cold coverage", result)
	}
	if tail, err := f.Tail(); err != nil || tail != 0 {
		t.Fatalf("freezer tail = %d/%v, want 0/nil", tail, err)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotChainFreezerTailPrune); err != nil || ok {
		t.Fatalf("StageSnapshotChainFreezerTailPrune after missing coverage = %d ok=%v err=%v, want absent", got, ok, err)
	}
}

func TestApplyChainFreezerTailPruneRejectsColdCoverageGap(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTestRows(t, f, 0, 9)

	snapshotDir := root + "/snapshot"
	refA, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 2)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 0..2: %v", err)
	}
	refB, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 4, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 4..9: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{refA, refB})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.ChainFreezerRangeCovered(0, 2); err != nil || !covered {
		t.Fatalf("ChainFreezerRangeCovered 0..2 = %v/%v, want true/nil", covered, err)
	}
	if covered, err := mgr.ChainFreezerRangeCovered(0, 6); err != nil || covered {
		t.Fatalf("ChainFreezerRangeCovered 0..6 = %v/%v, want false/nil due to gap", covered, err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingColdCoverage {
		t.Fatalf("apply result = %+v, want no apply due to cold coverage gap", result)
	}
	if tail, err := f.Tail(); err != nil || tail != 0 {
		t.Fatalf("freezer tail = %d/%v, want 0/nil", tail, err)
	}
}

func TestApplyChainFreezerTailPruneRejectsUnreadableColdCoverageSegment(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTestRows(t, f, 0, 9)

	snapshotDir := root + "/snapshot"
	refA, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 2)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 0..2: %v", err)
	}
	refB, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 3, 4)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 3..4: %v", err)
	}
	refC, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 5, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient 5..9: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{refA, refB, refC})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	if err := os.Remove(filepath.Join(snapshotDir, refB.Path)); err != nil {
		t.Fatalf("remove middle segment: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}
	if covered, err := mgr.ChainFreezerRangeCovered(0, 6); err == nil || covered {
		t.Fatalf("ChainFreezerRangeCovered 0..6 = %v/%v, want false/error due to missing middle file", covered, err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 8)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 8)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingColdCoverage {
		t.Fatalf("apply result = %+v, want no apply due to unreadable cold coverage segment", result)
	}
}

func TestVerifyColdChainFreezerTailCoverageRejectsGenericMiddleGap(t *testing.T) {
	cold := newChainTailPruneTestAncient()
	for _, table := range chainTailPruneAncientTables() {
		cold.put(table, 0, []byte(table+"-0"))
		cold.put(table, 2, []byte(table+"-2"))
	}

	if err := verifyColdChainFreezerTailCoverage(cold, 0, 3); !errors.Is(err, rawdb.ErrNotInAncient) {
		t.Fatalf("verifyColdChainFreezerTailCoverage with middle gap = %v, want ErrNotInAncient", err)
	}

	for _, table := range chainTailPruneAncientTables() {
		cold.put(table, 1, []byte(table+"-1"))
	}
	if err := verifyColdChainFreezerTailCoverage(cold, 0, 3); err != nil {
		t.Fatalf("verifyColdChainFreezerTailCoverage complete generic reader = %v, want nil", err)
	}
}

func TestApplyChainFreezerTailPrunePhysicallyReclaimsAndRestarts(t *testing.T) {
	root := t.TempDir()
	ancientDir := filepath.Join(root, "ancient")
	f := openChainFreezerSizedTestStore(t, ancientDir, 128)
	appendChainFreezerTailPruneBlockRows(t, f, 12)

	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 11)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	indexRef, err := BuildChainIndexSegmentFromChainFreezerSegment(snapshotDir, ref, "")
	if err != nil {
		t.Fatalf("BuildChainIndexSegmentFromChainFreezerSegment: %v", err)
	}
	eventRef := buildChainTailPruneEventLogSegment(t, snapshotDir, 1, 11)
	eventIndexRef, err := BuildEventLogIndexSegmentFromEventLogSegments(snapshotDir, []SegmentRef{eventRef}, "")
	if err != nil {
		t.Fatalf("BuildEventLogIndexSegmentFromEventLogSegments: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref, indexRef, eventRef, eventIndexRef})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	writeChainTailPruneDependencyStage(t, db, rawdb.StageChainFreezer, 10)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotChainLookupPrune, 10)
	writeChainTailPruneDependencyStage(t, db, rawdb.StageSnapshotEventLogBuild, 10)
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 11, 4)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if !result.Applied || result.NewTail != 8 || result.PrunedTailFiles == 0 {
		t.Fatalf("apply result = %+v, want tail 8 with physical file pruning", result)
	}
	if got, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSnapshotChainFreezerTailPrune); err != nil || !ok || got != 7 {
		t.Fatalf("StageSnapshotChainFreezerTailPrune = %d ok=%v err=%v, want 7", got, ok, err)
	}
	for _, name := range []string{"bodies.0000.cdat", "tx_infos.0000.cdat", "state_roots.0000.rdat"} {
		if _, err := os.Stat(filepath.Join(ancientDir, name)); !os.IsNotExist(err) {
			t.Fatalf("%s after physical prune err = %v, want not exist", name, err)
		}
	}
	if _, err := f.Ancient(rawdb.AncientBlocksTable, 7); !errors.Is(err, rawdbfreezer.ErrOutOfBounds) {
		t.Fatalf("local freezer read pruned block 7 = %v, want out of bounds", err)
	}
	if _, err := mgr.Ancient(rawdb.AncientBlocksTable, 7); err != nil {
		t.Fatalf("cold manager read pruned block 7: %v", err)
	}
	if _, err := f.Ancient(rawdb.AncientBlocksTable, 8); err != nil {
		t.Fatalf("local freezer read retained block 8: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close local freezer: %v", err)
	}

	reopened := openChainFreezerSizedTestStore(t, ancientDir, 128)
	defer reopened.Close()
	if tail, err := reopened.Tail(); err != nil || tail != 8 {
		t.Fatalf("reopened tail = %d/%v, want 8/nil", tail, err)
	}
	if _, err := reopened.Ancient(rawdb.AncientBlocksTable, 7); !errors.Is(err, rawdbfreezer.ErrOutOfBounds) {
		t.Fatalf("reopened local freezer read block 7 = %v, want out of bounds", err)
	}
	fallback := rawdb.NewFallbackAncientReader(rawdb.NewFreezerReader(reopened), mgr)
	if _, err := fallback.Ancient(rawdb.AncientBlocksTable, 7); err != nil {
		t.Fatalf("fallback read physically-pruned block 7: %v", err)
	}
	if _, err := fallback.Ancient(rawdb.AncientBlocksTable, 8); err != nil {
		t.Fatalf("fallback read retained block 8: %v", err)
	}
}

func appendChainFreezerTailPruneBlockRows(t *testing.T, f *rawdbfreezer.Freezer, count uint64) {
	t.Helper()
	rows := make([]chainFreezerRawTestRow, 0, count)
	for n := uint64(0); n < count; n++ {
		block, _, txInfosRaw := chainFreezerBlockWithTx(t, n)
		rows = append(rows, chainFreezerRawTestRow{
			block:      block,
			txInfosRaw: txInfosRaw,
			stateRoot:  chainFreezerTestStateRoot(n),
		})
	}
	appendChainFreezerRawRows(t, f, rows)
}

func buildChainTailPruneEventLogSegment(t *testing.T, snapshotDir string, fromBlock, toBlock uint64) SegmentRef {
	t.Helper()
	db := rawdb.NewMemoryChainDB()
	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		block, infos := eventLogTestBlock(t, blockNum, nil)
		if err := rawdb.WriteBlock(db, block); err != nil {
			t.Fatalf("WriteBlock %d: %v", blockNum, err)
		}
		if err := rawdb.WriteTransactionInfosByBlock(db, blockNum, infos); err != nil {
			t.Fatalf("WriteTransactionInfosByBlock %d: %v", blockNum, err)
		}
	}
	ref, err := BuildEventLogSegmentFromChain(db, snapshotDir, "", fromBlock, toBlock)
	if err != nil {
		t.Fatalf("BuildEventLogSegmentFromChain %d..%d: %v", fromBlock, toBlock, err)
	}
	return ref
}

func openChainFreezerSizedTestStore(t *testing.T, dir string, maxTableSize uint32) *rawdbfreezer.Freezer {
	t.Helper()
	f, err := rawdbfreezer.NewFreezer(dir, "", false, maxTableSize, map[string]rawdbfreezer.TableConfig{
		rawdb.AncientBlocksTable:     {NoSnappy: false, Prunable: true},
		rawdb.AncientTxInfosTable:    {NoSnappy: false, Prunable: true},
		rawdb.AncientStateRootsTable: {NoSnappy: true, Prunable: true},
	})
	if err != nil {
		t.Fatalf("NewFreezer: %v", err)
	}
	return f
}

func appendLargeChainFreezerTestRows(t *testing.T, f *rawdbfreezer.Freezer, from, to uint64) {
	t.Helper()
	if _, err := f.ModifyAncients(func(op rawdb.AncientWriteOp) error {
		for n := from; n <= to; n++ {
			if err := op.AppendRaw(rawdb.AncientBlocksTable, n, largeChainFreezerPayload("block", n)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientTxInfosTable, n, largeChainFreezerPayload("txinfos", n)); err != nil {
				return err
			}
			if err := op.AppendRaw(rawdb.AncientStateRootsTable, n, largeChainFreezerPayload("state-root", n)); err != nil {
				return err
			}
			if n == to {
				break
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ModifyAncients: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

func largeChainFreezerPayload(label string, n uint64) []byte {
	out := make([]byte, 72)
	seed := byte(len(label)) ^ byte(n*31)
	for i := range out {
		out[i] = seed + byte(i*17) + byte((i*i)%251)
	}
	return out
}

func chainTailPruneAncientTables() []string {
	return []string{rawdb.AncientBlocksTable, rawdb.AncientTxInfosTable, rawdb.AncientStateRootsTable}
}

type chainTailPruneTestAncient struct {
	rows map[string]map[uint64][]byte
}

func newChainTailPruneTestAncient() *chainTailPruneTestAncient {
	return &chainTailPruneTestAncient{rows: make(map[string]map[uint64][]byte)}
}

func (a *chainTailPruneTestAncient) put(kind string, number uint64, data []byte) {
	if a.rows[kind] == nil {
		a.rows[kind] = make(map[uint64][]byte)
	}
	a.rows[kind][number] = append([]byte(nil), data...)
}

func (a *chainTailPruneTestAncient) Ancient(kind string, number uint64) ([]byte, error) {
	table := a.rows[kind]
	if table == nil {
		return nil, rawdb.ErrNotInAncient
	}
	data, ok := table[number]
	if !ok {
		return nil, rawdb.ErrNotInAncient
	}
	return append([]byte(nil), data...), nil
}

func (a *chainTailPruneTestAncient) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	if count == 0 {
		return nil, nil
	}
	var (
		out        [][]byte
		totalBytes uint64
	)
	for i := uint64(0); i < count; i++ {
		number := start + i
		if number < start {
			break
		}
		data, err := a.Ancient(kind, number)
		if err != nil {
			if len(out) > 0 && errors.Is(err, rawdb.ErrNotInAncient) {
				break
			}
			return nil, err
		}
		if maxBytes > 0 && len(out) > 0 && totalBytes+uint64(len(data)) > maxBytes {
			break
		}
		out = append(out, data)
		totalBytes += uint64(len(data))
	}
	if len(out) == 0 {
		return nil, rawdb.ErrNotInAncient
	}
	return out, nil
}

func (a *chainTailPruneTestAncient) AncientCount(kind string) (uint64, error) {
	var count uint64
	for number := range a.rows[kind] {
		tail := number + 1
		if tail > count {
			count = tail
		}
	}
	return count, nil
}

func (a *chainTailPruneTestAncient) HasAncient(kind string, number uint64) (bool, error) {
	_, err := a.Ancient(kind, number)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, rawdb.ErrNotInAncient) {
		return false, nil
	}
	return false, err
}
