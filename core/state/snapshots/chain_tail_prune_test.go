package snapshots

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	rawdbfreezer "github.com/tronprotocol/go-tron/core/rawdb/freezer"
)

func TestPlanChainFreezerTailPruneRequiresStages(t *testing.T) {
	input := ChainFreezerTailPrunePlanInput{
		AncientHead:              100,
		HeadBlock:                100,
		RetainBlocks:             10,
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

	input.HasChainLookupPruneBlock = true
	input.HasEventLogBuildBlock = false
	if plan := PlanChainFreezerTailPrune(input); plan.CanPrune || plan.Reason != chainFreezerTailPruneReasonMissingEventLogStage {
		t.Fatalf("plan without event-log stage = %+v, want missing event-log stage", plan)
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
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 95); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 95); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 95); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}
	plan, err := PlanChainFreezerTailPruneFromDB(db, 0, 200, 100, 10)
	if err != nil {
		t.Fatalf("PlanChainFreezerTailPruneFromDB: %v", err)
	}
	if !plan.CanPrune || plan.TargetTail != 91 {
		t.Fatalf("plan from db = %+v, want target tail 91", plan)
	}
}

func TestPlanChainFreezerTailPruneFromDBRequiresEventLogBuildStage(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 95); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 95); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	plan, err := PlanChainFreezerTailPruneFromDB(db, 0, 200, 100, 10)
	if err != nil {
		t.Fatalf("PlanChainFreezerTailPruneFromDB: %v", err)
	}
	if plan.CanPrune || plan.Reason != chainFreezerTailPruneReasonMissingEventLogStage {
		t.Fatalf("plan without event-log build stage = %+v, want missing event-log stage", plan)
	}
}

func TestApplyChainFreezerTailPruneFromDBTruncatesTailWithColdCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTestRows(t, f, 0, 9)

	snapshotDir := root + "/snapshot"
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 9)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 8); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}
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
	if ok, err := f.HasAncient(rawdb.AncientBlocksTable, 6); err != nil || ok {
		t.Fatalf("HasAncient(6) = %v/%v, want false/nil", ok, err)
	}
	if ok, err := f.HasAncient(rawdb.AncientBlocksTable, 7); err != nil || !ok {
		t.Fatalf("HasAncient(7) = %v/%v, want true/nil", ok, err)
	}
}

func TestApplyChainFreezerTailPruneFromDBRequiresColdCoverage(t *testing.T) {
	root := t.TempDir()
	f := openChainFreezerTestStore(t, root+"/ancient")
	defer f.Close()
	appendChainFreezerTestRows(t, f, 0, 9)

	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 8); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}
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
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 8); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}
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
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 8); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 8); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}
	result, err := ApplyChainFreezerTailPruneFromDB(db, f, mgr, 9, 3)
	if err != nil {
		t.Fatalf("ApplyChainFreezerTailPruneFromDB: %v", err)
	}
	if result.Applied || result.Plan.Reason != chainFreezerTailPruneReasonMissingColdCoverage {
		t.Fatalf("apply result = %+v, want no apply due to unreadable cold coverage segment", result)
	}
}

func TestApplyChainFreezerTailPrunePhysicallyReclaimsAndRestarts(t *testing.T) {
	root := t.TempDir()
	ancientDir := filepath.Join(root, "ancient")
	f := openChainFreezerSizedTestStore(t, ancientDir, 128)
	appendLargeChainFreezerTestRows(t, f, 0, 11)

	snapshotDir := filepath.Join(root, "snapshot")
	ref, err := BuildChainFreezerSegmentFromAncient(rawdb.NewFreezerReader(f), snapshotDir, "", 0, 11)
	if err != nil {
		t.Fatalf("BuildChainFreezerSegmentFromAncient: %v", err)
	}
	if err := PublishManifest(snapshotDir, NewManifest(0, 0, []SegmentRef{ref})); err != nil {
		t.Fatalf("PublishManifest: %v", err)
	}
	mgr, err := OpenManager(snapshotDir)
	if err != nil {
		t.Fatalf("OpenManager: %v", err)
	}

	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStageProgress(db, rawdb.StageChainFreezer, 10); err != nil {
		t.Fatalf("WriteStageProgress ChainFreezer: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotChainLookupPrune, 10); err != nil {
		t.Fatalf("WriteStageProgress SnapshotChainLookupPrune: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSnapshotEventLogBuild, 10); err != nil {
		t.Fatalf("WriteStageProgress SnapshotEventLogBuild: %v", err)
	}
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
