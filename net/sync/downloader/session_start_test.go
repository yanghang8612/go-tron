package downloader

import (
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

func TestPlanSyncStartGate(t *testing.T) {
	base := SyncStartGateInput{
		PeerPresent:         true,
		AvailabilityChecked: true,
		PeerCanServe:        true,
	}
	if got := PlanSyncStartGate(base); !got.Allowed || got.SkipReason != SyncStartSkipNone {
		t.Fatalf("allowed gate = %+v, want allowed", got)
	}
	if got := PlanSyncStartGate(SyncStartGateInput{PeerPresent: true}); !got.Allowed {
		t.Fatalf("unchecked availability gate = %+v, want allowed", got)
	}

	tests := []struct {
		name string
		in   SyncStartGateInput
		want SyncStartSkipReason
	}{
		{name: "no peer", in: SyncStartGateInput{}, want: SyncStartSkipNoPeer},
		{name: "stopping", in: SyncStartGateInput{PeerPresent: true, Stopping: true}, want: SyncStartSkipStopping},
		{name: "paused", in: SyncStartGateInput{PeerPresent: true, Paused: true}, want: SyncStartSkipPaused},
		{name: "unavailable", in: SyncStartGateInput{PeerPresent: true, AvailabilityChecked: true}, want: SyncStartSkipPeerUnavailable},
	}
	for _, tt := range tests {
		got := PlanSyncStartGate(tt.in)
		if got.Allowed || got.SkipReason != tt.want {
			t.Fatalf("%s: gate = %+v, want skip %s", tt.name, got, tt.want)
		}
	}
}

func TestPlanSyncPeerAttach(t *testing.T) {
	fresh := PlanSyncPeerAttach(SyncPeerAttachInput{})
	if !fresh.Attach || !fresh.InitSession || !fresh.LogStarted ||
		!fresh.MarkChainRequested || !fresh.MirrorLegacy ||
		!fresh.SendChainSummary || !fresh.JoinAvailablePeers ||
		fresh.SkipReason != SyncStartSkipNone {
		t.Fatalf("fresh attach = %+v, want full session start", fresh)
	}

	joined := PlanSyncPeerAttach(SyncPeerAttachInput{Syncing: true})
	if !joined.Attach || joined.InitSession || joined.LogStarted ||
		!joined.MarkChainRequested || !joined.MirrorLegacy ||
		!joined.SendChainSummary || !joined.JoinAvailablePeers {
		t.Fatalf("peer join attach = %+v, want attach without session init", joined)
	}

	duplicate := PlanSyncPeerAttach(SyncPeerAttachInput{Syncing: true, PeerAlreadyJoined: true})
	if duplicate.Attach || duplicate.InitSession || duplicate.MarkChainRequested ||
		duplicate.SkipReason != SyncStartSkipAlreadyJoined {
		t.Fatalf("duplicate attach = %+v, want already-joined skip", duplicate)
	}

	staleMap := PlanSyncPeerAttach(SyncPeerAttachInput{Syncing: false, PeerAlreadyJoined: true})
	if !staleMap.Attach || !staleMap.InitSession || !staleMap.LogStarted {
		t.Fatalf("stale non-syncing attach = %+v, want fresh session to ignore stale peer map", staleMap)
	}
}

func TestPlanSessionStartup(t *testing.T) {
	got := PlanSessionStartup(SessionStartupInput{
		Head:         99,
		RestoreLimit: 32,
	})

	if got.InventoryFloor != 99 {
		t.Fatalf("inventory floor = %d, want 99", got.InventoryFloor)
	}
	if got.DeleteImportedThrough != 99 {
		t.Fatalf("delete imported through = %d, want 99", got.DeleteImportedThrough)
	}
	if got.RestoreStagedBodiesFrom != 100 {
		t.Fatalf("restore staged bodies from = %d, want 100", got.RestoreStagedBodiesFrom)
	}
	if got.RestoreLimit != 32 {
		t.Fatalf("restore limit = %d, want 32", got.RestoreLimit)
	}
	if !got.PruneStaleTail {
		t.Fatalf("prune stale tail = false, want true")
	}
	if !got.ResetPeerJoinThrottle {
		t.Fatalf("reset peer join throttle = false, want true")
	}
	wantSteps := []SessionStartupStep{
		{Action: SessionStartupRepairSyncPipeline},
		{Action: SessionStartupCompleteHeadSyncPipeline},
		{Action: SessionStartupRestoreInventoryTarget, InventoryFloor: 99},
		{Action: SessionStartupDeleteImportedBodies, DeleteImportedThrough: 99},
		{Action: SessionStartupRestoreStagedBodies, RestoreStagedBodiesFrom: 100, RestoreLimit: 32, PruneStaleTail: true},
		{Action: SessionStartupRefreshBodiesReady},
		{Action: SessionStartupRepairSyncPipelineOrder},
		{Action: SessionStartupCheckSyncPipelineOrder},
		{Action: SessionStartupDeriveSyncPipelineCursor},
	}
	if !reflect.DeepEqual(got.Steps, wantSteps) {
		t.Fatalf("startup steps = %+v, want %+v", got.Steps, wantSteps)
	}
}

func TestPlanSessionStartupClampsNegativeRestoreLimit(t *testing.T) {
	got := PlanSessionStartup(SessionStartupInput{
		Head:         7,
		RestoreLimit: -1,
	})

	if got.RestoreLimit != 0 {
		t.Fatalf("restore limit = %d, want 0", got.RestoreLimit)
	}
	if got.PruneStaleTail {
		t.Fatalf("prune stale tail = true, want false")
	}
	if got.RestoreStagedBodiesFrom != 8 {
		t.Fatalf("restore staged bodies from = %d, want 8", got.RestoreStagedBodiesFrom)
	}
	for _, step := range got.Steps {
		if step.Action == SessionStartupRestoreStagedBodies && step.PruneStaleTail {
			t.Fatalf("restore step prune stale tail = true, want false: %+v", step)
		}
	}
}

func TestPlanSessionStartupSaturatesRestoreStart(t *testing.T) {
	const maxUint64 = ^uint64(0)

	got := PlanSessionStartup(SessionStartupInput{
		Head:         maxUint64,
		RestoreLimit: 32,
	})

	if got.RestoreStagedBodiesFrom != maxUint64 {
		t.Fatalf("restore staged bodies from = %d, want %d", got.RestoreStagedBodiesFrom, maxUint64)
	}
	if got.DeleteImportedThrough != maxUint64 {
		t.Fatalf("delete imported through = %d, want %d", got.DeleteImportedThrough, maxUint64)
	}
}

func TestApplySessionStartupPlan(t *testing.T) {
	repairHash := tcommon.Hash{0x09}
	repairRows := []SyncStageProgressRepair{
		{
			Stage:  rawdb.StageSyncImport,
			Status: SyncStageProgressKept,
			Row: rawdb.StageProgress{
				Stage:        rawdb.StageSyncImport,
				BlockNum:     9,
				BlockHash:    repairHash,
				HasBlockHash: true,
			},
			CanonicalHash: repairHash,
		},
	}
	repairResult := SyncPipelineProgressRepairResult{
		Repairs: repairRows,
		Kept:    1,
	}
	restoreResult := StagedBodyRestoreResult{
		Restored:         2,
		TargetHead:       11,
		NextExpected:     12,
		LastRestoredNum:  11,
		LastRestoredHash: tcommon.Hash{0x0b},
		HaveLastRestored: true,
	}
	orderIssues := []SyncPipelineProgressOrderIssue{
		{
			Downstream:      rawdb.StageSyncExecution,
			DownstreamBlock: 10,
			Upstream:        rawdb.StageSyncImport,
			UpstreamBlock:   9,
		},
	}
	orderRepair := SyncPipelineProgressOrderRepairResult{
		Deleted:  1,
		Complete: true,
	}
	plan := SessionStartupPlan{
		Steps: []SessionStartupStep{
			{Action: SessionStartupRepairSyncPipeline},
			{Action: SessionStartupCompleteHeadSyncPipeline},
			{Action: SessionStartupRestoreInventoryTarget, InventoryFloor: 9},
			{Action: SessionStartupStepAction(255)},
			{Action: SessionStartupDeleteImportedBodies, DeleteImportedThrough: 8},
			{Action: SessionStartupRestoreStagedBodies, RestoreStagedBodiesFrom: 10, RestoreLimit: 32, PruneStaleTail: true},
			{Action: SessionStartupRefreshBodiesReady},
			{Action: SessionStartupRepairSyncPipelineOrder},
			{Action: SessionStartupCheckSyncPipelineOrder},
			{Action: SessionStartupDeriveSyncPipelineCursor},
		},
	}
	applier := recordingSessionStartupApplier{
		repair:      repairResult,
		restore:     restoreResult,
		orderRepair: orderRepair,
		orderIssues: orderIssues,
	}
	result := ApplySessionStartupPlan(plan, &applier)
	want := []recordedSessionStartupCall{
		{action: SessionStartupRepairSyncPipeline},
		{action: SessionStartupCompleteHeadSyncPipeline},
		{action: SessionStartupRestoreInventoryTarget, first: 9},
		{action: SessionStartupDeleteImportedBodies, first: 8},
		{action: SessionStartupRestoreStagedBodies, first: 10, limit: 32, prune: true},
		{action: SessionStartupRefreshBodiesReady},
		{action: SessionStartupRepairSyncPipelineOrder},
		{action: SessionStartupCheckSyncPipelineOrder},
	}
	if !reflect.DeepEqual(applier.calls, want) {
		t.Fatalf("calls = %+v, want %+v", applier.calls, want)
	}
	wantApplied := []SessionStartupStepAction{
		SessionStartupRepairSyncPipeline,
		SessionStartupCompleteHeadSyncPipeline,
		SessionStartupRestoreInventoryTarget,
		SessionStartupDeleteImportedBodies,
		SessionStartupRestoreStagedBodies,
		SessionStartupRefreshBodiesReady,
		SessionStartupRepairSyncPipelineOrder,
		SessionStartupCheckSyncPipelineOrder,
		SessionStartupDeriveSyncPipelineCursor,
	}
	if !reflect.DeepEqual(result.AppliedSteps, wantApplied) {
		t.Fatalf("applied steps = %+v, want %+v", result.AppliedSteps, wantApplied)
	}
	if !reflect.DeepEqual(result.UnknownSteps, []SessionStartupStepAction{SessionStartupStepAction(255)}) {
		t.Fatalf("unknown steps = %+v, want [255]", result.UnknownSteps)
	}
	if !reflect.DeepEqual(result.SyncPipelineRepairs, repairRows) {
		t.Fatalf("sync pipeline repairs = %+v, want %+v", result.SyncPipelineRepairs, repairRows)
	}
	if !result.HasSyncPipelineRepair || !reflect.DeepEqual(result.SyncPipelineRepairResult, repairResult) {
		t.Fatalf("sync pipeline repair result = %+v set=%v, want %+v set", result.SyncPipelineRepairResult, result.HasSyncPipelineRepair, repairResult)
	}
	if !result.HasSyncPipelineHead {
		t.Fatalf("sync pipeline head completion set=%v, want set", result.HasSyncPipelineHead)
	}
	if !result.HasStagedBodyRestore || !reflect.DeepEqual(result.StagedBodyRestore, restoreResult) {
		t.Fatalf("staged body restore = %+v set=%v, want %+v set", result.StagedBodyRestore, result.HasStagedBodyRestore, restoreResult)
	}
	if !result.HasSyncPipelineOrderRepair || !reflect.DeepEqual(result.SyncPipelineOrderRepair, orderRepair) {
		t.Fatalf("sync pipeline order repair = %+v set=%v, want %+v set", result.SyncPipelineOrderRepair, result.HasSyncPipelineOrderRepair, orderRepair)
	}
	if !result.HasSyncPipelineOrder || !reflect.DeepEqual(result.SyncPipelineOrderIssues, orderIssues) {
		t.Fatalf("sync pipeline order issues = %+v set=%v, want %+v set", result.SyncPipelineOrderIssues, result.HasSyncPipelineOrder, orderIssues)
	}
	if !reflect.DeepEqual(result.SyncPipelineOrderCheck.Issues, orderIssues) || len(result.SyncPipelineOrderErrors) != 0 {
		t.Fatalf("sync pipeline order check = %+v errors=%+v, want issues without read errors", result.SyncPipelineOrderCheck, result.SyncPipelineOrderErrors)
	}
	if !result.HasSyncPipelineCursor || !result.SyncPipelineCursor.HasBlocked ||
		result.SyncPipelineCursor.NextStage != rawdb.StageSyncExecution {
		t.Fatalf("sync pipeline cursor = %+v set=%v, want blocked cursor at execution", result.SyncPipelineCursor, result.HasSyncPipelineCursor)
	}

	if nilResult := ApplySessionStartupPlan(plan, nil); len(nilResult.AppliedSteps) != 0 || len(nilResult.UnknownSteps) != 0 || nilResult.HasSyncPipelineOrder {
		t.Fatalf("nil applier result = %+v, want empty", nilResult)
	}
}

func TestApplySessionStartupPlanRepairsHalfDownloadedAndHalfExecutedState(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block4 := testBufferedBlock(4)
	block6 := testBufferedBlock(6)
	for _, block := range []*types.Block{block2, block3, block4, block6} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, block6.Number()); err != nil {
		t.Fatalf("write inventory target: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block6.Number(), block6.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block6.Number(), block6.Hash()); err != nil {
		t.Fatalf("write sync bodies ready progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, block2.Number(), block2.Hash()); err != nil {
		t.Fatalf("write sync import progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncExecution, block2.Number(), block2.Hash()); err != nil {
		t.Fatalf("write sync execution progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncCommitment, block3.Number(), block3.Hash()); err != nil {
		t.Fatalf("write inconsistent sync commitment progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncFinish, block2.Number(), block2.Hash()); err != nil {
		t.Fatalf("write downstream sync finish progress: %v", err)
	}

	applier := newStartupRecoveryTestApplier(db, 2, map[uint64]tcommon.Hash{
		block2.Number(): block2.Hash(),
	})
	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         block2.Number(),
		RestoreLimit: 10,
	}), applier)

	wantRepairs := []SyncStageProgressRepairStatus{
		SyncStageProgressKept,
		SyncStageProgressKept,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
	}
	if len(result.SyncPipelineRepairs) != len(wantRepairs) {
		t.Fatalf("repairs = %+v, want %d entries", result.SyncPipelineRepairs, len(wantRepairs))
	}
	for i, status := range wantRepairs {
		if result.SyncPipelineRepairs[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, result.SyncPipelineRepairs[i], status)
		}
	}
	if !result.HasSyncPipelineRepair || result.SyncPipelineRepairResult.Kept != 2 ||
		result.SyncPipelineRepairResult.Deleted != 2 ||
		!result.SyncPipelineRepairResult.HasBlocked ||
		result.SyncPipelineRepairResult.FirstBlockedStage != rawdb.StageSyncCommitment ||
		result.SyncPipelineRepairResult.Complete {
		t.Fatalf("sync pipeline repair result = %+v, want kept import/execution and blocked commitment", result.SyncPipelineRepairResult)
	}
	if !result.HasSyncPipelineHead || !result.SyncPipelineHeadCompletion.Complete ||
		result.SyncPipelineHeadCompletion.Written != 2 ||
		!reflect.DeepEqual(result.SyncPipelineHeadCompletion.Plan.FillStages, []rawdb.StageID{rawdb.StageSyncCommitment, rawdb.StageSyncFinish}) {
		t.Fatalf("sync pipeline head completion = %+v set=%v, want commitment/finish filled", result.SyncPipelineHeadCompletion, result.HasSyncPipelineHead)
	}
	if !result.HasStagedBodyRestore {
		t.Fatal("startup result did not record staged body restore")
	}
	if !result.HasSyncPipelineOrder || len(result.SyncPipelineOrderIssues) != 0 {
		t.Fatalf("sync pipeline order issues = %+v set=%v, want checked with no issues", result.SyncPipelineOrderIssues, result.HasSyncPipelineOrder)
	}
	if cursor := result.SyncPipelineCursor; !result.HasSyncPipelineCursor || cursor.StageRows != 6 ||
		!cursor.HasLast || cursor.LastStage != rawdb.StageSyncFinish ||
		cursor.LastBlock != block2.Number() || cursor.LastHash != block2.Hash() ||
		cursor.HasNext || !cursor.Complete || cursor.HasBlocked || cursor.Interrupted {
		t.Fatalf("sync pipeline cursor = %+v set=%v, want completed current-head sync pipeline", cursor, result.HasSyncPipelineCursor)
	}
	if restore := result.StagedBodyRestore; restore.Restored != 2 || !restore.NeedPruneTail || restore.PruneFrom != 5 ||
		restore.TargetHead != 6 || restore.NextExpected != 5 || !restore.HaveLastRestored ||
		restore.LastRestoredNum != block4.Number() || restore.LastRestoredHash != block4.Hash() {
		t.Fatalf("restore result = %+v, want restored 3-4 and prune from 5", restore)
	}

	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block2.Number()); err != nil || ok {
		t.Fatalf("staged block2 = ok:%v err:%v, want deleted as already imported", ok, err)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block6.Number()); err != nil || ok {
		t.Fatalf("staged block6 = ok:%v err:%v, want pruned after gap", ok, err)
	}
	for _, block := range []*types.Block{block3, block4} {
		if row, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block.Number()); err != nil || !ok || row.Hash != block.Hash() {
			t.Fatalf("staged block%d = %+v ok=%v err=%v, want kept", block.Number(), row, ok, err)
		}
		if buffered, ok := applier.buffer[block.Number()]; !ok || buffered.Hash != block.Hash() {
			t.Fatalf("buffered block%d = %+v ok=%v, want restored", block.Number(), buffered, ok)
		}
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block4.Number() || row.BlockHash != block4.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block4", stage, row, ok, err)
		}
	}
	for _, stage := range SyncPipelineProgressStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block2 completed", stage, row, ok, err)
		}
	}
}

func TestApplySessionStartupPlanDeletesPipelineAfterImportForkHashMismatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block1 := testBufferedBlock(1)
	forkHash := block1.Hash()
	forkHash[0] ^= 0xff
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncImport, block1.Number(), forkHash); err != nil {
		t.Fatalf("write forked sync import progress: %v", err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncExecution, rawdb.StageSyncCommitment, rawdb.StageSyncFinish} {
		if err := rawdb.WriteStageProgressWithHash(db, stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}

	applier := newStartupRecoveryTestApplier(db, block1.Number(), map[uint64]tcommon.Hash{
		block1.Number(): block1.Hash(),
	})
	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         block1.Number(),
		RestoreLimit: 0,
	}), applier)

	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
		SyncStageProgressDeleted,
	}
	if len(result.SyncPipelineRepairs) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d entries", result.SyncPipelineRepairs, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if result.SyncPipelineRepairs[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, result.SyncPipelineRepairs[i], status)
		}
	}
	if !result.HasSyncPipelineRepair ||
		result.SyncPipelineRepairResult.Kept != 0 ||
		result.SyncPipelineRepairResult.Deleted != len(SyncPipelineProgressStages()) ||
		!result.SyncPipelineRepairResult.HasBlocked ||
		result.SyncPipelineRepairResult.FirstBlockedStage != rawdb.StageSyncImport ||
		result.SyncPipelineRepairResult.Complete {
		t.Fatalf("sync pipeline repair result = %+v, want all sync stages deleted after import fork hash", result.SyncPipelineRepairResult)
	}
	for _, stage := range SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want deleted", stage, row, ok, err)
		}
	}
	if !result.HasStagedBodyRestore || result.StagedBodyRestore.Restored != 0 || result.StagedBodyRestore.NeedPruneTail {
		t.Fatalf("staged body restore = %+v set=%v, want empty restore without tail prune", result.StagedBodyRestore, result.HasStagedBodyRestore)
	}
	if !result.HasSyncPipelineOrder || len(result.SyncPipelineOrderIssues) != 0 {
		t.Fatalf("sync pipeline order issues = %+v set=%v, want checked with no issues after fork repair", result.SyncPipelineOrderIssues, result.HasSyncPipelineOrder)
	}
	if cursor := result.SyncPipelineCursor; !result.HasSyncPipelineCursor ||
		cursor.StageRows != 0 ||
		cursor.HasLast ||
		!cursor.HasNext ||
		cursor.NextStage != rawdb.StageSyncBodies ||
		cursor.Complete ||
		cursor.HasBlocked ||
		cursor.Interrupted {
		t.Fatalf("sync pipeline cursor = %+v set=%v, want reset to body stage after fork repair", cursor, result.HasSyncPipelineCursor)
	}
}

func TestApplySessionStartupPlanKeepsUpstreamAfterFinishForkHashMismatch(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block1 := testBufferedBlock(1)
	for _, stage := range []rawdb.StageID{rawdb.StageSyncImport, rawdb.StageSyncExecution, rawdb.StageSyncCommitment} {
		if err := rawdb.WriteStageProgressWithHash(db, stage, block1.Number(), block1.Hash()); err != nil {
			t.Fatalf("write %s progress: %v", stage, err)
		}
	}
	forkHash := block1.Hash()
	forkHash[0] ^= 0xff
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncFinish, block1.Number(), forkHash); err != nil {
		t.Fatalf("write forked sync finish progress: %v", err)
	}

	applier := newStartupRecoveryTestApplier(db, block1.Number(), map[uint64]tcommon.Hash{
		block1.Number(): block1.Hash(),
	})
	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         block1.Number(),
		RestoreLimit: 0,
	}), applier)

	wantStatuses := []SyncStageProgressRepairStatus{
		SyncStageProgressKept,
		SyncStageProgressKept,
		SyncStageProgressKept,
		SyncStageProgressDeleted,
	}
	if len(result.SyncPipelineRepairs) != len(wantStatuses) {
		t.Fatalf("repairs = %+v, want %d entries", result.SyncPipelineRepairs, len(wantStatuses))
	}
	for i, status := range wantStatuses {
		if result.SyncPipelineRepairs[i].Status != status {
			t.Fatalf("repair %d = %+v, want status %v", i, result.SyncPipelineRepairs[i], status)
		}
	}
	if !result.HasSyncPipelineRepair ||
		result.SyncPipelineRepairResult.Kept != 3 ||
		result.SyncPipelineRepairResult.Deleted != 1 ||
		!result.SyncPipelineRepairResult.HasBlocked ||
		result.SyncPipelineRepairResult.FirstBlockedStage != rawdb.StageSyncFinish ||
		result.SyncPipelineRepairResult.Complete {
		t.Fatalf("sync pipeline repair result = %+v, want upstream kept and finish blocked", result.SyncPipelineRepairResult)
	}
	if !result.HasSyncPipelineHead || !result.SyncPipelineHeadCompletion.Complete ||
		result.SyncPipelineHeadCompletion.Written != 1 ||
		!reflect.DeepEqual(result.SyncPipelineHeadCompletion.Plan.FillStages, []rawdb.StageID{rawdb.StageSyncFinish}) {
		t.Fatalf("sync pipeline head completion = %+v set=%v, want finish filled", result.SyncPipelineHeadCompletion, result.HasSyncPipelineHead)
	}
	for _, stage := range SyncPipelineProgressStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block1 completed", stage, row, ok, err)
		}
	}
	if !result.HasSyncPipelineOrder || len(result.SyncPipelineOrderIssues) != 0 {
		t.Fatalf("sync pipeline order issues = %+v set=%v, want checked with no issues after finish repair", result.SyncPipelineOrderIssues, result.HasSyncPipelineOrder)
	}
	if cursor := result.SyncPipelineCursor; !result.HasSyncPipelineCursor ||
		cursor.StageRows != 4 ||
		!cursor.HasLast ||
		cursor.LastStage != rawdb.StageSyncFinish ||
		cursor.LastBlock != block1.Number() ||
		cursor.LastHash != block1.Hash() ||
		cursor.HasNext ||
		!cursor.Complete ||
		cursor.HasBlocked ||
		cursor.Interrupted {
		t.Fatalf("sync pipeline cursor = %+v set=%v, want completed current-head pipeline", cursor, result.HasSyncPipelineCursor)
	}
}

func TestApplySessionStartupPlanRestoresHalfDownloadedBodiesBeforeExecution(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	block4 := testBufferedBlock(4)
	for _, block := range []*types.Block{block1, block2, block4} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, block4.Number()); err != nil {
		t.Fatalf("write inventory target: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies ready progress: %v", err)
	}

	applier := newStartupRecoveryTestApplier(db, 0, nil)
	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         0,
		RestoreLimit: 10,
	}), applier)

	if len(result.SyncPipelineRepairs) != len(SyncPipelineProgressStages()) {
		t.Fatalf("repairs = %+v, want one per sync import stage", result.SyncPipelineRepairs)
	}
	for _, repair := range result.SyncPipelineRepairs {
		if repair.Status != SyncStageProgressMissing {
			t.Fatalf("repair = %+v, want missing stage row before execution", repair)
		}
	}
	if !result.HasSyncPipelineRepair || result.SyncPipelineRepairResult.Missing != len(SyncPipelineProgressStages()) ||
		!result.SyncPipelineRepairResult.HasBlocked ||
		result.SyncPipelineRepairResult.FirstBlockedStage != rawdb.StageSyncImport ||
		result.SyncPipelineRepairResult.Complete {
		t.Fatalf("sync pipeline repair result = %+v, want missing import boundary before execution", result.SyncPipelineRepairResult)
	}
	if !result.HasStagedBodyRestore {
		t.Fatal("startup result did not record staged body restore")
	}
	if !result.HasSyncPipelineOrder || len(result.SyncPipelineOrderIssues) != 0 {
		t.Fatalf("sync pipeline order issues = %+v set=%v, want checked with no issues after body restore", result.SyncPipelineOrderIssues, result.HasSyncPipelineOrder)
	}
	if restore := result.StagedBodyRestore; restore.Restored != 2 || !restore.NeedPruneTail || restore.PruneFrom != 3 ||
		restore.TargetHead != block4.Number() || restore.NextExpected != 3 || !restore.HaveLastRestored ||
		restore.LastRestoredNum != block2.Number() || restore.LastRestoredHash != block2.Hash() {
		t.Fatalf("restore result = %+v, want restored block1-2 and prune from gap block3", restore)
	}

	if applier.target != block4.Number() {
		t.Fatalf("restored target = %d, want inventory target block4", applier.target)
	}
	for _, block := range []*types.Block{block1, block2} {
		if row, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block.Number()); err != nil || !ok || row.Hash != block.Hash() {
			t.Fatalf("staged block%d = %+v ok=%v err=%v, want retained", block.Number(), row, ok, err)
		}
		if buffered, ok := applier.buffer[block.Number()]; !ok || buffered.Hash != block.Hash() {
			t.Fatalf("buffered block%d = %+v ok=%v, want restored for local import", block.Number(), buffered, ok)
		}
		if _, ok := applier.hashes[block.Hash()]; !ok {
			t.Fatalf("buffered hash for block%d missing", block.Number())
		}
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block4.Number()); err != nil || ok {
		t.Fatalf("staged block4 = ok:%v err:%v, want pruned after block3 gap", ok, err)
	}
	if _, ok := applier.buffer[block4.Number()]; ok {
		t.Fatal("buffer restored gapped block4, want only contiguous prefix")
	}
	bodies, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodies)
	if err != nil || !ok || bodies.BlockNum != block2.Number() || bodies.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodies progress = %+v ok=%v err=%v, want rewound to block2", bodies, ok, err)
	}
	ready, ok, err := rawdb.ReadStageProgressRow(db, rawdb.StageSyncBodiesReady)
	if err != nil || !ok || ready.BlockNum != block2.Number() || ready.BlockHash != block2.Hash() {
		t.Fatalf("SyncBodiesReady progress = %+v ok=%v err=%v, want block2 contiguous ready frontier", ready, ok, err)
	}
	for _, stage := range SyncPipelineProgressStages() {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want absent before execution", stage, row, ok, err)
		}
	}
}

func TestApplySessionStartupPlanRepairsBodiesProgressBehindReadyFrontier(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	for _, block := range []*types.Block{block1, block2} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, block2.Number()); err != nil {
		t.Fatalf("write inventory target: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block1.Number(), block1.Hash()); err != nil {
		t.Fatalf("write stale sync bodies progress: %v", err)
	}

	applier := newStartupRecoveryTestApplier(db, 0, nil)
	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         0,
		RestoreLimit: 2,
	}), applier)

	if !result.HasStagedBodyRestore || result.StagedBodyRestore.Restored != 2 || result.StagedBodyRestore.NeedPruneTail {
		t.Fatalf("staged body restore = %+v set=%v, want restored block1-2 without tail prune", result.StagedBodyRestore, result.HasStagedBodyRestore)
	}
	if !result.HasSyncPipelineOrderRepair || !result.SyncPipelineOrderRepair.Complete ||
		result.SyncPipelineOrderRepair.Updated != 1 || result.SyncPipelineOrderRepair.Deleted != 0 ||
		len(result.SyncPipelineOrderRepair.Repairs) != 1 ||
		result.SyncPipelineOrderRepair.Repairs[0].Stage != rawdb.StageSyncBodies ||
		!result.SyncPipelineOrderRepair.Repairs[0].Updated {
		t.Fatalf("order repair = %+v, want one SyncBodies update from ready frontier", result.SyncPipelineOrderRepair)
	}
	if !result.HasSyncPipelineOrder || len(result.SyncPipelineOrderIssues) != 0 || len(result.SyncPipelineOrderErrors) != 0 {
		t.Fatalf("order check issues=%+v errors=%+v set=%v, want clean post-repair check", result.SyncPipelineOrderIssues, result.SyncPipelineOrderErrors, result.HasSyncPipelineOrder)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block2.Number() || row.BlockHash != block2.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block2", stage, row, ok, err)
		}
	}
	for _, block := range []*types.Block{block1, block2} {
		if buffered, ok := applier.buffer[block.Number()]; !ok || buffered.Hash != block.Hash() {
			t.Fatalf("buffered block%d = %+v ok=%v, want restored", block.Number(), buffered, ok)
		}
	}
}

func TestApplySessionStartupPlanPrunesCorruptStagedBodyRaw(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block1 := testBufferedBlock(1)
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block4 := testBufferedBlock(4)
	raw3, err := block3.Marshal()
	if err != nil {
		t.Fatalf("marshal block3: %v", err)
	}
	if err := rawdb.WriteSyncStagedBlock(db, block1); err != nil {
		t.Fatalf("write staged block1: %v", err)
	}
	if err := rawdb.WriteSyncStagedBlockRaw(db, block2, raw3); err != nil {
		t.Fatalf("write corrupt staged block2 raw: %v", err)
	}
	if err := rawdb.WriteSyncStagedBlock(db, block4); err != nil {
		t.Fatalf("write staged block4: %v", err)
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, block4.Number()); err != nil {
		t.Fatalf("write inventory target: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies ready progress: %v", err)
	}

	applier := newStartupRecoveryTestApplier(db, 0, nil)
	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         0,
		RestoreLimit: 10,
	}), applier)

	if !result.HasStagedBodyRestore {
		t.Fatal("startup result did not record staged body restore")
	}
	if restore := result.StagedBodyRestore; restore.Restored != 1 || restore.ReadError == nil ||
		!restore.NeedPruneTail || restore.PruneFrom != block2.Number() ||
		restore.NextExpected != block2.Number() || !restore.HaveLastRestored ||
		restore.LastRestoredNum != block1.Number() || restore.LastRestoredHash != block1.Hash() {
		t.Fatalf("restore result = %+v, want restored block1 then prune corrupt block2 tail", restore)
	}
	if buffered, ok := applier.buffer[block1.Number()]; !ok || buffered.Hash != block1.Hash() {
		t.Fatalf("buffered block1 = %+v ok=%v, want restored", buffered, ok)
	}
	if _, ok := applier.buffer[block2.Number()]; ok {
		t.Fatal("corrupt block2 was restored into buffer")
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block2.Number()); err != nil || ok {
		t.Fatalf("staged corrupt block2 after startup ok=%v err=%v, want pruned", ok, err)
	}
	if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block4.Number()); err != nil || ok {
		t.Fatalf("staged block4 after startup ok=%v err=%v, want pruned with corrupt tail", ok, err)
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady} {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block1.Number() || row.BlockHash != block1.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want rewound to block1", stage, row, ok, err)
		}
	}
}

func TestApplySessionStartupPlanPrunesForkedStagedBodyPath(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	block2 := testBufferedBlock(2)
	block3 := testBufferedBlock(3)
	block4 := testBufferedBlock(4)
	for _, block := range []*types.Block{block3, block4} {
		if err := rawdb.WriteSyncStagedBlock(db, block); err != nil {
			t.Fatalf("write staged block %d: %v", block.Number(), err)
		}
	}
	if err := rawdb.WriteStageProgress(db, rawdb.StageSyncInventory, block4.Number()); err != nil {
		t.Fatalf("write inventory target: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodies, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies progress: %v", err)
	}
	if err := rawdb.WriteStageProgressWithHash(db, rawdb.StageSyncBodiesReady, block4.Number(), block4.Hash()); err != nil {
		t.Fatalf("write sync bodies ready progress: %v", err)
	}

	applier := newStartupRecoveryTestApplier(db, block2.Number(), map[uint64]tcommon.Hash{
		block2.Number(): block2.Hash(),
	})
	forkHash := block3.Hash()
	forkHash[0] ^= 0xff
	applier.path[block3.Number()] = forkHash

	result := ApplySessionStartupPlan(PlanSessionStartup(SessionStartupInput{
		Head:         block2.Number(),
		RestoreLimit: 10,
	}), applier)

	if !result.HasStagedBodyRestore {
		t.Fatal("startup result did not record staged body restore")
	}
	if restore := result.StagedBodyRestore; restore.Restored != 0 || !restore.NeedPruneTail ||
		restore.PruneFrom != block3.Number() || restore.NextExpected != block3.Number() ||
		restore.HaveLastRestored {
		t.Fatalf("restore result = %+v, want prune from forked block3 without restored prefix", restore)
	}
	if _, ok := applier.buffer[block3.Number()]; ok {
		t.Fatal("forked staged block3 was restored into buffer")
	}
	if _, ok := applier.hashes[block3.Hash()]; ok {
		t.Fatal("forked staged block3 hash was registered")
	}
	if got := applier.path[block3.Number()]; got != forkHash {
		t.Fatalf("reserved fork path changed to %x, want %x", got, forkHash)
	}
	for _, block := range []*types.Block{block3, block4} {
		if _, ok, err := rawdb.ReadSyncStagedBlockRaw(db, block.Number()); err != nil || ok {
			t.Fatalf("staged block%d = ok:%v err:%v, want pruned", block.Number(), ok, err)
		}
	}
	for _, stage := range []rawdb.StageID{rawdb.StageSyncBodies, rawdb.StageSyncBodiesReady} {
		if row, ok, err := rawdb.ReadStageProgressRow(db, stage); err != nil || ok {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want deleted after forked tail prune", stage, row, ok, err)
		}
	}
}

type recordedSessionStartupCall struct {
	action SessionStartupStepAction
	first  uint64
	limit  int
	prune  bool
}

type errUnexpectedRepairResult struct{}

func (errUnexpectedRepairResult) Error() string { return "unexpected repair result" }

type recordingSessionStartupApplier struct {
	calls          []recordedSessionStartupCall
	repair         SyncPipelineProgressRepairResult
	headCompletion SyncPipelineProgressHeadCompletion
	restore        StagedBodyRestoreResult
	orderRepair    SyncPipelineProgressOrderRepairResult
	orderIssues    []SyncPipelineProgressOrderIssue
}

func (a *recordingSessionStartupApplier) RepairSyncPipeline() SyncPipelineProgressRepairResult {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRepairSyncPipeline})
	return a.repair
}

func (a *recordingSessionStartupApplier) CompleteCurrentHeadSyncPipeline(repair SyncPipelineProgressRepairResult) SyncPipelineProgressHeadCompletion {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupCompleteHeadSyncPipeline})
	if !reflect.DeepEqual(repair, a.repair) {
		return SyncPipelineProgressHeadCompletion{WriteError: errUnexpectedRepairResult{}}
	}
	return a.headCompletion
}

func (a *recordingSessionStartupApplier) RestoreInventoryTarget(inventoryFloor uint64) {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRestoreInventoryTarget, first: inventoryFloor})
}

func (a *recordingSessionStartupApplier) DeleteImportedBodies(through uint64) {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupDeleteImportedBodies, first: through})
}

func (a *recordingSessionStartupApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) StagedBodyRestoreResult {
	a.calls = append(a.calls, recordedSessionStartupCall{
		action: SessionStartupRestoreStagedBodies,
		first:  from,
		limit:  limit,
		prune:  pruneStaleTail,
	})
	return a.restore
}

func (a *recordingSessionStartupApplier) RefreshBodiesReady() {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRefreshBodiesReady})
}

func (a *recordingSessionStartupApplier) RepairSyncPipelineProgressOrder() SyncPipelineProgressOrderRepairResult {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupRepairSyncPipelineOrder})
	return a.orderRepair
}

func (a *recordingSessionStartupApplier) CheckSyncPipelineProgressOrder() SyncPipelineProgressOrderCheckResult {
	a.calls = append(a.calls, recordedSessionStartupCall{action: SessionStartupCheckSyncPipelineOrder})
	return SyncPipelineProgressOrderCheckResult{
		Issues: append([]SyncPipelineProgressOrderIssue(nil), a.orderIssues...),
	}
}

type startupRecoveryTestApplier struct {
	db        ethdb.KeyValueStore
	head      uint64
	target    uint64
	canonical map[uint64]tcommon.Hash
	buffer    map[uint64]BufferedBlock
	hashes    map[tcommon.Hash]struct{}
	path      BlockPath
}

func newStartupRecoveryTestApplier(db ethdb.KeyValueStore, head uint64, canonical map[uint64]tcommon.Hash) *startupRecoveryTestApplier {
	return &startupRecoveryTestApplier{
		db:        db,
		head:      head,
		target:    head,
		canonical: canonical,
		buffer:    make(map[uint64]BufferedBlock),
		hashes:    make(map[tcommon.Hash]struct{}),
		path:      NewBlockPath(),
	}
}

func (a *startupRecoveryTestApplier) RepairSyncPipeline() SyncPipelineProgressRepairResult {
	return RepairSyncPipelineProgressWithResult(a.db, a.head, func(number uint64) (tcommon.Hash, bool) {
		hash, ok := a.canonical[number]
		return hash, ok
	})
}

func (a *startupRecoveryTestApplier) CompleteCurrentHeadSyncPipeline(repair SyncPipelineProgressRepairResult) SyncPipelineProgressHeadCompletion {
	var result SyncPipelineProgressHeadCompletion
	headHash, ok := a.canonical[a.head]
	if !ok {
		return result
	}
	plan := PlanSyncPipelineProgressHeadCompletion(repair, a.head, headHash)
	result.Plan = plan
	if !plan.HasHeadPrefix {
		return result
	}
	if len(plan.FillStages) == 0 {
		result.Complete = plan.Complete
		return result
	}
	for _, stage := range plan.FillStages {
		if err := rawdb.WriteStageProgressWithHash(a.db, stage, plan.Head, plan.HeadHash); err != nil {
			result.WriteError = err
			result.ErrorStage = stage
			return result
		}
		result.Written++
	}
	result.Complete = true
	return result
}

func (a *startupRecoveryTestApplier) RestoreInventoryTarget(inventoryFloor uint64) {
	a.target = rawdb.RestoreSyncInventoryTarget(a.db, inventoryFloor).Target
}

func (a *startupRecoveryTestApplier) DeleteImportedBodies(through uint64) {
	DeleteImportedStagedBodiesThrough(a.db, through, a.head+1, a.target)
}

func (a *startupRecoveryTestApplier) RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool) StagedBodyRestoreResult {
	result := RestoreStagedBodies(from, limit, a.target, a.buffer, a.hashes, &a.path, func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error {
		return rawdb.IterateSyncStagedBlocksFrom(a.db, start, fn)
	})
	ApplyStagedBodyRestoreSettlementPlan(PlanStagedBodyRestoreSettlement(result, pruneStaleTail), a)
	return result
}

func (a *startupRecoveryTestApplier) RefreshBodiesReady() {
	RefreshStagedBodyReadyProgress(a.db, a.head+1, a.target)
}

func (a *startupRecoveryTestApplier) RepairSyncPipelineProgressOrder() SyncPipelineProgressOrderRepairResult {
	return RepairSyncPipelineProgressOrderFromDB(a.db, SyncPipelineProgressOrderOptions{})
}

func (a *startupRecoveryTestApplier) CheckSyncPipelineProgressOrder() SyncPipelineProgressOrderCheckResult {
	return CheckSyncPipelineProgressOrderFromDB(a.db, SyncPipelineProgressOrderOptions{})
}

func (a *startupRecoveryTestApplier) SetStagedBodyRestoreTargetHead(targetHead uint64) {
	a.target = targetHead
}

func (a *startupRecoveryTestApplier) PruneStaleStagedBodyTail(from uint64, lastRestoredNum uint64, lastRestoredHash tcommon.Hash, haveLastRestored bool) {
	PruneStaleStagedBodyTail(a.db, from, lastRestoredNum, lastRestoredHash, haveLastRestored, a.head+1, a.target)
}
