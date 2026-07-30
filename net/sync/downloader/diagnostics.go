package downloader

import (
	"fmt"
	"sort"
	"strings"
)

// PeerDiagnostics is the per-peer state shown in sync import detail logs.
type PeerDiagnostics struct {
	ID             string
	Inflight       int
	FetchListLen   int
	PendingLen     int
	RemainNum      int64
	ChainRequested bool
	Done           bool
}

// Diagnostics is the lock-free sync-session snapshot consumed by import
// summary logging.
type Diagnostics struct {
	BlockBufferLen                         int
	RequestedLen                           int
	RetryListLen                           int
	ImportExecutionPlannedBlocks           int
	ImportExecutionPlannedStages           int
	ImportExecutionBodyStages              int
	ImportExecutionPostBodyStages          int
	ImportExecutionExecStages              int
	ImportExecutionCommitStages            int
	ImportExecutionFinishStages            int
	ImportExecutionFirstBlock              uint64
	ImportExecutionLastBlock               uint64
	ImportAppliedPlannedBlocks             int
	ImportAppliedPlannedStages             int
	ImportAppliedBodyStages                int
	ImportAppliedPostBodyStages            int
	ImportAppliedExecStages                int
	ImportAppliedCommitStages              int
	ImportAppliedFinishStages              int
	ImportAppliedFirstBlock                uint64
	ImportAppliedLastBlock                 uint64
	PeerState                              string
	ImportStageScheduled                   int
	ImportStageCompleted                   int
	ImportStageComplete                    bool
	ImportStageNext                        string
	ImportStageNextBlock                   uint64
	ImportStageNextCanonical               string
	ImportStageNextSync                    string
	ImportStageBlockedStatus               string
	ImportPhaseCursorComplete              bool
	ImportPhaseCursorCompleted             int
	ImportPhaseCursorScheduled             int
	ImportPhaseCursorTaskCompleted         int
	ImportPhaseCursorTaskScheduled         int
	ImportPhaseCursorCurrent               string
	ImportPhaseCursorCurrentCanonical      string
	ImportPhaseCursorCurrentSync           string
	ImportPhaseCursorCurrentTaskIndex      int
	ImportPhaseCursorCurrentTaskCount      int
	ImportPhaseCursorCurrentTaskRemaining  int
	ImportPhaseCursorCurrentFromBlock      uint64
	ImportPhaseCursorCurrentToBlock        uint64
	ImportPhaseCursorNextBlock             uint64
	ImportPhaseCursorNextPhase             string
	ImportPhaseCursorNextCanonical         string
	ImportPhaseCursorNextSync              string
	ImportPhaseCursorBlockedStatus         string
	ImportPhaseProgressScheduled           int
	ImportPhaseProgressCompleted           int
	ImportPhaseProgressBlocked             string
	ImportPhaseProgressNextBlock           uint64
	ImportPhaseProgressBlockedStatus       string
	ImportPhaseProgressBodiesBlock         uint64
	ImportPhaseProgressExecutionBlock      uint64
	ImportPhaseProgressCommitmentBlock     uint64
	ImportPhaseProgressFinishBlock         uint64
	ImportPhaseProgressBodiesCompleted     int
	ImportPhaseProgressExecutionCompleted  int
	ImportPhaseProgressCommitmentCompleted int
	ImportPhaseProgressFinishCompleted     int
}

// NewDiagnostics builds a deterministic diagnostics snapshot. Peer state is
// sorted by peer ID so log output and tests do not depend on map iteration.
func NewDiagnostics(blockBufferLen, requestedLen, retryListLen int, peers []PeerDiagnostics) Diagnostics {
	diag := Diagnostics{
		BlockBufferLen: blockBufferLen,
		RequestedLen:   requestedLen,
		RetryListLen:   retryListLen,
	}
	if len(peers) == 0 {
		return diag
	}
	peers = append([]PeerDiagnostics(nil), peers...)
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].ID < peers[j].ID
	})
	parts := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer.ID == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s{inflight=%d fetchList=%d pending=%d remain=%d chainRequested=%t done=%t}",
			peer.ID, peer.Inflight, peer.FetchListLen, peer.PendingLen, peer.RemainNum, peer.ChainRequested, peer.Done))
	}
	diag.PeerState = strings.Join(parts, ";")
	return diag
}

// WithImportBatchExecutionDiagnostics adds downloader-owned planned execution
// schedule diagnostics to the existing sync-session snapshot.
func (d Diagnostics) WithImportBatchExecutionDiagnostics(execution ImportBatchExecutionPlanDiagnostics) Diagnostics {
	d.ImportExecutionPlannedBlocks = execution.PlannedBlocks
	d.ImportExecutionPlannedStages = execution.PlannedStages
	d.ImportExecutionBodyStages = execution.PlannedBodyStages
	d.ImportExecutionPostBodyStages = execution.PlannedPostBodyStages
	d.ImportExecutionExecStages = execution.PlannedExecutionStages
	d.ImportExecutionCommitStages = execution.PlannedCommitmentStages
	d.ImportExecutionFinishStages = execution.PlannedFinishStages
	d.ImportExecutionFirstBlock = execution.FirstBlockNum
	d.ImportExecutionLastBlock = execution.LastBlockNum
	return d
}

// WithImportAppliedStageDiagnostics adds the execution/commitment/finish
// schedule for the prefix that canonical import actually accepted.
func (d Diagnostics) WithImportAppliedStageDiagnostics(applied ImportBatchExecutionPlanDiagnostics) Diagnostics {
	d.ImportAppliedPlannedBlocks = applied.PlannedBlocks
	d.ImportAppliedPlannedStages = applied.PlannedStages
	d.ImportAppliedBodyStages = applied.PlannedBodyStages
	d.ImportAppliedPostBodyStages = applied.PlannedPostBodyStages
	d.ImportAppliedExecStages = applied.PlannedExecutionStages
	d.ImportAppliedCommitStages = applied.PlannedCommitmentStages
	d.ImportAppliedFinishStages = applied.PlannedFinishStages
	d.ImportAppliedFirstBlock = applied.FirstBlockNum
	d.ImportAppliedLastBlock = applied.LastBlockNum
	return d
}

// WithImportedBatchProgressPlan adds every downloader-owned import progress
// diagnostic carried by a planned staged-body import settlement.
func (d Diagnostics) WithImportedBatchProgressPlan(plan ImportedBatchProgressPlan) Diagnostics {
	if !plan.OK {
		return d
	}
	return d.
		WithImportBatchExecutionDiagnostics(plan.ExecutionDiagnostics).
		WithImportAppliedStageDiagnostics(plan.AppliedDiagnostics).
		WithImportStageDiagnostics(plan.StageDiagnostics).
		WithImportStagePhaseProgress(plan.StagePlan.Phases).
		WithImportStagePhaseCursor(plan.StagePhaseCursor)
}

// AppendImportPlanLogFields appends the stable log fields for downloader-owned
// import execution and stage-plan diagnostics.
func (d Diagnostics) AppendImportPlanLogFields(fields []any) []any {
	if d.HasImportStagePlan() {
		fields = append(fields,
			"syncStageComplete", d.ImportStageComplete,
			"syncStageCompleted", d.ImportStageCompleted,
			"syncStageScheduled", d.ImportStageScheduled,
		)
		if d.ImportStageNext != "" {
			fields = append(fields,
				"syncStageNext", d.ImportStageNext,
				"syncStageNextBlock", d.ImportStageNextBlock,
				"syncStageNextCanonical", d.ImportStageNextCanonical,
				"syncStageNextSync", d.ImportStageNextSync,
				"syncStageBlockedStatus", d.ImportStageBlockedStatus,
			)
		}
	}
	if d.HasImportStagePhaseCursor() {
		fields = append(fields,
			"syncPhaseCursorComplete", d.ImportPhaseCursorComplete,
			"syncPhaseCursorCompletedPhases", d.ImportPhaseCursorCompleted,
			"syncPhaseCursorScheduledPhases", d.ImportPhaseCursorScheduled,
			"syncPhaseCursorCompletedTasks", d.ImportPhaseCursorTaskCompleted,
			"syncPhaseCursorScheduledTasks", d.ImportPhaseCursorTaskScheduled,
		)
		if d.ImportPhaseCursorCurrent != "" {
			fields = append(fields,
				"syncPhaseCursorCurrent", d.ImportPhaseCursorCurrent,
				"syncPhaseCursorCurrentCanonical", d.ImportPhaseCursorCurrentCanonical,
				"syncPhaseCursorCurrentSync", d.ImportPhaseCursorCurrentSync,
				"syncPhaseCursorCurrentTaskIndex", d.ImportPhaseCursorCurrentTaskIndex,
				"syncPhaseCursorCurrentTaskCount", d.ImportPhaseCursorCurrentTaskCount,
				"syncPhaseCursorCurrentTaskRemaining", d.ImportPhaseCursorCurrentTaskRemaining,
				"syncPhaseCursorCurrentFromBlock", d.ImportPhaseCursorCurrentFromBlock,
				"syncPhaseCursorCurrentToBlock", d.ImportPhaseCursorCurrentToBlock,
			)
		}
		if d.ImportPhaseCursorNextBlock != 0 || d.ImportPhaseCursorNextPhase != "" || d.ImportPhaseCursorBlockedStatus != "" {
			fields = append(fields,
				"syncPhaseCursorNextBlock", d.ImportPhaseCursorNextBlock,
				"syncPhaseCursorNextPhase", d.ImportPhaseCursorNextPhase,
				"syncPhaseCursorNextCanonical", d.ImportPhaseCursorNextCanonical,
				"syncPhaseCursorNextSync", d.ImportPhaseCursorNextSync,
				"syncPhaseCursorBlockedStatus", d.ImportPhaseCursorBlockedStatus,
			)
		}
	}
	if d.HasImportStagePhaseProgress() {
		fields = append(fields,
			"syncPhaseProgressCompletedPhases", d.ImportPhaseProgressCompleted,
			"syncPhaseProgressScheduledPhases", d.ImportPhaseProgressScheduled,
			"syncPhaseProgressBodiesCompletedTasks", d.ImportPhaseProgressBodiesCompleted,
			"syncPhaseProgressExecutionCompletedTasks", d.ImportPhaseProgressExecutionCompleted,
			"syncPhaseProgressCommitmentCompletedTasks", d.ImportPhaseProgressCommitmentCompleted,
			"syncPhaseProgressFinishCompletedTasks", d.ImportPhaseProgressFinishCompleted,
		)
		if d.ImportPhaseProgressBodiesBlock != 0 {
			fields = append(fields, "syncPhaseProgressBodiesBlock", d.ImportPhaseProgressBodiesBlock)
		}
		if d.ImportPhaseProgressExecutionBlock != 0 {
			fields = append(fields, "syncPhaseProgressExecutionBlock", d.ImportPhaseProgressExecutionBlock)
		}
		if d.ImportPhaseProgressCommitmentBlock != 0 {
			fields = append(fields, "syncPhaseProgressCommitmentBlock", d.ImportPhaseProgressCommitmentBlock)
		}
		if d.ImportPhaseProgressFinishBlock != 0 {
			fields = append(fields, "syncPhaseProgressFinishBlock", d.ImportPhaseProgressFinishBlock)
		}
		if d.ImportPhaseProgressBlocked != "" {
			fields = append(fields,
				"syncPhaseProgressBlockedPhase", d.ImportPhaseProgressBlocked,
				"syncPhaseProgressNextBlock", d.ImportPhaseProgressNextBlock,
				"syncPhaseProgressBlockedStatus", d.ImportPhaseProgressBlockedStatus,
			)
		}
	}
	if d.HasImportBatchExecutionPlan() {
		fields = append(fields,
			"syncExecPlanBlocks", d.ImportExecutionPlannedBlocks,
			"syncExecPlanStages", d.ImportExecutionPlannedStages,
			"syncExecPlanBodyStages", d.ImportExecutionBodyStages,
			"syncExecPlanPostBodyStages", d.ImportExecutionPostBodyStages,
			"syncExecPlanExecutionStages", d.ImportExecutionExecStages,
			"syncExecPlanCommitmentStages", d.ImportExecutionCommitStages,
			"syncExecPlanFinishStages", d.ImportExecutionFinishStages,
			"syncExecPlanFirst", d.ImportExecutionFirstBlock,
			"syncExecPlanLast", d.ImportExecutionLastBlock,
		)
	}
	if d.HasImportAppliedStagePlan() {
		fields = append(fields,
			"syncAppliedPlanBlocks", d.ImportAppliedPlannedBlocks,
			"syncAppliedPlanStages", d.ImportAppliedPlannedStages,
			"syncAppliedPlanBodyStages", d.ImportAppliedBodyStages,
			"syncAppliedPlanPostBodyStages", d.ImportAppliedPostBodyStages,
			"syncAppliedPlanExecutionStages", d.ImportAppliedExecStages,
			"syncAppliedPlanCommitmentStages", d.ImportAppliedCommitStages,
			"syncAppliedPlanFinishStages", d.ImportAppliedFinishStages,
			"syncAppliedPlanFirst", d.ImportAppliedFirstBlock,
			"syncAppliedPlanLast", d.ImportAppliedLastBlock,
		)
	}
	return fields
}

// WithImportStagePlan adds downloader-owned import stage planner diagnostics to
// the existing sync-session snapshot.
func (d Diagnostics) WithImportStagePlan(plan ImportStagePlan) Diagnostics {
	return d.WithImportStageDiagnostics(plan.Diagnostics())
}

// WithImportStageDiagnostics adds downloader-owned import stage planner
// diagnostics to the existing sync-session snapshot.
func (d Diagnostics) WithImportStageDiagnostics(stage ImportStagePlanDiagnostics) Diagnostics {
	d.ImportStageScheduled = stage.Scheduled
	d.ImportStageCompleted = stage.Completed
	d.ImportStageComplete = stage.Complete
	if stage.HasBlocked {
		if stage.NextPhase != "" {
			d.ImportStageNext = string(stage.NextPhase)
		} else if stage.NextStage != "" {
			d.ImportStageNext = string(stage.NextStage)
		}
		d.ImportStageNextBlock = stage.NextBlockNum
		d.ImportStageNextCanonical = string(stage.NextCanonicalStage)
		d.ImportStageNextSync = string(stage.NextStage)
		d.ImportStageBlockedStatus = stage.BlockedStatus.String()
	}
	return d
}

// WithImportStagePhaseProgress adds the explicit per-phase stage planner
// result to the existing sync-session snapshot.
func (d Diagnostics) WithImportStagePhaseProgress(phases []ImportStagePhaseProgress) Diagnostics {
	d.ImportPhaseProgressScheduled = len(phases)
	for _, phase := range phases {
		if phase.Complete {
			d.ImportPhaseProgressCompleted++
		}
		if phase.HasBlocked && d.ImportPhaseProgressBlocked == "" {
			d.ImportPhaseProgressBlocked = string(phase.Phase)
			d.ImportPhaseProgressNextBlock = phase.Next.BlockNum
			d.ImportPhaseProgressBlockedStatus = phase.Blocked.Status.String()
		}
		block := uint64(0)
		if phase.HasProgress {
			block = phase.Progress.BlockNum
		}
		completed := len(phase.Completed)
		switch phase.Phase {
		case ImportStagePhaseBodies:
			d.ImportPhaseProgressBodiesBlock = block
			d.ImportPhaseProgressBodiesCompleted = completed
		case ImportStagePhaseExecution:
			d.ImportPhaseProgressExecutionBlock = block
			d.ImportPhaseProgressExecutionCompleted = completed
		case ImportStagePhaseCommitment:
			d.ImportPhaseProgressCommitmentBlock = block
			d.ImportPhaseProgressCommitmentCompleted = completed
		case ImportStagePhaseFinish:
			d.ImportPhaseProgressFinishBlock = block
			d.ImportPhaseProgressFinishCompleted = completed
		}
	}
	return d
}

// WithImportStagePhaseCursor adds the phase-level staged import cursor to the
// existing sync-session snapshot.
func (d Diagnostics) WithImportStagePhaseCursor(cursor ImportStagePhaseCursor) Diagnostics {
	d.ImportPhaseCursorComplete = cursor.Complete
	d.ImportPhaseCursorCompleted = cursor.CompletedPhases
	d.ImportPhaseCursorScheduled = cursor.ScheduledPhases
	d.ImportPhaseCursorTaskCompleted = cursor.CompletedTasks
	d.ImportPhaseCursorTaskScheduled = cursor.ScheduledTasks
	if cursor.HasCurrent {
		d.ImportPhaseCursorCurrent = string(cursor.CurrentPhase)
		d.ImportPhaseCursorCurrentCanonical = string(cursor.CurrentCanonicalStage)
		d.ImportPhaseCursorCurrentSync = string(cursor.CurrentSyncStage)
		d.ImportPhaseCursorCurrentTaskIndex = cursor.CurrentTaskIndex
		d.ImportPhaseCursorCurrentTaskCount = len(cursor.CurrentTasks)
		d.ImportPhaseCursorCurrentTaskRemaining = cursor.CurrentTaskRemaining()
		if remaining, ok := cursor.RemainingCurrentPhasePlan(); ok && len(remaining.Tasks) > 0 {
			d.ImportPhaseCursorCurrentFromBlock = remaining.Tasks[0].BlockNum
			d.ImportPhaseCursorCurrentToBlock = remaining.Tasks[len(remaining.Tasks)-1].BlockNum
		}
	}
	if cursor.HasNextTask {
		d.ImportPhaseCursorNextBlock = cursor.NextTask.BlockNum
		d.ImportPhaseCursorNextPhase = string(cursor.NextTask.Phase)
		d.ImportPhaseCursorNextCanonical = string(cursor.NextTask.CanonicalStage)
		d.ImportPhaseCursorNextSync = string(cursor.NextTask.SyncStage)
	}
	if cursor.HasBlocked {
		d.ImportPhaseCursorBlockedStatus = cursor.BlockedStatus.String()
	}
	return d
}

// HasImportStagePlan reports whether Diagnostics carries import stage planner
// state for the current imported batch.
func (d Diagnostics) HasImportStagePlan() bool {
	return d.ImportStageScheduled > 0
}

// HasImportStagePhaseCursor reports whether Diagnostics carries phase-level
// import cursor state for the current imported batch.
func (d Diagnostics) HasImportStagePhaseCursor() bool {
	return d.ImportPhaseCursorScheduled > 0
}

// HasImportStagePhaseProgress reports whether Diagnostics carries per-phase
// import-stage planner progress for the current imported batch.
func (d Diagnostics) HasImportStagePhaseProgress() bool {
	return d.ImportPhaseProgressScheduled > 0
}

// HasImportBatchExecutionPlan reports whether Diagnostics carries the planned
// local execution/commitment/finish schedule for the current imported batch.
func (d Diagnostics) HasImportBatchExecutionPlan() bool {
	return d.ImportExecutionPlannedStages > 0
}

// HasImportAppliedStagePlan reports whether Diagnostics carries the stage plan
// for the canonical prefix accepted from the current import batch.
func (d Diagnostics) HasImportAppliedStagePlan() bool {
	return d.ImportAppliedPlannedStages > 0
}
