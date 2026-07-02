#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "nile_sync_acceptance.py"

REQUIRED_SYNC_STAGES = [
    "SyncInventory",
    "SyncBodies",
    "SyncBodiesReady",
    "SyncImport",
    "SyncExecution",
    "SyncCommitment",
    "SyncFinish",
]

SYNC_STAGE_FIELDS = {
    "SyncInventory": "stageSyncInventory",
    "SyncBodies": "stageSyncBodies",
    "SyncBodiesReady": "stageSyncBodiesReady",
    "SyncImport": "stageSyncImport",
    "SyncExecution": "stageSyncExecution",
    "SyncCommitment": "stageSyncCommitment",
    "SyncFinish": "stageSyncFinish",
}


def write_result(path, rows):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


SNAPSHOT_POINT_PREFIXES = (
    "snapshotPointTxHashLookup",
    "snapshotPointEventLogIndex",
    "snapshotPointStateHistoryAccessor",
    "snapshotPointLatestBTree",
    "snapshotPointChainFreezerAccessor",
    "snapshotPointCodeDomain",
    "snapshotPointCommitmentSnapshot",
)


SNAPSHOT_POINT_METRIC_PREFIXES = {
    "snapshotPointTxHashLookup": "gtron_nile_sync_snapshot_point_tx_hash_lookup",
    "snapshotPointEventLogIndex": "gtron_nile_sync_snapshot_point_event_log_index",
    "snapshotPointStateHistoryAccessor": "gtron_nile_sync_snapshot_point_state_history_accessor",
    "snapshotPointLatestBTree": "gtron_nile_sync_snapshot_point_latest_btree",
    "snapshotPointChainFreezerAccessor": "gtron_nile_sync_snapshot_point_chain_freezer_accessor",
    "snapshotPointCodeDomain": "gtron_nile_sync_snapshot_point_code_domain",
    "snapshotPointCommitmentSnapshot": "gtron_nile_sync_snapshot_point_commitment_snapshot",
}


SNAPSHOT_POINT_FIELD_SUFFIXES = (
    ("Segments", "segments"),
    ("Bytes", "bytes"),
    ("PayloadBytes", "payload_bytes"),
    ("SidecarBytes", "sidecar_bytes"),
    ("SidecarShareMilli", "sidecar_share_milli"),
    ("SnapshotShareMilli", "snapshot_share_milli"),
)

STATE_PREFETCH_METRIC_FIELDS = (
    ("syncLogStatePrefetchEnqueued", "gtron_nile_sync_log_state_prefetch_enqueued"),
    ("syncLogStatePrefetchDropped", "gtron_nile_sync_log_state_prefetch_dropped"),
    ("syncLogStatePrefetchProcessed", "gtron_nile_sync_log_state_prefetch_processed"),
    ("syncLogStatePrefetchHits", "gtron_nile_sync_log_state_prefetch_hits"),
    ("syncLogStatePrefetchMisses", "gtron_nile_sync_log_state_prefetch_misses"),
    ("syncLogStatePrefetchErrors", "gtron_nile_sync_log_state_prefetch_errors"),
)

SYNC_PHASE_CURSOR_METRIC_FIELDS = (
    (
        "syncLogPhaseCursorCompletedPhases",
        "gtron_nile_sync_log_phase_cursor_completed_phases",
    ),
    (
        "syncLogPhaseCursorScheduledPhases",
        "gtron_nile_sync_log_phase_cursor_scheduled_phases",
    ),
    (
        "syncLogPhaseCursorIncompletePhases",
        "gtron_nile_sync_log_phase_cursor_incomplete_phases",
    ),
    (
        "syncLogPhaseCursorCompletedTasks",
        "gtron_nile_sync_log_phase_cursor_completed_tasks",
    ),
    (
        "syncLogPhaseCursorScheduledTasks",
        "gtron_nile_sync_log_phase_cursor_scheduled_tasks",
    ),
    (
        "syncLogPhaseCursorCurrentTaskIndex",
        "gtron_nile_sync_log_phase_cursor_current_task_index",
    ),
    (
        "syncLogPhaseCursorCurrentTaskCount",
        "gtron_nile_sync_log_phase_cursor_current_task_count",
    ),
    (
        "syncLogPhaseCursorCurrentTaskRemaining",
        "gtron_nile_sync_log_phase_cursor_current_task_remaining",
    ),
    (
        "syncLogPhaseCursorCurrentFromBlock",
        "gtron_nile_sync_log_phase_cursor_current_from_block",
    ),
    (
        "syncLogPhaseCursorCurrentToBlock",
        "gtron_nile_sync_log_phase_cursor_current_to_block",
    ),
    (
        "syncLogPhaseCursorNextBlock",
        "gtron_nile_sync_log_phase_cursor_next_block",
    ),
)

EVENT_LOG_INDEX_METRIC_FIELDS = (
    ("eventLogIndexSegments", "gtron_nile_sync_event_log_index_segments"),
    ("eventLogIndexFromBlock", "gtron_nile_sync_event_log_index_from_block"),
    ("eventLogIndexToBlock", "gtron_nile_sync_event_log_index_to_block"),
    ("eventLogIndexAddressKeys", "gtron_nile_sync_event_log_index_address_keys"),
    ("eventLogIndexAddressPostings", "gtron_nile_sync_event_log_index_address_postings"),
    (
        "eventLogIndexAddressAvgPostingsMilli",
        "gtron_nile_sync_event_log_index_address_avg_postings_milli",
    ),
    ("eventLogIndexAddressMaxPostings", "gtron_nile_sync_event_log_index_address_max_postings"),
    (
        "eventLogIndexAddressSingletonKeys",
        "gtron_nile_sync_event_log_index_address_singleton_keys",
    ),
    (
        "eventLogIndexAddressMultiPostingKeys",
        "gtron_nile_sync_event_log_index_address_multi_posting_keys",
    ),
    ("eventLogIndexTopicKeys", "gtron_nile_sync_event_log_index_topic_keys"),
    ("eventLogIndexTopicPostings", "gtron_nile_sync_event_log_index_topic_postings"),
    (
        "eventLogIndexTopicAvgPostingsMilli",
        "gtron_nile_sync_event_log_index_topic_avg_postings_milli",
    ),
    ("eventLogIndexTopicMaxPostings", "gtron_nile_sync_event_log_index_topic_max_postings"),
    (
        "eventLogIndexTopicSingletonKeys",
        "gtron_nile_sync_event_log_index_topic_singleton_keys",
    ),
    (
        "eventLogIndexTopicMultiPostingKeys",
        "gtron_nile_sync_event_log_index_topic_multi_posting_keys",
    ),
)


def snapshot_point_fields(prefix, segments, total, payload, sidecar, sidecar_share, snapshot_share):
    values = (segments, total, payload, sidecar, sidecar_share, snapshot_share)
    return {
        f"{prefix}{field_suffix}": value
        for (field_suffix, _), value in zip(SNAPSHOT_POINT_FIELD_SUFFIXES, values)
    }


def snapshot_point_profile_evidence():
    evidence = {}
    evidence.update(snapshot_point_fields("snapshotPointTxHashLookup", 1, 100, 0, 100, 1000, 63))
    evidence.update(snapshot_point_fields("snapshotPointEventLogIndex", 1, 200, 0, 200, 1000, 125))
    for prefix in SNAPSHOT_POINT_PREFIXES[2:]:
        evidence.update(snapshot_point_fields(prefix, 0, 0, 0, 0, 0, 0))
    return evidence


def snapshot_point_prometheus_lines(row, labels):
    lines = []
    for prefix, metric_prefix in SNAPSHOT_POINT_METRIC_PREFIXES.items():
        for field_suffix, metric_suffix in SNAPSHOT_POINT_FIELD_SUFFIXES:
            field = f"{prefix}{field_suffix}"
            if field not in row:
                continue
            metric = f"{metric_prefix}_{metric_suffix}"
            lines.append(f"# TYPE {metric} gauge")
            lines.append(f"{metric}{{{labels}}} {row[field]}")
    return lines


def state_prefetch_prometheus_lines(row, labels):
    lines = []
    for field, metric in STATE_PREFETCH_METRIC_FIELDS:
        if field not in row or row[field] < 0:
            continue
        lines.append(f"# TYPE {metric} gauge")
        lines.append(f"{metric}{{{labels}}} {row[field]}")
    return lines


def sync_phase_cursor_prometheus_lines(row, labels):
    lines = []
    for field, metric in SYNC_PHASE_CURSOR_METRIC_FIELDS:
        if field not in row or row[field] < 0:
            continue
        lines.append(f"# TYPE {metric} gauge")
        lines.append(f"{metric}{{{labels}}} {row[field]}")
    return lines


def event_log_index_prometheus_lines(row, labels):
    lines = []
    for field, metric in EVENT_LOG_INDEX_METRIC_FIELDS:
        if field not in row or row[field] < 0:
            continue
        lines.append(f"# TYPE {metric} gauge")
        lines.append(f"{metric}{{{labels}}} {row[field]}")
    return lines


def full_stage_details(blocks=None, verified=None):
    blocks = blocks or {}
    verified = verified or {}
    return [
        {
            "stage": stage,
            "field": SYNC_STAGE_FIELDS[stage],
            "present": True,
            "block": blocks.get(stage, 1000),
            "verified": verified.get(stage, "unbound" if stage == "SyncInventory" else "canonical"),
        }
        for stage in REQUIRED_SYNC_STAGES
    ]


def clean_full_staged_sync_row():
    row = {
        "unix": 10,
        "network": "nile",
        "mode": "full",
        "sampleStatus": "ok",
        "soakHealthStatus": "ok",
        "stageStatusFileStatus": "ok",
        "fullStagedSyncStatus": "caught-up",
        "fullStagedSyncReady": True,
        "fullStagedSyncCompleteAtHead": True,
        "stageSyncPipelineMonotonic": True,
        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
        "fullStagedSyncMissingStages": [],
        "fullStagedSyncHashIssues": [],
        "fullStagedSyncUnverifiedStages": [],
        "fullStagedSyncStageCoverageRatio": 1.0,
        "fullStagedSyncVerificationRatio": 1.0,
        "fullStagedSyncCompleteBlock": 1000,
        "fullStagedSyncHeadBlock": 1000,
        "fullStagedSyncHeadLagBlocks": 0,
        "fullStagedSyncCompletionRatio": 1.0,
        "fullStagedSyncPipelineLagBlocks": 0,
        "fullStagedSyncBottleneck": "none",
        "fullStagedSyncBottleneckLagBlocks": 0,
        "fullStagedSyncBottleneckLagShare": -1.0,
        "stageSyncPipelineLagBlocks": 0,
        "stageSyncBottleneck": "none",
        "stageSyncBottleneckLagBlocks": 0,
        "stageSyncInventoryBodiesGapBlocks": 0,
        "stageSyncBodiesReadyGapBlocks": 0,
        "stageSyncImportExecutionLagBlocks": 0,
        "stageSyncExecutionCommitmentLagBlocks": 0,
        "stageSyncCommitmentFinishLagBlocks": 0,
        "stageSyncFinishHeadLagBlocks": 0,
        "heightRegressionBlocks": 0,
        "stageProgressRegressionCount": 0,
        "stageMismatchRows": 0,
        "stageMissingCanonicalRows": 0,
        "stageStagedBodyIssueRows": 0,
        "stageIssueRows": 0,
        "stageOrderIssueRows": 0,
        "stageSyncPipelineViolationCount": 0,
        "height": 1000,
    }
    for field in SYNC_STAGE_FIELDS.values():
        row[field] = 1000
    return row


def add_clean_storage_alerts(row):
    row.update(
        {
            "storageAlertStatus": "ok",
            "freezerAlertStatus": "ok",
            "freezerAlertIssues": 0,
            "stageVerifyStatus": "ok",
            "stageVerifyIssues": 0,
            "stageAlertPipelineIssues": 0,
            "modeAlertStatus": "ok",
            "modeAlertIssues": 0,
            "snapshotAlertStatus": "ok",
            "snapshotAlertIssues": 0,
        }
    )
    return row


def add_clean_prune_mode(row, mode=None):
    prune_mode = mode or row.get("mode", "full")
    row.update(
        {
            "mode": prune_mode,
            "pruneMode": prune_mode,
            "pruneModePersisted": True,
            "signedColdPrune": 0,
            "coldFreezerToBlock": -1,
            "chainLookupPruneToBlock": -1,
            "tailPrunedThroughBlock": -1,
            "tailPrunedFiles": 0,
        }
    )
    return row


def add_clean_startup_recovery(row):
    row.update(
        {
            "syncStartupRepairStatus": "ok",
            "syncStartupRepairSummaries": 1,
            "syncStartupRepairComplete": True,
            "syncStartupRepairHasBlocked": False,
            "syncStartupRepairFirstBlocked": "",
            "syncStartupRepairInterrupted": False,
            "syncStartupRepairErrorStage": "",
            "syncStartupHeadCompletionChecked": True,
            "syncStartupHeadCompletionComplete": True,
            "syncStartupHeadCompletionErrorStage": "",
            "syncStartupPipelineOrderChecked": True,
            "syncStartupPipelineOrderIssues": 0,
            "syncStartupPipelineOrderReadErrors": 0,
            "syncStartupPipelineOrderRepairChecked": True,
            "syncStartupPipelineOrderRepairComplete": True,
            "syncStartupPipelineOrderRepairInterrupted": False,
            "syncStartupPipelineOrderRepairErrorStage": "",
            "syncStartupPipelineCursorChecked": True,
            "syncStartupPipelineCursorComplete": True,
            "syncStartupPipelineCursorBlocked": False,
            "syncStartupPipelineCursorInterrupted": False,
            "syncStartupPipelineCursorErrorStage": "",
        }
    )
    return row


def add_snapshot_profile_evidence(row):
    row.update(
        {
            "snapshotManifestProfileStatus": "ok",
            "snapshotProfileSegments": 4,
            "snapshotProfileVerifyFiles": True,
            "snapshotProfileVerifiedSegments": 4,
            "snapshotProfileTotalBytes": 1600,
            "snapshotPayloadBytes": 1300,
            "snapshotSidecarBytes": 300,
            "snapshotSidecarShareMilli": 188,
            "snapshotLatestSidecarBytes": 0,
            "snapshotLatestSidecarShareMilli": -1,
            "snapshotStateHistorySidecarBytes": 0,
            "snapshotStateHistorySidecarShareMilli": -1,
            "snapshotChainFreezerSidecarBytes": 100,
            "snapshotChainFreezerSidecarShareMilli": 91,
            "snapshotEventLogSidecarBytes": 200,
            "snapshotEventLogSidecarShareMilli": 400,
            "snapshotBalanceTraceSidecarBytes": 0,
            "snapshotBalanceTraceSidecarShareMilli": -1,
            "snapshotSectionBloomSidecarBytes": 0,
            "snapshotSectionBloomSidecarShareMilli": -1,
        }
    )
    row.update(snapshot_point_profile_evidence())
    return row


def sample_prometheus_labels(extra=None):
    labels = {
        "datadir": "/tmp/nile",
        "label": "",
        "mode": "full",
        "network": "nile",
    }
    if extra:
        labels.update(extra)
    return ",".join(f'{key}="{labels[key]}"' for key in sorted(labels))


def stage_detail_verified(stage, verification):
    if stage == "SyncInventory":
        return verification == "unbound"
    if stage in {"SyncBodies", "SyncBodiesReady"}:
        return verification in {"staged", "canonical"}
    return verification == "canonical"


def add_state_prefetch_evidence(
    row,
    *,
    enqueued=12,
    dropped=1,
    processed=11,
    hits=8,
    misses=2,
    errors=1,
):
    row.update(
        {
            "syncLogStatus": "ok",
            "syncLogStatePrefetchEnqueued": enqueued,
            "syncLogStatePrefetchDropped": dropped,
            "syncLogStatePrefetchProcessed": processed,
            "syncLogStatePrefetchHits": hits,
            "syncLogStatePrefetchMisses": misses,
            "syncLogStatePrefetchErrors": errors,
        }
    )
    return row


def add_sync_phase_cursor_evidence(row, **overrides):
    values = {
        "syncLogStatus": "ok",
        "syncLogPhaseCursorComplete": False,
        "syncLogPhaseCursorCompletedPhases": 2,
        "syncLogPhaseCursorScheduledPhases": 4,
        "syncLogPhaseCursorIncompletePhases": 2,
        "syncLogPhaseCursorCompletedTasks": 59,
        "syncLogPhaseCursorScheduledTasks": 80,
        "syncLogPhaseCursorCurrent": "commitment",
        "syncLogPhaseCursorCurrentCanonical": "Commitment",
        "syncLogPhaseCursorCurrentSync": "SyncCommitment",
        "syncLogPhaseCursorCurrentTaskIndex": 19,
        "syncLogPhaseCursorCurrentTaskCount": 20,
        "syncLogPhaseCursorCurrentTaskRemaining": 1,
        "syncLogPhaseCursorCurrentFromBlock": 100,
        "syncLogPhaseCursorCurrentToBlock": 100,
        "syncLogPhaseCursorNextBlock": 100,
        "syncLogPhaseCursorNextPhase": "commitment",
        "syncLogPhaseCursorNextCanonical": "Commitment",
        "syncLogPhaseCursorNextSync": "SyncCommitment",
        "syncLogPhaseCursorBlockedStatus": "missing",
    }
    values.update(overrides)
    if "syncLogPhaseCursorCompletionRatio" not in overrides:
        scheduled = values["syncLogPhaseCursorScheduledPhases"]
        values["syncLogPhaseCursorCompletionRatio"] = (
            values["syncLogPhaseCursorCompletedPhases"] / scheduled if scheduled else -1.0
        )
    if "syncLogPhaseCursorTaskCompletionRatio" not in overrides:
        scheduled = values["syncLogPhaseCursorScheduledTasks"]
        values["syncLogPhaseCursorTaskCompletionRatio"] = (
            values["syncLogPhaseCursorCompletedTasks"] / scheduled if scheduled else -1.0
        )
    row.update(values)
    return row


def add_event_log_index_evidence(
    row,
    *,
    segments=2,
    from_block=1,
    to_block=1000,
    address_keys=3,
    address_postings=6,
    address_max=3,
    address_singleton=1,
    address_multi=2,
    topic_keys=2,
    topic_postings=3,
    topic_max=2,
    topic_singleton=1,
    topic_multi=1,
):
    row.update(
        {
            "eventLogIndexStatsStatus": "ok",
            "eventLogIndexSegments": segments,
            "eventLogIndexFromBlock": from_block,
            "eventLogIndexToBlock": to_block,
            "eventLogIndexAddressKeys": address_keys,
            "eventLogIndexAddressPostings": address_postings,
            "eventLogIndexAddressAvgPostingsMilli": (
                (address_postings * 1000 + address_keys // 2) // address_keys
                if address_keys
                else 0
            ),
            "eventLogIndexAddressMaxPostings": address_max,
            "eventLogIndexAddressSingletonKeys": address_singleton,
            "eventLogIndexAddressMultiPostingKeys": address_multi,
            "eventLogIndexTopicKeys": topic_keys,
            "eventLogIndexTopicPostings": topic_postings,
            "eventLogIndexTopicAvgPostingsMilli": (
                (topic_postings * 1000 + topic_keys // 2) // topic_keys
                if topic_keys
                else 0
            ),
            "eventLogIndexTopicMaxPostings": topic_max,
            "eventLogIndexTopicSingletonKeys": topic_singleton,
            "eventLogIndexTopicMultiPostingKeys": topic_multi,
        }
    )
    return row


def add_sample_prometheus_evidence(row, path, *, height=None):
    row.update(
        {
            "datadir": "/tmp/nile",
            "samplePrometheusStatus": "ok",
            "samplePrometheus": str(path),
            "syncTargetLagBlocks": 0,
            "datadirBytes": 4096,
            "chaindataBytes": 1024,
            "coldArchiveBytes": 2048,
            "derivedIndexBytes": 512,
            "bytesPerBlock": 4.096,
            "chaindataBytesPerBlock": 1.024,
            "coldArchiveBytesPerBlock": 2.048,
            "derivedIndexBytesPerBlock": 0.512,
            "soakEfficiencyDatadirBytesPerBlock": 4.096,
            "soakEfficiencyHotBytesPerBlock": 1.024,
            "soakEfficiencyColdArchiveBytesPerBlock": 2.048,
            "soakEfficiencyDerivedIndexBytesPerBlock": 0.512,
            "stageSyncInventoryBodiesGapBlocks": 0,
            "stageSyncBodiesReadyGapBlocks": 0,
            "stageSyncImportExecutionLagBlocks": 0,
            "stageSyncExecutionCommitmentLagBlocks": 0,
            "stageSyncCommitmentFinishLagBlocks": 0,
            "stageSyncFinishHeadLagBlocks": 0,
            "stageSyncPipelineLagBlocks": 0,
            "stageSyncBottleneckLagBlocks": 0,
            "stageChainFreezerHeadLagBlocks": 0,
            "stageSnapshotEventLogBuildHeadLagBlocks": 0,
            "intervalStageSyncInventoryBlocksPerSecond": 0.0,
            "intervalStageSyncBodiesBlocksPerSecond": 0.0,
            "intervalStageSyncBodiesReadyBlocksPerSecond": 0.0,
            "intervalStageSyncImportBlocksPerSecond": 0.0,
            "intervalStageSyncExecutionBlocksPerSecond": 0.0,
            "intervalStageSyncCommitmentBlocksPerSecond": 0.0,
            "intervalStageSyncFinishBlocksPerSecond": 0.0,
            "intervalStageChainFreezerBlocksPerSecond": 0.0,
            "intervalStageSnapshotEventLogBuildBlocksPerSecond": 0.0,
            "snapshotSidecarShareMilli": 188,
            "signedColdPrune": 0,
            "coldFreezerToBlock": -1,
            "chainLookupPruneToBlock": -1,
            "tailPrunedThroughBlock": -1,
            "tailPrunedFiles": 0,
            "balanceTracePruneToBlock": -1,
            "sectionBloomPruneToSection": -1,
            "archiveApiFailures": 0,
            "archiveApiDepthBlocks": 1,
            "stageStalled": False,
        }
    )
    add_state_prefetch_evidence(row)
    row.update(snapshot_point_profile_evidence())
    metric_height = row["height"] if height is None else height
    labels = 'datadir="/tmp/nile",label="",mode="full",network="nile"'
    path.write_text(
        "\n".join(
            [
                "# TYPE gtron_nile_sync_sample_status gauge",
                f'gtron_nile_sync_sample_status{{{labels},status="ok"}} 0',
                "# TYPE gtron_nile_sync_soak_health_status gauge",
                f'gtron_nile_sync_soak_health_status{{{labels},status="ok"}} 0',
                "# TYPE gtron_nile_sync_height gauge",
                f"gtron_nile_sync_height{{{labels}}} {metric_height}",
                "# TYPE gtron_nile_sync_target_lag_blocks gauge",
                f'gtron_nile_sync_target_lag_blocks{{{labels}}} {row["syncTargetLagBlocks"]}',
                *state_prefetch_prometheus_lines(row, labels),
                "# TYPE gtron_nile_sync_full_staged_sync_head_lag_blocks gauge",
                f'gtron_nile_sync_full_staged_sync_head_lag_blocks{{{labels}}} {row["fullStagedSyncHeadLagBlocks"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_ready gauge",
                f'gtron_nile_sync_full_staged_sync_ready{{{labels}}} {1 if row["fullStagedSyncReady"] else 0}',
                "# TYPE gtron_nile_sync_full_staged_sync_complete_at_head gauge",
                f'gtron_nile_sync_full_staged_sync_complete_at_head{{{labels}}} {1 if row["fullStagedSyncCompleteAtHead"] else 0}',
                "# TYPE gtron_nile_sync_full_staged_sync_complete_block gauge",
                f'gtron_nile_sync_full_staged_sync_complete_block{{{labels}}} {row["fullStagedSyncCompleteBlock"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_head_block gauge",
                f'gtron_nile_sync_full_staged_sync_head_block{{{labels}}} {row["fullStagedSyncHeadBlock"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_completion_ratio gauge",
                f'gtron_nile_sync_full_staged_sync_completion_ratio{{{labels}}} {row["fullStagedSyncCompletionRatio"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_pipeline_lag_blocks gauge",
                f'gtron_nile_sync_full_staged_sync_pipeline_lag_blocks{{{labels}}} {row["fullStagedSyncPipelineLagBlocks"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_bottleneck_lag_blocks gauge",
                f'gtron_nile_sync_full_staged_sync_bottleneck_lag_blocks{{{labels}}} {row["fullStagedSyncBottleneckLagBlocks"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_bottleneck_lag_share gauge",
                f'gtron_nile_sync_full_staged_sync_bottleneck_lag_share{{{labels}}} {row["fullStagedSyncBottleneckLagShare"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_stage_count gauge",
                f'gtron_nile_sync_full_staged_sync_stage_count{{{labels}}} {row["fullStagedSyncStageCount"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_present_stage_count gauge",
                f'gtron_nile_sync_full_staged_sync_present_stage_count{{{labels}}} {row["fullStagedSyncPresentStageCount"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_verified_stage_count gauge",
                f'gtron_nile_sync_full_staged_sync_verified_stage_count{{{labels}}} {row["fullStagedSyncVerifiedStageCount"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_stage_coverage_ratio gauge",
                f'gtron_nile_sync_full_staged_sync_stage_coverage_ratio{{{labels}}} {row["fullStagedSyncStageCoverageRatio"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_verification_ratio gauge",
                f'gtron_nile_sync_full_staged_sync_verification_ratio{{{labels}}} {row["fullStagedSyncVerificationRatio"]}',
                "# TYPE gtron_nile_sync_stage_sync_inventory_bodies_gap_blocks gauge",
                f'gtron_nile_sync_stage_sync_inventory_bodies_gap_blocks{{{labels}}} {row["stageSyncInventoryBodiesGapBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_bodies_ready_gap_blocks gauge",
                f'gtron_nile_sync_stage_sync_bodies_ready_gap_blocks{{{labels}}} {row["stageSyncBodiesReadyGapBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_import_execution_lag_blocks gauge",
                f'gtron_nile_sync_stage_sync_import_execution_lag_blocks{{{labels}}} {row["stageSyncImportExecutionLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_execution_commitment_lag_blocks gauge",
                f'gtron_nile_sync_stage_sync_execution_commitment_lag_blocks{{{labels}}} {row["stageSyncExecutionCommitmentLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_commitment_finish_lag_blocks gauge",
                f'gtron_nile_sync_stage_sync_commitment_finish_lag_blocks{{{labels}}} {row["stageSyncCommitmentFinishLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_finish_head_lag_blocks gauge",
                f'gtron_nile_sync_stage_sync_finish_head_lag_blocks{{{labels}}} {row["stageSyncFinishHeadLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_pipeline_lag_blocks gauge",
                f'gtron_nile_sync_stage_sync_pipeline_lag_blocks{{{labels}}} {row["stageSyncPipelineLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_sync_bottleneck_lag_blocks gauge",
                f'gtron_nile_sync_stage_sync_bottleneck_lag_blocks{{{labels}}} {row["stageSyncBottleneckLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_chain_freezer_head_lag_blocks gauge",
                f'gtron_nile_sync_stage_chain_freezer_head_lag_blocks{{{labels}}} {row["stageChainFreezerHeadLagBlocks"]}',
                "# TYPE gtron_nile_sync_stage_snapshot_event_log_build_head_lag_blocks gauge",
                f'gtron_nile_sync_stage_snapshot_event_log_build_head_lag_blocks{{{labels}}} {row["stageSnapshotEventLogBuildHeadLagBlocks"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_inventory_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_inventory_blocks_per_second{{{labels}}} {row["intervalStageSyncInventoryBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_bodies_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_bodies_blocks_per_second{{{labels}}} {row["intervalStageSyncBodiesBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_bodies_ready_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_bodies_ready_blocks_per_second{{{labels}}} {row["intervalStageSyncBodiesReadyBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_import_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_import_blocks_per_second{{{labels}}} {row["intervalStageSyncImportBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_execution_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_execution_blocks_per_second{{{labels}}} {row["intervalStageSyncExecutionBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_commitment_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_commitment_blocks_per_second{{{labels}}} {row["intervalStageSyncCommitmentBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_sync_finish_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_sync_finish_blocks_per_second{{{labels}}} {row["intervalStageSyncFinishBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_chain_freezer_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_chain_freezer_blocks_per_second{{{labels}}} {row["intervalStageChainFreezerBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_interval_stage_snapshot_event_log_build_blocks_per_second gauge",
                f'gtron_nile_sync_interval_stage_snapshot_event_log_build_blocks_per_second{{{labels}}} {row["intervalStageSnapshotEventLogBuildBlocksPerSecond"]}',
                "# TYPE gtron_nile_sync_full_staged_sync_status gauge",
                f'gtron_nile_sync_full_staged_sync_status{{{labels},status="{row["fullStagedSyncStatus"]}"}} 0',
                "# TYPE gtron_nile_sync_full_staged_sync_bottleneck gauge",
                f'gtron_nile_sync_full_staged_sync_bottleneck{{{labels},bottleneck="{row["fullStagedSyncBottleneck"]}"}} 1',
                "# TYPE gtron_nile_sync_datadir_bytes gauge",
                f'gtron_nile_sync_datadir_bytes{{{labels}}} {row["datadirBytes"]}',
                "# TYPE gtron_nile_sync_chaindata_bytes gauge",
                f'gtron_nile_sync_chaindata_bytes{{{labels}}} {row["chaindataBytes"]}',
                "# TYPE gtron_nile_sync_cold_archive_bytes gauge",
                f'gtron_nile_sync_cold_archive_bytes{{{labels}}} {row["coldArchiveBytes"]}',
                "# TYPE gtron_nile_sync_derived_index_bytes gauge",
                f'gtron_nile_sync_derived_index_bytes{{{labels}}} {row["derivedIndexBytes"]}',
                "# TYPE gtron_nile_sync_datadir_bytes_per_block gauge",
                f'gtron_nile_sync_datadir_bytes_per_block{{{labels}}} {row["bytesPerBlock"]}',
                "# TYPE gtron_nile_sync_chaindata_bytes_per_block gauge",
                f'gtron_nile_sync_chaindata_bytes_per_block{{{labels}}} {row["chaindataBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_cold_archive_bytes_per_block gauge",
                f'gtron_nile_sync_cold_archive_bytes_per_block{{{labels}}} {row["coldArchiveBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_derived_index_bytes_per_block gauge",
                f'gtron_nile_sync_derived_index_bytes_per_block{{{labels}}} {row["derivedIndexBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_soak_efficiency_datadir_bytes_per_block gauge",
                f'gtron_nile_sync_soak_efficiency_datadir_bytes_per_block{{{labels}}} {row["soakEfficiencyDatadirBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_soak_efficiency_hot_bytes_per_block gauge",
                f'gtron_nile_sync_soak_efficiency_hot_bytes_per_block{{{labels}}} {row["soakEfficiencyHotBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_soak_efficiency_cold_archive_bytes_per_block gauge",
                f'gtron_nile_sync_soak_efficiency_cold_archive_bytes_per_block{{{labels}}} {row["soakEfficiencyColdArchiveBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_soak_efficiency_derived_index_bytes_per_block gauge",
                f'gtron_nile_sync_soak_efficiency_derived_index_bytes_per_block{{{labels}}} {row["soakEfficiencyDerivedIndexBytesPerBlock"]}',
                "# TYPE gtron_nile_sync_snapshot_sidecar_share_milli gauge",
                f'gtron_nile_sync_snapshot_sidecar_share_milli{{{labels}}} {row["snapshotSidecarShareMilli"]}',
                *snapshot_point_prometheus_lines(row, labels),
                "# TYPE gtron_nile_sync_signed_cold_prune gauge",
                f'gtron_nile_sync_signed_cold_prune{{{labels}}} {row["signedColdPrune"]}',
                "# TYPE gtron_nile_sync_cold_freezer_to_block gauge",
                f'gtron_nile_sync_cold_freezer_to_block{{{labels}}} {row["coldFreezerToBlock"]}',
                "# TYPE gtron_nile_sync_chain_lookup_prune_to_block gauge",
                f'gtron_nile_sync_chain_lookup_prune_to_block{{{labels}}} {row["chainLookupPruneToBlock"]}',
                "# TYPE gtron_nile_sync_tail_pruned_through_block gauge",
                f'gtron_nile_sync_tail_pruned_through_block{{{labels}}} {row["tailPrunedThroughBlock"]}',
                "# TYPE gtron_nile_sync_tail_pruned_files gauge",
                f'gtron_nile_sync_tail_pruned_files{{{labels}}} {row["tailPrunedFiles"]}',
                "# TYPE gtron_nile_sync_balance_trace_prune_to_block gauge",
                f'gtron_nile_sync_balance_trace_prune_to_block{{{labels}}} {row["balanceTracePruneToBlock"]}',
                "# TYPE gtron_nile_sync_section_bloom_prune_to_section gauge",
                f'gtron_nile_sync_section_bloom_prune_to_section{{{labels}}} {row["sectionBloomPruneToSection"]}',
            "# TYPE gtron_nile_sync_archive_api_failures gauge",
            "# TYPE gtron_nile_sync_archive_api_depth_blocks gauge",
            f'gtron_nile_sync_archive_api_depth_blocks{{{labels}}} {row["archiveApiDepthBlocks"]}',
            f'gtron_nile_sync_archive_api_failures{{{labels}}} {row["archiveApiFailures"]}',
                "# TYPE gtron_nile_sync_stage_stalled gauge",
                f"gtron_nile_sync_stage_stalled{{{labels}}} 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    details = row.get("fullStagedSyncStageDetails")
    if isinstance(details, list) and details:
        lines = [
            "# TYPE gtron_nile_sync_full_staged_sync_stage_block gauge",
            "# TYPE gtron_nile_sync_full_staged_sync_stage_present gauge",
            "# TYPE gtron_nile_sync_full_staged_sync_stage_verified gauge",
        ]
        for detail in details:
            labels = sample_prometheus_labels(
                {"stage": detail["stage"], "field": detail["field"]}
            )
            lines.extend(
                [
                    f'gtron_nile_sync_full_staged_sync_stage_block{{{labels}}} {detail["block"]}',
                    f'gtron_nile_sync_full_staged_sync_stage_present{{{labels}}} {1 if detail["present"] else 0}',
                ]
            )
            verification = str(detail.get("verified", ""))
            verified_labels = sample_prometheus_labels(
                {
                    "stage": detail["stage"],
                    "field": detail["field"],
                    "verification": verification,
                }
            )
            lines.append(
                f"gtron_nile_sync_full_staged_sync_stage_verified{{{verified_labels}}} "
                f"{1 if stage_detail_verified(detail['stage'], verification) else 0}"
            )
        with path.open("a", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    return row


def append_event_log_index_prometheus_metrics(path, row):
    labels = 'datadir="/tmp/nile",label="",mode="full",network="nile"'
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(event_log_index_prometheus_lines(row, labels)) + "\n")


def add_archive_trace_evidence(row):
    row.update(
        {
            "archiveApiStatus": "ok",
            "archiveApiChecks": 21,
            "archiveApiFailures": 0,
            "archiveApiBlock": 999,
            "archiveApiDepthBlocks": 1,
            "archiveApiCallProbe": True,
            "archiveApiTraceTransactionProbe": True,
            "archiveApiMethods": [
                "eth_getBlockByNumber",
                "eth_getBlockTransactionCountByNumber",
                "eth_getUncleCountByBlockNumber",
                "eth_getUncleByBlockNumberAndIndex",
                "eth_getBlockReceipts",
                "eth_getBalance",
                "eth_getCode",
                "eth_call",
                "debug_traceCall",
                "eth_estimateGas",
                "eth_getStorageAt",
                "eth_getLogs",
                "eth_getBlockByHash",
                "eth_getBlockTransactionCountByHash",
                "eth_getUncleCountByBlockHash",
                "eth_getUncleByBlockHashAndIndex",
                "eth_getTransactionByHash",
                "eth_getTransactionReceipt",
                "eth_getTransactionByBlockNumberAndIndex",
                "eth_getTransactionByBlockHashAndIndex",
                "debug_traceTransaction",
            ],
            "archiveApiTxProbe": True,
            "archiveApiTxHash": "0x" + "ab" * 32,
            "archiveApiTxMethods": [
                "eth_getTransactionByHash",
                "eth_getTransactionReceipt",
                "eth_getTransactionByBlockNumberAndIndex",
                "eth_getTransactionByBlockHashAndIndex",
                "debug_traceTransaction",
            ],
        }
    )
    return row


def append_archive_trace_prometheus_metrics(path, row, *, include_trace=True):
    labels = 'datadir="/tmp/nile",label="",mode="full",network="nile"'
    lines = [
        "# TYPE gtron_nile_sync_archive_api_checks gauge",
        f'gtron_nile_sync_archive_api_checks{{{labels}}} {row["archiveApiChecks"]}',
        "# TYPE gtron_nile_sync_archive_api_block gauge",
        f'gtron_nile_sync_archive_api_block{{{labels}}} {row["archiveApiBlock"]}',
        "# TYPE gtron_nile_sync_archive_api_depth_blocks gauge",
        f'gtron_nile_sync_archive_api_depth_blocks{{{labels}}} {row["archiveApiDepthBlocks"]}',
        "# TYPE gtron_nile_sync_archive_api_method_success gauge",
    ]
    for method in row["archiveApiMethods"]:
        if method == "debug_traceTransaction" and not include_trace:
            continue
        lines.append(
            f'gtron_nile_sync_archive_api_method_success{{{labels},method="{method}"}} 1'
        )
    lines.append("# TYPE gtron_nile_sync_archive_api_tx_method_success gauge")
    for method in row["archiveApiTxMethods"]:
        if method == "debug_traceTransaction" and not include_trace:
            continue
        lines.append(
            f'gtron_nile_sync_archive_api_tx_method_success{{{labels},method="{method}"}} 1'
        )
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


class NileSyncAcceptanceTest(unittest.TestCase):
    def test_accepts_sample_prometheus_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_accepts_sample_prometheus_without_state_prefetch_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            for field, _ in STATE_PREFETCH_METRIC_FIELDS:
                row[field] = -1
            row["syncLogStatus"] = "skipped"
            text = prom.read_text(encoding="utf-8")
            lines = [
                line
                for line in text.splitlines()
                if "gtron_nile_sync_log_state_prefetch_" not in line
            ]
            prom.write_text("\n".join(lines) + "\n", encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_accepts_state_prefetch_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_state_prefetch_evidence(clean_full_staged_sync_row())
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-state-prefetch-evidence",
                    "--max-state-prefetch-errors",
                    "1",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_missing_state_prefetch_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-state-prefetch-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "syncLogStatus=None, want 'ok' for state prefetch evidence",
                proc.stderr,
            )
            self.assertIn(
                "syncLogStatePrefetchEnqueued=None, want non-negative integer",
                proc.stderr,
            )

    def test_rejects_state_prefetch_accounting_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_state_prefetch_evidence(
                clean_full_staged_sync_row(),
                processed=11,
                hits=8,
                misses=2,
                errors=0,
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-state-prefetch-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "state prefetch processed=11, want hits+misses+errors=10",
                proc.stderr,
            )

    def test_rejects_missing_state_prefetch_activity(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_state_prefetch_evidence(
                clean_full_staged_sync_row(),
                enqueued=0,
                dropped=0,
                processed=0,
                hits=0,
                misses=0,
                errors=0,
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-state-prefetch-activity",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "state prefetch activity missing: enqueued=0 processed=0",
                proc.stderr,
            )

    def test_rejects_state_prefetch_error_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_state_prefetch_evidence(
                clean_full_staged_sync_row(),
                processed=11,
                hits=8,
                misses=1,
                errors=2,
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--max-state-prefetch-errors",
                    "0",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("syncLogStatePrefetchErrors=2, want <= 0", proc.stderr)

    def test_accepts_sync_phase_cursor_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_sync_phase_cursor_evidence(clean_full_staged_sync_row())
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sync-phase-cursor-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_missing_sync_phase_cursor_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sync-phase-cursor-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "syncLogStatus=None, want 'ok' for sync phase cursor evidence",
                proc.stderr,
            )
            self.assertIn("syncLogPhaseCursorComplete is missing", proc.stderr)
            self.assertIn(
                "syncLogPhaseCursorCompletedPhases=None, want non-negative integer",
                proc.stderr,
            )

    def test_rejects_invalid_sync_phase_cursor_accounting(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_sync_phase_cursor_evidence(
                clean_full_staged_sync_row(),
                syncLogPhaseCursorCompletedPhases=5,
                syncLogPhaseCursorScheduledPhases=4,
                syncLogPhaseCursorCompletedTasks=81,
                syncLogPhaseCursorScheduledTasks=80,
                syncLogPhaseCursorCurrentTaskIndex=18,
                syncLogPhaseCursorCurrentTaskCount=20,
                syncLogPhaseCursorCurrentTaskRemaining=1,
                syncLogPhaseCursorCurrentFromBlock=101,
                syncLogPhaseCursorCurrentToBlock=100,
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sync-phase-cursor-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "sync phase cursor completed phases=5 exceeds scheduled phases=4",
                proc.stderr,
            )
            self.assertIn(
                "sync phase cursor completed tasks=81 exceeds scheduled tasks=80",
                proc.stderr,
            )
            self.assertIn(
                "syncLogPhaseCursorCurrentTaskRemaining=1, "
                "want currentTaskCount-currentTaskIndex=2",
                proc.stderr,
            )
            self.assertIn("sync phase cursor block range inverted: from=101 to=100", proc.stderr)

    def test_accepts_sample_prometheus_sync_phase_cursor_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            row = add_sync_phase_cursor_evidence(row)
            labels = sample_prometheus_labels()
            with prom.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(sync_phase_cursor_prometheus_lines(row, labels)) + "\n")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-sync-phase-cursor-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_sample_prometheus_missing_sync_phase_cursor_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            row = add_sync_phase_cursor_evidence(row)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-sync-phase-cursor-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "missing gtron_nile_sync_log_phase_cursor_completed_phases",
                proc.stderr,
            )

    def test_accepts_event_log_index_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_event_log_index_evidence(clean_full_staged_sync_row())
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-event-log-index-evidence",
                    "--require-event-log-index-non-empty",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_missing_event_log_index_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("event-log index evidence is missing", proc.stderr)
            self.assertIn("eventLogIndexStatsStatus=None, want 'ok'", proc.stderr)
            self.assertIn("eventLogIndexSegments=None, want positive integer", proc.stderr)

    def test_rejects_invalid_event_log_index_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_event_log_index_evidence(
                clean_full_staged_sync_row(),
                segments=0,
                address_keys=3,
                address_postings=2,
                address_max=0,
                address_singleton=1,
                address_multi=1,
                topic_keys=0,
                topic_postings=1,
                topic_max=1,
                topic_singleton=0,
                topic_multi=0,
            )
            row["eventLogIndexAddressAvgPostingsMilli"] = 123
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("eventLogIndexSegments=0, want > 0", proc.stderr)
            self.assertIn("address singleton+multi=2 must equal keys=3", proc.stderr)
            self.assertIn("address postings=2 must be >= keys=3", proc.stderr)
            self.assertIn("address maxPostings must be > 0 when keys=3", proc.stderr)
            self.assertIn("address avgPostingsMilli=123, want 667", proc.stderr)
            self.assertIn("topic postings=1 must be 0 when keys=0", proc.stderr)
            self.assertIn("topic maxPostings=1 must be 0 when keys=0", proc.stderr)

    def test_rejects_event_log_index_tail_prune_gap(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_event_log_index_evidence(
                clean_full_staged_sync_row(),
                from_block=1,
                to_block=70,
            )
            row["tailPrunedThroughBlock"] = 75
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "event-log index range [1,70] must cover tailPrunedThroughBlock=75",
                proc.stderr,
            )

    def test_rejects_sample_prometheus_missing_state_prefetch_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            text = prom.read_text(encoding="utf-8")
            text = text.replace(
                'gtron_nile_sync_log_state_prefetch_processed{datadir="/tmp/nile",label="",mode="full",network="nile"} 11\n',
                "",
            )
            prom.write_text(text, encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "missing gtron_nile_sync_log_state_prefetch_processed",
                proc.stderr,
            )

    def test_accepts_sample_prometheus_event_log_index_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_event_log_index_evidence(
                add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            )
            append_event_log_index_prometheus_metrics(prom, row)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_sample_prometheus_missing_event_log_index_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_event_log_index_evidence(
                add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            )
            append_event_log_index_prometheus_metrics(prom, row)
            text = prom.read_text(encoding="utf-8")
            text = text.replace(
                'gtron_nile_sync_event_log_index_address_postings{datadir="/tmp/nile",label="",mode="full",network="nile"} 6\n',
                "",
            )
            prom.write_text(text, encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "missing gtron_nile_sync_event_log_index_address_postings",
                proc.stderr,
            )

    def test_accepts_sample_prometheus_archive_method_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_archive_trace_evidence(
                add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            )
            append_archive_trace_prometheus_metrics(prom, row)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_sample_prometheus_missing_archive_method_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_archive_trace_evidence(
                add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            )
            append_archive_trace_prometheus_metrics(prom, row, include_trace=False)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "missing gtron_nile_sync_archive_api_method_success{method='debug_traceTransaction'}",
                proc.stderr,
            )
            self.assertIn(
                "missing gtron_nile_sync_archive_api_tx_method_success{method='debug_traceTransaction'}",
                proc.stderr,
            )

    def test_rejects_sample_prometheus_archive_probe_metric_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_archive_trace_evidence(
                add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            )
            append_archive_trace_prometheus_metrics(
                prom,
                {**row, "archiveApiBlock": 998, "archiveApiDepthBlocks": 2},
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "gtron_nile_sync_archive_api_block=998, want 999",
                proc.stderr,
            )
            self.assertIn(
                "gtron_nile_sync_archive_api_depth_blocks=2, want 1",
                proc.stderr,
            )

    def test_rejects_mismatched_sample_prometheus_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom, height=999)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("gtron_nile_sync_height=999, want 1000", proc.stderr)

    def test_rejects_fractional_sample_prometheus_integer_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            row["syncTargetLagBlocks"] = 0.5
            labels = sample_prometheus_labels()
            text = prom.read_text(encoding="utf-8")
            text = text.replace(
                f"gtron_nile_sync_stage_sync_bodies_ready_gap_blocks{{{labels}}} 0",
                f"gtron_nile_sync_stage_sync_bodies_ready_gap_blocks{{{labels}}} 0.5",
            )
            prom.write_text(text, encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "syncTargetLagBlocks=0.5, want non-negative integer",
                proc.stderr,
            )
            self.assertIn(
                "gtron_nile_sync_stage_sync_bodies_ready_gap_blocks=0.5, "
                "want non-negative integer",
                proc.stderr,
            )

    def test_rejects_sample_prometheus_prune_boundary_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            row["chainLookupPruneToBlock"] = 80
            text = prom.read_text(encoding="utf-8")
            text = text.replace(
                'gtron_nile_sync_chain_lookup_prune_to_block{datadir="/tmp/nile",label="",mode="full",network="nile"} -1',
                'gtron_nile_sync_chain_lookup_prune_to_block{datadir="/tmp/nile",label="",mode="full",network="nile"} 79',
            )
            prom.write_text(text, encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "gtron_nile_sync_chain_lookup_prune_to_block=79, want 80",
                proc.stderr,
            )

    def test_rejects_sample_prometheus_full_staged_sync_status_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = add_sample_prometheus_evidence(clean_full_staged_sync_row(), prom)
            text = prom.read_text(encoding="utf-8")
            text = text.replace(
                'gtron_nile_sync_full_staged_sync_status{datadir="/tmp/nile",label="",mode="full",network="nile",status="caught-up"} 0',
                'gtron_nile_sync_full_staged_sync_status{datadir="/tmp/nile",label="",mode="full",network="nile",status="caught-up"} 1',
            )
            prom.write_text(text, encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "gtron_nile_sync_full_staged_sync_status{status='caught-up'}=1, want 0",
                proc.stderr,
            )

    def test_accepts_sample_prometheus_stage_detail_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["fullStagedSyncStageDetails"] = full_stage_details(
                verified={"SyncBodies": "staged"}
            )
            row = add_sample_prometheus_evidence(row, prom)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-stage-detail-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_sample_prometheus_stage_detail_metric_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["fullStagedSyncStageDetails"] = full_stage_details()
            row = add_sample_prometheus_evidence(row, prom)
            labels = sample_prometheus_labels(
                {"stage": "SyncExecution", "field": "stageSyncExecution"}
            )
            text = prom.read_text(encoding="utf-8")
            text = text.replace(
                f"gtron_nile_sync_full_staged_sync_stage_block{{{labels}}} 1000",
                f"gtron_nile_sync_full_staged_sync_stage_block{{{labels}}} 999",
            )
            prom.write_text(text, encoding="utf-8")
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-stage-detail-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "gtron_nile_sync_full_staged_sync_stage_block"
                "{stage='SyncExecution',field='stageSyncExecution'}=999, want 1000",
                proc.stderr,
            )

    def test_rejects_fractional_sample_prometheus_stage_detail_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "sync.prom"
            result = tmpdir / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["fullStagedSyncStageDetails"] = full_stage_details(
                blocks={"SyncExecution": 1000.5}
            )
            row = add_sample_prometheus_evidence(row, prom)
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-sample-prometheus-artifact",
                    "--require-stage-detail-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "fullStagedSyncStageDetails[4].block=1000.5, want non-negative integer",
                proc.stderr,
            )
            self.assertIn(
                "gtron_nile_sync_full_staged_sync_stage_block"
                "{stage='SyncExecution',field='stageSyncExecution'}=1000.5, "
                "want non-negative integer",
                proc.stderr,
            )

    def test_accepts_snapshot_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(result, [add_snapshot_profile_evidence(clean_full_staged_sync_row())])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-snapshot-profile-evidence",
                    "--max-snapshot-point-sidecar-share-milli",
                    "1000",
                    "--max-snapshot-point-snapshot-share-milli",
                    "200",
                    "--max",
                    "snapshotSidecarShareMilli=200",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_invalid_snapshot_point_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            row = add_snapshot_profile_evidence(clean_full_staged_sync_row())
            row["snapshotPointEventLogIndexSnapshotShareMilli"] = 999
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-snapshot-profile-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "snapshotPointEventLogIndexSnapshotShareMilli=999, want 125 "
                "for snapshotPointEventLogIndexBytes=200 totalBytes=1600",
                proc.stderr,
            )

    def test_rejects_snapshot_point_profile_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(result, [add_snapshot_profile_evidence(clean_full_staged_sync_row())])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--max-snapshot-point-sidecar-share-milli",
                    "999",
                    "--max-snapshot-point-snapshot-share-milli",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("snapshotPointTxHashLookupSidecarShareMilli=1000 exceeds max 999", proc.stderr)
            self.assertIn("snapshotPointEventLogIndexSnapshotShareMilli=125 exceeds max 100", proc.stderr)

    def test_rejects_snapshot_point_threshold_without_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--max-snapshot-point-snapshot-share-milli",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("snapshot point threshold requires snapshot manifest profile evidence", proc.stderr)

    def test_rejects_missing_snapshot_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-snapshot-profile-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("snapshot manifest profile evidence is missing", proc.stderr)

    def test_rejects_invalid_snapshot_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row.update(
                {
                    "snapshotManifestProfileStatus": "missing",
                    "snapshotProfileSegments": 0,
                    "snapshotProfileVerifyFiles": False,
                    "snapshotProfileVerifiedSegments": 3,
                    "snapshotProfileTotalBytes": 1600,
                    "snapshotPayloadBytes": 1200,
                    "snapshotSidecarBytes": 300,
                    "snapshotSidecarShareMilli": 111,
                    "snapshotLatestSidecarBytes": 1,
                    "snapshotLatestSidecarShareMilli": -1,
                    "snapshotStateHistorySidecarBytes": 0,
                    "snapshotStateHistorySidecarShareMilli": -1,
                    "snapshotChainFreezerSidecarBytes": 100,
                    "snapshotChainFreezerSidecarShareMilli": 1001,
                    "snapshotEventLogSidecarBytes": 200,
                    "snapshotEventLogSidecarShareMilli": 400,
                    "snapshotBalanceTraceSidecarBytes": 0,
                    "snapshotBalanceTraceSidecarShareMilli": -1,
                    "snapshotSectionBloomSidecarBytes": 0,
                    "snapshotSectionBloomSidecarShareMilli": -1,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-snapshot-profile-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("snapshotManifestProfileStatus='missing', want 'ok'", proc.stderr)
            self.assertIn("snapshotProfileSegments=0, want > 0", proc.stderr)
            self.assertIn("snapshotProfileVerifyFiles must be true", proc.stderr)
            self.assertIn("snapshotProfileVerifiedSegments=3, want snapshotProfileSegments=0", proc.stderr)
            self.assertIn("snapshot payload+sidecar=1500 must equal total=1600", proc.stderr)
            self.assertIn(
                "snapshotSidecarShareMilli=111, want 188 for sidecarBytes=300 totalBytes=1600",
                proc.stderr,
            )
            self.assertIn(
                "snapshotLatestSidecarShareMilli=-1, want >= 0 when snapshotLatestSidecarBytes=1",
                proc.stderr,
            )
            self.assertIn("snapshotChainFreezerSidecarShareMilli=1001, want -1..1000", proc.stderr)

    def test_accepts_clean_latest_staged_sync_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_complete{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_pending{datadir="/tmp/nile"} 2\n'
                'gtron_storage_stage_pipeline_issues{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/nile",stage="SnapshotBuild",status="missing",upstream="Finish"} 1000\n'
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/nile",stage="SnapshotBuild",status="missing",upstream="Finish"} 990\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "label": "candidate",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "catching-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": False,
                        "stageSyncPipelineMonotonic": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "fullStagedSyncCompleteBlock": 988,
                        "fullStagedSyncHeadBlock": 1000,
                        "fullStagedSyncHeadLagBlocks": 12,
                        "fullStagedSyncCompletionRatio": 0.988,
                        "fullStagedSyncPipelineLagBlocks": 12,
                        "fullStagedSyncBottleneck": "finish-head",
                        "fullStagedSyncBottleneckLagBlocks": 12,
                        "fullStagedSyncBottleneckLagShare": 1.0,
                        "stageSyncPipelineLagBlocks": 12,
                        "stageSyncBottleneck": "finish-head",
                        "stageSyncBottleneckLagBlocks": 12,
                        "stageStalled": False,
                        "stageStalledCount": 0,
                        "stageStalledStage": "",
                        "stageStalledSeconds": 0,
                        "stageStalledLagBlocks": -1,
                        "stageStalls": [],
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "storageAlertStatus": "ok",
                        "freezerAlertStatus": "ok",
                        "freezerAlertIssues": 0,
                        "stageVerifyStatus": "ok",
                        "stageVerifyIssues": 0,
                        "modeAlertStatus": "ok",
                        "modeAlertIssues": 0,
                        "snapshotAlertStatus": "ok",
                        "snapshotAlertIssues": 0,
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 1000,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 990,
                        "height": 1000,
                        "intervalStageSyncFinishBlocksPerMinute": 30.5,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--label",
                    "candidate",
                    "--require-offline-db-check",
                    "--require-stage-stall-evidence",
                    "--min-height",
                    "1000",
                    "--max-lag-blocks",
                    "20",
                    "--min",
                    "intervalStageSyncFinishBlocksPerMinute=10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)
            self.assertIn("status=catching-up", proc.stdout)

    def test_accepts_prune_mode_semantics(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [add_clean_prune_mode(clean_full_staged_sync_row())])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_prune_mode_semantic_violations(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_prune_mode(clean_full_staged_sync_row())
            row.update(
                {
                    "pruneMode": "minimal",
                    "pruneModePersisted": False,
                    "tailPrunedThroughBlock": 7,
                    "tailPrunedFiles": 1,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("pruneMode='minimal' does not match mode='full'", proc.stderr)
            self.assertIn("pruneModePersisted must be true", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=7 is not allowed for full mode", proc.stderr)
            self.assertIn("tailPrunedFiles=1 is not allowed for full mode", proc.stderr)

    def test_rejects_signed_cold_prune_without_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_prune_mode(clean_full_staged_sync_row())
            row.update(
                {
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "coldFreezerToBlock": 49,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("coldFreezerToBlock=49.0 must cover chainLookupPruneToBlock=50", proc.stderr)

    def test_rejects_archive_prune_mode_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_prune_mode(clean_full_staged_sync_row(), "archive")
            row.update(
                {
                    "network": "nile",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 12,
                    "tailPrunedThroughBlock": 9,
                    "balanceTracePruneToBlock": 8,
                    "sectionBloomPruneToSection": 2,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "archive",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("signedColdPrune must be false for archive", proc.stderr)
            self.assertIn("chainLookupPruneToBlock=12 is not allowed for archive mode", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=9 is not allowed for archive mode", proc.stderr)
            self.assertIn("balanceTracePruneToBlock=8 is not allowed for archive mode", proc.stderr)
            self.assertIn("sectionBloomPruneToSection=2 is not allowed for archive mode", proc.stderr)

    def test_rejects_fractional_prune_mode_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_prune_mode(clean_full_staged_sync_row(), "minimal")
            row.update(
                {
                    "network": "nile",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50.5,
                    "coldFreezerToBlock": 50.5,
                    "tailPrunedThroughBlock": 45.5,
                    "tailPrunedFiles": 1.5,
                    "balanceTracePruneToBlock": 44.5,
                    "sectionBloomPruneToSection": 2.5,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "minimal",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("chainLookupPruneToBlock=50.5, want integer", proc.stderr)
            self.assertIn("coldFreezerToBlock=50.5, want integer", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=45.5, want integer", proc.stderr)
            self.assertIn("tailPrunedFiles=1.5, want non-negative integer", proc.stderr)
            self.assertIn("balanceTracePruneToBlock=44.5, want integer", proc.stderr)
            self.assertIn("sectionBloomPruneToSection=2.5, want integer", proc.stderr)

    def test_rejects_missing_prune_mode_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("pruneMode is missing or unknown", proc.stderr)
            self.assertIn("pruneModePersisted must be true", proc.stderr)

    def test_rejects_minimal_tail_prune_without_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_prune_mode(clean_full_staged_sync_row(), "minimal")
            row.update({"tailPrunedThroughBlock": -1, "tailPrunedFiles": 1})
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "minimal",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "tailPrunedThroughBlock must be >= 0 when tailPrunedFiles is positive "
                "for minimal mode",
                proc.stderr,
            )

    def test_rejects_minimal_tail_prune_without_chain_lookup_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_prune_mode(clean_full_staged_sync_row(), "minimal")
            row.update(
                {
                    "signedColdPrune": 1,
                    "coldFreezerToBlock": 50,
                    "chainLookupPruneToBlock": 10,
                    "tailPrunedThroughBlock": 12,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "minimal",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("tailPrunedThroughBlock=12 exceeds chainLookupPruneToBlock=10", proc.stderr)

    def test_accepts_max_cold_stage_lag_blocks_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["stageChainFreezerHeadLagBlocks"] = 120
            row["stageSnapshotEventLogBuildHeadLagBlocks"] = 200
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-stage-lag-blocks",
                    "500",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_cold_stage_lag_above_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["stageChainFreezerHeadLagBlocks"] = 120
            row["stageSnapshotEventLogBuildHeadLagBlocks"] = 600
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-stage-lag-blocks",
                    "500",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "stageSnapshotEventLogBuildHeadLagBlocks=600 failed <= max cold stage lag 500",
                proc.stderr,
            )

    def test_rejects_cold_stage_lag_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["stageChainFreezerHeadLagBlocks"] = 120
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-stage-lag-blocks",
                    "500",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "cold stage lag evidence missing: "
                "stageSnapshotEventLogBuildHeadLagBlocks is missing or not a non-negative integer",
                proc.stderr,
            )

    def test_rejects_fractional_cold_stage_lag_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["stageChainFreezerHeadLagBlocks"] = 120.5
            row["stageSnapshotEventLogBuildHeadLagBlocks"] = 200
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-stage-lag-blocks",
                    "500",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "cold stage lag evidence missing: "
                "stageChainFreezerHeadLagBlocks is missing or not a non-negative integer",
                proc.stderr,
            )

    def test_accepts_chain_freezer_metric_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["debugMetricsStatus"] = "ok"
            row["debugMetricChainFreezerBlocks"] = 12000
            row["debugMetricChainFreezerPasses"] = 3
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-chain-freezer-blocks",
                    "10000",
                    "--min-chain-freezer-passes",
                    "2",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_chain_freezer_metric_below_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["debugMetricsStatus"] = "ok"
            row["debugMetricChainFreezerBlocks"] = 9000
            row["debugMetricChainFreezerPasses"] = 1
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-chain-freezer-blocks",
                    "10000",
                    "--min-chain-freezer-passes",
                    "2",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "debugMetricChainFreezerBlocks=9000 failed >= min chain freezer blocks 10000",
                proc.stderr,
            )
            self.assertIn(
                "debugMetricChainFreezerPasses=1 failed >= min chain freezer passes 2",
                proc.stderr,
            )

    def test_rejects_chain_freezer_metric_without_debug_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["debugMetricChainFreezerBlocks"] = 12000
            row["debugMetricChainFreezerPasses"] = 3
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-chain-freezer-blocks",
                    "10000",
                    "--min-chain-freezer-passes",
                    "2",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "debugMetricsStatus=None, want 'ok' for chain freezer metric evidence",
                proc.stderr,
            )

    def test_accepts_offline_storage_alert_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_storage_alerts(clean_full_staged_sync_row())
            row["offlineDbCheck"] = True
            row["offlineDbCheckStatus"] = "ok"
            row["offlineDbCheckPrometheusStatus"] = "skipped"
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_offline_storage_alert_status_issue(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_storage_alerts(clean_full_staged_sync_row())
            row["offlineDbCheck"] = True
            row["offlineDbCheckStatus"] = "ok"
            row["offlineDbCheckPrometheusStatus"] = "skipped"
            row["freezerAlertStatus"] = "critical"
            row["freezerAlertIssues"] = 2
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("freezerAlertStatus='critical', want 'ok'", proc.stderr)
            self.assertIn("freezerAlertIssues=2, want 0", proc.stderr)

    def test_accepts_startup_recovery_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_startup_recovery(clean_full_staged_sync_row())
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-startup-recovery-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_startup_recovery_without_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-startup-recovery-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("syncStartupRepairStatus=None, want 'ok'", proc.stderr)
            self.assertIn("syncStartupRepairSummaries=None, want > 0", proc.stderr)

    def test_rejects_startup_recovery_blocked_or_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = add_clean_startup_recovery(clean_full_staged_sync_row())
            row["syncStartupRepairComplete"] = False
            row["syncStartupRepairHasBlocked"] = True
            row["syncStartupRepairFirstBlocked"] = "SyncCommitment"
            row["syncStartupPipelineOrderIssues"] = 1
            row["syncStartupPipelineOrderReadErrors"] = 1
            row["syncStartupPipelineOrderRepairComplete"] = False
            row["syncStartupPipelineOrderRepairInterrupted"] = True
            row["syncStartupPipelineOrderRepairErrorStage"] = "SyncCommitment"
            row["syncStartupPipelineCursorComplete"] = False
            row["syncStartupPipelineCursorBlocked"] = True
            row["syncStartupPipelineCursorNextStage"] = "SyncCommitment"
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-startup-recovery-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("syncStartupRepairComplete is not true", proc.stderr)
            self.assertIn(
                "syncStartupRepairHasBlocked=true: firstBlocked='SyncCommitment'",
                proc.stderr,
            )
            self.assertIn("syncStartupPipelineOrderIssues=1, want 0", proc.stderr)
            self.assertIn("syncStartupPipelineOrderReadErrors=1, want 0", proc.stderr)
            self.assertIn("syncStartupPipelineOrderRepairComplete is not true", proc.stderr)
            self.assertIn("syncStartupPipelineOrderRepairInterrupted=true", proc.stderr)
            self.assertIn(
                "syncStartupPipelineOrderRepairErrorStage='SyncCommitment', want ''",
                proc.stderr,
            )
            self.assertIn("syncStartupPipelineCursorComplete is not true", proc.stderr)
            self.assertIn(
                "syncStartupPipelineCursorBlocked=true: nextStage='SyncCommitment'",
                proc.stderr,
            )

    def test_accepts_min_sync_rate_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["intervalBlocksPerSecond"] = 12.5
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_min_sync_rate_below_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["intervalBlocksPerSecond"] = 1.25
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "2",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "intervalBlocksPerSecond=1.25 failed >= min sync rate 2 blocks/s",
                proc.stderr,
            )

    def test_rejects_min_sync_rate_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "1",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("sync rate evidence missing", proc.stderr)

    def test_accepts_min_sync_rate_sample_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["intervalBlocksPerSecond"] = 12.5
            row["intervalBlocks"] = 120
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "10",
                    "--min-sync-rate-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_min_sync_rate_sample_blocks_below_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["intervalBlocksPerSecond"] = 12.5
            row["intervalBlocks"] = 3
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "10",
                    "--min-sync-rate-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "intervalBlocks=3 failed >= min sync rate sample blocks 100 "
                "for intervalBlocksPerSecond",
                proc.stderr,
            )

    def test_rejects_fractional_min_sync_rate_sample_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["intervalBlocksPerSecond"] = 12.5
            row["intervalBlocks"] = 100.5
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "10",
                    "--min-sync-rate-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "sync rate sample size evidence invalid for "
                "intervalBlocksPerSecond: intervalBlocks=100.5, "
                "want non-negative integer",
                proc.stderr,
            )

    def test_rejects_min_sync_rate_sample_blocks_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["intervalBlocksPerSecond"] = 12.5
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-sync-rate",
                    "10",
                    "--min-sync-rate-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "sync rate sample size evidence missing for intervalBlocksPerSecond",
                proc.stderr,
            )

    def test_accepts_max_datadir_bytes_per_block_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyDatadirBytesPerBlock"] = 150000
            row["intervalDatadirBytesPerBlock"] = 180000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_datadir_bytes_per_block_above_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyDatadirBytesPerBlock"] = 170000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "soakEfficiencyDatadirBytesPerBlock=170000 failed <= max datadir bytes per block 160000",
                proc.stderr,
            )

    def test_rejects_datadir_bytes_per_block_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("datadir bytes-per-block evidence missing", proc.stderr)

    def test_accepts_storage_bytes_per_block_sample_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row.update(
                {
                    "soakEfficiencyWindow": "interval",
                    "intervalBlocks": 250,
                    "soakEfficiencyDatadirBytesPerBlock": 150000,
                    "soakEfficiencyHotBytesPerBlock": 9000,
                    "soakEfficiencyColdArchiveBytesPerBlock": 35000,
                    "soakEfficiencyDerivedIndexBytesPerBlock": 12000,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                    "--max-hot-bytes-per-block",
                    "10000",
                    "--max-cold-archive-bytes-per-block",
                    "40000",
                    "--max-derived-index-bytes-per-block",
                    "15000",
                    "--min-storage-sample-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_storage_bytes_per_block_sample_blocks_below_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "interval"
            row["intervalBlocks"] = 3
            row["soakEfficiencyDatadirBytesPerBlock"] = 150000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                    "--min-storage-sample-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "intervalBlocks=3 failed >= min datadir bytes-per-block "
                "sample blocks 100 for soakEfficiencyDatadirBytesPerBlock",
                proc.stderr,
            )

    def test_rejects_fractional_storage_bytes_per_block_sample_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "interval"
            row["intervalBlocks"] = 100.5
            row["soakEfficiencyDatadirBytesPerBlock"] = 150000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                    "--min-storage-sample-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "datadir bytes-per-block sample size evidence invalid for "
                "soakEfficiencyDatadirBytesPerBlock: intervalBlocks=100.5, "
                "want non-negative integer",
                proc.stderr,
            )

    def test_rejects_storage_bytes_per_block_sample_blocks_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "interval"
            row["soakEfficiencyDatadirBytesPerBlock"] = 150000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-datadir-bytes-per-block",
                    "160000",
                    "--min-storage-sample-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "datadir bytes-per-block sample size evidence missing for "
                "soakEfficiencyDatadirBytesPerBlock",
                proc.stderr,
            )

    def test_accepts_max_hot_bytes_per_block_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyHotBytesPerBlock"] = 9000
            row["intervalChaindataBytesPerBlock"] = 12000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-bytes-per-block",
                    "10000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_hot_bytes_per_block_above_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyHotBytesPerBlock"] = 12000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-bytes-per-block",
                    "10000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "soakEfficiencyHotBytesPerBlock=12000 failed <= max hot bytes per block 10000",
                proc.stderr,
            )

    def test_rejects_hot_bytes_per_block_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-bytes-per-block",
                    "10000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("hot bytes-per-block evidence missing", proc.stderr)

    def test_accepts_max_hot_growth_share_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "interval"
            row["intervalPositiveDiskGrowthBytes"] = 1000000
            row["intervalChaindataGrowthShare"] = 0.35
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-growth-share",
                    "0.4",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_hot_growth_share_above_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "interval"
            row["intervalPositiveDiskGrowthBytes"] = 1000000
            row["intervalChaindataGrowthShare"] = 0.55
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-growth-share",
                    "0.4",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "intervalChaindataGrowthShare=0.55 failed <= max hot growth share 0.4",
                proc.stderr,
            )

    def test_rejects_hot_growth_share_without_interval_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "cumulative"
            row["intervalPositiveDiskGrowthBytes"] = 0
            row["intervalChaindataGrowthShare"] = 0
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-growth-share",
                    "0.4",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "hot growth share evidence requires soakEfficiencyWindow='interval'",
                proc.stderr,
            )

    def test_rejects_hot_growth_share_without_share_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyWindow"] = "interval"
            row["intervalPositiveDiskGrowthBytes"] = 1000000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-hot-growth-share",
                    "0.4",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("hot growth share evidence missing", proc.stderr)

    def test_accepts_max_cold_archive_bytes_per_block_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyColdArchiveBytesPerBlock"] = 20000
            row["intervalColdArchiveBytesPerBlock"] = 30000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-archive-bytes-per-block",
                    "25000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_cold_archive_bytes_per_block_above_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyColdArchiveBytesPerBlock"] = 26000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-archive-bytes-per-block",
                    "25000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "soakEfficiencyColdArchiveBytesPerBlock=26000 failed <= max cold archive bytes per block 25000",
                proc.stderr,
            )

    def test_rejects_cold_archive_bytes_per_block_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-cold-archive-bytes-per-block",
                    "25000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("cold archive bytes-per-block evidence missing", proc.stderr)

    def test_accepts_max_derived_index_bytes_per_block_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyDerivedIndexBytesPerBlock"] = 7000
            row["intervalDerivedIndexBytesPerBlock"] = 9000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-derived-index-bytes-per-block",
                    "8000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_derived_index_bytes_per_block_above_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["soakEfficiencyDerivedIndexBytesPerBlock"] = 9000
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-derived-index-bytes-per-block",
                    "8000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "soakEfficiencyDerivedIndexBytesPerBlock=9000 failed <= max derived index bytes per block 8000",
                proc.stderr,
            )

    def test_rejects_derived_index_bytes_per_block_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--max-derived-index-bytes-per-block",
                    "8000",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("derived index bytes-per-block evidence missing", proc.stderr)

    def test_rejects_full_staged_sync_lag_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "catching-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": False,
                        "stageSyncPipelineMonotonic": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "fullStagedSyncCompleteBlock": 990,
                        "fullStagedSyncHeadBlock": 1000,
                        "fullStagedSyncHeadLagBlocks": 12,
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                        "height": 1000,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "fullStagedSyncHeadLagBlocks=12, want 10",
                proc.stderr,
            )

    def test_rejects_full_staged_sync_derived_metric_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "catching-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": False,
                        "stageSyncPipelineMonotonic": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "fullStagedSyncCompleteBlock": 990,
                        "fullStagedSyncHeadBlock": 1000,
                        "fullStagedSyncHeadLagBlocks": 10,
                        "fullStagedSyncCompletionRatio": 0.1,
                        "fullStagedSyncPipelineLagBlocks": 9,
                        "fullStagedSyncBottleneck": "none",
                        "fullStagedSyncBottleneckLagBlocks": 12,
                        "fullStagedSyncBottleneckLagShare": 0.5,
                        "stageSyncPipelineLagBlocks": 10,
                        "stageSyncBottleneck": "finish-head",
                        "stageSyncBottleneckLagBlocks": 10,
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                        "height": 1000,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("fullStagedSyncCompletionRatio=0.1, want 0.99", proc.stderr)
            self.assertIn(
                "fullStagedSyncPipelineLagBlocks=9 is below fullStagedSyncHeadLagBlocks=10",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncPipelineLagBlocks=9, want stageSyncPipelineLagBlocks=10",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncBottleneckLagBlocks=12 exceeds fullStagedSyncPipelineLagBlocks=9",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncBottleneck='none', want a concrete bottleneck",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncBottleneck='none', want stageSyncBottleneck='finish-head'",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncBottleneckLagBlocks=12, want stageSyncBottleneckLagBlocks=10",
                proc.stderr,
            )
            self.assertIn("fullStagedSyncBottleneckLagShare=0.5, want 1.33333", proc.stderr)

    def test_rejects_stage_sync_derived_gap_and_bottleneck_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row.update(
                {
                    "fullStagedSyncStatus": "catching-up",
                    "fullStagedSyncReady": True,
                    "fullStagedSyncCompleteAtHead": False,
                    "fullStagedSyncCompleteBlock": 950,
                    "fullStagedSyncHeadBlock": 1000,
                    "fullStagedSyncHeadLagBlocks": 50,
                    "fullStagedSyncCompletionRatio": 0.95,
                    "fullStagedSyncPipelineLagBlocks": 80,
                    "fullStagedSyncBottleneck": "inventory-bodies",
                    "fullStagedSyncBottleneckLagBlocks": 1,
                    "fullStagedSyncBottleneckLagShare": 1 / 80,
                    "stageSyncInventory": 1000,
                    "stageSyncBodies": 980,
                    "stageSyncBodiesReady": 970,
                    "stageSyncImport": 970,
                    "stageSyncExecution": 965,
                    "stageSyncCommitment": 960,
                    "stageSyncFinish": 950,
                    "stageSyncInventoryBodiesGapBlocks": 1,
                    "stageSyncBodiesReadyGapBlocks": 10,
                    "stageSyncImportExecutionLagBlocks": 5,
                    "stageSyncExecutionCommitmentLagBlocks": 5,
                    "stageSyncCommitmentFinishLagBlocks": 10,
                    "stageSyncFinishHeadLagBlocks": 50,
                    "stageSyncPipelineLagBlocks": 80,
                    "stageSyncBottleneck": "inventory-bodies",
                    "stageSyncBottleneckLagBlocks": 1,
                    "height": 1000,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "stageSyncInventoryBodiesGapBlocks=1, want 20 "
                "from stageSyncInventory-stageSyncBodies",
                proc.stderr,
            )
            self.assertIn(
                "stageSyncPipelineLagBlocks=80, want 100 from sync stage lag fields",
                proc.stderr,
            )
            self.assertIn(
                "stageSyncBottleneck='inventory-bodies', want 'finish-head' "
                "from sync stage lag fields",
                proc.stderr,
            )
            self.assertIn(
                "stageSyncBottleneckLagBlocks=1, want 50 from sync stage lag fields",
                proc.stderr,
            )

    def test_rejects_inventory_interval_metric_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row.update(
                {
                    "intervalBlocks": 30,
                    "intervalSeconds": 10,
                    "intervalStageSyncInventoryBlocks": 15,
                    "intervalStageSyncInventoryBlocksPerSecond": 2.0,
                    "intervalStageSyncInventoryBlocksPerMinute": 60.0,
                    "intervalStageSyncInventoryToTargetRatio": 0.4,
                    "intervalStageSyncBodiesBlocks": 20,
                    "intervalStageSyncBodiesToInventoryRatio": 1.0,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "intervalStageSyncInventoryToTargetRatio=0.4, want 0.5 "
                "from intervalStageSyncInventoryBlocks/intervalBlocks",
                proc.stderr,
            )
            self.assertIn(
                "intervalStageSyncBodiesToInventoryRatio=1, want 1.33333 "
                "from intervalStageSyncBodiesBlocks/intervalStageSyncInventoryBlocks",
                proc.stderr,
            )
            self.assertIn(
                "intervalStageSyncInventoryBlocksPerSecond=2, want 1.5 "
                "from intervalStageSyncInventoryBlocks/intervalSeconds",
                proc.stderr,
            )
            self.assertIn(
                "intervalStageSyncInventoryBlocksPerMinute=60, want 90 "
                "from intervalStageSyncInventoryBlocksPerSecond*60",
                proc.stderr,
            )

    def test_rejects_caught_up_row_with_ready_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": False,
                        "fullStagedSyncCompleteAtHead": True,
                        "stageSyncPipelineMonotonic": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "fullStagedSyncCompleteBlock": 1000,
                        "fullStagedSyncHeadBlock": 1000,
                        "fullStagedSyncHeadLagBlocks": 0,
                        "fullStagedSyncCompletionRatio": 1.0,
                        "fullStagedSyncPipelineLagBlocks": 0,
                        "fullStagedSyncBottleneck": "none",
                        "fullStagedSyncBottleneckLagBlocks": 0,
                        "fullStagedSyncBottleneckLagShare": -1.0,
                        "stageSyncPipelineLagBlocks": 0,
                        "stageSyncBottleneck": "none",
                        "stageSyncBottleneckLagBlocks": 0,
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                        "height": 1000,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-caught-up",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "fullStagedSyncReady=False, want True for status 'caught-up'",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncCompleteAtHead=True, want False from ready=False",
                proc.stderr,
            )
            self.assertIn("full staged sync is not caught up", proc.stderr)

    def test_accepts_warning_stage_stall_with_consistent_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "warning",
                        "soakHealthIssues": ["stage-stalled"],
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "catching-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": False,
                        "stageSyncPipelineMonotonic": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "fullStagedSyncCompleteBlock": 990,
                        "fullStagedSyncHeadBlock": 1000,
                        "fullStagedSyncHeadLagBlocks": 10,
                        "fullStagedSyncCompletionRatio": 0.99,
                        "fullStagedSyncPipelineLagBlocks": 10,
                        "fullStagedSyncBottleneck": "finish-head",
                        "fullStagedSyncBottleneckLagBlocks": 10,
                        "fullStagedSyncBottleneckLagShare": 1.0,
                        "stageSyncPipelineLagBlocks": 10,
                        "stageSyncBottleneck": "finish-head",
                        "stageSyncBottleneckLagBlocks": 10,
                        "stageStalled": True,
                        "stageStalledCount": 1,
                        "stageStalledStage": "stageSyncExecution",
                        "stageStalledSeconds": 120,
                        "stageStalledLagBlocks": 7,
                        "stageStalls": [
                            {
                                "stage": "stageSyncExecution",
                                "stalledSeconds": 120,
                                "lagBlocks": 7,
                            }
                        ],
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                        "height": 1000,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--allow-warning-health",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_inconsistent_stage_stall_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "warning",
                        "soakHealthIssues": [],
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "catching-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": False,
                        "stageSyncPipelineMonotonic": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "fullStagedSyncCompleteBlock": 990,
                        "fullStagedSyncHeadBlock": 1000,
                        "fullStagedSyncHeadLagBlocks": 10,
                        "fullStagedSyncCompletionRatio": 0.99,
                        "fullStagedSyncPipelineLagBlocks": 10,
                        "fullStagedSyncBottleneck": "finish-head",
                        "fullStagedSyncBottleneckLagBlocks": 10,
                        "fullStagedSyncBottleneckLagShare": 1.0,
                        "stageSyncPipelineLagBlocks": 10,
                        "stageSyncBottleneck": "finish-head",
                        "stageSyncBottleneckLagBlocks": 10,
                        "stageStalled": True,
                        "stageStalledCount": 2,
                        "stageStalledStage": "stageSyncExecution",
                        "stageStalledSeconds": 10,
                        "stageStalledLagBlocks": 5,
                        "stageStalls": [
                            {
                                "stage": "stageSyncImport",
                                "stalledSeconds": 20,
                                "lagBlocks": 7,
                            }
                        ],
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                        "height": 1000,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--allow-warning-health",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("stageStalledCount=2, want len(stageStalls)=1", proc.stderr)
            self.assertIn("stageStalled=true but soakHealthIssues lacks 'stage-stalled'", proc.stderr)
            self.assertIn(
                "stageStalledStage='stageSyncExecution', want primary stalled stage 'stageSyncImport'",
                proc.stderr,
            )
            self.assertIn("stageStalledSeconds=10, want primary stalled seconds 20", proc.stderr)
            self.assertIn("stageStalledLagBlocks=5, want primary stalled lag 7", proc.stderr)

    def test_rejects_fractional_stage_stall_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row.update(
                {
                    "soakHealthStatus": "warning",
                    "soakHealthIssues": ["stage-stalled"],
                    "stageStalled": True,
                    "stageStalledCount": 1.5,
                    "stageStalledStage": "stageSyncExecution",
                    "stageStalledSeconds": 120.5,
                    "stageStalledLagBlocks": 7.5,
                    "stageStalls": [
                        {
                            "stage": "stageSyncExecution",
                            "stalledSeconds": 120.5,
                            "lagBlocks": 7.5,
                        }
                    ],
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--allow-warning-health",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("stageStalledCount=1.5, want non-negative integer", proc.stderr)
            self.assertIn("stageStalledSeconds=120.5, want non-negative integer", proc.stderr)
            self.assertIn("stageStalledLagBlocks=7.5, want integer", proc.stderr)
            self.assertIn(
                "stageStalls entry 'stageSyncExecution' stalledSeconds=120.5, "
                "want non-negative integer",
                proc.stderr,
            )
            self.assertIn(
                "stageStalls entry 'stageSyncExecution' lagBlocks=7.5, "
                "want non-negative integer",
                proc.stderr,
            )

    def test_requires_stage_stall_evidence_when_requested(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 1000,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--no-require-stage-status",
                    "--require-stage-stall-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "stage stall evidence missing fields: "
                "stageStalled,stageStalledCount,stageStalledStage,"
                "stageStalledSeconds,stageStalledLagBlocks,stageStalls",
                proc.stderr,
            )

    def test_rejects_ready_row_without_full_stage_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "stageSyncPipelineMonotonic": True,
                        "heightRegressionBlocks": 0,
                        "stageProgressRegressionCount": 0,
                        "stageMismatchRows": 0,
                        "stageMissingCanonicalRows": 0,
                        "stageStagedBodyIssueRows": 0,
                        "stageIssueRows": 0,
                        "stageOrderIssueRows": 0,
                        "stageSyncPipelineViolationCount": 0,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("fullStagedSyncRequiredStages=None", proc.stderr)
            self.assertIn("fullStagedSyncStageCount=None, want 7", proc.stderr)
            self.assertIn("fullStagedSyncMissingStages=None, want []", proc.stderr)

    def test_requires_stage_detail_evidence_when_requested(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(result, [clean_full_staged_sync_row()])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-stage-detail-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("fullStagedSyncStageDetails is missing", proc.stderr)

    def test_rejects_fractional_full_staged_sync_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["fullStagedSyncStageCount"] = 6.5
            row["fullStagedSyncHeadBlock"] = 1000.5
            row["fullStagedSyncHeadLagBlocks"] = 0.5
            row["fullStagedSyncStageDetails"] = full_stage_details(
                blocks={"SyncFinish": 1000.5}
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-stage-detail-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("fullStagedSyncStageCount=6.5, want 7", proc.stderr)
            self.assertIn(
                "fullStagedSyncHeadBlock=1000.5, want non-negative integer",
                proc.stderr,
            )
            self.assertIn(
                "fullStagedSyncHeadLagBlocks=0.5, want non-negative integer",
                proc.stderr,
            )
            self.assertIn(
                "SyncFinish detail block=1000.5, want non-negative integer",
                proc.stderr,
            )

    def test_rejects_mismatched_stage_detail_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["fullStagedSyncStageDetails"] = full_stage_details(
                blocks={"SyncFinish": 900},
                verified={"SyncExecution": "mismatch"},
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--require-stage-detail-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("fullStagedSyncVerifiedStageCount=7, want detail-derived 6", proc.stderr)
            self.assertIn(
                "fullStagedSyncHashIssues=[], want detail-derived "
                "[{'stage': 'SyncExecution', 'verified': 'mismatch'}]",
                proc.stderr,
            )
            self.assertIn("SyncFinish detail block=900, want stageSyncFinish=1000", proc.stderr)
            self.assertIn(
                "fullStagedSyncCompleteBlock=1000, want SyncFinish detail block=900",
                proc.stderr,
            )

    def test_rejects_stage_order_and_offline_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "height-mismatch",
                        "soakHealthStatus": "critical",
                        "stageStatusFileStatus": "missing",
                        "fullStagedSyncStatus": "pipeline-violation",
                        "fullStagedSyncReady": False,
                        "stageSyncPipelineMonotonic": False,
                        "heightRegressionBlocks": 3,
                        "stageMismatchRows": 1,
                        "stageStagedBodyIssueRows": 1,
                        "stageIssueRows": 2,
                        "stageOrderIssueRows": 1,
                        "stageSyncPipelineViolationCount": 1,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "error",
                        "offlineDbCheckPrometheusStatus": "error",
                        "height": 999,
                        "fullStagedSyncHeadLagBlocks": 500,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                    "--require-caught-up",
                    "--min-height",
                    "1000",
                    "--max-lag-blocks",
                    "10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            for want in (
                "sampleStatus='height-mismatch'",
                "stageStatusFileStatus='missing'",
                "full staged sync is not caught up",
                "stageSyncPipelineMonotonic=false",
                "stageOrderIssueRows=1",
                "offlineDbCheckStatus='error'",
                "height=999",
                "fullStagedSyncHeadLagBlocks=500",
            ):
                self.assertIn(want, proc.stderr)

    def test_rejects_non_integer_zero_issue_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["heightRegressionBlocks"] = 0.5
            row["stageMismatchRows"] = -1
            row["stageSyncPipelineViolationCount"] = 0.0
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "heightRegressionBlocks=0.5, want non-negative integer zero",
                proc.stderr,
            )
            self.assertIn(
                "stageMismatchRows=-1, want non-negative integer zero",
                proc.stderr,
            )
            self.assertNotIn("stageSyncPipelineViolationCount=0", proc.stderr)

    def test_rejects_fractional_height_and_lag_threshold_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "samples.jsonl"
            row = clean_full_staged_sync_row()
            row["height"] = 1000.5
            row["fullStagedSyncHeadLagBlocks"] = 0.5
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--network",
                    "nile",
                    "--mode",
                    "full",
                    "--min-height",
                    "1000",
                    "--max-lag-blocks",
                    "10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("height=1000.5, want non-negative integer", proc.stderr)
            self.assertIn(
                "fullStagedSyncHeadLagBlocks=0.5, want non-negative integer",
                proc.stderr,
            )

    def test_rejects_offline_prometheus_artifact_without_issue_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing gtron_storage_alert_issue", proc.stderr)

    def test_rejects_prometheus_alert_status_for_wrong_datadir(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/other"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "datadir": "/tmp/nile",
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing gtron_storage_alert_status", proc.stderr)

    def test_rejects_prometheus_alert_status_value_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "storageAlertStatus": "critical",
                        "datadir": "/tmp/nile",
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("gtron_storage_alert_status=0, want 2", proc.stderr)

    def test_rejects_offline_prometheus_prune_boundary_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_signed_cold_prune gauge\n'
                '# TYPE gtron_storage_prune_boundary_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n'
                'gtron_storage_signed_cold_prune{datadir="/tmp/nile"} 1\n'
                'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="chainLookupPruneToBlock"} 40\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            row = add_clean_storage_alerts(clean_full_staged_sync_row())
            row.update(
                {
                    "offlineDbCheck": True,
                    "offlineDbCheckStatus": "ok",
                    "offlineDbCheckPrometheusStatus": "ok",
                    "offlineDbCheckPrometheus": str(prom),
                    "datadir": "/tmp/nile",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "tailPrunedThroughBlock": 45,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "gtron_storage_prune_boundary_block field='chainLookupPruneToBlock'=40, want 50",
                proc.stderr,
            )
            self.assertIn(
                "missing gtron_storage_prune_boundary_block field='tailPrunedThroughBlock'",
                proc.stderr,
            )

    def test_rejects_fractional_offline_prometheus_prune_boundary_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_signed_cold_prune gauge\n'
                '# TYPE gtron_storage_prune_boundary_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n'
                'gtron_storage_signed_cold_prune{datadir="/tmp/nile"} 1\n'
                'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="chainLookupPruneToBlock"} 50.5\n'
                'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="tailPrunedThroughBlock"} 45.5\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            row = add_clean_storage_alerts(clean_full_staged_sync_row())
            row.update(
                {
                    "offlineDbCheck": True,
                    "offlineDbCheckStatus": "ok",
                    "offlineDbCheckPrometheusStatus": "ok",
                    "offlineDbCheckPrometheus": str(prom),
                    "datadir": "/tmp/nile",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50.5,
                    "tailPrunedThroughBlock": 45.5,
                }
            )
            write_result(result, [row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "chainLookupPruneToBlock=50.5, want integer for prometheus prune boundary evidence",
                proc.stderr,
            )
            self.assertIn(
                "tailPrunedThroughBlock=45.5, want integer for prometheus prune boundary evidence",
                proc.stderr,
            )

    def test_rejects_offline_prometheus_artifact_missing_structured_issue_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "stageVerifyDetails": [
                            {
                                "severity": "critical",
                                "kind": "stage-verification",
                                "detail": "Finish verified=missing-canonical",
                            }
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("component='stage'", proc.stderr)
            self.assertIn("kind='stage-verification'", proc.stderr)

    def test_rejects_offline_prometheus_artifact_missing_stage_pipeline_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 1000,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 990,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing gtron_storage_stage_pipeline_pending", proc.stderr)
            self.assertIn("missing gtron_storage_stage_pipeline_next_target_block", proc.stderr)
            self.assertIn("missing gtron_storage_stage_pipeline_next_current_block", proc.stderr)

    def test_rejects_prometheus_stage_pipeline_for_wrong_datadir(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_stage_pipeline_complete gauge\n'
                '# TYPE gtron_storage_stage_pipeline_pending gauge\n'
                '# TYPE gtron_storage_stage_pipeline_issues gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_target_block gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_current_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/other"} 0\n'
                'gtron_storage_stage_pipeline_complete{datadir="/tmp/other"} 0\n'
                'gtron_storage_stage_pipeline_pending{datadir="/tmp/other"} 2\n'
                'gtron_storage_stage_pipeline_issues{datadir="/tmp/other"} 0\n'
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/other",stage="SnapshotBuild",status="missing",upstream="Finish"} 1000\n'
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/other",stage="SnapshotBuild",status="missing",upstream="Finish"} 990\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "fullStagedSyncRequiredStages": list(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncPresentStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncVerifiedStageCount": len(REQUIRED_SYNC_STAGES),
                        "fullStagedSyncMissingStages": [],
                        "fullStagedSyncHashIssues": [],
                        "fullStagedSyncUnverifiedStages": [],
                        "fullStagedSyncStageCoverageRatio": 1.0,
                        "fullStagedSyncVerificationRatio": 1.0,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "datadir": "/tmp/nile",
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 1000,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 990,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing gtron_storage_stage_pipeline_pending", proc.stderr)
            self.assertIn("missing next pipeline target", proc.stderr)

    def test_rejects_offline_prometheus_artifact_mismatched_stage_pipeline_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_stage_pipeline_complete gauge\n'
                '# TYPE gtron_storage_stage_pipeline_pending gauge\n'
                '# TYPE gtron_storage_stage_pipeline_issues gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_target_block gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_current_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_complete{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_pending{datadir="/tmp/nile"} 3\n'
                'gtron_storage_stage_pipeline_issues{datadir="/tmp/nile"} 1\n'
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/nile",stage="SnapshotBuild",status="missing",upstream="Finish"} 999\n'
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/nile",stage="SnapshotBuild",status="missing",upstream="Finish"} 998\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 1000,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 990,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("gtron_storage_stage_pipeline_pending=3, want 2", proc.stderr)
            self.assertIn("gtron_storage_stage_pipeline_issues=1, want 0", proc.stderr)
            self.assertIn("value=999, want 1000", proc.stderr)
            self.assertIn("value=998, want 990", proc.stderr)

    def test_rejects_fractional_offline_prometheus_stage_pipeline_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "storage-alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_stage_pipeline_complete gauge\n'
                '# TYPE gtron_storage_stage_pipeline_pending gauge\n'
                '# TYPE gtron_storage_stage_pipeline_issues gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_target_block gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_current_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_complete{datadir="/tmp/nile"} 0\n'
                'gtron_storage_stage_pipeline_pending{datadir="/tmp/nile"} 2.5\n'
                'gtron_storage_stage_pipeline_issues{datadir="/tmp/nile"} 0.5\n'
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/nile",stage="SnapshotBuild",status="missing",upstream="Finish"} 1000.5\n'
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/nile",stage="SnapshotBuild",status="missing",upstream="Finish"} 990.5\n',
                encoding="utf-8",
            )
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "full",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "offlineDbCheck": True,
                        "offlineDbCheckStatus": "ok",
                        "offlineDbCheckPrometheusStatus": "ok",
                        "offlineDbCheckPrometheus": str(prom),
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2.5,
                        "stageAlertPipelineIssues": 0.5,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 1000.5,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 990.5,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-offline-db-check",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("stageAlertPipelinePending=2.5, want non-negative integer", proc.stderr)
            self.assertIn("stageAlertPipelineIssues=0.5, want non-negative integer", proc.stderr)
            self.assertIn("stageAlertPipelineNextTarget=1000.5, want non-negative integer", proc.stderr)
            self.assertIn("stageAlertPipelineNextCurrent=990.5, want non-negative integer", proc.stderr)

    def test_accepts_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 13,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBlockTransactionCountByNumber",
                            "eth_getUncleCountByBlockNumber",
                            "eth_getUncleByBlockNumberAndIndex",
                            "eth_getBlockReceipts",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                            "eth_getBlockByHash",
                            "eth_getBlockTransactionCountByHash",
                            "eth_getUncleCountByBlockHash",
                            "eth_getUncleByBlockHashAndIndex",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_accepts_archive_api_depth_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 1000,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 13,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 850,
                        "archiveApiDepthBlocks": 150,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBlockTransactionCountByNumber",
                            "eth_getUncleCountByBlockNumber",
                            "eth_getUncleByBlockNumberAndIndex",
                            "eth_getBlockReceipts",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                            "eth_getBlockByHash",
                            "eth_getBlockTransactionCountByHash",
                            "eth_getUncleCountByBlockHash",
                            "eth_getUncleByBlockHashAndIndex",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                    "--min-archive-api-depth-blocks",
                    "100",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_rejects_archive_api_depth_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 5,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiDepthBlocks": 2,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiDepthBlocks=2, want height - archiveApiBlock = 1",
                proc.stderr,
            )

    def test_rejects_archive_api_depth_below_threshold(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 5,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                    "--min-archive-api-depth-blocks",
                    "10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiBlock depth=1 failed >= min archive API depth 10 blocks",
                proc.stderr,
            )

    def test_rejects_archive_api_depth_without_height(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 5,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                    "--min-archive-api-depth-blocks",
                    "10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archive API depth evidence requires numeric height", proc.stderr)

    def test_accepts_archive_tx_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 17,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBlockTransactionCountByNumber",
                            "eth_getUncleCountByBlockNumber",
                            "eth_getUncleByBlockNumberAndIndex",
                            "eth_getBlockReceipts",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                            "eth_getBlockByHash",
                            "eth_getBlockTransactionCountByHash",
                            "eth_getUncleCountByBlockHash",
                            "eth_getUncleByBlockHashAndIndex",
                            "eth_getTransactionByHash",
                            "eth_getTransactionReceipt",
                            "eth_getTransactionByBlockNumberAndIndex",
                            "eth_getTransactionByBlockHashAndIndex",
                        ],
                        "archiveApiTxProbe": True,
                        "archiveApiTxHash": "0x" + "ab" * 32,
                        "archiveApiTxMethods": [
                            "eth_getTransactionByHash",
                            "eth_getTransactionReceipt",
                            "eth_getTransactionByBlockNumberAndIndex",
                            "eth_getTransactionByBlockHashAndIndex",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

    def test_requires_archive_trace_transaction_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            base_row = {
                "unix": 10,
                "network": "nile",
                "mode": "minimal",
                "sampleStatus": "ok",
                "soakHealthStatus": "ok",
                "fullStagedSyncStatus": "caught-up",
                "fullStagedSyncReady": True,
                "fullStagedSyncCompleteAtHead": True,
                "height": 100,
                "archiveApiStatus": "ok",
                "archiveApiChecks": 18,
                "archiveApiFailures": 0,
                "archiveApiBlock": 99,
                "archiveApiTraceTransactionProbe": True,
                "archiveApiMethods": [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getUncleCountByBlockNumber",
                    "eth_getUncleByBlockNumberAndIndex",
                    "eth_getBlockReceipts",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockByHash",
                    "eth_getBlockTransactionCountByHash",
                    "eth_getUncleCountByBlockHash",
                    "eth_getUncleByBlockHashAndIndex",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                    "debug_traceTransaction",
                ],
                "archiveApiTxProbe": True,
                "archiveApiTxHash": "0x" + "ab" * 32,
                "archiveApiTxMethods": [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                    "debug_traceTransaction",
                ],
            }
            write_result(result, [base_row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

            missing_trace = dict(base_row)
            missing_trace["archiveApiChecks"] = 17
            missing_trace["archiveApiMethods"] = [
                method for method in base_row["archiveApiMethods"] if method != "debug_traceTransaction"
            ]
            missing_trace["archiveApiTxMethods"] = [
                method for method in base_row["archiveApiTxMethods"] if method != "debug_traceTransaction"
            ]
            write_result(result, [missing_trace])
            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiMethods missing required methods: debug_traceTransaction",
                proc.stderr,
            )
            self.assertIn(
                "archiveApiTxMethods missing required methods: debug_traceTransaction",
                proc.stderr,
            )

            trace_not_requested = dict(base_row)
            trace_not_requested["archiveApiTraceTransactionProbe"] = False
            write_result(result, [trace_not_requested])
            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiTraceTransactionProbe is not true", proc.stderr)

    def test_requires_archive_trace_block_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            base_row = {
                "unix": 10,
                "network": "nile",
                "mode": "minimal",
                "sampleStatus": "ok",
                "soakHealthStatus": "ok",
                "fullStagedSyncStatus": "caught-up",
                "fullStagedSyncReady": True,
                "fullStagedSyncCompleteAtHead": True,
                "height": 100,
                "archiveApiStatus": "ok",
                "archiveApiChecks": 15,
                "archiveApiFailures": 0,
                "archiveApiBlock": 99,
                "archiveApiTraceBlockProbe": True,
                "archiveApiMethods": [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getUncleCountByBlockNumber",
                    "eth_getUncleByBlockNumberAndIndex",
                    "eth_getBlockReceipts",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockByHash",
                    "eth_getBlockTransactionCountByHash",
                    "eth_getUncleCountByBlockHash",
                    "eth_getUncleByBlockHashAndIndex",
                    "debug_traceBlockByNumber",
                    "debug_traceBlockByHash",
                ],
            }
            write_result(result, [base_row])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-trace-block",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

            trace_not_requested = dict(base_row)
            trace_not_requested["archiveApiTraceBlockProbe"] = False
            write_result(result, [trace_not_requested])
            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-trace-block",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiTraceBlockProbe is not true", proc.stderr)

    def test_rejects_archive_tx_evidence_without_transaction_probe(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 5,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                        "archiveApiTxProbe": False,
                        "archiveApiTxHash": "",
                        "archiveApiTxMethods": [],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiMethods missing required methods", proc.stderr)
            self.assertIn("archiveApiTxProbe is not true", proc.stderr)
            self.assertIn("archiveApiTxHash is missing", proc.stderr)
            self.assertIn("archiveApiTxMethods must be a non-empty list", proc.stderr)

    def test_rejects_archive_tx_evidence_short_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 7,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                            "eth_getTransactionByHash",
                            "eth_getTransactionReceipt",
                        ],
                        "archiveApiTxProbe": True,
                        "archiveApiTxHash": "0xabc",
                        "archiveApiTxMethods": [
                            "eth_getTransactionByHash",
                            "eth_getTransactionReceipt",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiTxHash must be a 0x-prefixed 32-byte hash", proc.stderr)

    def test_rejects_archive_tx_evidence_without_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiTxProbe": True,
                        "archiveApiTxHash": "0x" + "ab" * 32,
                        "archiveApiTxMethods": [
                            "eth_getTransactionByHash",
                            "eth_getTransactionReceipt",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archive API evidence is missing for archive tx evidence", proc.stderr)

    def test_rejects_archive_tx_evidence_missing_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 16,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBlockTransactionCountByNumber",
                            "eth_getUncleCountByBlockNumber",
                            "eth_getUncleByBlockNumberAndIndex",
                            "eth_getBlockReceipts",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                            "eth_getBlockByHash",
                            "eth_getBlockTransactionCountByHash",
                            "eth_getUncleCountByBlockHash",
                            "eth_getUncleByBlockHashAndIndex",
                            "eth_getTransactionByHash",
                            "eth_getTransactionByBlockNumberAndIndex",
                            "eth_getTransactionByBlockHashAndIndex",
                        ],
                        "archiveApiTxProbe": True,
                        "archiveApiTxHash": "0x" + "ab" * 32,
                        "archiveApiTxMethods": [
                            "eth_getTransactionByHash",
                            "eth_getTransactionByBlockNumberAndIndex",
                            "eth_getTransactionByBlockHashAndIndex",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiMethods missing required methods: eth_getTransactionReceipt",
                proc.stderr,
            )
            self.assertIn(
                "archiveApiTxMethods missing required methods: eth_getTransactionReceipt",
                proc.stderr,
            )

    def test_rejects_invalid_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "failed",
                        "archiveApiChecks": 0,
                        "archiveApiFailures": 1,
                        "archiveApiBlock": 100,
                        "archiveApiMethods": ["eth_getBalance"],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                    "--archive-api-method",
                    "eth_call",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiStatus='failed', want 'ok'", proc.stderr)
            self.assertIn("archiveApiChecks=0, want positive integer", proc.stderr)
            self.assertIn("archiveApiFailures=1, want 0", proc.stderr)
            self.assertIn("archiveApiBlock=100 must be below height=100", proc.stderr)
            self.assertIn("archiveApiMethods missing required methods", proc.stderr)

    def test_rejects_fractional_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 13,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 80.5,
                        "archiveApiDepthBlocks": 19.5,
                        "chainLookupPruneToBlock": 80.5,
                        "tailPrunedThroughBlock": 75.5,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBlockByHash",
                            "eth_getBlockTransactionCountByNumber",
                            "eth_getBlockTransactionCountByHash",
                            "eth_getUncleCountByBlockNumber",
                            "eth_getUncleCountByBlockHash",
                            "eth_getUncleByBlockNumberAndIndex",
                            "eth_getUncleByBlockHashAndIndex",
                            "eth_getBlockReceipts",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiBlock=80.5, want non-negative integer historical block",
                proc.stderr,
            )
            self.assertIn("chainLookupPruneToBlock=80.5, want integer", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=75.5, want integer", proc.stderr)

    def test_rejects_archive_api_check_count_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "minimal",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 2,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "minimal",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiChecks=2 must equal successful archiveApiMethods=5 when archiveApiFailures=0",
                proc.stderr,
            )

    def test_rejects_archive_api_probe_above_chain_lookup_prune_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "network": "nile",
                        "mode": "blocks",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "fullStagedSyncStatus": "caught-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": True,
                        "height": 100,
                        "chainLookupPruneToBlock": 50,
                        "archiveApiStatus": "ok",
                        "archiveApiChecks": 5,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                        ],
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--mode",
                    "blocks",
                    "--no-require-stage-status",
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "archiveApiBlock=99 must be <= chainLookupPruneToBlock=50",
                proc.stderr,
            )


if __name__ == "__main__":
    unittest.main()
