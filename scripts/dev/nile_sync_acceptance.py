#!/usr/bin/env python3
"""Validate nile_sync_sample.sh JSONL output for staged-sync acceptance."""

import argparse
import json
import re
import sys
from pathlib import Path


ZERO_ISSUE_FIELDS = (
    "heightRegressionBlocks",
    "stageProgressRegressionCount",
    "stageMismatchRows",
    "stageMissingCanonicalRows",
    "stageStagedBodyIssueRows",
    "stageIssueRows",
    "stageOrderIssueRows",
    "stageSyncPipelineViolationCount",
)

PROMETHEUS_REQUIRED_SNIPPETS = (
    ("gtron_storage_alert_status{", "gtron_storage_alert_status"),
    ("# TYPE gtron_storage_alert_issue gauge", "gtron_storage_alert_issue"),
)

PROMETHEUS_PRUNE_BOUNDARY_FIELDS = (
    "coldFreezerToBlock",
    "chainLookupPruneToBlock",
    "tailPrunedThroughBlock",
    "balanceTracePruneToBlock",
    "sectionBloomPruneToSection",
)

SAMPLE_PROMETHEUS_REQUIRED_SNIPPETS = (
    ("gtron_nile_sync_sample_status{", "gtron_nile_sync_sample_status"),
    ("gtron_nile_sync_soak_health_status{", "gtron_nile_sync_soak_health_status"),
    ("gtron_nile_sync_stage_status_source{", "gtron_nile_sync_stage_status_source"),
    (
        "gtron_nile_sync_stage_status_collection_status{",
        "gtron_nile_sync_stage_status_collection_status",
    ),
    ("gtron_nile_sync_height{", "gtron_nile_sync_height"),
)

STATE_PREFETCH_EVIDENCE_FIELDS = (
    "syncLogStatePrefetchEnqueued",
    "syncLogStatePrefetchDropped",
    "syncLogStatePrefetchProcessed",
    "syncLogStatePrefetchHits",
    "syncLogStatePrefetchMisses",
    "syncLogStatePrefetchErrors",
)

STATE_PREFETCH_PROMETHEUS_FIELD_METRICS = (
    ("gtron_nile_sync_log_state_prefetch_enqueued", "syncLogStatePrefetchEnqueued"),
    ("gtron_nile_sync_log_state_prefetch_dropped", "syncLogStatePrefetchDropped"),
    ("gtron_nile_sync_log_state_prefetch_processed", "syncLogStatePrefetchProcessed"),
    ("gtron_nile_sync_log_state_prefetch_hits", "syncLogStatePrefetchHits"),
    ("gtron_nile_sync_log_state_prefetch_misses", "syncLogStatePrefetchMisses"),
    ("gtron_nile_sync_log_state_prefetch_errors", "syncLogStatePrefetchErrors"),
)

SYNC_PHASE_CURSOR_INTEGER_FIELDS = (
    "syncLogPhaseCursorCompletedPhases",
    "syncLogPhaseCursorScheduledPhases",
    "syncLogPhaseCursorIncompletePhases",
    "syncLogPhaseCursorCompletedTasks",
    "syncLogPhaseCursorScheduledTasks",
    "syncLogPhaseCursorCurrentTaskIndex",
    "syncLogPhaseCursorCurrentTaskCount",
    "syncLogPhaseCursorCurrentTaskRemaining",
    "syncLogPhaseCursorCurrentFromBlock",
    "syncLogPhaseCursorCurrentToBlock",
    "syncLogPhaseCursorNextBlock",
)

SYNC_PHASE_CURSOR_PROMETHEUS_FIELD_METRICS = (
    (
        "gtron_nile_sync_log_phase_cursor_completed_phases",
        "syncLogPhaseCursorCompletedPhases",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_scheduled_phases",
        "syncLogPhaseCursorScheduledPhases",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_incomplete_phases",
        "syncLogPhaseCursorIncompletePhases",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_completed_tasks",
        "syncLogPhaseCursorCompletedTasks",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_scheduled_tasks",
        "syncLogPhaseCursorScheduledTasks",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_current_task_index",
        "syncLogPhaseCursorCurrentTaskIndex",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_current_task_count",
        "syncLogPhaseCursorCurrentTaskCount",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_current_task_remaining",
        "syncLogPhaseCursorCurrentTaskRemaining",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_current_from_block",
        "syncLogPhaseCursorCurrentFromBlock",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_current_to_block",
        "syncLogPhaseCursorCurrentToBlock",
    ),
    (
        "gtron_nile_sync_log_phase_cursor_next_block",
        "syncLogPhaseCursorNextBlock",
    ),
)

EVENT_LOG_INDEX_RANGE_FIELDS = (
    "eventLogIndexFromBlock",
    "eventLogIndexToBlock",
)

EVENT_LOG_INDEX_LOOKUP_FIELDS = (
    "eventLogIndexAddressKeys",
    "eventLogIndexAddressPostings",
    "eventLogIndexAddressAvgPostingsMilli",
    "eventLogIndexAddressMaxPostings",
    "eventLogIndexAddressSingletonKeys",
    "eventLogIndexAddressMultiPostingKeys",
    "eventLogIndexTopicKeys",
    "eventLogIndexTopicPostings",
    "eventLogIndexTopicAvgPostingsMilli",
    "eventLogIndexTopicMaxPostings",
    "eventLogIndexTopicSingletonKeys",
    "eventLogIndexTopicMultiPostingKeys",
)

EVENT_LOG_INDEX_EVIDENCE_FIELDS = (
    "eventLogIndexStatsStatus",
    "eventLogIndexSegments",
    *EVENT_LOG_INDEX_RANGE_FIELDS,
    *EVENT_LOG_INDEX_LOOKUP_FIELDS,
)

EVENT_LOG_INDEX_PROMETHEUS_FIELD_METRICS = (
    ("gtron_nile_sync_event_log_index_segments", "eventLogIndexSegments"),
    ("gtron_nile_sync_event_log_index_from_block", "eventLogIndexFromBlock"),
    ("gtron_nile_sync_event_log_index_to_block", "eventLogIndexToBlock"),
    ("gtron_nile_sync_event_log_index_address_keys", "eventLogIndexAddressKeys"),
    ("gtron_nile_sync_event_log_index_address_postings", "eventLogIndexAddressPostings"),
    (
        "gtron_nile_sync_event_log_index_address_avg_postings_milli",
        "eventLogIndexAddressAvgPostingsMilli",
    ),
    ("gtron_nile_sync_event_log_index_address_max_postings", "eventLogIndexAddressMaxPostings"),
    (
        "gtron_nile_sync_event_log_index_address_singleton_keys",
        "eventLogIndexAddressSingletonKeys",
    ),
    (
        "gtron_nile_sync_event_log_index_address_multi_posting_keys",
        "eventLogIndexAddressMultiPostingKeys",
    ),
    ("gtron_nile_sync_event_log_index_topic_keys", "eventLogIndexTopicKeys"),
    ("gtron_nile_sync_event_log_index_topic_postings", "eventLogIndexTopicPostings"),
    (
        "gtron_nile_sync_event_log_index_topic_avg_postings_milli",
        "eventLogIndexTopicAvgPostingsMilli",
    ),
    ("gtron_nile_sync_event_log_index_topic_max_postings", "eventLogIndexTopicMaxPostings"),
    (
        "gtron_nile_sync_event_log_index_topic_singleton_keys",
        "eventLogIndexTopicSingletonKeys",
    ),
    (
        "gtron_nile_sync_event_log_index_topic_multi_posting_keys",
        "eventLogIndexTopicMultiPostingKeys",
    ),
)

SAMPLE_PROMETHEUS_FIELD_METRICS = (
    ("gtron_nile_sync_height", "height"),
    ("gtron_nile_sync_target_lag_blocks", "syncTargetLagBlocks"),
    *STATE_PREFETCH_PROMETHEUS_FIELD_METRICS,
    *SYNC_PHASE_CURSOR_PROMETHEUS_FIELD_METRICS,
    ("gtron_nile_sync_full_staged_sync_head_lag_blocks", "fullStagedSyncHeadLagBlocks"),
    ("gtron_nile_sync_full_staged_sync_ready", "fullStagedSyncReady"),
    ("gtron_nile_sync_full_staged_sync_complete_at_head", "fullStagedSyncCompleteAtHead"),
    ("gtron_nile_sync_full_staged_sync_complete_block", "fullStagedSyncCompleteBlock"),
    ("gtron_nile_sync_full_staged_sync_head_block", "fullStagedSyncHeadBlock"),
    ("gtron_nile_sync_full_staged_sync_completion_ratio", "fullStagedSyncCompletionRatio"),
    ("gtron_nile_sync_full_staged_sync_pipeline_lag_blocks", "fullStagedSyncPipelineLagBlocks"),
    ("gtron_nile_sync_full_staged_sync_bottleneck_lag_blocks", "fullStagedSyncBottleneckLagBlocks"),
    ("gtron_nile_sync_full_staged_sync_bottleneck_lag_share", "fullStagedSyncBottleneckLagShare"),
    ("gtron_nile_sync_full_staged_sync_stage_count", "fullStagedSyncStageCount"),
    ("gtron_nile_sync_full_staged_sync_present_stage_count", "fullStagedSyncPresentStageCount"),
    ("gtron_nile_sync_full_staged_sync_verified_stage_count", "fullStagedSyncVerifiedStageCount"),
    ("gtron_nile_sync_full_staged_sync_stage_coverage_ratio", "fullStagedSyncStageCoverageRatio"),
    ("gtron_nile_sync_full_staged_sync_verification_ratio", "fullStagedSyncVerificationRatio"),
    ("gtron_nile_sync_stage_sync_inventory_bodies_gap_blocks", "stageSyncInventoryBodiesGapBlocks"),
    ("gtron_nile_sync_stage_sync_bodies_ready_gap_blocks", "stageSyncBodiesReadyGapBlocks"),
    ("gtron_nile_sync_stage_sync_import_execution_lag_blocks", "stageSyncImportExecutionLagBlocks"),
    ("gtron_nile_sync_stage_sync_execution_commitment_lag_blocks", "stageSyncExecutionCommitmentLagBlocks"),
    ("gtron_nile_sync_stage_sync_commitment_finish_lag_blocks", "stageSyncCommitmentFinishLagBlocks"),
    ("gtron_nile_sync_stage_sync_finish_head_lag_blocks", "stageSyncFinishHeadLagBlocks"),
    ("gtron_nile_sync_stage_sync_pipeline_lag_blocks", "stageSyncPipelineLagBlocks"),
    ("gtron_nile_sync_stage_sync_bottleneck_lag_blocks", "stageSyncBottleneckLagBlocks"),
    ("gtron_nile_sync_stage_chain_freezer_head_lag_blocks", "stageChainFreezerHeadLagBlocks"),
    ("gtron_nile_sync_stage_snapshot_event_log_build_head_lag_blocks", "stageSnapshotEventLogBuildHeadLagBlocks"),
    ("gtron_nile_sync_interval_stage_sync_inventory_blocks_per_second", "intervalStageSyncInventoryBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_sync_bodies_blocks_per_second", "intervalStageSyncBodiesBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_sync_bodies_ready_blocks_per_second", "intervalStageSyncBodiesReadyBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_sync_import_blocks_per_second", "intervalStageSyncImportBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_sync_execution_blocks_per_second", "intervalStageSyncExecutionBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_sync_commitment_blocks_per_second", "intervalStageSyncCommitmentBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_sync_finish_blocks_per_second", "intervalStageSyncFinishBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_chain_freezer_blocks_per_second", "intervalStageChainFreezerBlocksPerSecond"),
    ("gtron_nile_sync_interval_stage_snapshot_event_log_build_blocks_per_second", "intervalStageSnapshotEventLogBuildBlocksPerSecond"),
    ("gtron_nile_sync_datadir_bytes", "datadirBytes"),
    ("gtron_nile_sync_chaindata_bytes", "chaindataBytes"),
    ("gtron_nile_sync_cold_archive_bytes", "coldArchiveBytes"),
    ("gtron_nile_sync_derived_index_bytes", "derivedIndexBytes"),
    ("gtron_nile_sync_datadir_bytes_per_block", "bytesPerBlock"),
    ("gtron_nile_sync_chaindata_bytes_per_block", "chaindataBytesPerBlock"),
    ("gtron_nile_sync_cold_archive_bytes_per_block", "coldArchiveBytesPerBlock"),
    ("gtron_nile_sync_derived_index_bytes_per_block", "derivedIndexBytesPerBlock"),
    ("gtron_nile_sync_soak_efficiency_datadir_bytes_per_block", "soakEfficiencyDatadirBytesPerBlock"),
    ("gtron_nile_sync_soak_efficiency_hot_bytes_per_block", "soakEfficiencyHotBytesPerBlock"),
    ("gtron_nile_sync_soak_efficiency_cold_archive_bytes_per_block", "soakEfficiencyColdArchiveBytesPerBlock"),
    ("gtron_nile_sync_soak_efficiency_derived_index_bytes_per_block", "soakEfficiencyDerivedIndexBytesPerBlock"),
    ("gtron_nile_sync_snapshot_profile_verify_files", "snapshotProfileVerifyFiles"),
    ("gtron_nile_sync_snapshot_profile_verified_segments", "snapshotProfileVerifiedSegments"),
    ("gtron_nile_sync_snapshot_sidecar_share_milli", "snapshotSidecarShareMilli"),
    ("gtron_nile_sync_snapshot_state_history_bytes", "snapshotStateHistoryBytes"),
    (
        "gtron_nile_sync_snapshot_state_history_compressed_segments",
        "snapshotStateHistoryCompressedSegments",
    ),
    (
        "gtron_nile_sync_snapshot_state_history_compressed_bytes",
        "snapshotStateHistoryCompressedBytes",
    ),
    (
        "gtron_nile_sync_snapshot_state_history_compressed_share_milli",
        "snapshotStateHistoryCompressedShareMilli",
    ),
    ("gtron_nile_sync_snapshot_point_tx_hash_lookup_bytes", "snapshotPointTxHashLookupBytes"),
    ("gtron_nile_sync_snapshot_point_tx_hash_lookup_segments", "snapshotPointTxHashLookupSegments"),
    ("gtron_nile_sync_snapshot_point_tx_hash_lookup_payload_bytes", "snapshotPointTxHashLookupPayloadBytes"),
    ("gtron_nile_sync_snapshot_point_tx_hash_lookup_sidecar_bytes", "snapshotPointTxHashLookupSidecarBytes"),
    ("gtron_nile_sync_snapshot_point_tx_hash_lookup_sidecar_share_milli", "snapshotPointTxHashLookupSidecarShareMilli"),
    (
        "gtron_nile_sync_snapshot_point_tx_hash_lookup_snapshot_share_milli",
        "snapshotPointTxHashLookupSnapshotShareMilli",
    ),
    ("gtron_nile_sync_snapshot_point_event_log_index_bytes", "snapshotPointEventLogIndexBytes"),
    ("gtron_nile_sync_snapshot_point_event_log_index_segments", "snapshotPointEventLogIndexSegments"),
    ("gtron_nile_sync_snapshot_point_event_log_index_payload_bytes", "snapshotPointEventLogIndexPayloadBytes"),
    ("gtron_nile_sync_snapshot_point_event_log_index_sidecar_bytes", "snapshotPointEventLogIndexSidecarBytes"),
    ("gtron_nile_sync_snapshot_point_event_log_index_sidecar_share_milli", "snapshotPointEventLogIndexSidecarShareMilli"),
    (
        "gtron_nile_sync_snapshot_point_event_log_index_snapshot_share_milli",
        "snapshotPointEventLogIndexSnapshotShareMilli",
    ),
    *EVENT_LOG_INDEX_PROMETHEUS_FIELD_METRICS,
    (
        "gtron_nile_sync_snapshot_point_state_history_accessor_bytes",
        "snapshotPointStateHistoryAccessorBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_state_history_accessor_segments",
        "snapshotPointStateHistoryAccessorSegments",
    ),
    (
        "gtron_nile_sync_snapshot_point_state_history_accessor_payload_bytes",
        "snapshotPointStateHistoryAccessorPayloadBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_state_history_accessor_sidecar_bytes",
        "snapshotPointStateHistoryAccessorSidecarBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_state_history_accessor_sidecar_share_milli",
        "snapshotPointStateHistoryAccessorSidecarShareMilli",
    ),
    (
        "gtron_nile_sync_snapshot_point_state_history_accessor_snapshot_share_milli",
        "snapshotPointStateHistoryAccessorSnapshotShareMilli",
    ),
    ("gtron_nile_sync_snapshot_point_latest_btree_bytes", "snapshotPointLatestBTreeBytes"),
    ("gtron_nile_sync_snapshot_point_latest_btree_segments", "snapshotPointLatestBTreeSegments"),
    ("gtron_nile_sync_snapshot_point_latest_btree_payload_bytes", "snapshotPointLatestBTreePayloadBytes"),
    ("gtron_nile_sync_snapshot_point_latest_btree_sidecar_bytes", "snapshotPointLatestBTreeSidecarBytes"),
    ("gtron_nile_sync_snapshot_point_latest_btree_sidecar_share_milli", "snapshotPointLatestBTreeSidecarShareMilli"),
    (
        "gtron_nile_sync_snapshot_point_latest_btree_snapshot_share_milli",
        "snapshotPointLatestBTreeSnapshotShareMilli",
    ),
    (
        "gtron_nile_sync_snapshot_point_chain_freezer_accessor_bytes",
        "snapshotPointChainFreezerAccessorBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_chain_freezer_accessor_segments",
        "snapshotPointChainFreezerAccessorSegments",
    ),
    (
        "gtron_nile_sync_snapshot_point_chain_freezer_accessor_payload_bytes",
        "snapshotPointChainFreezerAccessorPayloadBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_chain_freezer_accessor_sidecar_bytes",
        "snapshotPointChainFreezerAccessorSidecarBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_chain_freezer_accessor_sidecar_share_milli",
        "snapshotPointChainFreezerAccessorSidecarShareMilli",
    ),
    (
        "gtron_nile_sync_snapshot_point_chain_freezer_accessor_snapshot_share_milli",
        "snapshotPointChainFreezerAccessorSnapshotShareMilli",
    ),
    ("gtron_nile_sync_snapshot_point_code_domain_bytes", "snapshotPointCodeDomainBytes"),
    ("gtron_nile_sync_snapshot_point_code_domain_segments", "snapshotPointCodeDomainSegments"),
    ("gtron_nile_sync_snapshot_point_code_domain_payload_bytes", "snapshotPointCodeDomainPayloadBytes"),
    ("gtron_nile_sync_snapshot_point_code_domain_sidecar_bytes", "snapshotPointCodeDomainSidecarBytes"),
    ("gtron_nile_sync_snapshot_point_code_domain_sidecar_share_milli", "snapshotPointCodeDomainSidecarShareMilli"),
    (
        "gtron_nile_sync_snapshot_point_code_domain_snapshot_share_milli",
        "snapshotPointCodeDomainSnapshotShareMilli",
    ),
    (
        "gtron_nile_sync_snapshot_point_commitment_snapshot_bytes",
        "snapshotPointCommitmentSnapshotBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_commitment_snapshot_segments",
        "snapshotPointCommitmentSnapshotSegments",
    ),
    (
        "gtron_nile_sync_snapshot_point_commitment_snapshot_payload_bytes",
        "snapshotPointCommitmentSnapshotPayloadBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_commitment_snapshot_sidecar_bytes",
        "snapshotPointCommitmentSnapshotSidecarBytes",
    ),
    (
        "gtron_nile_sync_snapshot_point_commitment_snapshot_sidecar_share_milli",
        "snapshotPointCommitmentSnapshotSidecarShareMilli",
    ),
    (
        "gtron_nile_sync_snapshot_point_commitment_snapshot_snapshot_share_milli",
        "snapshotPointCommitmentSnapshotSnapshotShareMilli",
    ),
    ("gtron_nile_sync_signed_cold_prune", "signedColdPrune"),
    ("gtron_nile_sync_cold_freezer_to_block", "coldFreezerToBlock"),
    ("gtron_nile_sync_chain_lookup_prune_to_block", "chainLookupPruneToBlock"),
    ("gtron_nile_sync_tail_pruned_through_block", "tailPrunedThroughBlock"),
    ("gtron_nile_sync_tail_pruned_files", "tailPrunedFiles"),
    ("gtron_nile_sync_balance_trace_prune_to_block", "balanceTracePruneToBlock"),
    ("gtron_nile_sync_section_bloom_prune_to_section", "sectionBloomPruneToSection"),
    ("gtron_nile_sync_archive_api_checks", "archiveApiChecks"),
    ("gtron_nile_sync_archive_api_block", "archiveApiBlock"),
    ("gtron_nile_sync_archive_api_depth_blocks", "archiveApiDepthBlocks"),
    ("gtron_nile_sync_archive_api_failures", "archiveApiFailures"),
)

SAMPLE_PROMETHEUS_SIGNED_INTEGER_FIELDS = set(PROMETHEUS_PRUNE_BOUNDARY_FIELDS)

SAMPLE_PROMETHEUS_NON_NEGATIVE_INTEGER_FIELDS = {
    "height",
    "syncTargetLagBlocks",
    "fullStagedSyncHeadLagBlocks",
    "fullStagedSyncCompleteBlock",
    "fullStagedSyncHeadBlock",
    "fullStagedSyncPipelineLagBlocks",
    "fullStagedSyncBottleneckLagBlocks",
    "fullStagedSyncStageCount",
    "fullStagedSyncPresentStageCount",
    "fullStagedSyncVerifiedStageCount",
    "stageSyncInventoryBodiesGapBlocks",
    "stageSyncBodiesReadyGapBlocks",
    "stageSyncImportExecutionLagBlocks",
    "stageSyncExecutionCommitmentLagBlocks",
    "stageSyncCommitmentFinishLagBlocks",
    "stageSyncFinishHeadLagBlocks",
    "stageSyncPipelineLagBlocks",
    "stageSyncBottleneckLagBlocks",
    "stageChainFreezerHeadLagBlocks",
    "stageSnapshotEventLogBuildHeadLagBlocks",
    "snapshotProfileVerifiedSegments",
    "snapshotStateHistoryBytes",
    "snapshotStateHistoryCompressedSegments",
    "snapshotStateHistoryCompressedBytes",
    "snapshotPointTxHashLookupSegments",
    "snapshotPointEventLogIndexSegments",
    "snapshotPointStateHistoryAccessorSegments",
    "snapshotPointLatestBTreeSegments",
    "snapshotPointChainFreezerAccessorSegments",
    "snapshotPointCodeDomainSegments",
    "snapshotPointCommitmentSnapshotSegments",
    "tailPrunedFiles",
    "archiveApiChecks",
    "archiveApiBlock",
    "archiveApiDepthBlocks",
    "archiveApiFailures",
    *STATE_PREFETCH_EVIDENCE_FIELDS,
    *SYNC_PHASE_CURSOR_INTEGER_FIELDS,
    "eventLogIndexSegments",
    *EVENT_LOG_INDEX_RANGE_FIELDS,
    *EVENT_LOG_INDEX_LOOKUP_FIELDS,
}

SAMPLE_PROMETHEUS_OPTIONAL_NON_NEGATIVE_INTEGER_FIELDS = {
    *STATE_PREFETCH_EVIDENCE_FIELDS,
    *SYNC_PHASE_CURSOR_INTEGER_FIELDS,
    *EVENT_LOG_INDEX_RANGE_FIELDS,
}

PROMETHEUS_STATUS_VALUES = {
    "ok": 0,
    "warning": 1,
    "critical": 2,
}

FULL_STAGED_SYNC_STATUS_VALUES = {
    "caught-up": 0,
    "catching-up": 1,
    "missing-stage": 2,
    "hash-issue": 3,
    "unverified-stage": 4,
    "pipeline-violation": 5,
    "unknown": 6,
}

STAGE_STATUS_SOURCE_VALUES = {
    "skipped": 0,
    "file": 1,
    "rpc": 2,
    "unknown": 3,
}

STAGE_STATUS_COLLECTION_STATUS_VALUES = {
    "ok": 0,
    "skipped": 1,
    "missing": 2,
    "rpc-error": 3,
    "rpc-invalid": 4,
    "unknown": 5,
}

FULL_STAGED_SYNC_REQUIRED_STAGES = (
    "SyncInventory",
    "SyncBodies",
    "SyncBodiesReady",
    "SyncImport",
    "SyncExecution",
    "SyncCommitment",
    "SyncFinish",
)

FULL_STAGED_SYNC_STAGE_FIELDS = {
    "SyncInventory": "stageSyncInventory",
    "SyncBodies": "stageSyncBodies",
    "SyncBodiesReady": "stageSyncBodiesReady",
    "SyncImport": "stageSyncImport",
    "SyncExecution": "stageSyncExecution",
    "SyncCommitment": "stageSyncCommitment",
    "SyncFinish": "stageSyncFinish",
}

COLD_STAGE_LAG_FIELDS = (
    "stageChainFreezerHeadLagBlocks",
    "stageSnapshotEventLogBuildHeadLagBlocks",
)

CHAIN_FREEZER_MINIMUM_FIELDS = (
    ("debugMetricChainFreezerBlocks", "min chain freezer blocks"),
    ("debugMetricChainFreezerPasses", "min chain freezer passes"),
)

STORAGE_ALERT_STATUS_FIELDS = (
    "storageAlertStatus",
    "freezerAlertStatus",
    "stageVerifyStatus",
    "modeAlertStatus",
    "snapshotAlertStatus",
)

STORAGE_ALERT_ZERO_ISSUE_FIELDS = (
    "freezerAlertIssues",
    "stageVerifyIssues",
    "stageAlertPipelineIssues",
    "modeAlertIssues",
    "snapshotAlertIssues",
)

STARTUP_RECOVERY_ZERO_FIELDS = (
    "syncStartupPipelineOrderIssues",
    "syncStartupPipelineOrderReadErrors",
)

DEFAULT_ARCHIVE_API_METHODS = (
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
)

ARCHIVE_API_TX_METHODS = (
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionByBlockHashAndIndex",
)

ARCHIVE_API_CALL_METHODS = (
    "eth_call",
    "debug_traceCall",
    "eth_estimateGas",
)

ARCHIVE_API_TRACE_TX_METHOD = "debug_traceTransaction"
ARCHIVE_API_TRACE_BLOCK_METHODS = (
    "debug_traceBlockByNumber",
    "debug_traceBlockByHash",
)

ARCHIVE_API_METHOD_SUCCESS_METRIC = "gtron_nile_sync_archive_api_method_success"
ARCHIVE_API_TX_METHOD_SUCCESS_METRIC = "gtron_nile_sync_archive_api_tx_method_success"

ARCHIVE_API_EVIDENCE_FIELDS = (
    "archiveApiStatus",
    "archiveApiChecks",
    "archiveApiMethods",
    "archiveApiBlock",
)

SYNC_RATE_FIELDS = (
    "intervalBlocksPerSecond",
    "intervalStageSyncFinishBlocksPerSecond",
    "soakEfficiencyBlocksPerSecond",
    "syncLogBlocksPerSecond",
    "blocksPerSecond",
)

SYNC_RATE_SAMPLE_BLOCK_FIELDS = {
    "intervalBlocksPerSecond": ("intervalBlocks",),
    "intervalStageSyncFinishBlocksPerSecond": ("intervalStageSyncFinishBlocks",),
    "syncLogBlocksPerSecond": ("syncLogSegmentBlocks",),
    "blocksPerSecond": ("height",),
}

DATADIR_BYTES_PER_BLOCK_FIELDS = (
    "soakEfficiencyDatadirBytesPerBlock",
    "intervalDatadirBytesPerBlock",
    "datadirBytesPerBlock",
)

HOT_BYTES_PER_BLOCK_FIELDS = (
    "soakEfficiencyHotBytesPerBlock",
    "intervalChaindataBytesPerBlock",
    "chaindataBytesPerBlock",
)

HOT_GROWTH_SHARE_FIELDS = (
    "intervalChaindataGrowthShare",
)

COLD_ARCHIVE_BYTES_PER_BLOCK_FIELDS = (
    "soakEfficiencyColdArchiveBytesPerBlock",
    "intervalColdArchiveBytesPerBlock",
    "coldArchiveBytesPerBlock",
)

DERIVED_INDEX_BYTES_PER_BLOCK_FIELDS = (
    "soakEfficiencyDerivedIndexBytesPerBlock",
    "intervalDerivedIndexBytesPerBlock",
    "derivedIndexBytesPerBlock",
)

BYTES_PER_BLOCK_SAMPLE_BLOCK_FIELDS = {
    "intervalDatadirBytesPerBlock": ("intervalBlocks",),
    "intervalChaindataBytesPerBlock": ("intervalBlocks",),
    "intervalColdArchiveBytesPerBlock": ("intervalBlocks",),
    "intervalDerivedIndexBytesPerBlock": ("intervalBlocks",),
    "datadirBytesPerBlock": ("height",),
    "chaindataBytesPerBlock": ("height",),
    "coldArchiveBytesPerBlock": ("height",),
    "derivedIndexBytesPerBlock": ("height",),
}

SNAPSHOT_PROFILE_EVIDENCE_FIELDS = (
    "snapshotManifestProfileStatus",
    "snapshotProfileSegments",
    "snapshotProfileVerifyFiles",
    "snapshotProfileVerifiedSegments",
    "snapshotProfileTotalBytes",
    "snapshotPayloadBytes",
    "snapshotSidecarBytes",
    "snapshotSidecarShareMilli",
    "snapshotStateHistoryBytes",
    "snapshotStateHistoryCompressedSegments",
    "snapshotStateHistoryCompressedBytes",
    "snapshotStateHistoryCompressedShareMilli",
)

SNAPSHOT_PROFILE_FAMILY_FIELDS = (
    "snapshotLatestSidecar",
    "snapshotStateHistorySidecar",
    "snapshotChainFreezerSidecar",
    "snapshotEventLogSidecar",
    "snapshotBalanceTraceSidecar",
    "snapshotSectionBloomSidecar",
)

SNAPSHOT_PROFILE_POINT_FIELDS = (
    "snapshotPointTxHashLookup",
    "snapshotPointEventLogIndex",
    "snapshotPointStateHistoryAccessor",
    "snapshotPointLatestBTree",
    "snapshotPointChainFreezerAccessor",
    "snapshotPointCodeDomain",
    "snapshotPointCommitmentSnapshot",
)


def load_rows(path):
    rows = []
    issues = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return [], [f"read {path}: {exc}"]
    for line_no, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            issues.append(f"{path}:{line_no}: invalid JSON: {exc}")
            continue
        if not isinstance(row, dict):
            issues.append(f"{path}:{line_no}: expected JSON object row")
            continue
        row["_line"] = line_no
        rows.append(row)
    if not rows and not issues:
        issues.append(f"{path}: no JSONL rows found")
    return rows, issues


def row_sort_key(row):
    try:
        unix = int(row.get("unix", 0))
    except (TypeError, ValueError):
        unix = 0
    return unix, row.get("_line", 0)


def row_label(row):
    parts = [f"line {row.get('_line', '?')}"]
    for key in ("network", "mode", "label"):
        value = row.get(key)
        if value:
            parts.append(f"{key}={value}")
    return " ".join(parts)


def as_number(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        try:
            return int(value, 10)
        except ValueError:
            return None
    return None


def as_non_negative_int(row, field):
    parsed = as_int(row, field)
    if parsed is None:
        return None
    if parsed < 0:
        return None
    return parsed


def as_bool(row, field):
    value = row.get(field)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "ok"}
    if isinstance(value, (int, float)):
        return value != 0
    return False


def split_csv_values(values):
    out = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item:
                out.append(item)
    return out


def approx_equal(got, want, tolerance=1e-9):
    if got is None or want is None:
        return False
    return abs(got - want) <= tolerance


def require_non_negative_int(row, field, issues, context=""):
    value = as_non_negative_int(row, field)
    if value is None:
        prefix = f"{context} " if context else ""
        issues.append(f"{prefix}{field}={row.get(field)!r}, want non-negative integer")
    return value


def sampler_lag(high, low):
    if high < 0 or low < 0 or high < low:
        return -1
    return high - low


def sampler_ratio(numerator, denominator):
    if numerator < 0 or denominator <= 0:
        return -1.0
    return numerator / denominator


def optional_int(row, field, issues):
    if not field_present(row, field):
        return None, False
    value = as_int(row, field)
    if value is None:
        issues.append(f"{field}={row.get(field)!r}, want integer")
    return value, True


def optional_number(row, field, issues):
    if not field_present(row, field):
        return None, False
    value = as_number(row, field)
    if value is None:
        issues.append(f"{field}={row.get(field)!r}, want number")
    return value, True


def check_derived_int(row, field, want, issues, source):
    if not field_present(row, field):
        issues.append(f"{field} is missing while {source} is present")
        return
    got = as_int(row, field)
    if got is None:
        issues.append(f"{field}={row.get(field)!r}, want integer")
    elif got != want:
        issues.append(f"{field}={got:g}, want {want:g} from {source}")


def check_derived_number(row, field, want, issues, source):
    if not field_present(row, field):
        issues.append(f"{field} is missing while {source} is present")
        return
    got = as_number(row, field)
    if got is None:
        issues.append(f"{field}={row.get(field)!r}, want number")
    elif not approx_equal(got, want):
        issues.append(f"{field}={got:g}, want {want:g} from {source}")


def filter_rows(rows, args):
    out = []
    for row in rows:
        if args.network and str(row.get("network", "")) != args.network:
            continue
        if args.mode and str(row.get("mode", "")) != args.mode:
            continue
        if args.label and str(row.get("label", "")) != args.label:
            continue
        out.append(row)
    return out


def parse_threshold(raw):
    if "=" not in raw:
        raise ValueError(f"{raw!r} must use FIELD=VALUE")
    field, value = raw.split("=", 1)
    field = field.strip()
    if not field:
        raise ValueError(f"{raw!r} has an empty field")
    try:
        want = float(value)
    except ValueError as exc:
        raise ValueError(f"{raw!r} has a non-numeric threshold") from exc
    return field, want


def parse_non_negative_int_arg(raw):
    try:
        value = int(raw, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{raw!r} must be an integer") from exc
    if value < 0:
        raise argparse.ArgumentTypeError(f"{raw!r} must be non-negative")
    return value


def check_thresholds(row, raws, op_name, predicate):
    issues = []
    for raw in raws:
        try:
            field, want = parse_threshold(raw)
        except ValueError as exc:
            issues.append(str(exc))
            continue
        got = as_number(row, field)
        if got is None:
            issues.append(f"{raw}: {field!r} is missing or non-numeric")
            continue
        if not predicate(got, want):
            issues.append(f"{raw}: {field}={got:g} failed {op_name} {want:g}")
    return issues


def sync_rate_evidence(row):
    for field in SYNC_RATE_FIELDS:
        value = as_number(row, field)
        if value is not None and value >= 0:
            return field, value
    return None, None


def sync_rate_sample_block_evidence(row, rate_field):
    fields = SYNC_RATE_SAMPLE_BLOCK_FIELDS.get(rate_field, ())
    if rate_field == "soakEfficiencyBlocksPerSecond":
        window = str(row.get("soakEfficiencyWindow", ""))
        if window == "interval":
            fields = ("intervalBlocks",)
        elif window == "cumulative":
            fields = ("height",)
        else:
            fields = ("intervalBlocks", "height")
    invalid = []
    for field in fields:
        if not field_present(row, field):
            continue
        value = as_non_negative_int(row, field)
        if value is not None:
            return field, value, invalid
        invalid.append(field)
    return None, None, invalid


def check_min_sync_rate(row, minimum, min_blocks=None):
    if minimum is None:
        return []
    field, value = sync_rate_evidence(row)
    if field is None:
        return [
            "sync rate evidence missing: none of "
            + ",".join(SYNC_RATE_FIELDS)
            + " is present and non-negative"
        ]
    issues = []
    if min_blocks is not None:
        block_field, blocks, invalid = sync_rate_sample_block_evidence(row, field)
        if block_field is None:
            if invalid:
                issues.append(
                    f"sync rate sample size evidence invalid for {field}: "
                    + ",".join(
                        f"{sample_field}={row.get(sample_field)!r}"
                        for sample_field in invalid
                    )
                    + ", want non-negative integer"
                )
            else:
                issues.append(f"sync rate sample size evidence missing for {field}")
        elif blocks < min_blocks:
            issues.append(
                f"{block_field}={blocks:g} failed >= min sync rate sample blocks "
                f"{min_blocks:g} for {field}"
            )
    if value < minimum:
        issues.append(f"{field}={value:g} failed >= min sync rate {minimum:g} blocks/s")
    return issues


def datadir_bytes_per_block_evidence(row):
    for field in DATADIR_BYTES_PER_BLOCK_FIELDS:
        value = as_number(row, field)
        if value is not None and value >= 0:
            return field, value
    return None, None


def bytes_per_block_sample_block_evidence(row, bytes_field):
    fields = BYTES_PER_BLOCK_SAMPLE_BLOCK_FIELDS.get(bytes_field, ())
    if bytes_field.startswith("soakEfficiency"):
        window = str(row.get("soakEfficiencyWindow", ""))
        if window == "interval":
            fields = ("intervalBlocks",)
        elif window == "cumulative":
            fields = ("height",)
        else:
            fields = ("intervalBlocks", "height")
    invalid = []
    for field in fields:
        if not field_present(row, field):
            continue
        value = as_non_negative_int(row, field)
        if value is not None:
            return field, value, invalid
        invalid.append(field)
    return None, None, invalid


def check_bytes_per_block_sample_blocks(row, bytes_field, min_blocks, label):
    if min_blocks is None:
        return []
    block_field, blocks, invalid = bytes_per_block_sample_block_evidence(row, bytes_field)
    if block_field is None:
        if invalid:
            return [
                f"{label} sample size evidence invalid for {bytes_field}: "
                + ",".join(
                    f"{field}={row.get(field)!r}" for field in invalid
                )
                + ", want non-negative integer"
            ]
        return [f"{label} sample size evidence missing for {bytes_field}"]
    if blocks < min_blocks:
        return [
            f"{block_field}={blocks:g} failed >= min {label} sample blocks "
            f"{min_blocks:g} for {bytes_field}"
        ]
    return []


def check_max_datadir_bytes_per_block(row, maximum, min_blocks=None):
    if maximum is None:
        return []
    field, value = datadir_bytes_per_block_evidence(row)
    if field is None:
        return [
            "datadir bytes-per-block evidence missing: none of "
            + ",".join(DATADIR_BYTES_PER_BLOCK_FIELDS)
            + " is present and non-negative"
        ]
    issues = check_bytes_per_block_sample_blocks(
        row, field, min_blocks, "datadir bytes-per-block"
    )
    if value > maximum:
        issues.append(f"{field}={value:g} failed <= max datadir bytes per block {maximum:g}")
    return issues


def hot_bytes_per_block_evidence(row):
    for field in HOT_BYTES_PER_BLOCK_FIELDS:
        value = as_number(row, field)
        if value is not None and value >= 0:
            return field, value
    return None, None


def check_max_hot_bytes_per_block(row, maximum, min_blocks=None):
    if maximum is None:
        return []
    field, value = hot_bytes_per_block_evidence(row)
    if field is None:
        return [
            "hot bytes-per-block evidence missing: none of "
            + ",".join(HOT_BYTES_PER_BLOCK_FIELDS)
            + " is present and non-negative"
        ]
    issues = check_bytes_per_block_sample_blocks(
        row, field, min_blocks, "hot bytes-per-block"
    )
    if value > maximum:
        issues.append(f"{field}={value:g} failed <= max hot bytes per block {maximum:g}")
    return issues


def hot_growth_share_evidence(row):
    for field in HOT_GROWTH_SHARE_FIELDS:
        value = as_number(row, field)
        if value is not None and value >= 0:
            return field, value
    return None, None


def check_max_hot_growth_share(row, maximum):
    if maximum is None:
        return []
    window = str(row.get("soakEfficiencyWindow", ""))
    if window != "interval":
        return [
            "hot growth share evidence requires soakEfficiencyWindow='interval', "
            f"got {window!r}"
        ]
    positive_growth = as_number(row, "intervalPositiveDiskGrowthBytes")
    if positive_growth is None or positive_growth <= 0:
        return [
            "hot growth share evidence requires intervalPositiveDiskGrowthBytes > 0, "
            f"got {positive_growth}"
        ]
    field, value = hot_growth_share_evidence(row)
    if field is None:
        return [
            "hot growth share evidence missing: none of "
            + ",".join(HOT_GROWTH_SHARE_FIELDS)
            + " is present and non-negative"
        ]
    if value > maximum:
        return [f"{field}={value:g} failed <= max hot growth share {maximum:g}"]
    return []


def check_max_cold_stage_lag_blocks(row, maximum):
    if maximum is None:
        return []
    issues = []
    for field in COLD_STAGE_LAG_FIELDS:
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(
                f"cold stage lag evidence missing: {field} is missing or not a non-negative integer"
            )
            continue
        if value > maximum:
            issues.append(f"{field}={value:g} failed <= max cold stage lag {maximum:g}")
    return issues


def check_min_chain_freezer_metrics(row, min_blocks, min_passes):
    if min_blocks is None and min_passes is None:
        return []
    issues = []
    status = row.get("debugMetricsStatus")
    if status != "ok":
        issues.append(
            f"debugMetricsStatus={status!r}, want 'ok' for chain freezer metric evidence"
        )
    for minimum, (field, label) in zip((min_blocks, min_passes), CHAIN_FREEZER_MINIMUM_FIELDS):
        if minimum is None:
            continue
        value = as_number(row, field)
        if value is None:
            issues.append(f"{field}={value}, want >= {minimum:g} ({label})")
        elif value < minimum:
            issues.append(f"{field}={value:g} failed >= {label} {minimum:g}")
    return issues


def check_storage_alert_evidence(row):
    issues = []
    for field in STORAGE_ALERT_STATUS_FIELDS:
        value = row.get(field)
        if str(value).lower() != "ok":
            issues.append(f"{field}={value!r}, want 'ok'")
    for field in STORAGE_ALERT_ZERO_ISSUE_FIELDS:
        value = as_number(row, field)
        if value is None:
            issues.append(f"{field}={value}, want 0")
        elif value != 0:
            issues.append(f"{field}={value:g}, want 0")
    return issues


def check_startup_recovery_evidence(row):
    issues = []
    status = row.get("syncStartupRepairStatus")
    if status != "ok":
        issues.append(f"syncStartupRepairStatus={status!r}, want 'ok'")
    summaries = as_number(row, "syncStartupRepairSummaries")
    if summaries is None or summaries <= 0:
        issues.append(f"syncStartupRepairSummaries={summaries}, want > 0")
    if not as_bool(row, "syncStartupRepairComplete"):
        issues.append("syncStartupRepairComplete is not true")
    if as_bool(row, "syncStartupRepairHasBlocked"):
        issues.append(
            "syncStartupRepairHasBlocked=true: "
            f"firstBlocked={row.get('syncStartupRepairFirstBlocked')!r}"
        )
    if as_bool(row, "syncStartupRepairInterrupted"):
        issues.append("syncStartupRepairInterrupted=true")
    if row.get("syncStartupRepairErrorStage"):
        issues.append(
            f"syncStartupRepairErrorStage={row.get('syncStartupRepairErrorStage')!r}, want ''"
        )

    if not as_bool(row, "syncStartupHeadCompletionChecked"):
        issues.append("syncStartupHeadCompletionChecked is not true")
    if not as_bool(row, "syncStartupHeadCompletionComplete"):
        issues.append("syncStartupHeadCompletionComplete is not true")
    if row.get("syncStartupHeadCompletionErrorStage"):
        issues.append(
            "syncStartupHeadCompletionErrorStage="
            f"{row.get('syncStartupHeadCompletionErrorStage')!r}, want ''"
        )

    if not as_bool(row, "syncStartupPipelineOrderChecked"):
        issues.append("syncStartupPipelineOrderChecked is not true")
    for field in STARTUP_RECOVERY_ZERO_FIELDS:
        value = as_number(row, field)
        if value is None:
            issues.append(f"{field}={value}, want 0")
        elif value != 0:
            issues.append(f"{field}={value:g}, want 0")

    if as_bool(row, "syncStartupPipelineOrderRepairChecked"):
        if not as_bool(row, "syncStartupPipelineOrderRepairComplete"):
            issues.append("syncStartupPipelineOrderRepairComplete is not true")
        if as_bool(row, "syncStartupPipelineOrderRepairInterrupted"):
            issues.append("syncStartupPipelineOrderRepairInterrupted=true")
        if row.get("syncStartupPipelineOrderRepairErrorStage"):
            issues.append(
                "syncStartupPipelineOrderRepairErrorStage="
                f"{row.get('syncStartupPipelineOrderRepairErrorStage')!r}, want ''"
            )

    if as_bool(row, "syncStartupPipelineCursorChecked"):
        if not as_bool(row, "syncStartupPipelineCursorComplete"):
            issues.append("syncStartupPipelineCursorComplete is not true")
        if as_bool(row, "syncStartupPipelineCursorBlocked"):
            issues.append(
                "syncStartupPipelineCursorBlocked=true: "
                f"nextStage={row.get('syncStartupPipelineCursorNextStage')!r}"
            )
        if as_bool(row, "syncStartupPipelineCursorInterrupted"):
            issues.append("syncStartupPipelineCursorInterrupted=true")
        if row.get("syncStartupPipelineCursorErrorStage"):
            issues.append(
                "syncStartupPipelineCursorErrorStage="
                f"{row.get('syncStartupPipelineCursorErrorStage')!r}, want ''"
            )
    return issues


def cold_archive_bytes_per_block_evidence(row):
    for field in COLD_ARCHIVE_BYTES_PER_BLOCK_FIELDS:
        value = as_number(row, field)
        if value is not None and value >= 0:
            return field, value
    return None, None


def check_max_cold_archive_bytes_per_block(row, maximum, min_blocks=None):
    if maximum is None:
        return []
    field, value = cold_archive_bytes_per_block_evidence(row)
    if field is None:
        return [
            "cold archive bytes-per-block evidence missing: none of "
            + ",".join(COLD_ARCHIVE_BYTES_PER_BLOCK_FIELDS)
            + " is present and non-negative"
        ]
    issues = check_bytes_per_block_sample_blocks(
        row, field, min_blocks, "cold archive bytes-per-block"
    )
    if value > maximum:
        issues.append(
            f"{field}={value:g} failed <= max cold archive bytes per block {maximum:g}"
        )
    return issues


def derived_index_bytes_per_block_evidence(row):
    for field in DERIVED_INDEX_BYTES_PER_BLOCK_FIELDS:
        value = as_number(row, field)
        if value is not None and value >= 0:
            return field, value
    return None, None


def check_max_derived_index_bytes_per_block(row, maximum, min_blocks=None):
    if maximum is None:
        return []
    field, value = derived_index_bytes_per_block_evidence(row)
    if field is None:
        return [
            "derived index bytes-per-block evidence missing: none of "
            + ",".join(DERIVED_INDEX_BYTES_PER_BLOCK_FIELDS)
            + " is present and non-negative"
        ]
    issues = check_bytes_per_block_sample_blocks(
        row, field, min_blocks, "derived index bytes-per-block"
    )
    if value > maximum:
        issues.append(
            f"{field}={value:g} failed <= max derived index bytes per block {maximum:g}"
        )
    return issues


def resolve_artifact(result_path, raw_path):
    path = Path(str(raw_path))
    if path.is_absolute():
        return path
    return result_path.parent / path


def check_prometheus_artifact(result_path, row):
    raw_path = row.get("offlineDbCheckPrometheus")
    if not raw_path:
        return ["offlineDbCheckPrometheus is missing while status is ok"]
    path = resolve_artifact(result_path, raw_path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"offlineDbCheckPrometheus artifact {path}: {exc}"]
    issues = []
    for needle, name in PROMETHEUS_REQUIRED_SNIPPETS:
        if needle not in text:
            issues.append(f"offlineDbCheckPrometheus artifact {path} missing {name}")
    if "gtron_storage_alert_status{" in text:
        issues.extend(
            check_prometheus_metric_present(path, text, "gtron_storage_alert_status", row)
        )
        issues.extend(check_prometheus_alert_status_value(path, text, row))
    issues.extend(check_prometheus_issue_kinds(path, text, row))
    issues.extend(check_prometheus_stage_pipeline(path, text, row))
    issues.extend(check_prometheus_prune_boundaries(path, text, row))
    return issues


def check_sample_prometheus_artifact(result_path, row):
    issues = []
    if row.get("samplePrometheusStatus") != "ok":
        issues.append(f"samplePrometheusStatus={row.get('samplePrometheusStatus')!r}, want 'ok'")
    raw_path = row.get("samplePrometheus")
    if not raw_path:
        issues.append("samplePrometheus is missing")
        return issues
    path = resolve_artifact(result_path, raw_path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [*issues, f"samplePrometheus artifact {path}: {exc}"]
    for needle, name in SAMPLE_PROMETHEUS_REQUIRED_SNIPPETS:
        if needle not in text:
            issues.append(f"samplePrometheus artifact {path} missing {name}")

    status_metric = prometheus_metric_value(text, "gtron_nile_sync_sample_status", row)
    if status_metric is None:
        issues.append(f"samplePrometheus artifact {path} missing gtron_nile_sync_sample_status")
    elif row.get("sampleStatus") == "ok" and status_metric != 0:
        issues.append(
            f"samplePrometheus artifact {path} gtron_nile_sync_sample_status={status_metric:g}, want 0"
        )

    for metric, field in SAMPLE_PROMETHEUS_FIELD_METRICS:
        integer_field = (
            field in SAMPLE_PROMETHEUS_SIGNED_INTEGER_FIELDS
            or field in SAMPLE_PROMETHEUS_NON_NEGATIVE_INTEGER_FIELDS
        )
        if integer_field:
            if field not in row:
                continue
            if field in SAMPLE_PROMETHEUS_SIGNED_INTEGER_FIELDS:
                want = as_int(row, field)
                want_text = "integer"
            else:
                if field in SAMPLE_PROMETHEUS_OPTIONAL_NON_NEGATIVE_INTEGER_FIELDS:
                    raw_int = as_int(row, field)
                    if raw_int is None:
                        issues.append(
                            f"samplePrometheus artifact {path} {field}={row.get(field)!r}, "
                            "want integer"
                        )
                        continue
                    if raw_int < 0:
                        got = prometheus_metric_value(text, metric, row)
                        if got is not None:
                            issues.append(
                                f"samplePrometheus artifact {path} {metric}={got:g}, "
                                f"want omitted while {field}={raw_int:g}"
                            )
                        continue
                want = as_non_negative_int(row, field)
                want_text = "non-negative integer"
            if want is None:
                issues.append(
                    f"samplePrometheus artifact {path} {field}={row.get(field)!r}, "
                    f"want {want_text}"
                )
                continue
        else:
            want = as_number(row, field)
        if want is None:
            continue
        got = prometheus_metric_value(text, metric, row)
        if got is None:
            issues.append(f"samplePrometheus artifact {path} missing {metric}")
        elif integer_field and (
            not got.is_integer()
            or (field in SAMPLE_PROMETHEUS_NON_NEGATIVE_INTEGER_FIELDS and got < 0)
        ):
            issues.append(
                f"samplePrometheus artifact {path} {metric}={got:g}, want {want_text}"
            )
        elif integer_field and int(got) != want:
            issues.append(
                f"samplePrometheus artifact {path} {metric}={int(got):g}, want {want:g}"
            )
        elif got != want:
            issues.append(f"samplePrometheus artifact {path} {metric}={got:g}, want {want:g}")
    if "stageStalled" in row:
        got = prometheus_metric_value(text, "gtron_nile_sync_stage_stalled", row)
        want = 1.0 if as_bool(row, "stageStalled") else 0.0
        if got is None:
            issues.append(f"samplePrometheus artifact {path} missing gtron_nile_sync_stage_stalled")
        elif got != want:
            issues.append(f"samplePrometheus artifact {path} gtron_nile_sync_stage_stalled={got:g}, want {want:g}")
    issues.extend(check_sample_prometheus_stage_status(path, text, row))
    issues.extend(check_sample_prometheus_full_staged_sync(path, text, row))
    issues.extend(check_sample_prometheus_archive_api_methods(path, text, row))
    return issues


def check_sample_prometheus_enum(path, text, row, metric, label, raw_value, values):
    value = str(raw_value or "unknown").lower()
    want = values.get(value, values["unknown"])
    got = sample_prometheus_metric_value(text, metric, row, {label: value})
    if got is None:
        return [f"samplePrometheus artifact {path} missing {metric}{{{label}={value!r}}}"]
    if got != want:
        return [
            f"samplePrometheus artifact {path} {metric}{{{label}={value!r}}}={got:g}, "
            f"want {want:g}"
        ]
    return []


def check_sample_prometheus_stage_status(path, text, row):
    issues = []
    if "stageStatusSource" in row:
        issues.extend(
            check_sample_prometheus_enum(
                path,
                text,
                row,
                "gtron_nile_sync_stage_status_source",
                "source",
                row.get("stageStatusSource"),
                STAGE_STATUS_SOURCE_VALUES,
            )
        )
    if "stageStatusFileStatus" in row:
        issues.extend(
            check_sample_prometheus_enum(
                path,
                text,
                row,
                "gtron_nile_sync_stage_status_collection_status",
                "status",
                row.get("stageStatusFileStatus"),
                STAGE_STATUS_COLLECTION_STATUS_VALUES,
            )
        )
    return issues


def check_sample_prometheus_full_staged_sync(path, text, row):
    issues = []
    if "fullStagedSyncStatus" in row:
        status = str(row.get("fullStagedSyncStatus", "unknown")).lower()
        want = FULL_STAGED_SYNC_STATUS_VALUES.get(
            status, FULL_STAGED_SYNC_STATUS_VALUES["unknown"]
        )
        got = sample_prometheus_metric_value(
            text,
            "gtron_nile_sync_full_staged_sync_status",
            row,
            {"status": status},
        )
        if got is None:
            issues.append(
                f"samplePrometheus artifact {path} missing "
                f"gtron_nile_sync_full_staged_sync_status{{status={status!r}}}"
            )
        elif got != want:
            issues.append(
                f"samplePrometheus artifact {path} "
                f"gtron_nile_sync_full_staged_sync_status{{status={status!r}}}="
                f"{got:g}, want {want:g}"
            )
    bottleneck = row.get("fullStagedSyncBottleneck")
    if bottleneck is not None:
        got = sample_prometheus_metric_value(
            text,
            "gtron_nile_sync_full_staged_sync_bottleneck",
            row,
            {"bottleneck": str(bottleneck)},
        )
        if got is None:
            issues.append(
                f"samplePrometheus artifact {path} missing "
                f"gtron_nile_sync_full_staged_sync_bottleneck"
                f"{{bottleneck={str(bottleneck)!r}}}"
            )
        elif got != 1:
            issues.append(
                f"samplePrometheus artifact {path} "
                f"gtron_nile_sync_full_staged_sync_bottleneck"
                f"{{bottleneck={str(bottleneck)!r}}}={got:g}, want 1"
            )
    details = row.get("fullStagedSyncStageDetails")
    if isinstance(details, list):
        for index, detail in enumerate(details):
            issues.extend(
                check_sample_prometheus_full_staged_sync_stage_detail(
                    path, text, row, index, detail
                )
            )
    return issues


def check_sample_prometheus_full_staged_sync_stage_detail(path, text, row, index, detail):
    if not isinstance(detail, dict):
        return [
            f"samplePrometheus artifact {path} cannot check "
            f"fullStagedSyncStageDetails[{index}]={detail!r}: want object"
        ]
    stage = str(detail.get("stage", ""))
    field = str(detail.get("field", ""))
    if not stage or not field:
        return [
            f"samplePrometheus artifact {path} cannot check "
            f"fullStagedSyncStageDetails[{index}]: missing stage/field"
        ]

    issues = []
    labels = {"stage": stage, "field": field}
    present_want = 1.0 if bool(detail.get("present")) else 0.0
    present_got = sample_prometheus_metric_value(
        text,
        "gtron_nile_sync_full_staged_sync_stage_present",
        row,
        labels,
    )
    if present_got is None:
        issues.append(
            f"samplePrometheus artifact {path} missing "
            f"gtron_nile_sync_full_staged_sync_stage_present"
            f"{{stage={stage!r},field={field!r}}}"
        )
    elif present_got != present_want:
        issues.append(
            f"samplePrometheus artifact {path} "
            f"gtron_nile_sync_full_staged_sync_stage_present"
            f"{{stage={stage!r},field={field!r}}}={present_got:g}, "
            f"want {present_want:g}"
        )

    block_want = None
    block_present = field_present(detail, "block")
    if block_present:
        block_want = as_non_negative_int(detail, "block")
        if block_want is None:
            issues.append(
                f"samplePrometheus artifact {path} "
                f"fullStagedSyncStageDetails[{index}].block={detail.get('block')!r}, "
                "want non-negative integer"
            )
        block_got = sample_prometheus_metric_value(
            text,
            "gtron_nile_sync_full_staged_sync_stage_block",
            row,
            labels,
        )
        if block_got is None:
            issues.append(
                f"samplePrometheus artifact {path} missing "
                f"gtron_nile_sync_full_staged_sync_stage_block"
                f"{{stage={stage!r},field={field!r}}}"
            )
        elif block_got < 0 or not block_got.is_integer():
            issues.append(
                f"samplePrometheus artifact {path} "
                f"gtron_nile_sync_full_staged_sync_stage_block"
                f"{{stage={stage!r},field={field!r}}}={block_got:g}, "
                "want non-negative integer"
            )
        elif block_want is not None and int(block_got) != block_want:
            issues.append(
                f"samplePrometheus artifact {path} "
                f"gtron_nile_sync_full_staged_sync_stage_block"
                f"{{stage={stage!r},field={field!r}}}={int(block_got):g}, "
                f"want {block_want:g}"
            )

    verification = str(detail.get("verified", ""))
    verified_want = 1.0 if stage_detail_is_verified(stage, verification) else 0.0
    verified_got = sample_prometheus_metric_value(
        text,
        "gtron_nile_sync_full_staged_sync_stage_verified",
        row,
        {"stage": stage, "field": field, "verification": verification},
    )
    if verified_got is None:
        issues.append(
            f"samplePrometheus artifact {path} missing "
            f"gtron_nile_sync_full_staged_sync_stage_verified"
            f"{{stage={stage!r},field={field!r},verification={verification!r}}}"
        )
    elif verified_got != verified_want:
        issues.append(
            f"samplePrometheus artifact {path} "
            f"gtron_nile_sync_full_staged_sync_stage_verified"
            f"{{stage={stage!r},field={field!r},verification={verification!r}}}="
            f"{verified_got:g}, want {verified_want:g}"
        )
    return issues


def expected_archive_api_method_metrics(row, successful_methods):
    expected = set(successful_methods)
    expected.update(DEFAULT_ARCHIVE_API_METHODS)
    if as_bool(row, "archiveApiCallProbe"):
        expected.update(ARCHIVE_API_CALL_METHODS)
    if as_bool(row, "archiveApiTxProbe"):
        expected.update(ARCHIVE_API_TX_METHODS)
    if as_bool(row, "archiveApiTraceTransactionProbe"):
        expected.add(ARCHIVE_API_TRACE_TX_METHOD)
    if as_bool(row, "archiveApiTraceBlockProbe"):
        expected.update(ARCHIVE_API_TRACE_BLOCK_METHODS)
    return sorted(expected)


def check_sample_prometheus_method_metric(path, text, row, metric, method, want):
    got = sample_prometheus_metric_value(text, metric, row, {"method": method})
    if got is None:
        return [f"samplePrometheus artifact {path} missing {metric}{{method={method!r}}}"]
    if got != want:
        return [
            f"samplePrometheus artifact {path} {metric}{{method={method!r}}}={got:g}, want {want:g}"
        ]
    return []


def check_sample_prometheus_archive_api_methods(path, text, row):
    issues = []
    methods = archive_api_methods(row)
    if methods is None:
        return issues
    for method in expected_archive_api_method_metrics(row, methods):
        want = 1.0 if method in methods else 0.0
        issues.extend(
            check_sample_prometheus_method_metric(
                path,
                text,
                row,
                ARCHIVE_API_METHOD_SUCCESS_METRIC,
                method,
                want,
            )
        )

    tx_methods = string_set_field(row, "archiveApiTxMethods")
    if tx_methods is None and not as_bool(row, "archiveApiTxProbe"):
        return issues
    if tx_methods is None:
        tx_methods = set()
    expected_tx_methods = set(tx_methods)
    if as_bool(row, "archiveApiTxProbe"):
        expected_tx_methods.update(ARCHIVE_API_TX_METHODS)
    if as_bool(row, "archiveApiTraceTransactionProbe"):
        expected_tx_methods.add(ARCHIVE_API_TRACE_TX_METHOD)
    for method in sorted(expected_tx_methods):
        want = 1.0 if method in tx_methods else 0.0
        issues.extend(
            check_sample_prometheus_method_metric(
                path,
                text,
                row,
                ARCHIVE_API_TX_METHOD_SUCCESS_METRIC,
                method,
                want,
            )
        )
    return issues


def parse_prometheus_labels(raw):
    labels = {}
    for match in re.finditer(r'([A-Za-z_][A-Za-z0-9_]*)="((?:\\.|[^"\\])*)"', raw):
        value = match.group(2)
        value = value.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")
        labels[match.group(1)] = value
    return labels


def prometheus_label_matches(row, labels):
    want_datadir = row.get("datadir")
    if want_datadir:
        return labels.get("datadir") == str(want_datadir)
    return True


def prometheus_issue_keys(text, row):
    keys = set()
    for line in text.splitlines():
        match = re.match(r"^gtron_storage_alert_issue\{([^}]*)\}\s+", line.strip())
        if not match:
            continue
        labels = parse_prometheus_labels(match.group(1))
        if not prometheus_label_matches(row, labels):
            continue
        component = labels.get("component")
        kind = labels.get("kind")
        severity = labels.get("severity")
        if component and kind and severity:
            keys.add((component, kind, severity))
    return keys


def prometheus_metric_samples(text, metric):
    samples = []
    pattern = re.compile(rf"^{re.escape(metric)}(?:\{{([^}}]*)\}})?\s+([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)$")
    for line in text.splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        try:
            value = float(match.group(2))
        except ValueError:
            continue
        samples.append((parse_prometheus_labels(match.group(1) or ""), value))
    return samples


def prometheus_metric_value(text, metric, row=None):
    samples = prometheus_metric_samples(text, metric)
    if row is not None:
        samples = [(labels, value) for labels, value in samples if prometheus_label_matches(row, labels)]
    if not samples:
        return None
    return samples[-1][1]


def sample_prometheus_label_matches(row, labels, extra=None):
    if not prometheus_label_matches(row, labels):
        return False
    for field in ("network", "mode", "label"):
        if field in row and row.get(field) is not None and labels.get(field) != str(row.get(field)):
            return False
    if extra:
        for key, value in extra.items():
            if labels.get(key) != str(value):
                return False
    return True


def sample_prometheus_metric_value(text, metric, row, extra=None):
    samples = [
        (labels, value)
        for labels, value in prometheus_metric_samples(text, metric)
        if sample_prometheus_label_matches(row, labels, extra)
    ]
    if not samples:
        return None
    return samples[-1][1]


def check_prometheus_metric_value(path, text, metric, want, row):
    got = prometheus_metric_value(text, metric, row)
    if got is None:
        return [f"offlineDbCheckPrometheus artifact {path} missing {metric}"]
    if got != float(want):
        return [f"offlineDbCheckPrometheus artifact {path} {metric}={got:g}, want {want:g}"]
    return []


def check_prometheus_metric_present(path, text, metric, row):
    if prometheus_metric_value(text, metric, row) is None:
        return [f"offlineDbCheckPrometheus artifact {path} missing {metric}"]
    return []


def prometheus_prune_boundary_value(text, field, row):
    matched = [
        value
        for labels, value in prometheus_metric_samples(
            text, "gtron_storage_prune_boundary_block"
        )
        if prometheus_label_matches(row, labels) and labels.get("field") == field
    ]
    if not matched:
        return None
    return matched[-1]


def check_prometheus_prune_boundaries(path, text, row):
    issues = []
    if "signedColdPrune" in row:
        want = 1 if as_bool(row, "signedColdPrune") else 0
        issues.extend(
            check_prometheus_metric_value(
                path,
                text,
                "gtron_storage_signed_cold_prune",
                want,
                row,
            )
        )
    for field in PROMETHEUS_PRUNE_BOUNDARY_FIELDS:
        want = as_int(row, field)
        if want is None:
            if field_present(row, field):
                issues.append(
                    f"{field}={row.get(field)!r}, want integer "
                    "for prometheus prune boundary evidence"
                )
            continue
        got = prometheus_prune_boundary_value(text, field, row)
        if got is None:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} missing "
                f"gtron_storage_prune_boundary_block field={field!r}"
            )
        elif not got.is_integer():
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} "
                f"gtron_storage_prune_boundary_block field={field!r}={got:g}, "
                "want integer"
            )
        elif int(got) != want:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} "
                f"gtron_storage_prune_boundary_block field={field!r}={int(got):g}, "
                f"want {want:g}"
            )
    return issues


def check_prometheus_alert_status_value(path, text, row):
    status = str(row.get("storageAlertStatus", "")).lower()
    if status not in PROMETHEUS_STATUS_VALUES:
        return []
    return check_prometheus_metric_value(
        path,
        text,
        "gtron_storage_alert_status",
        PROMETHEUS_STATUS_VALUES[status],
        row,
    )


def row_alert_issue_keys(row):
    keys = set()
    fields = {
        "freezer": "freezerAlertDetails",
        "stage": "stageVerifyDetails",
        "mode": "modeAlertDetails",
        "snapshot": "snapshotAlertDetails",
    }
    for component, field in fields.items():
        details = row.get(field)
        if not isinstance(details, list):
            continue
        for detail in details:
            if not isinstance(detail, dict):
                continue
            kind = detail.get("kind")
            severity = detail.get("severity")
            if kind and severity:
                keys.add((component, str(kind), str(severity)))
    return keys


def check_prometheus_issue_kinds(path, text, row):
    issues = []
    actual = prometheus_issue_keys(text, row)
    for component, kind, severity in sorted(row_alert_issue_keys(row)):
        if (component, kind, severity) not in actual:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} missing "
                f"gtron_storage_alert_issue component={component!r} kind={kind!r} severity={severity!r}"
            )
    return issues


def check_prometheus_stage_pipeline(path, text, row):
    if "stageAlertPipelinePending" not in row and "stageAlertPipelineIssues" not in row:
        return []
    issues = []
    required = (
        ("gtron_storage_stage_pipeline_pending{", "gtron_storage_stage_pipeline_pending"),
        ("gtron_storage_stage_pipeline_issues{", "gtron_storage_stage_pipeline_issues"),
    )
    if "stageAlertPipelineComplete" in row:
        required = (
            ("gtron_storage_stage_pipeline_complete{", "gtron_storage_stage_pipeline_complete"),
            *required,
        )
    if row.get("stageAlertPipelineNext"):
        required = (
            *required,
            (
                "gtron_storage_stage_pipeline_next_target_block{",
                "gtron_storage_stage_pipeline_next_target_block",
            ),
            (
                "gtron_storage_stage_pipeline_next_current_block{",
                "gtron_storage_stage_pipeline_next_current_block",
            ),
        )
    for needle, name in required:
        if needle not in text:
            issues.append(f"offlineDbCheckPrometheus artifact {path} missing {name}")
    if "stageAlertPipelineComplete" in row:
        issues.extend(
            check_prometheus_metric_value(
                path,
                text,
                "gtron_storage_stage_pipeline_complete",
                1 if as_bool(row, "stageAlertPipelineComplete") else 0,
                row,
            )
        )
    if "stageAlertPipelinePending" in row:
        pending = as_non_negative_int(row, "stageAlertPipelinePending")
        if pending is not None:
            issues.extend(
                check_prometheus_metric_value(
                    path, text, "gtron_storage_stage_pipeline_pending", pending, row
                )
            )
        else:
            issues.append(
                "stageAlertPipelinePending="
                f"{row.get('stageAlertPipelinePending')!r}, want non-negative integer"
            )
    if "stageAlertPipelineIssues" in row:
        count = as_non_negative_int(row, "stageAlertPipelineIssues")
        if count is not None:
            issues.extend(
                check_prometheus_metric_value(
                    path, text, "gtron_storage_stage_pipeline_issues", count, row
                )
            )
        else:
            issues.append(
                "stageAlertPipelineIssues="
                f"{row.get('stageAlertPipelineIssues')!r}, want non-negative integer"
            )
    next_stage = row.get("stageAlertPipelineNext")
    if next_stage:
        want_target = as_non_negative_int(row, "stageAlertPipelineNextTarget")
        if want_target is None:
            issues.append(
                "stageAlertPipelineNextTarget="
                f"{row.get('stageAlertPipelineNextTarget')!r}, want non-negative integer"
            )
        want_status = str(row.get("stageAlertPipelineNextStatus", ""))
        want_upstream = str(row.get("stageAlertPipelineNextUpstream", ""))

        def labels_match(labels):
            return (
                prometheus_label_matches(row, labels) and labels.get("stage") == str(next_stage)
                and (not want_status or labels.get("status") == want_status)
                and (not want_upstream or labels.get("upstream") == want_upstream)
            )

        candidates = prometheus_metric_samples(text, "gtron_storage_stage_pipeline_next_target_block")
        matched = [
            value
            for labels, value in candidates
            if labels_match(labels)
        ]
        if not matched:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} missing next pipeline target "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r}"
            )
        elif want_target is not None and matched[-1] != want_target:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} next pipeline target "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r} "
                f"value={matched[-1]:g}, want {want_target:g}"
            )

        want_current = as_non_negative_int(row, "stageAlertPipelineNextCurrent")
        if want_current is None:
            issues.append(
                "stageAlertPipelineNextCurrent="
                f"{row.get('stageAlertPipelineNextCurrent')!r}, want non-negative integer"
            )
        candidates = prometheus_metric_samples(text, "gtron_storage_stage_pipeline_next_current_block")
        matched = [
            value
            for labels, value in candidates
            if labels_match(labels)
        ]
        if not matched:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} missing next pipeline current "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r}"
            )
        elif want_current is not None and matched[-1] != want_current:
            issues.append(
                f"offlineDbCheckPrometheus artifact {path} next pipeline current "
                f"stage={next_stage!r} status={want_status!r} upstream={want_upstream!r} "
                f"value={matched[-1]:g}, want {want_current:g}"
            )
    return issues


def check_full_staged_sync_derived_stage_metrics(row):
    issues = []
    gap_edges = (
        (
            "stageSyncInventoryBodiesGapBlocks",
            "stageSyncInventory",
            "stageSyncBodies",
            "stageSyncInventory-stageSyncBodies",
        ),
        (
            "stageSyncBodiesReadyGapBlocks",
            "stageSyncBodies",
            "stageSyncBodiesReady",
            "stageSyncBodies-stageSyncBodiesReady",
        ),
        (
            "stageSyncImportExecutionLagBlocks",
            "stageSyncImport",
            "stageSyncExecution",
            "stageSyncImport-stageSyncExecution",
        ),
        (
            "stageSyncExecutionCommitmentLagBlocks",
            "stageSyncExecution",
            "stageSyncCommitment",
            "stageSyncExecution-stageSyncCommitment",
        ),
        (
            "stageSyncCommitmentFinishLagBlocks",
            "stageSyncCommitment",
            "stageSyncFinish",
            "stageSyncCommitment-stageSyncFinish",
        ),
    )
    expected_gaps = {}
    for gap_field, high_field, low_field, source in gap_edges:
        high, high_present = optional_int(row, high_field, issues)
        low, low_present = optional_int(row, low_field, issues)
        if not (high_present and low_present) or high is None or low is None:
            continue
        want = sampler_lag(high, low)
        expected_gaps[gap_field] = want
        check_derived_int(row, gap_field, want, issues, source)

    head_field = "height" if field_present(row, "height") else "fullStagedSyncHeadBlock"
    head, head_present = optional_int(row, head_field, issues)
    finish, finish_present = optional_int(row, "stageSyncFinish", issues)
    if head_present and finish_present and head is not None and finish is not None:
        want = sampler_lag(head, finish)
        expected_gaps["stageSyncFinishHeadLagBlocks"] = want
        check_derived_int(
            row,
            "stageSyncFinishHeadLagBlocks",
            want,
            issues,
            f"{head_field}-stageSyncFinish",
        )

    pipeline_fields = (
        "stageSyncInventoryBodiesGapBlocks",
        "stageSyncBodiesReadyGapBlocks",
        "stageSyncImportExecutionLagBlocks",
        "stageSyncExecutionCommitmentLagBlocks",
        "stageSyncCommitmentFinishLagBlocks",
        "stageSyncFinishHeadLagBlocks",
    )
    if all(field in expected_gaps for field in pipeline_fields):
        valid = [expected_gaps[field] for field in pipeline_fields if expected_gaps[field] >= 0]
        pipeline_want = sum(valid) if valid else -1
        check_derived_int(
            row,
            "stageSyncPipelineLagBlocks",
            pipeline_want,
            issues,
            "sync stage lag fields",
        )

        candidates = (
            ("inventory-bodies", expected_gaps["stageSyncInventoryBodiesGapBlocks"]),
            ("bodies-ready-gap", expected_gaps["stageSyncBodiesReadyGapBlocks"]),
            ("import-execution", expected_gaps["stageSyncImportExecutionLagBlocks"]),
            ("execution-commitment", expected_gaps["stageSyncExecutionCommitmentLagBlocks"]),
            ("commitment-finish", expected_gaps["stageSyncCommitmentFinishLagBlocks"]),
            ("finish-head", expected_gaps["stageSyncFinishHeadLagBlocks"]),
        )
        valid_candidates = [(name, value) for name, value in candidates if value >= 0]
        if not valid_candidates:
            bottleneck_want, bottleneck_lag_want = "unknown", -1
        else:
            bottleneck_want, bottleneck_lag_want = max(valid_candidates, key=lambda item: item[1])
            if bottleneck_lag_want == 0:
                bottleneck_want = "none"
        bottleneck = str(row.get("stageSyncBottleneck", ""))
        if bottleneck != bottleneck_want:
            issues.append(
                f"stageSyncBottleneck={bottleneck!r}, want {bottleneck_want!r} "
                "from sync stage lag fields"
            )
        check_derived_int(
            row,
            "stageSyncBottleneckLagBlocks",
            bottleneck_lag_want,
            issues,
            "sync stage lag fields",
        )

    return issues


def check_full_staged_sync_inventory_interval_metrics(row):
    issues = []
    inventory_blocks, inventory_present = optional_int(
        row, "intervalStageSyncInventoryBlocks", issues
    )
    interval_blocks, interval_blocks_present = optional_int(row, "intervalBlocks", issues)
    if (
        inventory_present
        and interval_blocks_present
        and inventory_blocks is not None
        and interval_blocks is not None
    ):
        check_derived_number(
            row,
            "intervalStageSyncInventoryToTargetRatio",
            sampler_ratio(inventory_blocks, interval_blocks),
            issues,
            "intervalStageSyncInventoryBlocks/intervalBlocks",
        )

    bodies_blocks, bodies_present = optional_int(row, "intervalStageSyncBodiesBlocks", issues)
    if (
        bodies_present
        and inventory_present
        and bodies_blocks is not None
        and inventory_blocks is not None
    ):
        check_derived_number(
            row,
            "intervalStageSyncBodiesToInventoryRatio",
            sampler_ratio(bodies_blocks, inventory_blocks),
            issues,
            "intervalStageSyncBodiesBlocks/intervalStageSyncInventoryBlocks",
        )

    interval_seconds, interval_seconds_present = optional_number(row, "intervalSeconds", issues)
    if (
        inventory_present
        and interval_seconds_present
        and inventory_blocks is not None
        and interval_seconds is not None
    ):
        want = float(inventory_blocks) / interval_seconds if interval_seconds > 0 else 0.0
        check_derived_number(
            row,
            "intervalStageSyncInventoryBlocksPerSecond",
            want,
            issues,
            "intervalStageSyncInventoryBlocks/intervalSeconds",
        )
        check_derived_number(
            row,
            "intervalStageSyncInventoryBlocksPerMinute",
            want * 60.0,
            issues,
            "intervalStageSyncInventoryBlocksPerSecond*60",
        )

    return issues


def check_full_staged_sync_evidence(row, require_stage_details=False):
    issues = []
    required = row.get("fullStagedSyncRequiredStages")
    expected = list(FULL_STAGED_SYNC_REQUIRED_STAGES)
    if required != expected:
        issues.append(f"fullStagedSyncRequiredStages={required!r}, want {expected!r}")

    stage_count = as_non_negative_int(row, "fullStagedSyncStageCount")
    present_count = as_non_negative_int(row, "fullStagedSyncPresentStageCount")
    verified_count = as_non_negative_int(row, "fullStagedSyncVerifiedStageCount")
    expected_count = len(expected)
    if stage_count != expected_count:
        got = stage_count if stage_count is not None else row.get("fullStagedSyncStageCount")
        issues.append(f"fullStagedSyncStageCount={got}, want {len(expected)}")
    if present_count != expected_count:
        got = present_count if present_count is not None else row.get("fullStagedSyncPresentStageCount")
        issues.append(f"fullStagedSyncPresentStageCount={got}, want {len(expected)}")
    if verified_count != expected_count:
        got = verified_count if verified_count is not None else row.get("fullStagedSyncVerifiedStageCount")
        issues.append(f"fullStagedSyncVerifiedStageCount={got}, want {len(expected)}")

    for field in (
        "fullStagedSyncMissingStages",
        "fullStagedSyncHashIssues",
        "fullStagedSyncUnverifiedStages",
    ):
        value = row.get(field)
        if value != []:
            issues.append(f"{field}={value!r}, want []")

    for field in ("fullStagedSyncStageCoverageRatio", "fullStagedSyncVerificationRatio"):
        value = as_number(row, field)
        if value != 1.0:
            issues.append(f"{field}={value}, want 1")

    complete = require_non_negative_int(row, "fullStagedSyncCompleteBlock", issues)
    head = require_non_negative_int(row, "fullStagedSyncHeadBlock", issues)
    lag = require_non_negative_int(row, "fullStagedSyncHeadLagBlocks", issues)
    if complete is not None and head is not None and lag is not None:
        if head < complete:
            issues.append(f"fullStagedSyncHeadBlock={head:g} is below complete block {complete:g}")
        elif lag != head - complete:
            issues.append(
                f"fullStagedSyncHeadLagBlocks={lag:g}, want {head - complete:g} "
                "from fullStagedSyncHeadBlock-fullStagedSyncCompleteBlock"
            )
    completion_ratio = as_number(row, "fullStagedSyncCompletionRatio")
    if complete is not None and head is not None and head > 0:
        want = complete / head
        if not approx_equal(completion_ratio, want):
            issues.append(
                f"fullStagedSyncCompletionRatio={completion_ratio}, want {want:g} "
                "from fullStagedSyncCompleteBlock/fullStagedSyncHeadBlock"
            )

    pipeline_lag = require_non_negative_int(row, "fullStagedSyncPipelineLagBlocks", issues)
    if pipeline_lag is not None and lag is not None and lag >= 0 and pipeline_lag < lag:
        issues.append(
            f"fullStagedSyncPipelineLagBlocks={pipeline_lag:g} is below "
            f"fullStagedSyncHeadLagBlocks={lag:g}"
        )
    stage_pipeline_lag = None
    if field_present(row, "stageSyncPipelineLagBlocks"):
        stage_pipeline_lag = require_non_negative_int(row, "stageSyncPipelineLagBlocks", issues)
    if (
        pipeline_lag is not None
        and stage_pipeline_lag is not None
        and not approx_equal(pipeline_lag, stage_pipeline_lag)
    ):
        issues.append(
            f"fullStagedSyncPipelineLagBlocks={pipeline_lag:g}, "
            f"want stageSyncPipelineLagBlocks={stage_pipeline_lag:g}"
        )

    bottleneck = str(row.get("fullStagedSyncBottleneck", ""))
    bottleneck_lag = require_non_negative_int(row, "fullStagedSyncBottleneckLagBlocks", issues)
    if bottleneck_lag is not None and pipeline_lag is not None and pipeline_lag >= 0:
        if bottleneck_lag > pipeline_lag:
            issues.append(
                f"fullStagedSyncBottleneckLagBlocks={bottleneck_lag:g} exceeds "
                f"fullStagedSyncPipelineLagBlocks={pipeline_lag:g}"
            )
        if pipeline_lag == 0 and bottleneck != "none":
            issues.append(f"fullStagedSyncBottleneck={bottleneck!r}, want 'none' when pipeline lag is 0")
        if pipeline_lag > 0 and bottleneck in {"", "none", "unknown"}:
            issues.append(
                f"fullStagedSyncBottleneck={bottleneck!r}, want a concrete bottleneck "
                "when pipeline lag is positive"
            )
    stage_bottleneck = str(row.get("stageSyncBottleneck", ""))
    if bottleneck and stage_bottleneck and bottleneck != stage_bottleneck:
        issues.append(
            f"fullStagedSyncBottleneck={bottleneck!r}, want stageSyncBottleneck={stage_bottleneck!r}"
        )
    stage_bottleneck_lag = None
    if field_present(row, "stageSyncBottleneckLagBlocks"):
        stage_bottleneck_lag = require_non_negative_int(row, "stageSyncBottleneckLagBlocks", issues)
    if (
        bottleneck_lag is not None
        and stage_bottleneck_lag is not None
        and not approx_equal(bottleneck_lag, stage_bottleneck_lag)
    ):
        issues.append(
            f"fullStagedSyncBottleneckLagBlocks={bottleneck_lag:g}, "
            f"want stageSyncBottleneckLagBlocks={stage_bottleneck_lag:g}"
        )
    bottleneck_share = as_number(row, "fullStagedSyncBottleneckLagShare")
    if pipeline_lag is not None and bottleneck_lag is not None:
        want_share = (bottleneck_lag / pipeline_lag) if pipeline_lag > 0 else -1.0
        if not approx_equal(bottleneck_share, want_share):
            issues.append(
                f"fullStagedSyncBottleneckLagShare={bottleneck_share}, want {want_share:g} "
                "from fullStagedSyncBottleneckLagBlocks/fullStagedSyncPipelineLagBlocks"
            )

    height = None
    if field_present(row, "height"):
        height = require_non_negative_int(row, "height", issues)
    if height is not None and head is not None and head != height:
        issues.append(f"fullStagedSyncHeadBlock={head:g}, want height {height:g}")

    issues.extend(check_full_staged_sync_derived_stage_metrics(row))
    issues.extend(check_full_staged_sync_inventory_interval_metrics(row))

    status = str(row.get("fullStagedSyncStatus", "unknown"))
    complete_at_head = as_bool(row, "fullStagedSyncCompleteAtHead")
    ready = as_bool(row, "fullStagedSyncReady")
    want_ready = status in {"catching-up", "caught-up"}
    if ready != want_ready:
        issues.append(
            f"fullStagedSyncReady={row.get('fullStagedSyncReady')!r}, "
            f"want {want_ready!r} for status {status!r}"
        )
    if lag is not None:
        want_complete_at_head = ready and lag == 0
        if complete_at_head != want_complete_at_head:
            issues.append(
                f"fullStagedSyncCompleteAtHead={row.get('fullStagedSyncCompleteAtHead')!r}, "
                f"want {want_complete_at_head!r} from ready={ready!r} "
                f"and fullStagedSyncHeadLagBlocks={lag:g}"
            )
    if status == "caught-up" and (lag != 0 or not complete_at_head):
        issues.append(
            "full staged sync caught-up row is inconsistent: "
            f"lag={lag} completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
        )
    if status == "catching-up" and ((lag is not None and lag <= 0) or complete_at_head):
        issues.append(
            "full staged sync catching-up row is inconsistent: "
            f"lag={lag} completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
        )
    issues.extend(check_full_staged_sync_stage_details(row, require=require_stage_details))
    return issues


def stage_detail_is_verified(stage, verified):
    if stage == "SyncInventory":
        return verified == "unbound"
    if stage in {"SyncBodies", "SyncBodiesReady"}:
        return verified in {"staged", "canonical"}
    return verified == "canonical"


def check_full_staged_sync_stage_details(row, require=False):
    details = row.get("fullStagedSyncStageDetails")
    if details is None:
        return ["fullStagedSyncStageDetails is missing"] if require else []
    if not isinstance(details, list):
        return [f"fullStagedSyncStageDetails={details!r}, want list"]

    issues = []
    expected = list(FULL_STAGED_SYNC_REQUIRED_STAGES)
    if len(details) != len(expected):
        issues.append(f"fullStagedSyncStageDetails length={len(details)}, want {len(expected)}")

    missing = []
    hash_issues = []
    unverified = []
    present_values = []
    present_count = 0
    verified_count = 0

    for idx, stage in enumerate(expected):
        if idx >= len(details):
            missing.append(stage)
            continue
        detail = details[idx]
        if not isinstance(detail, dict):
            issues.append(f"fullStagedSyncStageDetails[{idx}]={detail!r}, want object")
            missing.append(stage)
            continue

        got_stage = str(detail.get("stage", ""))
        if got_stage != stage:
            issues.append(f"fullStagedSyncStageDetails[{idx}].stage={got_stage!r}, want {stage!r}")

        field = str(detail.get("field", ""))
        want_field = FULL_STAGED_SYNC_STAGE_FIELDS[stage]
        if field != want_field:
            issues.append(f"{stage} detail field={field!r}, want {want_field!r}")

        present = as_bool(detail, "present")
        block_number = as_number(detail, "block")
        block = as_non_negative_int(detail, "block") if present else block_number
        verified = str(detail.get("verified", ""))
        row_stage_value = None
        if field_present(row, want_field):
            row_stage_value = as_non_negative_int(row, want_field)
            if row_stage_value is None:
                issues.append(f"{want_field}={row.get(want_field)!r}, want non-negative integer")
        if not present:
            missing.append(stage)
            if block_number is not None and block_number >= 0:
                issues.append(f"{stage} detail present=false but block={block_number:g}")
            continue

        present_count += 1
        if block is None:
            issues.append(
                f"{stage} detail block={detail.get('block')!r}, want non-negative integer"
            )
        else:
            present_values.append((stage, block))
            if row_stage_value is not None and block != row_stage_value:
                issues.append(f"{stage} detail block={block:g}, want {want_field}={row_stage_value:g}")

        if stage_detail_is_verified(stage, verified):
            verified_count += 1
        elif verified:
            hash_issues.append({"stage": stage, "verified": verified})
        else:
            unverified.append(stage)

    for field, got, want in (
        ("fullStagedSyncPresentStageCount", as_non_negative_int(row, "fullStagedSyncPresentStageCount"), present_count),
        ("fullStagedSyncVerifiedStageCount", as_non_negative_int(row, "fullStagedSyncVerifiedStageCount"), verified_count),
    ):
        if got is not None and got != want:
            issues.append(f"{field}={got:g}, want detail-derived {want}")

    expected_missing = row.get("fullStagedSyncMissingStages")
    if isinstance(expected_missing, list) and expected_missing != missing:
        issues.append(f"fullStagedSyncMissingStages={expected_missing!r}, want detail-derived {missing!r}")
    expected_hash_issues = row.get("fullStagedSyncHashIssues")
    if isinstance(expected_hash_issues, list) and expected_hash_issues != hash_issues:
        issues.append(f"fullStagedSyncHashIssues={expected_hash_issues!r}, want detail-derived {hash_issues!r}")
    expected_unverified = row.get("fullStagedSyncUnverifiedStages")
    if isinstance(expected_unverified, list) and expected_unverified != unverified:
        issues.append(f"fullStagedSyncUnverifiedStages={expected_unverified!r}, want detail-derived {unverified!r}")

    finish_detail = next((item for item in details if isinstance(item, dict) and item.get("stage") == "SyncFinish"), None)
    finish_block = as_non_negative_int(finish_detail, "block") if finish_detail else None
    complete = as_non_negative_int(row, "fullStagedSyncCompleteBlock")
    if finish_block is not None and finish_block >= 0 and complete is not None and finish_block != complete:
        issues.append(f"fullStagedSyncCompleteBlock={complete:g}, want SyncFinish detail block={finish_block:g}")

    if present_values:
        min_stage, min_value = min(present_values, key=lambda item: item[1])
        row_min_stage = str(row.get("fullStagedSyncMinStage", ""))
        row_min_value = None
        if field_present(row, "fullStagedSyncMinStageBlock"):
            row_min_value = as_non_negative_int(row, "fullStagedSyncMinStageBlock")
            if row_min_value is None:
                issues.append(
                    f"fullStagedSyncMinStageBlock={row.get('fullStagedSyncMinStageBlock')!r}, "
                    "want non-negative integer"
                )
        if row_min_stage and row_min_stage != min_stage:
            issues.append(f"fullStagedSyncMinStage={row_min_stage!r}, want detail-derived {min_stage!r}")
        if row_min_value is not None and row_min_value != min_value:
            issues.append(f"fullStagedSyncMinStageBlock={row_min_value:g}, want detail-derived {min_value:g}")

    return issues


STAGE_STALL_EVIDENCE_FIELDS = (
    "stageStalled",
    "stageStalledCount",
    "stageStalledStage",
    "stageStalledSeconds",
    "stageStalledLagBlocks",
    "stageStalls",
)


def require_stage_stall_int(row, field, issues, *, non_negative=False, context=""):
    value = as_non_negative_int(row, field) if non_negative else as_int(row, field)
    if value is None:
        prefix = f"{context} " if context else ""
        want = "non-negative integer" if non_negative else "integer"
        issues.append(f"{prefix}{field}={row.get(field)!r}, want {want}")
    return value


def check_stage_stall_evidence(row):
    if not any(field in row for field in STAGE_STALL_EVIDENCE_FIELDS):
        return []

    issues = []
    stalled = as_bool(row, "stageStalled")
    stalls = row.get("stageStalls")
    if stalls is None:
        stalls = []
    if not isinstance(stalls, list):
        issues.append(f"stageStalls={stalls!r}, want list")
        stalls = []

    count = None
    if field_present(row, "stageStalledCount"):
        count = require_stage_stall_int(
            row, "stageStalledCount", issues, non_negative=True
        )
    if count is not None and count != len(stalls):
        issues.append(f"stageStalledCount={count:g}, want len(stageStalls)={len(stalls)}")

    health_issues = row.get("soakHealthIssues")
    has_stage_stalled_issue = isinstance(health_issues, list) and "stage-stalled" in health_issues
    if stalled and not has_stage_stalled_issue:
        issues.append("stageStalled=true but soakHealthIssues lacks 'stage-stalled'")
    if not stalled and has_stage_stalled_issue:
        issues.append("soakHealthIssues contains 'stage-stalled' but stageStalled is false")

    stage = str(row.get("stageStalledStage", ""))
    seconds = None
    if field_present(row, "stageStalledSeconds"):
        seconds = require_stage_stall_int(
            row, "stageStalledSeconds", issues, non_negative=True
        )
    lag = None
    if field_present(row, "stageStalledLagBlocks"):
        lag = require_stage_stall_int(row, "stageStalledLagBlocks", issues)
    if not stalled:
        if stalls:
            issues.append(f"stageStalls has {len(stalls)} entries while stageStalled is false")
        if stage:
            issues.append(f"stageStalledStage={stage!r}, want '' when stageStalled is false")
        if seconds is not None and seconds != 0:
            issues.append(f"stageStalledSeconds={seconds:g}, want 0 when stageStalled is false")
        if lag is not None and lag > 0:
            issues.append(f"stageStalledLagBlocks={lag:g}, want <= 0 when stageStalled is false")
        return issues

    if not stalls:
        issues.append("stageStalled=true but stageStalls is empty")
    if not stage:
        issues.append("stageStalledStage is empty while stageStalled is true")
    if seconds is None or seconds <= 0:
        issues.append(f"stageStalledSeconds={seconds}, want > 0 when stageStalled is true")
    if lag is None or lag <= 0:
        issues.append(f"stageStalledLagBlocks={lag}, want > 0 when stageStalled is true")

    primary = None
    for stall in stalls:
        if not isinstance(stall, dict):
            issues.append(f"stageStalls entry {stall!r} is not an object")
            continue
        require_stage_stall_int(
            stall,
            "stalledSeconds",
            issues,
            non_negative=True,
            context=f"stageStalls entry {stall.get('stage', '')!r}",
        )
        require_stage_stall_int(
            stall,
            "lagBlocks",
            issues,
            non_negative=True,
            context=f"stageStalls entry {stall.get('stage', '')!r}",
        )
        if primary is None:
            primary = stall
            continue
        current_key = (
            as_non_negative_int(stall, "stalledSeconds") or 0,
            as_non_negative_int(stall, "lagBlocks") or 0,
        )
        primary_key = (
            as_non_negative_int(primary, "stalledSeconds") or 0,
            as_non_negative_int(primary, "lagBlocks") or 0,
        )
        if current_key > primary_key:
            primary = stall
    if primary is not None:
        primary_stage = str(primary.get("stage", ""))
        primary_seconds = as_non_negative_int(primary, "stalledSeconds")
        primary_lag = as_non_negative_int(primary, "lagBlocks")
        if stage and primary_stage and stage != primary_stage:
            issues.append(f"stageStalledStage={stage!r}, want primary stalled stage {primary_stage!r}")
        if seconds is not None and primary_seconds is not None and seconds != primary_seconds:
            issues.append(
                f"stageStalledSeconds={seconds:g}, want primary stalled seconds {primary_seconds:g}"
            )
        if lag is not None and primary_lag is not None and lag != primary_lag:
            issues.append(f"stageStalledLagBlocks={lag:g}, want primary stalled lag {primary_lag:g}")

    return issues


def check_required_stage_stall_evidence(row):
    issues = []
    missing = [field for field in STAGE_STALL_EVIDENCE_FIELDS if field not in row]
    if missing:
        issues.append("stage stall evidence missing fields: " + ",".join(missing))
    issues.extend(check_stage_stall_evidence(row))
    return issues


def field_present(row, field):
    return field in row and row.get(field) not in {None, ""}


def check_non_negative_forbidden(row, field, reason):
    if not field_present(row, field):
        return []
    value = as_number(row, field)
    if value is not None and value >= 0:
        return [f"{field}={value:g} is not allowed for {reason}"]
    return []


def check_positive_forbidden(row, field, reason):
    if not field_present(row, field):
        return []
    value = as_number(row, field)
    if value is not None and value > 0:
        return [f"{field}={value:g} is not allowed for {reason}"]
    return []


PRUNE_BOUNDARY_INTEGER_FIELDS = (
    "coldFreezerToBlock",
    "chainLookupPruneToBlock",
    "tailPrunedThroughBlock",
    "balanceTracePruneToBlock",
    "sectionBloomPruneToSection",
)

PRUNE_COUNT_INTEGER_FIELDS = ("tailPrunedFiles",)


def check_integer_fields(row, fields):
    issues = []
    for field in fields:
        if field_present(row, field) and as_int(row, field) is None:
            issues.append(f"{field}={row.get(field)!r}, want integer")
    return issues


def check_non_negative_integer_fields(row, fields):
    issues = []
    for field in fields:
        if field_present(row, field) and as_non_negative_int(row, field) is None:
            issues.append(f"{field}={row.get(field)!r}, want non-negative integer")
    return issues


def check_prune_mode_semantics(row):
    issues = []
    issues.extend(check_integer_fields(row, PRUNE_BOUNDARY_INTEGER_FIELDS))
    issues.extend(check_non_negative_integer_fields(row, PRUNE_COUNT_INTEGER_FIELDS))

    mode = str(row.get("mode", "")).lower()
    if not mode:
        issues.append("mode is missing")
        return issues

    persisted_mode = str(row.get("pruneMode", "")).lower()
    if not persisted_mode or persisted_mode == "unknown":
        issues.append("pruneMode is missing or unknown")
    elif persisted_mode != mode:
        issues.append(f"pruneMode={row.get('pruneMode')!r} does not match mode={mode!r}")

    if not as_bool(row, "pruneModePersisted"):
        issues.append("pruneModePersisted must be true")

    if mode == "archive":
        if as_number(row, "signedColdPrune") == 1.0:
            issues.append("signedColdPrune must be false for archive")
        for field in (
            "chainLookupPruneToBlock",
            "tailPrunedThroughBlock",
            "balanceTracePruneToBlock",
            "sectionBloomPruneToSection",
        ):
            issues.extend(check_non_negative_forbidden(row, field, "archive mode"))

    if mode not in {"archive", "minimal"}:
        issues.extend(check_non_negative_forbidden(row, "tailPrunedThroughBlock", f"{mode} mode"))
    if mode != "minimal":
        issues.extend(check_positive_forbidden(row, "tailPrunedFiles", f"{mode} mode"))

    signed_cold_prune = as_number(row, "signedColdPrune") == 1.0
    chain_lookup = as_number(row, "chainLookupPruneToBlock")
    cold_freezer = as_number(row, "coldFreezerToBlock")
    if mode != "archive" and signed_cold_prune:
        if chain_lookup is None or chain_lookup < 0:
            issues.append(
                f"chainLookupPruneToBlock must be >= 0 when signedColdPrune is true for {mode} mode"
            )
        elif cold_freezer is None or cold_freezer < chain_lookup:
            issues.append(
                f"coldFreezerToBlock={cold_freezer} must cover "
                f"chainLookupPruneToBlock={chain_lookup:g}"
            )

    tail_pruned_files = as_number(row, "tailPrunedFiles")
    tail_pruned = as_number(row, "tailPrunedThroughBlock")
    if mode == "minimal" and tail_pruned_files is not None and tail_pruned_files > 0:
        if tail_pruned is None or tail_pruned < 0:
            issues.append(
                "tailPrunedThroughBlock must be >= 0 when tailPrunedFiles is positive "
                "for minimal mode"
            )
    if mode == "minimal" and tail_pruned is not None and tail_pruned >= 0:
        if chain_lookup is None or chain_lookup < 0:
            issues.append(
                "chainLookupPruneToBlock must be >= 0 when tailPrunedThroughBlock is set "
                "for minimal mode"
            )
        elif tail_pruned > chain_lookup:
            issues.append(
                f"tailPrunedThroughBlock={tail_pruned:g} exceeds "
                f"chainLookupPruneToBlock={chain_lookup:g}"
            )
        if cold_freezer is None or cold_freezer < tail_pruned:
            issues.append(
                f"coldFreezerToBlock={cold_freezer} must cover "
                f"tailPrunedThroughBlock={tail_pruned:g}"
            )

    return issues


def string_set_field(row, field):
    raw = row.get(field)
    if raw is None:
        return None
    if not isinstance(raw, list):
        return set()
    return {str(value) for value in raw}


def archive_api_methods(row):
    return string_set_field(row, "archiveApiMethods")


def has_archive_api_evidence(row):
    return any(field in row for field in ARCHIVE_API_EVIDENCE_FIELDS)


def archive_api_method_count(row):
    raw = row.get("archiveApiMethods")
    if not isinstance(raw, list):
        return None
    return len(raw)


def check_archive_api_evidence(row, required_methods, min_depth_blocks=None, require_trace_block=False):
    issues = []
    status = str(row.get("archiveApiStatus", "")).lower()
    if status != "ok":
        issues.append(f"archiveApiStatus={row.get('archiveApiStatus')!r}, want 'ok'")

    checks = as_non_negative_int(row, "archiveApiChecks")
    if checks is None or checks <= 0:
        issues.append(
            f"archiveApiChecks={row.get('archiveApiChecks')!r}, want positive integer"
        )

    failures = as_non_negative_int(row, "archiveApiFailures")
    if failures is None:
        issues.append(
            f"archiveApiFailures={row.get('archiveApiFailures')!r}, want non-negative integer"
        )
    elif failures != 0:
        issues.append(f"archiveApiFailures={failures:g}, want 0")

    if require_trace_block and not as_bool(row, "archiveApiTraceBlockProbe"):
        issues.append(
            "archiveApiTraceBlockProbe is not true; run nile_sync_sample.sh with --archive-api-trace-block"
        )

    chain_lookup = None
    if field_present(row, "chainLookupPruneToBlock"):
        chain_lookup = as_int(row, "chainLookupPruneToBlock")
        if chain_lookup is None:
            issues.append(
                f"chainLookupPruneToBlock={row.get('chainLookupPruneToBlock')!r}, want integer"
            )
    tail_pruned = None
    if field_present(row, "tailPrunedThroughBlock"):
        tail_pruned = as_int(row, "tailPrunedThroughBlock")
        if tail_pruned is None:
            issues.append(
                f"tailPrunedThroughBlock={row.get('tailPrunedThroughBlock')!r}, want integer"
            )

    block = as_non_negative_int(row, "archiveApiBlock")
    if block is None:
        issues.append(
            f"archiveApiBlock={row.get('archiveApiBlock')!r}, "
            "want non-negative integer historical block"
        )
    else:
        height = None
        if field_present(row, "height"):
            height = as_non_negative_int(row, "height")
            if height is None:
                issues.append(
                    f"height={row.get('height')!r}, "
                    "want non-negative integer for archive API depth evidence"
                )
        depth = None
        if height is not None and block >= height:
            issues.append(f"archiveApiBlock={block:g} must be below height={height:g}")
        if height is not None:
            depth = height - block
            if field_present(row, "archiveApiDepthBlocks"):
                reported_depth = as_non_negative_int(row, "archiveApiDepthBlocks")
                if reported_depth is None:
                    issues.append(
                        f"archiveApiDepthBlocks={row.get('archiveApiDepthBlocks')!r}, "
                        "want non-negative integer"
                    )
                elif not approx_equal(reported_depth, depth):
                    issues.append(
                        f"archiveApiDepthBlocks={reported_depth:g}, "
                        f"want height - archiveApiBlock = {depth:g}"
                    )
        if min_depth_blocks is not None:
            if height is None:
                issues.append("archive API depth evidence requires numeric height")
            else:
                if depth < min_depth_blocks:
                    issues.append(
                        f"archiveApiBlock depth={depth:g} failed >= min archive API "
                        f"depth {min_depth_blocks:g} blocks"
                    )
        if chain_lookup is not None and chain_lookup >= 0 and block > chain_lookup:
            issues.append(
                f"archiveApiBlock={block:g} must be <= chainLookupPruneToBlock={chain_lookup:g} "
                "to prove post-chain-lookup-prune archive reads"
            )
        if tail_pruned is not None and tail_pruned >= 0 and block > tail_pruned:
            issues.append(
                f"archiveApiBlock={block:g} must be <= tailPrunedThroughBlock={tail_pruned:g} "
                "to prove post-prune archive reads"
            )

    methods = archive_api_methods(row)
    if methods is None:
        issues.append("archiveApiMethods is missing")
    elif not methods:
        issues.append("archiveApiMethods must be a non-empty list")
    else:
        method_count = archive_api_method_count(row)
        if (
            failures == 0
            and checks is not None
            and checks > 0
            and method_count is not None
            and checks != method_count
        ):
            issues.append(
                f"archiveApiChecks={checks:g} must equal "
                f"successful archiveApiMethods={method_count} when archiveApiFailures=0"
            )
        missing = sorted(set(required_methods) - methods)
        if missing:
            issues.append("archiveApiMethods missing required methods: " + ",".join(missing))

    return issues


def archive_api_tx_required_methods(require_trace_transaction=False):
    methods = list(ARCHIVE_API_TX_METHODS)
    if require_trace_transaction and ARCHIVE_API_TRACE_TX_METHOD not in methods:
        methods.append(ARCHIVE_API_TRACE_TX_METHOD)
    return methods


def check_archive_tx_evidence(row, require_trace_transaction=False):
    issues = []
    required_methods = archive_api_tx_required_methods(require_trace_transaction)
    if not has_archive_api_evidence(row):
        issues.append("archive API evidence is missing for archive tx evidence")
    if not as_bool(row, "archiveApiTxProbe"):
        issues.append(
            "archiveApiTxProbe is not true; choose an archive-api-block with at least one transaction"
        )
    if require_trace_transaction and not as_bool(row, "archiveApiTraceTransactionProbe"):
        issues.append(
            "archiveApiTraceTransactionProbe is not true; "
            "run nile_sync_sample.sh with --archive-api-trace-transaction"
        )
    tx_hash = row.get("archiveApiTxHash")
    if not isinstance(tx_hash, str) or not tx_hash:
        issues.append("archiveApiTxHash is missing")
    elif re.fullmatch(r"0x[0-9a-fA-F]{64}", tx_hash) is None:
        issues.append("archiveApiTxHash must be a 0x-prefixed 32-byte hash")

    tx_methods = string_set_field(row, "archiveApiTxMethods")
    if tx_methods is None:
        issues.append("archiveApiTxMethods is missing")
    elif not tx_methods:
        issues.append("archiveApiTxMethods must be a non-empty list")
    else:
        missing = sorted(set(required_methods) - tx_methods)
        if missing:
            issues.append("archiveApiTxMethods missing required methods: " + ",".join(missing))

    return issues


def event_log_index_evidence_row(row):
    if row.get("eventLogIndexStatsStatus") == "ok":
        return True
    segments = as_number(row, "eventLogIndexSegments")
    return segments is not None and segments > 0


def rounded_milli(postings, keys):
    if keys == 0:
        return 0
    return (postings * 1000 + keys // 2) // keys


def check_event_log_index_lookup_stats(row, label, prefix):
    issues = []
    fields = {
        "keys": f"eventLogIndex{prefix}Keys",
        "postings": f"eventLogIndex{prefix}Postings",
        "avg": f"eventLogIndex{prefix}AvgPostingsMilli",
        "max": f"eventLogIndex{prefix}MaxPostings",
        "singleton": f"eventLogIndex{prefix}SingletonKeys",
        "multi": f"eventLogIndex{prefix}MultiPostingKeys",
    }
    values = {}
    for name, field in fields.items():
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{field}={row.get(field)!r}, want non-negative integer")
        values[name] = value
    if issues:
        return issues

    keys = values["keys"]
    postings = values["postings"]
    avg = values["avg"]
    max_postings = values["max"]
    singleton = values["singleton"]
    multi = values["multi"]
    if singleton + multi != keys:
        issues.append(f"{label} singleton+multi={singleton + multi} must equal keys={keys}")
    if keys == 0:
        if postings != 0:
            issues.append(f"{label} postings={postings} must be 0 when keys=0")
        if avg != 0:
            issues.append(f"{label} avgPostingsMilli={avg} must be 0 when keys=0")
        if max_postings != 0:
            issues.append(f"{label} maxPostings={max_postings} must be 0 when keys=0")
        return issues
    if postings < keys:
        issues.append(f"{label} postings={postings} must be >= keys={keys}")
    if max_postings == 0:
        issues.append(f"{label} maxPostings must be > 0 when keys={keys}")
    if max_postings > postings:
        issues.append(f"{label} maxPostings={max_postings} must be <= postings={postings}")
    want_avg = rounded_milli(postings, keys)
    if avg != want_avg:
        issues.append(
            f"{label} avgPostingsMilli={avg}, want {want_avg} "
            f"for postings={postings} keys={keys}"
        )
    return issues


def check_event_log_index_evidence(row, require_non_empty=False):
    issues = []
    if row.get("eventLogIndexStatsStatus") != "ok":
        issues.append(f"eventLogIndexStatsStatus={row.get('eventLogIndexStatsStatus')!r}, want 'ok'")

    segments = as_non_negative_int(row, "eventLogIndexSegments")
    if segments is None:
        issues.append(f"eventLogIndexSegments={row.get('eventLogIndexSegments')!r}, want positive integer")
    elif segments <= 0:
        issues.append(f"eventLogIndexSegments={segments:g}, want > 0")

    from_block = as_non_negative_int(row, "eventLogIndexFromBlock")
    to_block = as_non_negative_int(row, "eventLogIndexToBlock")
    if from_block is None:
        issues.append(
            f"eventLogIndexFromBlock={row.get('eventLogIndexFromBlock')!r}, "
            "want non-negative integer"
        )
    if to_block is None:
        issues.append(
            f"eventLogIndexToBlock={row.get('eventLogIndexToBlock')!r}, "
            "want non-negative integer"
        )
    if from_block is not None and to_block is not None:
        if from_block > to_block:
            issues.append(f"event-log index range [{from_block},{to_block}] is inverted")
        if field_present(row, "derivedIndexToBlock"):
            derived_to = as_non_negative_int(row, "derivedIndexToBlock")
            if derived_to is None:
                issues.append(
                    f"derivedIndexToBlock={row.get('derivedIndexToBlock')!r}, "
                    "want non-negative integer"
                )
            elif derived_to != to_block:
                issues.append(
                    f"eventLogIndexToBlock={to_block} must match derivedIndexToBlock={derived_to}"
                )
        tail_pruned = None
        if field_present(row, "tailPrunedThroughBlock"):
            tail_pruned = as_int(row, "tailPrunedThroughBlock")
            if tail_pruned is None:
                issues.append(
                    f"tailPrunedThroughBlock={row.get('tailPrunedThroughBlock')!r}, want integer"
                )
        if tail_pruned is not None and tail_pruned >= 0:
            if from_block > tail_pruned or to_block < tail_pruned:
                issues.append(
                    f"event-log index range [{from_block},{to_block}] "
                    f"must cover tailPrunedThroughBlock={tail_pruned:g}"
                )

    issues.extend(check_event_log_index_lookup_stats(row, "address", "Address"))
    issues.extend(check_event_log_index_lookup_stats(row, "topic", "Topic"))
    if require_non_empty:
        address_postings = as_non_negative_int(row, "eventLogIndexAddressPostings")
        if address_postings is None or address_postings <= 0:
            issues.append(
                f"eventLogIndexAddressPostings={row.get('eventLogIndexAddressPostings')!r}, want > 0"
            )
    return issues


def snapshot_profile_evidence_row(row):
    return (
        any(field in row for field in SNAPSHOT_PROFILE_EVIDENCE_FIELDS)
        or any(
            f"{prefix}Bytes" in row or f"{prefix}SnapshotShareMilli" in row
            for prefix in SNAPSHOT_PROFILE_POINT_FIELDS
        )
    )


def sidecar_share_milli(sidecar_bytes, total_bytes):
    if sidecar_bytes <= 0 or total_bytes <= 0:
        return 0
    return (sidecar_bytes * 1000 + total_bytes - 1) // total_bytes


def check_snapshot_profile_row(row):
    issues = []
    status = str(row.get("snapshotManifestProfileStatus", "")).lower()
    if status != "ok":
        issues.append(f"snapshotManifestProfileStatus={row.get('snapshotManifestProfileStatus')!r}, want 'ok'")

    segments = as_non_negative_int(row, "snapshotProfileSegments")
    if segments is None or segments <= 0:
        issues.append(f"snapshotProfileSegments={row.get('snapshotProfileSegments')!r}, want > 0")
    if not as_bool(row, "snapshotProfileVerifyFiles"):
        issues.append("snapshotProfileVerifyFiles must be true")
    verified_segments = as_non_negative_int(row, "snapshotProfileVerifiedSegments")
    if verified_segments is None:
        issues.append(
            f"snapshotProfileVerifiedSegments={row.get('snapshotProfileVerifiedSegments')!r}, want non-negative integer"
        )
    elif segments is not None and verified_segments != segments:
        issues.append(
            f"snapshotProfileVerifiedSegments={verified_segments}, want snapshotProfileSegments={segments}"
        )

    total_bytes = as_non_negative_int(row, "snapshotProfileTotalBytes")
    payload_bytes = as_non_negative_int(row, "snapshotPayloadBytes")
    sidecar_bytes = as_non_negative_int(row, "snapshotSidecarBytes")
    share = as_non_negative_int(row, "snapshotSidecarShareMilli")
    state_history_bytes = as_non_negative_int(row, "snapshotStateHistoryBytes")
    state_history_compressed_segments = as_non_negative_int(
        row, "snapshotStateHistoryCompressedSegments"
    )
    state_history_compressed_bytes = as_non_negative_int(
        row, "snapshotStateHistoryCompressedBytes"
    )
    state_history_compressed_share = as_number(row, "snapshotStateHistoryCompressedShareMilli")
    if total_bytes is None or total_bytes <= 0:
        issues.append(f"snapshotProfileTotalBytes={row.get('snapshotProfileTotalBytes')!r}, want > 0")
    if payload_bytes is None:
        issues.append(f"snapshotPayloadBytes={row.get('snapshotPayloadBytes')!r}, want non-negative integer")
    if sidecar_bytes is None:
        issues.append(f"snapshotSidecarBytes={row.get('snapshotSidecarBytes')!r}, want non-negative integer")
    if share is None:
        issues.append(f"snapshotSidecarShareMilli={row.get('snapshotSidecarShareMilli')!r}, want non-negative integer")
    if state_history_bytes is None:
        issues.append(
            f"snapshotStateHistoryBytes={row.get('snapshotStateHistoryBytes')!r}, "
            "want non-negative integer"
        )
    if state_history_compressed_segments is None:
        issues.append(
            "snapshotStateHistoryCompressedSegments="
            f"{row.get('snapshotStateHistoryCompressedSegments')!r}, want non-negative integer"
        )
    if state_history_compressed_bytes is None:
        issues.append(
            "snapshotStateHistoryCompressedBytes="
            f"{row.get('snapshotStateHistoryCompressedBytes')!r}, want non-negative integer"
        )
    if state_history_compressed_share is None:
        issues.append(
            "snapshotStateHistoryCompressedShareMilli="
            f"{row.get('snapshotStateHistoryCompressedShareMilli')!r}, want numeric value"
        )
    elif state_history_compressed_share < -1 or state_history_compressed_share > 1000:
        issues.append(
            "snapshotStateHistoryCompressedShareMilli="
            f"{state_history_compressed_share:g}, want -1..1000"
        )

    if (
        total_bytes is not None
        and payload_bytes is not None
        and sidecar_bytes is not None
        and total_bytes != payload_bytes + sidecar_bytes
    ):
        issues.append(
            f"snapshot payload+sidecar={payload_bytes + sidecar_bytes} must equal total={total_bytes}"
        )
    if total_bytes is not None and sidecar_bytes is not None and share is not None:
        want_share = sidecar_share_milli(sidecar_bytes, total_bytes)
        if share != want_share:
            issues.append(
                f"snapshotSidecarShareMilli={share}, want {want_share} "
                f"for sidecarBytes={sidecar_bytes} totalBytes={total_bytes}"
            )
        if share > 1000:
            issues.append(f"snapshotSidecarShareMilli={share}, want <= 1000")
    if (
        state_history_bytes is not None
        and state_history_compressed_bytes is not None
        and state_history_compressed_bytes > state_history_bytes
    ):
        issues.append(
            f"snapshotStateHistoryCompressedBytes={state_history_compressed_bytes} "
            f"must be <= snapshotStateHistoryBytes={state_history_bytes}"
        )
    if (
        state_history_compressed_bytes is not None
        and state_history_compressed_bytes > 0
        and state_history_compressed_segments is not None
        and state_history_compressed_segments <= 0
    ):
        issues.append(
            f"snapshotStateHistoryCompressedSegments={state_history_compressed_segments}, "
            f"want > 0 when snapshotStateHistoryCompressedBytes={state_history_compressed_bytes}"
        )
    if (
        state_history_bytes is not None
        and state_history_compressed_bytes is not None
        and state_history_compressed_share is not None
        and state_history_compressed_share >= 0
    ):
        want_share = sidecar_share_milli(state_history_compressed_bytes, state_history_bytes)
        if state_history_compressed_share != want_share:
            issues.append(
                "snapshotStateHistoryCompressedShareMilli="
                f"{state_history_compressed_share:g}, want {want_share} "
                f"for compressedBytes={state_history_compressed_bytes} "
                f"stateHistoryBytes={state_history_bytes}"
            )

    for prefix in SNAPSHOT_PROFILE_FAMILY_FIELDS:
        bytes_field = f"{prefix}Bytes"
        share_field = f"{prefix}ShareMilli"
        family_bytes = as_non_negative_int(row, bytes_field)
        family_share = as_number(row, share_field)
        if family_bytes is None:
            issues.append(f"{bytes_field}={row.get(bytes_field)!r}, want non-negative integer")
        if family_share is None:
            issues.append(f"{share_field}={row.get(share_field)!r}, want numeric value")
        elif family_share < -1 or family_share > 1000:
            issues.append(f"{share_field}={family_share:g}, want -1..1000")
        elif family_bytes is not None and family_bytes > 0 and family_share < 0:
            issues.append(
                f"{share_field}={family_share:g}, want >= 0 when {bytes_field}={family_bytes}"
            )
    for prefix in SNAPSHOT_PROFILE_POINT_FIELDS:
        segments_field = f"{prefix}Segments"
        bytes_field = f"{prefix}Bytes"
        payload_field = f"{prefix}PayloadBytes"
        sidecar_field = f"{prefix}SidecarBytes"
        sidecar_share_field = f"{prefix}SidecarShareMilli"
        share_field = f"{prefix}SnapshotShareMilli"
        point_segments = as_non_negative_int(row, segments_field)
        point_bytes = as_non_negative_int(row, bytes_field)
        point_payload = as_non_negative_int(row, payload_field)
        point_sidecar = as_non_negative_int(row, sidecar_field)
        point_sidecar_share = as_number(row, sidecar_share_field)
        point_share = as_number(row, share_field)
        if point_segments is None:
            issues.append(f"{segments_field}={row.get(segments_field)!r}, want non-negative integer")
        if point_bytes is None:
            issues.append(f"{bytes_field}={row.get(bytes_field)!r}, want non-negative integer")
        if point_payload is None:
            issues.append(f"{payload_field}={row.get(payload_field)!r}, want non-negative integer")
        if point_sidecar is None:
            issues.append(f"{sidecar_field}={row.get(sidecar_field)!r}, want non-negative integer")
        if (
            point_bytes is not None
            and point_payload is not None
            and point_sidecar is not None
            and point_bytes != point_payload + point_sidecar
        ):
            issues.append(
                f"{prefix} payload+sidecar={point_payload + point_sidecar} "
                f"must equal bytes={point_bytes}"
            )
        if point_sidecar_share is None:
            issues.append(f"{sidecar_share_field}={row.get(sidecar_share_field)!r}, want numeric value")
        elif point_sidecar_share < -1 or point_sidecar_share > 1000:
            issues.append(f"{sidecar_share_field}={point_sidecar_share:g}, want -1..1000")
        elif point_sidecar is not None and point_sidecar > 0 and point_sidecar_share < 0:
            issues.append(
                f"{sidecar_share_field}={point_sidecar_share:g}, want >= 0 "
                f"when {sidecar_field}={point_sidecar}"
            )
        elif point_bytes is not None and point_sidecar is not None and point_sidecar_share >= 0:
            want_sidecar_share = sidecar_share_milli(point_sidecar, point_bytes)
            if point_sidecar_share != want_sidecar_share:
                issues.append(
                    f"{sidecar_share_field}={point_sidecar_share:g}, want {want_sidecar_share} "
                    f"for {sidecar_field}={point_sidecar} {bytes_field}={point_bytes}"
                )
        if point_share is None:
            issues.append(f"{share_field}={row.get(share_field)!r}, want numeric value")
        elif point_share < -1 or point_share > 1000:
            issues.append(f"{share_field}={point_share:g}, want -1..1000")
        elif point_bytes is not None and point_bytes > 0 and point_share < 0:
            issues.append(
                f"{share_field}={point_share:g}, want >= 0 when {bytes_field}={point_bytes}"
            )
        elif total_bytes is not None and point_bytes is not None and point_share >= 0:
            want_share = sidecar_share_milli(point_bytes, total_bytes)
            if point_share != want_share:
                issues.append(
                    f"{share_field}={point_share:g}, want {want_share} "
                    f"for {bytes_field}={point_bytes} totalBytes={total_bytes}"
                )
    return issues


def check_snapshot_point_thresholds(row, max_sidecar_share_milli, max_snapshot_share_milli):
    if max_sidecar_share_milli is None and max_snapshot_share_milli is None:
        return []
    if not snapshot_profile_evidence_row(row):
        return ["snapshot point threshold requires snapshot manifest profile evidence"]
    issues = []
    for prefix in SNAPSHOT_PROFILE_POINT_FIELDS:
        segments_field = f"{prefix}Segments"
        segments = as_non_negative_int(row, segments_field)
        if segments is None:
            issues.append(f"{segments_field}={row.get(segments_field)!r}, want non-negative integer")
            continue
        if segments <= 0:
            continue
        if max_sidecar_share_milli is not None:
            field = f"{prefix}SidecarShareMilli"
            share = as_number(row, field)
            if share is None:
                issues.append(f"{field}={row.get(field)!r}, want numeric value")
            elif share > max_sidecar_share_milli:
                issues.append(f"{field}={share:g} exceeds max {max_sidecar_share_milli:g}")
        if max_snapshot_share_milli is not None:
            field = f"{prefix}SnapshotShareMilli"
            share = as_number(row, field)
            if share is None:
                issues.append(f"{field}={row.get(field)!r}, want numeric value")
            elif share > max_snapshot_share_milli:
                issues.append(f"{field}={share:g} exceeds max {max_snapshot_share_milli:g}")
    return issues


def check_compressed_state_history_evidence(row):
    issues = []
    segments = as_non_negative_int(row, "snapshotStateHistoryCompressedSegments")
    if segments is None or segments <= 0:
        issues.append(
            "snapshotStateHistoryCompressedSegments="
            f"{row.get('snapshotStateHistoryCompressedSegments')!r}, want > 0"
        )
    compressed_bytes = as_non_negative_int(row, "snapshotStateHistoryCompressedBytes")
    if compressed_bytes is None or compressed_bytes <= 0:
        issues.append(
            "snapshotStateHistoryCompressedBytes="
            f"{row.get('snapshotStateHistoryCompressedBytes')!r}, want > 0"
        )
    total_bytes = as_non_negative_int(row, "snapshotStateHistoryBytes")
    if total_bytes is None or total_bytes <= 0:
        issues.append(
            f"snapshotStateHistoryBytes={row.get('snapshotStateHistoryBytes')!r}, want > 0"
        )
    return issues


def check_state_prefetch_evidence(row, require_activity=False, max_errors=None):
    issues = []
    if row.get("syncLogStatus") != "ok":
        issues.append(
            f"syncLogStatus={row.get('syncLogStatus')!r}, want 'ok' "
            "for state prefetch evidence"
        )

    values = {}
    for field in STATE_PREFETCH_EVIDENCE_FIELDS:
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{field}={row.get(field)!r}, want non-negative integer")
        else:
            values[field] = value
    if len(values) != len(STATE_PREFETCH_EVIDENCE_FIELDS):
        return issues

    enqueued = values["syncLogStatePrefetchEnqueued"]
    processed = values["syncLogStatePrefetchProcessed"]
    hits = values["syncLogStatePrefetchHits"]
    misses = values["syncLogStatePrefetchMisses"]
    errors = values["syncLogStatePrefetchErrors"]
    want_processed = hits + misses + errors
    if processed != want_processed:
        issues.append(
            f"state prefetch processed={processed:g}, "
            f"want hits+misses+errors={want_processed:g}"
        )
    if processed > enqueued:
        issues.append(
            f"state prefetch processed={processed:g} exceeds enqueued={enqueued:g}"
        )
    if require_activity and (enqueued <= 0 or processed <= 0):
        issues.append(
            f"state prefetch activity missing: enqueued={enqueued:g} "
            f"processed={processed:g}"
        )
    if max_errors is not None and errors > max_errors:
        issues.append(
            f"syncLogStatePrefetchErrors={errors:g}, want <= {max_errors:g}"
        )
    return issues


def check_sync_phase_cursor_evidence(row):
    issues = []
    if row.get("syncLogStatus") != "ok":
        issues.append(
            f"syncLogStatus={row.get('syncLogStatus')!r}, want 'ok' "
            "for sync phase cursor evidence"
        )

    if not field_present(row, "syncLogPhaseCursorComplete"):
        issues.append("syncLogPhaseCursorComplete is missing")
    complete = as_bool(row, "syncLogPhaseCursorComplete")

    required_integer_fields = (
        "syncLogPhaseCursorCompletedPhases",
        "syncLogPhaseCursorScheduledPhases",
        "syncLogPhaseCursorIncompletePhases",
        "syncLogPhaseCursorCompletedTasks",
        "syncLogPhaseCursorScheduledTasks",
    )
    values = {}
    for field in required_integer_fields:
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{field}={row.get(field)!r}, want non-negative integer")
        else:
            values[field] = value
    if len(values) != len(required_integer_fields):
        return issues

    completed_phases = values["syncLogPhaseCursorCompletedPhases"]
    scheduled_phases = values["syncLogPhaseCursorScheduledPhases"]
    incomplete_phases = values["syncLogPhaseCursorIncompletePhases"]
    completed_tasks = values["syncLogPhaseCursorCompletedTasks"]
    scheduled_tasks = values["syncLogPhaseCursorScheduledTasks"]

    if scheduled_phases <= 0:
        issues.append(f"sync phase cursor scheduled phases={scheduled_phases:g}, want positive")
    if completed_phases > scheduled_phases:
        issues.append(
            f"sync phase cursor completed phases={completed_phases:g} "
            f"exceeds scheduled phases={scheduled_phases:g}"
        )
    expected_incomplete_phases = max(scheduled_phases - completed_phases, 0)
    if incomplete_phases != expected_incomplete_phases:
        issues.append(
            f"sync phase cursor incomplete phases={incomplete_phases:g}, "
            f"want scheduled-completed={expected_incomplete_phases:g}"
        )

    if scheduled_tasks <= 0:
        issues.append(f"sync phase cursor scheduled tasks={scheduled_tasks:g}, want positive")
    if completed_tasks > scheduled_tasks:
        issues.append(
            f"sync phase cursor completed tasks={completed_tasks:g} "
            f"exceeds scheduled tasks={scheduled_tasks:g}"
        )

    phase_ratio = as_number(row, "syncLogPhaseCursorCompletionRatio")
    if phase_ratio is not None and scheduled_phases > 0:
        expected = completed_phases / scheduled_phases
        if abs(phase_ratio - expected) > 1e-9:
            issues.append(
                f"syncLogPhaseCursorCompletionRatio={phase_ratio:g}, want {expected:g}"
            )
    task_ratio = as_number(row, "syncLogPhaseCursorTaskCompletionRatio")
    if task_ratio is not None and scheduled_tasks > 0:
        expected = completed_tasks / scheduled_tasks
        if abs(task_ratio - expected) > 1e-9:
            issues.append(
                f"syncLogPhaseCursorTaskCompletionRatio={task_ratio:g}, want {expected:g}"
            )

    if complete:
        if completed_phases != scheduled_phases:
            issues.append(
                "sync phase cursor complete=true but completed phases "
                f"{completed_phases:g} != scheduled phases {scheduled_phases:g}"
            )
        if incomplete_phases != 0:
            issues.append(
                f"sync phase cursor complete=true but incomplete phases={incomplete_phases:g}"
            )
        if completed_tasks != scheduled_tasks:
            issues.append(
                "sync phase cursor complete=true but completed tasks "
                f"{completed_tasks:g} != scheduled tasks {scheduled_tasks:g}"
            )
        return issues

    string_fields = (
        "syncLogPhaseCursorCurrent",
        "syncLogPhaseCursorCurrentCanonical",
        "syncLogPhaseCursorCurrentSync",
        "syncLogPhaseCursorNextPhase",
        "syncLogPhaseCursorNextCanonical",
        "syncLogPhaseCursorNextSync",
    )
    missing = [field for field in string_fields if not field_present(row, field)]
    if missing:
        issues.append("sync phase cursor missing fields: " + ",".join(missing))

    task_fields = (
        "syncLogPhaseCursorCurrentTaskIndex",
        "syncLogPhaseCursorCurrentTaskCount",
        "syncLogPhaseCursorCurrentTaskRemaining",
        "syncLogPhaseCursorCurrentFromBlock",
        "syncLogPhaseCursorCurrentToBlock",
        "syncLogPhaseCursorNextBlock",
    )
    task_values = {}
    for field in task_fields:
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{field}={row.get(field)!r}, want non-negative integer")
        else:
            task_values[field] = value
    if len(task_values) != len(task_fields):
        return issues

    current_task_index = task_values["syncLogPhaseCursorCurrentTaskIndex"]
    current_task_count = task_values["syncLogPhaseCursorCurrentTaskCount"]
    current_task_remaining = task_values["syncLogPhaseCursorCurrentTaskRemaining"]
    if current_task_count <= 0:
        issues.append(
            f"syncLogPhaseCursorCurrentTaskCount={current_task_count:g}, want positive"
        )
    elif current_task_index >= current_task_count:
        issues.append(
            f"syncLogPhaseCursorCurrentTaskIndex={current_task_index:g} "
            f"must be less than current task count {current_task_count:g}"
        )
    else:
        expected_remaining = current_task_count - current_task_index
        if current_task_remaining != expected_remaining:
            issues.append(
                f"syncLogPhaseCursorCurrentTaskRemaining={current_task_remaining:g}, "
                f"want currentTaskCount-currentTaskIndex={expected_remaining:g}"
            )

    current_from = task_values["syncLogPhaseCursorCurrentFromBlock"]
    current_to = task_values["syncLogPhaseCursorCurrentToBlock"]
    if current_from > current_to:
        issues.append(
            f"sync phase cursor block range inverted: from={current_from:g} to={current_to:g}"
        )
    return issues


def check_row(row, args):
    issues = []
    if row.get("sampleStatus") != "ok":
        issues.append(f"sampleStatus={row.get('sampleStatus')!r}, want 'ok'")

    health = str(row.get("soakHealthStatus", "unknown"))
    if args.allow_warning_health:
        if health not in {"ok", "warning"}:
            issues.append(f"soakHealthStatus={health!r}, want ok/warning")
    elif health != "ok":
        issues.append(f"soakHealthStatus={health!r}, want 'ok'")

    if args.require_stage_status and row.get("stageStatusFileStatus") != "ok":
        issues.append(f"stageStatusFileStatus={row.get('stageStatusFileStatus')!r}, want 'ok'")
    if args.require_stage_status_source:
        source = row.get("stageStatusSource")
        if source != args.require_stage_status_source:
            issues.append(
                f"stageStatusSource={source!r}, want {args.require_stage_status_source!r}"
            )
    if args.require_stage_status:
        issues.extend(
            check_full_staged_sync_evidence(
                row,
                require_stage_details=args.require_stage_detail_evidence,
            )
        )
    elif args.require_stage_detail_evidence:
        issues.extend(check_full_staged_sync_stage_details(row, require=True))

    status = str(row.get("fullStagedSyncStatus", "unknown"))
    if args.require_caught_up:
        if (
            status != "caught-up"
            or not as_bool(row, "fullStagedSyncReady")
            or not as_bool(row, "fullStagedSyncCompleteAtHead")
        ):
            issues.append(
                "full staged sync is not caught up: "
                f"status={status!r} ready={row.get('fullStagedSyncReady')!r} "
                f"completeAtHead={row.get('fullStagedSyncCompleteAtHead')!r}"
            )
    elif status not in {"catching-up", "caught-up"} or not as_bool(row, "fullStagedSyncReady"):
        issues.append(
            "full staged sync is not ready: "
            f"status={status!r} ready={row.get('fullStagedSyncReady')!r}"
        )

    if row.get("stageSyncPipelineMonotonic") is not None and not as_bool(row, "stageSyncPipelineMonotonic"):
        issues.append("stageSyncPipelineMonotonic=false")
    if args.require_stage_stall_evidence:
        issues.extend(check_required_stage_stall_evidence(row))
    else:
        issues.extend(check_stage_stall_evidence(row))
    if args.require_prune_mode_semantics:
        issues.extend(check_prune_mode_semantics(row))
    if (
        args.require_archive_api_evidence
        or args.require_archive_tx_evidence
        or args.require_archive_trace_transaction
        or args.require_archive_trace_block
        or args.archive_api_methods_requested
        or args.min_archive_api_depth_blocks is not None
    ):
        required_archive_methods = list(args.archive_api_methods_required)
        if args.require_archive_tx_evidence or args.require_archive_trace_transaction:
            for method in archive_api_tx_required_methods(args.require_archive_trace_transaction):
                if method not in required_archive_methods:
                    required_archive_methods.append(method)
        if args.require_archive_trace_block:
            for method in ARCHIVE_API_TRACE_BLOCK_METHODS:
                if method not in required_archive_methods:
                    required_archive_methods.append(method)
        issues.extend(
            check_archive_api_evidence(
                row,
                required_archive_methods,
                args.min_archive_api_depth_blocks,
                require_trace_block=args.require_archive_trace_block,
            )
        )
    if args.require_archive_tx_evidence or args.require_archive_trace_transaction:
        issues.extend(
            check_archive_tx_evidence(
                row,
                require_trace_transaction=args.require_archive_trace_transaction,
            )
        )
    if args.require_startup_recovery_evidence:
        issues.extend(check_startup_recovery_evidence(row))

    if (
        args.require_state_prefetch_evidence
        or args.require_state_prefetch_activity
        or args.max_state_prefetch_errors is not None
    ):
        issues.extend(
            check_state_prefetch_evidence(
                row,
                require_activity=args.require_state_prefetch_activity,
                max_errors=args.max_state_prefetch_errors,
            )
        )

    if args.require_sync_phase_cursor_evidence:
        issues.extend(check_sync_phase_cursor_evidence(row))

    if args.require_event_log_index_evidence or args.require_event_log_index_non_empty:
        if not event_log_index_evidence_row(row):
            issues.append("event-log index evidence is missing")
        issues.extend(
            check_event_log_index_evidence(
                row,
                require_non_empty=args.require_event_log_index_non_empty,
            )
        )

    if args.require_sample_prometheus_artifact:
        issues.extend(check_sample_prometheus_artifact(args.result, row))

    if args.require_snapshot_profile_evidence:
        if not snapshot_profile_evidence_row(row):
            issues.append("snapshot manifest profile evidence is missing")
        else:
            issues.extend(check_snapshot_profile_row(row))
    if args.require_compressed_state_history:
        issues.extend(check_compressed_state_history_evidence(row))
    issues.extend(
        check_snapshot_point_thresholds(
            row,
            args.max_snapshot_point_sidecar_share_milli,
            args.max_snapshot_point_snapshot_share_milli,
        )
    )

    for field in ZERO_ISSUE_FIELDS:
        if not field_present(row, field):
            continue
        value = as_non_negative_int(row, field)
        if value is None:
            issues.append(f"{field}={row.get(field)!r}, want non-negative integer zero")
        elif value != 0:
            issues.append(f"{field}={value:g}, want 0")

    if args.require_offline_db_check:
        if not as_bool(row, "offlineDbCheck"):
            issues.append("offlineDbCheck is not true")
        if row.get("offlineDbCheckStatus") != "ok":
            issues.append(f"offlineDbCheckStatus={row.get('offlineDbCheckStatus')!r}, want 'ok'")
        issues.extend(check_storage_alert_evidence(row))
        if row.get("offlineDbCheckPrometheusStatus") not in {None, "", "ok", "skipped"}:
            issues.append(
                "offlineDbCheckPrometheusStatus="
                f"{row.get('offlineDbCheckPrometheusStatus')!r}, want ok/skipped"
            )
        if row.get("offlineDbCheckPrometheusStatus") == "ok":
            issues.extend(check_prometheus_artifact(args.result, row))

    if args.min_height is not None:
        height = as_non_negative_int(row, "height")
        if height is None:
            issues.append(f"height={row.get('height')!r}, want non-negative integer")
        elif height < args.min_height:
            issues.append(f"height={height:g}, want >= {args.min_height}")
    if args.max_lag_blocks is not None:
        lag = as_non_negative_int(row, "fullStagedSyncHeadLagBlocks")
        if lag is None:
            issues.append(
                "fullStagedSyncHeadLagBlocks="
                f"{row.get('fullStagedSyncHeadLagBlocks')!r}, want non-negative integer"
            )
        elif lag > args.max_lag_blocks:
            issues.append(f"fullStagedSyncHeadLagBlocks={lag:g}, want <= {args.max_lag_blocks}")

    issues.extend(check_max_cold_stage_lag_blocks(row, args.max_cold_stage_lag_blocks))
    issues.extend(
        check_min_chain_freezer_metrics(
            row,
            args.min_chain_freezer_blocks,
            args.min_chain_freezer_passes,
        )
    )
    issues.extend(check_min_sync_rate(row, args.min_sync_rate, args.min_sync_rate_blocks))
    issues.extend(
        check_max_datadir_bytes_per_block(
            row,
            args.max_datadir_bytes_per_block,
            args.min_storage_sample_blocks,
        )
    )
    issues.extend(
        check_max_hot_bytes_per_block(
            row,
            args.max_hot_bytes_per_block,
            args.min_storage_sample_blocks,
        )
    )
    issues.extend(check_max_hot_growth_share(row, args.max_hot_growth_share))
    issues.extend(
        check_max_cold_archive_bytes_per_block(
            row,
            args.max_cold_archive_bytes_per_block,
            args.min_storage_sample_blocks,
        )
    )
    issues.extend(
        check_max_derived_index_bytes_per_block(
            row,
            args.max_derived_index_bytes_per_block,
            args.min_storage_sample_blocks,
        )
    )
    issues.extend(check_thresholds(row, args.minimums, ">=", lambda got, want: got >= want))
    issues.extend(check_thresholds(row, args.maximums, "<=", lambda got, want: got <= want))
    return issues


def build_parser():
    parser = argparse.ArgumentParser(
        description="Validate nile_sync_sample.sh JSONL output for full staged-sync acceptance.",
    )
    parser.add_argument("result", type=Path, help="nile_sync_sample.sh JSONL output")
    parser.add_argument("--network", help="only consider rows with this network value")
    parser.add_argument("--mode", help="only consider rows with this mode value")
    parser.add_argument("--label", help="only consider rows with this label value")
    parser.add_argument(
        "--all",
        action="store_true",
        help="validate every selected row instead of only the latest selected row",
    )
    parser.add_argument(
        "--allow-warning-health",
        action="store_true",
        help="allow soakHealthStatus=warning; critical still fails",
    )
    parser.add_argument(
        "--no-require-stage-status",
        dest="require_stage_status",
        action="store_false",
        default=True,
        help="do not require stageStatusFileStatus=ok",
    )
    parser.add_argument(
        "--require-stage-status-source",
        choices=("rpc", "file", "skipped"),
        help="require stageStatusSource to match the expected sampler source",
    )
    parser.add_argument(
        "--require-caught-up",
        action="store_true",
        help="require fullStagedSyncStatus=caught-up and completeAtHead=true",
    )
    parser.add_argument(
        "--require-offline-db-check",
        action="store_true",
        help="require offline db check fields to report ok",
    )
    parser.add_argument(
        "--require-stage-stall-evidence",
        action="store_true",
        help="require selected rows to include stageStalled* diagnostics from the sampler",
    )
    parser.add_argument(
        "--require-stage-detail-evidence",
        action="store_true",
        help=(
            "require per-stage fullStagedSyncStageDetails evidence and cross-check it "
            "against aggregate staged-sync fields"
        ),
    )
    parser.add_argument(
        "--require-prune-mode-semantics",
        action="store_true",
        help=(
            "require selected rows to carry persisted prune-mode evidence matching "
            "the sampled mode and reject mode-incompatible prune progress"
        ),
    )
    parser.add_argument(
        "--require-archive-api-evidence",
        action="store_true",
        help="require selected rows to include successful historical archive API evidence",
    )
    parser.add_argument(
        "--require-archive-tx-evidence",
        action="store_true",
        help=(
            "require archive API evidence for a historical block with at least one "
            "transaction plus successful tx and receipt lookups"
        ),
    )
    parser.add_argument(
        "--require-archive-trace-transaction",
        action="store_true",
        help=(
            "require archive tx evidence to include a successful "
            "debug_traceTransaction probe for the selected transaction"
        ),
    )
    parser.add_argument(
        "--require-archive-trace-block",
        action="store_true",
        help=(
            "require archive API evidence to include successful "
            "debug_traceBlockByNumber and debug_traceBlockByHash probes"
        ),
    )
    parser.add_argument(
        "--require-startup-recovery-evidence",
        action="store_true",
        help="require selected rows to include healthy staged-sync startup recovery evidence",
    )
    parser.add_argument(
        "--require-state-prefetch-evidence",
        action="store_true",
        help="require selected rows to include complete non-negative syncLogStatePrefetch* counters",
    )
    parser.add_argument(
        "--require-state-prefetch-activity",
        action="store_true",
        help="require state-prefetch evidence and at least one enqueued and processed prefetch key",
    )
    parser.add_argument(
        "--max-state-prefetch-errors",
        type=parse_non_negative_int_arg,
        metavar="N",
        help="fail if syncLogStatePrefetchErrors exceeds N; also requires state-prefetch evidence",
    )
    parser.add_argument(
        "--require-sync-phase-cursor-evidence",
        action="store_true",
        help="require selected rows to include consistent syncLogPhaseCursor* runtime progress evidence",
    )
    parser.add_argument(
        "--require-event-log-index-evidence",
        action="store_true",
        help="require selected rows to include valid eventLogIndex* sidecar coverage and lookup stats",
    )
    parser.add_argument(
        "--require-event-log-index-non-empty",
        action="store_true",
        help="require event-log-index evidence and at least one indexed address posting",
    )
    parser.add_argument(
        "--require-sample-prometheus-artifact",
        action="store_true",
        help="require selected rows to include a readable sync/sample Prometheus artifact matching key JSONL fields",
    )
    parser.add_argument(
        "--require-snapshot-profile-evidence",
        action="store_true",
        help="require selected rows to include valid snapshot manifest payload/sidecar profile counters",
    )
    parser.add_argument(
        "--require-compressed-state-history",
        action="store_true",
        help="require snapshot profile evidence to include at least one block-compressed state-history segment",
    )
    parser.add_argument(
        "--max-snapshot-point-sidecar-share-milli",
        type=float,
        metavar="N",
        help="fail if any present snapshot point candidate sidecar share exceeds N/1000 of candidate bytes",
    )
    parser.add_argument(
        "--max-snapshot-point-snapshot-share-milli",
        type=float,
        metavar="N",
        help="fail if any present snapshot point candidate bytes exceed N/1000 of total snapshot bytes",
    )
    parser.add_argument(
        "--archive-api-method",
        action="append",
        default=[],
        help="additional archive API method that must appear in archiveApiMethods; repeatable",
    )
    parser.add_argument(
        "--archive-api-methods",
        action="append",
        default=[],
        help="comma-separated additional archive API methods that must appear in archiveApiMethods",
    )
    parser.add_argument(
        "--min-archive-api-depth-blocks",
        type=float,
        metavar="BLOCKS",
        help=(
            "when archive API evidence is required, require archiveApiBlock "
            "to be at least this many blocks below height"
        ),
    )
    parser.add_argument("--min-height", type=float, help="require latest height to be at least this value")
    parser.add_argument(
        "--max-lag-blocks",
        type=float,
        help="require fullStagedSyncHeadLagBlocks to be no greater than this value",
    )
    parser.add_argument(
        "--max-cold-stage-lag-blocks",
        type=float,
        metavar="BLOCKS",
        help=(
            "require cold/archive builder stage head lag fields to be no "
            "greater than this value"
        ),
    )
    parser.add_argument(
        "--min-chain-freezer-blocks",
        type=float,
        metavar="BLOCKS",
        help="require debug metrics to prove at least this many frozen chain blocks",
    )
    parser.add_argument(
        "--min-chain-freezer-passes",
        type=float,
        metavar="PASSES",
        help="require debug metrics to prove at least this many chain-freezer passes",
    )
    parser.add_argument(
        "--min-sync-rate",
        type=float,
        metavar="BLOCKS_PER_SECOND",
        help=(
            "require selected rows to prove at least this sync rate using the "
            "best available interval/stage/log throughput field"
        ),
    )
    parser.add_argument(
        "--min-sync-rate-blocks",
        type=float,
        metavar="BLOCKS",
        help=(
            "when --min-sync-rate is set, require the selected sync-rate "
            "evidence to come from at least this many imported blocks"
        ),
    )
    parser.add_argument(
        "--max-hot-bytes-per-block",
        type=float,
        metavar="BYTES",
        help=(
            "require selected rows to prove hot Pebble storage growth is no "
            "greater than this many bytes per imported block"
        ),
    )
    parser.add_argument(
        "--max-datadir-bytes-per-block",
        type=float,
        metavar="BYTES",
        help=(
            "require selected rows to prove total datadir growth is no greater "
            "than this many bytes per imported block"
        ),
    )
    parser.add_argument(
        "--max-hot-growth-share",
        type=float,
        metavar="FRACTION",
        help=(
            "require selected interval rows to prove hot Pebble growth is no "
            "more than this fraction of positive disk growth"
        ),
    )
    parser.add_argument(
        "--min-storage-sample-blocks",
        type=float,
        metavar="BLOCKS",
        help=(
            "when bytes-per-block storage gates are set, require their selected "
            "evidence to come from at least this many imported blocks"
        ),
    )
    parser.add_argument(
        "--max-cold-archive-bytes-per-block",
        type=float,
        metavar="BYTES",
        help=(
            "require selected rows to prove cold archive/snapshot growth is no "
            "greater than this many bytes per imported block"
        ),
    )
    parser.add_argument(
        "--max-derived-index-bytes-per-block",
        type=float,
        metavar="BYTES",
        help=(
            "require selected rows to prove derived index growth is no greater "
            "than this many bytes per imported block"
        ),
    )
    parser.add_argument(
        "--min",
        dest="minimums",
        action="append",
        default=[],
        metavar="FIELD=VALUE",
        help="numeric lower bound for a top-level JSONL field; repeatable",
    )
    parser.add_argument(
        "--max",
        dest="maximums",
        action="append",
        default=[],
        metavar="FIELD=VALUE",
        help="numeric upper bound for a top-level JSONL field; repeatable",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.archive_api_methods_requested = split_csv_values(
        args.archive_api_method + args.archive_api_methods
    )
    args.archive_api_methods_required = list(DEFAULT_ARCHIVE_API_METHODS)
    for method in args.archive_api_methods_requested:
        if method not in args.archive_api_methods_required:
            args.archive_api_methods_required.append(method)
    rows, issues = load_rows(args.result)
    selected = filter_rows(rows, args)
    if not selected:
        issues.append("no rows matched selection filters")

    rows_to_check = selected if args.all else ([max(selected, key=row_sort_key)] if selected else [])
    for row in rows_to_check:
        for issue in check_row(row, args):
            issues.append(f"{row_label(row)}: {issue}")

    if issues:
        print("nile sync acceptance: failed", file=sys.stderr)
        for issue in issues:
            print(f"- {issue}", file=sys.stderr)
        return 1

    latest = max(selected, key=row_sort_key)
    print(
        "nile sync acceptance: ok "
        f"rows={len(rows_to_check)} latestLine={latest.get('_line')} "
        f"height={latest.get('height', '-')} "
        f"status={latest.get('fullStagedSyncStatus', '-')} "
        f"lag={latest.get('fullStagedSyncHeadLagBlocks', '-')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
