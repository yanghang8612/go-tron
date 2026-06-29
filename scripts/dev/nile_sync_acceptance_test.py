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
    "SyncBodies",
    "SyncBodiesReady",
    "SyncImport",
    "SyncExecution",
    "SyncCommitment",
    "SyncFinish",
]

SYNC_STAGE_FIELDS = {
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


def full_stage_details(blocks=None, verified=None):
    blocks = blocks or {}
    verified = verified or {}
    return [
        {
            "stage": stage,
            "field": SYNC_STAGE_FIELDS[stage],
            "present": True,
            "block": blocks.get(stage, 1000),
            "verified": verified.get(stage, "canonical"),
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
        "fullStagedSyncStageCount": 6,
        "fullStagedSyncPresentStageCount": 6,
        "fullStagedSyncVerifiedStageCount": 6,
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
            "snapshotSidecarShareMilli": 188,
            "archiveApiFailures": 0,
            "stageStalled": False,
        }
    )
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
                "# TYPE gtron_nile_sync_full_staged_sync_head_lag_blocks gauge",
                f'gtron_nile_sync_full_staged_sync_head_lag_blocks{{{labels}}} {row["fullStagedSyncHeadLagBlocks"]}',
                "# TYPE gtron_nile_sync_datadir_bytes gauge",
                f'gtron_nile_sync_datadir_bytes{{{labels}}} {row["datadirBytes"]}',
                "# TYPE gtron_nile_sync_chaindata_bytes gauge",
                f'gtron_nile_sync_chaindata_bytes{{{labels}}} {row["chaindataBytes"]}',
                "# TYPE gtron_nile_sync_cold_archive_bytes gauge",
                f'gtron_nile_sync_cold_archive_bytes{{{labels}}} {row["coldArchiveBytes"]}',
                "# TYPE gtron_nile_sync_derived_index_bytes gauge",
                f'gtron_nile_sync_derived_index_bytes{{{labels}}} {row["derivedIndexBytes"]}',
                "# TYPE gtron_nile_sync_snapshot_sidecar_share_milli gauge",
                f'gtron_nile_sync_snapshot_sidecar_share_milli{{{labels}}} {row["snapshotSidecarShareMilli"]}',
                "# TYPE gtron_nile_sync_archive_api_failures gauge",
                f'gtron_nile_sync_archive_api_failures{{{labels}}} {row["archiveApiFailures"]}',
                "# TYPE gtron_nile_sync_stage_stalled gauge",
                f"gtron_nile_sync_stage_stalled{{{labels}}} 0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return row


def add_archive_trace_evidence(row):
    row.update(
        {
            "archiveApiStatus": "ok",
            "archiveApiChecks": 10,
            "archiveApiFailures": 0,
            "archiveApiBlock": 999,
            "archiveApiCallProbe": True,
            "archiveApiTraceTransactionProbe": True,
            "archiveApiMethods": [
                "eth_getBlockByNumber",
                "eth_getBalance",
                "eth_getCode",
                "eth_call",
                "debug_traceCall",
                "eth_getStorageAt",
                "eth_getLogs",
                "eth_getTransactionByHash",
                "eth_getTransactionReceipt",
                "debug_traceTransaction",
            ],
            "archiveApiTxProbe": True,
            "archiveApiTxHash": "0x" + "ab" * 32,
            "archiveApiTxMethods": [
                "eth_getTransactionByHash",
                "eth_getTransactionReceipt",
                "debug_traceTransaction",
            ],
        }
    )
    return row


def append_archive_trace_prometheus_metrics(path, row, *, include_trace=True):
    labels = 'datadir="/tmp/nile",label="",mode="full",network="nile"'
    lines = [
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
                    "--max",
                    "snapshotSidecarShareMilli=200",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                "stageSnapshotEventLogBuildHeadLagBlocks is missing or non-numeric",
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
            self.assertIn("fullStagedSyncStageCount=None, want 6", proc.stderr)
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
            self.assertIn("fullStagedSyncVerifiedStageCount=6, want detail-derived 5", proc.stderr)
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
                "height=999.0",
                "fullStagedSyncHeadLagBlocks=500.0",
            ):
                self.assertIn(want, proc.stderr)

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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                        "fullStagedSyncRequiredStages": [
                            "SyncBodies",
                            "SyncBodiesReady",
                            "SyncImport",
                            "SyncExecution",
                            "SyncCommitment",
                            "SyncFinish",
                        ],
                        "fullStagedSyncStageCount": 6,
                        "fullStagedSyncPresentStageCount": 6,
                        "fullStagedSyncVerifiedStageCount": 6,
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
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("nile sync acceptance: ok", proc.stdout)

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
                "archiveApiChecks": 8,
                "archiveApiFailures": 0,
                "archiveApiBlock": 99,
                "archiveApiTraceTransactionProbe": True,
                "archiveApiMethods": [
                    "eth_getBlockByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "debug_traceTransaction",
                ],
                "archiveApiTxProbe": True,
                "archiveApiTxHash": "0x" + "ab" * 32,
                "archiveApiTxMethods": [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
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
            missing_trace["archiveApiChecks"] = 7
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
                        "archiveApiChecks": 6,
                        "archiveApiFailures": 0,
                        "archiveApiBlock": 99,
                        "archiveApiMethods": [
                            "eth_getBlockByNumber",
                            "eth_getBalance",
                            "eth_getCode",
                            "eth_getStorageAt",
                            "eth_getLogs",
                            "eth_getTransactionByHash",
                        ],
                        "archiveApiTxProbe": True,
                        "archiveApiTxHash": "0x" + "ab" * 32,
                        "archiveApiTxMethods": ["eth_getTransactionByHash"],
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
            self.assertIn("archiveApiChecks=0.0, want > 0", proc.stderr)
            self.assertIn("archiveApiFailures=1, want 0", proc.stderr)
            self.assertIn("archiveApiBlock=100 must be below height=100", proc.stderr)
            self.assertIn("archiveApiMethods missing required methods", proc.stderr)

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


if __name__ == "__main__":
    unittest.main()
