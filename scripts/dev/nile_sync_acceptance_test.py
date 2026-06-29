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


class NileSyncAcceptanceTest(unittest.TestCase):
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
