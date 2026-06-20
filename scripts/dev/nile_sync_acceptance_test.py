#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "nile_sync_acceptance.py"


def write_result(path, rows):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


class NileSyncAcceptanceTest(unittest.TestCase):
    def test_accepts_clean_latest_staged_sync_row(self):
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
                        "label": "candidate",
                        "sampleStatus": "ok",
                        "soakHealthStatus": "ok",
                        "stageStatusFileStatus": "ok",
                        "fullStagedSyncStatus": "catching-up",
                        "fullStagedSyncReady": True,
                        "fullStagedSyncCompleteAtHead": False,
                        "stageSyncPipelineMonotonic": True,
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
                        "height": 1000,
                        "fullStagedSyncHeadLagBlocks": 12,
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


if __name__ == "__main__":
    unittest.main()
