#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "storage_benchmark_acceptance.py"


def write_result(path, rows):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


class StorageBenchmarkAcceptanceTest(unittest.TestCase):
    def test_accepts_clean_required_modes_artifacts_and_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            for name in ("full.prom", "blocks.prom", "minimal.prom"):
                (tmpdir / name).write_text(
                    '# TYPE gtron_storage_alert_status gauge\n'
                    '# TYPE gtron_storage_alert_issue gauge\n'
                    'gtron_storage_alert_status{datadir="/tmp/gtron"} 0\n',
                    encoding="utf-8",
                )

            result = tmpdir / "results.jsonl"
            base = {
                "profile": "producer",
                "role": "producer",
                "status": "ok",
                "freezerAlertStatus": "ok",
                "stageVerifyStatus": "ok",
                "modeAlertStatus": "ok",
                "snapshotAlertStatus": "ok",
                "height": 120,
                "freezerAlertIssues": 0,
            }
            rows = [
                {
                    **base,
                    "unix": 10,
                    "mode": "full",
                    "storageAlertPrometheus": str(tmpdir / "full.prom"),
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "blocks",
                    "storageAlertPrometheus": str(tmpdir / "blocks.prom"),
                },
                {
                    **base,
                    "unix": 30,
                    "mode": "minimal",
                    "storageAlertPrometheus": str(tmpdir / "minimal.prom"),
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "tailPrunedThroughBlock": 45,
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-modes",
                    "full,blocks,minimal",
                    "--require-prometheus-artifacts",
                    "--require-minimal-tail-prune",
                    "--min",
                    "minimal.producer.tailPrunedThroughBlock=40",
                    "--max",
                    "full.producer.freezerAlertIssues=0",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)
            self.assertIn("modes=blocks,full,minimal", proc.stdout)

    def test_rejects_missing_mode_artifact_status_and_minimal_tail_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "full",
                    "role": "producer",
                    "status": "storage-alerts-critical",
                    "freezerAlertStatus": "critical",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "storageAlertPrometheus": str(tmpdir / "missing.prom"),
                    "height": 120,
                },
                {
                    "unix": 20,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "storageAlertPrometheus": str(tmpdir / "also-missing.prom"),
                    "signedColdPrune": 0,
                    "chainLookupPruneToBlock": -1,
                    "tailPrunedThroughBlock": -1,
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-modes",
                    "full,minimal,archive",
                    "--require-prometheus-artifacts",
                    "--require-minimal-tail-prune",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("required mode 'archive'", proc.stderr)
            self.assertIn("status='storage-alerts-critical'", proc.stderr)
            self.assertIn("prometheus artifact", proc.stderr)
            self.assertIn("signedColdPrune must be true", proc.stderr)
            self.assertIn("tailPrunedThroughBlock must be >= 0", proc.stderr)

    def test_accepts_prune_mode_semantics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            base = {
                "profile": "producer",
                "role": "producer",
                "status": "ok",
                "freezerAlertStatus": "ok",
                "stageVerifyStatus": "ok",
                "modeAlertStatus": "ok",
                "snapshotAlertStatus": "ok",
                "pruneModePersisted": True,
            }
            rows = [
                {
                    **base,
                    "unix": 10,
                    "mode": "archive",
                    "pruneMode": "archive",
                    "signedColdPrune": 0,
                    "chainLookupPruneToBlock": -1,
                    "tailPrunedThroughBlock": -1,
                    "balanceTracePruneToBlock": -1,
                    "sectionBloomPruneToSection": -1,
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "blocks",
                    "pruneMode": "blocks",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "tailPrunedThroughBlock": -1,
                },
                {
                    **base,
                    "unix": 30,
                    "mode": "minimal",
                    "pruneMode": "minimal",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "tailPrunedThroughBlock": 45,
                },
                {
                    **base,
                    "unix": 40,
                    "mode": "full",
                    "pruneMode": "full",
                    "tailPrunedThroughBlock": -1,
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-modes",
                    "archive,blocks,full,minimal",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_prune_mode_semantic_violations(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            base = {
                "profile": "producer",
                "role": "producer",
                "status": "ok",
                "freezerAlertStatus": "ok",
                "stageVerifyStatus": "ok",
                "modeAlertStatus": "ok",
                "snapshotAlertStatus": "ok",
                "pruneModePersisted": True,
            }
            rows = [
                {
                    **base,
                    "unix": 10,
                    "mode": "archive",
                    "pruneMode": "archive",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 12,
                    "tailPrunedThroughBlock": 9,
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "blocks",
                    "pruneMode": "blocks",
                    "tailPrunedThroughBlock": 7,
                },
                {
                    **base,
                    "unix": 30,
                    "mode": "full",
                    "pruneMode": "minimal",
                    "pruneModePersisted": "false",
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("signedColdPrune must be false for archive", proc.stderr)
            self.assertIn("chainLookupPruneToBlock=12 is not allowed for archive mode", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=7 is not allowed for blocks mode", proc.stderr)
            self.assertIn("pruneMode='minimal' does not match mode='full'", proc.stderr)
            self.assertIn("pruneModePersisted must be true", proc.stderr)

    def test_rejects_prometheus_artifact_without_issue_metric(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/gtron"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "full",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "storageAlertPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prometheus-artifacts",
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
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/other"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "full",
                        "role": "producer",
                        "datadir": "/tmp/gtron",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "storageAlertPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prometheus-artifacts",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing gtron_storage_alert_status", proc.stderr)

    def test_rejects_prometheus_artifact_missing_structured_issue_kind(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/gtron"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "full",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "stageVerifyDetails": [
                            {
                                "severity": "critical",
                                "kind": "stage-verification",
                                "detail": "Finish verified=missing-canonical",
                            }
                        ],
                        "storageAlertPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prometheus-artifacts",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("component='stage'", proc.stderr)
            self.assertIn("kind='stage-verification'", proc.stderr)

    def test_rejects_prometheus_artifact_missing_stage_pipeline_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/gtron"} 0\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "full",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 10,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 8,
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "storageAlertPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prometheus-artifacts",
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
            prom = tmpdir / "alerts.prom"
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
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/other",stage="SnapshotBuild",status="missing",upstream="Finish"} 10\n'
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/other",stage="SnapshotBuild",status="missing",upstream="Finish"} 8\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "full",
                        "role": "producer",
                        "datadir": "/tmp/gtron",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 10,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 8,
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "storageAlertPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prometheus-artifacts",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing gtron_storage_stage_pipeline_pending", proc.stderr)
            self.assertIn("missing next pipeline target", proc.stderr)

    def test_rejects_prometheus_artifact_mismatched_stage_pipeline_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_stage_pipeline_complete gauge\n'
                '# TYPE gtron_storage_stage_pipeline_pending gauge\n'
                '# TYPE gtron_storage_stage_pipeline_issues gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_target_block gauge\n'
                '# TYPE gtron_storage_stage_pipeline_next_current_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/gtron"} 0\n'
                'gtron_storage_stage_pipeline_complete{datadir="/tmp/gtron"} 0\n'
                'gtron_storage_stage_pipeline_pending{datadir="/tmp/gtron"} 3\n'
                'gtron_storage_stage_pipeline_issues{datadir="/tmp/gtron"} 1\n'
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/gtron",stage="SnapshotBuild",status="missing",upstream="Finish"} 9\n'
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/gtron",stage="SnapshotBuild",status="missing",upstream="Finish"} 7\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "full",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "stageAlertPipelineComplete": False,
                        "stageAlertPipelinePending": 2,
                        "stageAlertPipelineIssues": 0,
                        "stageAlertPipelineNext": "SnapshotBuild",
                        "stageAlertPipelineNextStatus": "missing",
                        "stageAlertPipelineNextTarget": 10,
                        "stageAlertPipelineNextUpstream": "Finish",
                        "stageAlertPipelineNextCurrent": 8,
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "storageAlertPrometheus": str(prom),
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prometheus-artifacts",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("gtron_storage_stage_pipeline_pending=3, want 2", proc.stderr)
            self.assertIn("gtron_storage_stage_pipeline_issues=1, want 0", proc.stderr)
            self.assertIn("value=9, want 10", proc.stderr)
            self.assertIn("value=7, want 8", proc.stderr)


if __name__ == "__main__":
    unittest.main()
