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


if __name__ == "__main__":
    unittest.main()
