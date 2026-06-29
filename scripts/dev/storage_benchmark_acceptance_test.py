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
                lines = [
                    "# TYPE gtron_storage_alert_status gauge",
                    "# TYPE gtron_storage_alert_issue gauge",
                    'gtron_storage_alert_status{datadir="/tmp/gtron"} 0',
                ]
                if name == "minimal.prom":
                    lines.extend(
                        [
                            "# TYPE gtron_storage_signed_cold_prune gauge",
                            'gtron_storage_signed_cold_prune{datadir="/tmp/gtron"} 1',
                            "# TYPE gtron_storage_prune_boundary_block gauge",
                            'gtron_storage_prune_boundary_block{datadir="/tmp/gtron",field="chainLookupPruneToBlock"} 50',
                            'gtron_storage_prune_boundary_block{datadir="/tmp/gtron",field="tailPrunedThroughBlock"} 45',
                        ]
                    )
                (tmpdir / name).write_text("\n".join(lines) + "\n", encoding="utf-8")

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

    def test_accepts_minimal_physical_tail_prune_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 100,
                    "tailPrunedThroughBlock": 95,
                    "tailPrunedFiles": 2,
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
                    "--require-minimal-physical-tail-prune",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_missing_minimal_physical_tail_prune_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 100,
                    "tailPrunedThroughBlock": 95,
                    "tailPrunedFiles": 0,
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
                    "--require-minimal-physical-tail-prune",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("tailPrunedFiles=0.0, want > 0", proc.stderr)

    def test_rejects_minimal_tail_files_without_tail_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "pruneMode": "minimal",
                    "pruneModePersisted": True,
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 100,
                    "coldFreezerToBlock": 100,
                    "derivedIndexToBlock": 100,
                    "tailPrunedThroughBlock": -1,
                    "tailPrunedFiles": 1,
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
            self.assertIn(
                "tailPrunedThroughBlock must be >= 0 when tailPrunedFiles is positive",
                proc.stderr,
            )

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
                    "tailPrunedFiles": 0,
                    "coldFreezerToBlock": -1,
                    "derivedIndexToBlock": -1,
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
                    "coldFreezerToBlock": 50,
                    "tailPrunedThroughBlock": -1,
                    "tailPrunedFiles": 0,
                },
                {
                    **base,
                    "unix": 30,
                    "mode": "minimal",
                    "pruneMode": "minimal",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "coldFreezerToBlock": 50,
                    "derivedIndexToBlock": 50,
                    "tailPrunedThroughBlock": 45,
                    "tailPrunedFiles": 0,
                },
                {
                    **base,
                    "unix": 40,
                    "mode": "full",
                    "pruneMode": "full",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "coldFreezerToBlock": 50,
                    "tailPrunedThroughBlock": -1,
                    "tailPrunedFiles": 0,
                },
                {
                    **base,
                    "unix": 50,
                    "mode": "snap",
                    "pruneMode": "snap",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "coldFreezerToBlock": 50,
                    "tailPrunedThroughBlock": -1,
                    "tailPrunedFiles": 0,
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
                    "archive,blocks,full,minimal,snap",
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
                    "tailPrunedFiles": 1,
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "blocks",
                    "pruneMode": "blocks",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": -1,
                    "tailPrunedThroughBlock": 7,
                    "tailPrunedFiles": 2,
                },
                {
                    **base,
                    "unix": 25,
                    "role": "observer",
                    "mode": "blocks",
                    "pruneMode": "blocks",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "coldFreezerToBlock": 49,
                    "tailPrunedThroughBlock": -1,
                    "tailPrunedFiles": 1,
                },
                {
                    **base,
                    "unix": 30,
                    "mode": "minimal",
                    "pruneMode": "minimal",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 10,
                    "coldFreezerToBlock": 11,
                    "derivedIndexToBlock": 9,
                    "tailPrunedThroughBlock": 12,
                },
                {
                    **base,
                    "unix": 40,
                    "mode": "full",
                    "pruneMode": "minimal",
                    "pruneModePersisted": "false",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": -1,
                },
                {
                    **base,
                    "unix": 50,
                    "mode": "snap",
                    "pruneMode": "snap",
                    "signedColdPrune": 1,
                    "chainLookupPruneToBlock": 50,
                    "coldFreezerToBlock": 49,
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-prune-mode-semantics",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("signedColdPrune must be false for archive", proc.stderr)
            self.assertIn("chainLookupPruneToBlock=12 is not allowed for archive mode", proc.stderr)
            self.assertIn("tailPrunedFiles=1 is not allowed for archive mode", proc.stderr)
            self.assertIn("chainLookupPruneToBlock must be >= 0 when signedColdPrune is true for blocks mode", proc.stderr)
            self.assertIn("coldFreezerToBlock=49.0 must cover chainLookupPruneToBlock=50", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=7 is not allowed for blocks mode", proc.stderr)
            self.assertIn("tailPrunedFiles=2 is not allowed for blocks mode", proc.stderr)
            self.assertIn("tailPrunedThroughBlock=12 exceeds chainLookupPruneToBlock=10", proc.stderr)
            self.assertIn("coldFreezerToBlock=11.0 must cover tailPrunedThroughBlock=12", proc.stderr)
            self.assertIn("derivedIndexToBlock=9.0 must cover tailPrunedThroughBlock=12", proc.stderr)
            self.assertIn("pruneMode='minimal' does not match mode='full'", proc.stderr)
            self.assertIn("pruneModePersisted must be true", proc.stderr)
            self.assertIn("chainLookupPruneToBlock must be >= 0 when signedColdPrune is true for full mode", proc.stderr)
            self.assertIn("coldFreezerToBlock=49.0 must cover chainLookupPruneToBlock=50", proc.stderr)

    def test_accepts_required_size_reduction(self):
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
            }
            rows = [
                {
                    **base,
                    "unix": 10,
                    "mode": "full",
                    "chaindataBytes": 1000,
                    "datadirBytes": 2000,
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "minimal",
                    "chaindataBytes": 550,
                    "datadirBytes": 1500,
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
                    "full,minimal",
                    "--require-size-reduction",
                    "minimal:full:chaindataBytes=0.40",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_missing_required_size_reduction(self):
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
            }
            rows = [
                {
                    **base,
                    "unix": 10,
                    "mode": "full",
                    "chaindataBytes": 1000,
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "minimal",
                    "chaindataBytes": 850,
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
                    "--require-size-reduction",
                    "minimal:full:chaindataBytes=0.40",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "minimal chaindataBytes reduction=15.00%, want >= 40.00% versus full",
                proc.stderr,
            )

    def test_rejects_malformed_required_size_reduction(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "role": "producer",
                        "status": "ok",
                        "mode": "full",
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-size-reduction",
                    "minimal:full",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("must use MODE:BASE_MODE:FIELD=RATIO", proc.stderr)

    def test_accepts_storage_bytes_per_block_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "mode": "minimal",
                        "height": 100,
                        "datadirBytes": 10000,
                        "chaindataBytes": 2000,
                        "ancientBytes": 3000,
                        "snapshotBytes": 1000,
                        "derivedIndexBytes": 500,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--max-datadir-bytes-per-block",
                    "120",
                    "--max-hot-bytes-per-block",
                    "25",
                    "--max-cold-archive-bytes-per-block",
                    "45",
                    "--max-derived-index-bytes-per-block",
                    "6",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_storage_bytes_per_block_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "mode": "minimal",
                        "height": 100,
                        "datadirBytes": 13000,
                        "chaindataBytes": 3000,
                        "ancientBytes": 3000,
                        "snapshotBytes": 2000,
                        "derivedIndexBytes": 700,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--max-datadir-bytes-per-block",
                    "120",
                    "--max-hot-bytes-per-block",
                    "25",
                    "--max-cold-archive-bytes-per-block",
                    "45",
                    "--max-derived-index-bytes-per-block",
                    "6",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("datadirBytesPerBlock=130 failed <= max datadir bytes per block 120", proc.stderr)
            self.assertIn("hotBytesPerBlock=30 failed <= max hot bytes per block 25", proc.stderr)
            self.assertIn(
                "coldArchiveBytesPerBlock=50 failed <= max cold archive bytes per block 45",
                proc.stderr,
            )
            self.assertIn(
                "derivedIndexBytesPerBlock=7 failed <= max derived index bytes per block 6",
                proc.stderr,
            )

    def test_rejects_storage_bytes_per_block_without_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = Path(tmp) / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "mode": "minimal",
                        "height": 0,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--max-datadir-bytes-per-block",
                    "120",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("height=0, want > 0 for datadir bytes-per-block evidence", proc.stderr)

    def test_accepts_retired_prune_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "retiredPruneRan": True,
                    "retiredPruneSegments": 1,
                    "retiredPruneDeleted": 2,
                    "retiredPruneMissing": 0,
                    "retiredPruneSkippedActive": 0,
                    "retiredPruneBytesDeleted": 4096,
                    "snapshotRetiredSegments": 0,
                    "snapshotRetiredFiles": 0,
                    "snapshotRetiredMissing": 0,
                    "snapshotRetiredSkippedActive": 0,
                    "snapshotRetiredBytes": 0,
                }
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-retired-prune-evidence",
                    "--require-retired-prune-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_invalid_retired_prune_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "retiredPruneRan": False,
                    "retiredPruneSegments": 1,
                    "retiredPruneDeleted": 0,
                    "retiredPruneMissing": 1,
                    "retiredPruneSkippedActive": 1,
                    "retiredPruneBytesDeleted": 0,
                    "snapshotRetiredSegments": 1,
                    "snapshotRetiredFiles": 2,
                    "snapshotRetiredMissing": 1,
                    "snapshotRetiredSkippedActive": 1,
                    "snapshotRetiredBytes": 1024,
                }
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-retired-prune-evidence",
                    "--require-retired-prune-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("retiredPruneRan=False, want true", proc.stderr)
            self.assertIn("retiredPruneMissing=1, want 0", proc.stderr)
            self.assertIn("retiredPruneSkippedActive=1, want 0", proc.stderr)
            self.assertIn("snapshotRetiredBytes=1024, want 0 after prune-retired", proc.stderr)

    def test_rejects_retired_prune_evidence_missing_required_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                }
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-retired-prune-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "line 1 minimal/producer missing retired-prune evidence for required mode 'minimal'",
                proc.stderr,
            )

    def test_accepts_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 200,
                    "tailPrunedThroughBlock": 90,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 5,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
                    "archiveApiMethods": [
                        "eth_getBlockByNumber",
                        "eth_getBalance",
                        "eth_getCode",
                        "eth_getStorageAt",
                        "eth_getLogs",
                    ],
                }
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-archive-api-evidence",
                    "--require-archive-api-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

            require_call = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-archive-api-evidence",
                    "--archive-api-method",
                    "eth_call",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(
                require_call.returncode,
                0,
                require_call.stdout + require_call.stderr,
            )
            self.assertIn("archiveApiMethods missing required methods: eth_call", require_call.stderr)

    def test_accepts_archive_tx_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 200,
                    "tailPrunedThroughBlock": 90,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 7,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
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
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-archive-tx-evidence",
                    "--require-archive-tx-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_requires_archive_trace_transaction_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            base_row = {
                "unix": 10,
                "profile": "producer",
                "mode": "minimal",
                "role": "producer",
                "status": "ok",
                "freezerAlertStatus": "ok",
                "stageVerifyStatus": "ok",
                "modeAlertStatus": "ok",
                "snapshotAlertStatus": "ok",
                "height": 200,
                "tailPrunedThroughBlock": 90,
                "archiveApiStatus": "ok",
                "archiveApiChecks": 8,
                "archiveApiFailures": 0,
                "archiveApiBlock": 80,
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
                    "--role",
                    "producer",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

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
                    "--role",
                    "producer",
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
                    "--role",
                    "producer",
                    "--require-archive-trace-transaction",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiTraceTransactionProbe is not true", proc.stderr)

    def test_rejects_archive_tx_evidence_missing_required_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 200,
                    "tailPrunedThroughBlock": 90,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 5,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
                    "archiveApiMethods": [
                        "eth_getBlockByNumber",
                        "eth_getBalance",
                        "eth_getCode",
                        "eth_getStorageAt",
                        "eth_getLogs",
                    ],
                }
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-archive-tx-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "line 1 minimal/producer missing archive tx evidence for required mode 'minimal'",
                proc.stderr,
            )

    def test_rejects_invalid_archive_tx_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 200,
                    "tailPrunedThroughBlock": 90,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 5,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
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
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiMethods missing required tx methods", proc.stderr)
            self.assertIn("archiveApiTxProbe is not true", proc.stderr)
            self.assertIn("archiveApiTxHash is missing", proc.stderr)
            self.assertIn("archiveApiTxMethods must be a non-empty list", proc.stderr)

    def test_rejects_archive_tx_evidence_short_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 200,
                    "tailPrunedThroughBlock": 90,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 7,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
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
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-archive-tx-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiTxHash must be a 0x-prefixed 32-byte hash", proc.stderr)

    def test_rejects_missing_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "minimal",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("required archive API evidence has no selected latest row", proc.stderr)

    def test_rejects_archive_api_evidence_missing_required_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 100,
                    "tailPrunedThroughBlock": 40,
                },
                {
                    "unix": 20,
                    "profile": "producer",
                    "mode": "archive",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 100,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 5,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
                    "archiveApiMethods": [
                        "eth_getBlockByNumber",
                        "eth_getBalance",
                        "eth_getCode",
                        "eth_getStorageAt",
                        "eth_getLogs",
                    ],
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-archive-api-evidence",
                    "--require-archive-api-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "line 1 minimal/producer missing archive API evidence for required mode 'minimal'",
                proc.stderr,
            )

    def test_rejects_invalid_archive_api_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            rows = [
                {
                    "unix": 10,
                    "profile": "producer",
                    "mode": "minimal",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 100,
                    "tailPrunedThroughBlock": 40,
                    "archiveApiStatus": "failed",
                    "archiveApiChecks": 0,
                    "archiveApiFailures": 2,
                    "archiveApiBlock": 100,
                    "archiveApiMethods": ["eth_getBalance"],
                },
                {
                    "unix": 20,
                    "profile": "producer",
                    "mode": "snap",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 100,
                    "tailPrunedThroughBlock": 40,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 1,
                    "archiveApiBlock": 45,
                    "archiveApiMethods": "eth_getBalance",
                },
                {
                    "unix": 30,
                    "profile": "producer",
                    "mode": "archive",
                    "role": "producer",
                    "status": "ok",
                    "freezerAlertStatus": "ok",
                    "stageVerifyStatus": "ok",
                    "modeAlertStatus": "ok",
                    "snapshotAlertStatus": "ok",
                    "height": 100,
                    "archiveApiStatus": "ok",
                    "archiveApiChecks": 2,
                    "archiveApiFailures": 0,
                    "archiveApiBlock": 80,
                    "archiveApiMethods": [
                        "eth_getBlockByNumber",
                        "eth_getBalance",
                        "eth_getCode",
                        "eth_getStorageAt",
                        "eth_getLogs",
                    ],
                },
            ]
            write_result(result, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--require-archive-api-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("archiveApiStatus='failed', want 'ok'", proc.stderr)
            self.assertIn("archiveApiChecks=0.0, want > 0", proc.stderr)
            self.assertIn("archiveApiFailures=2, want 0", proc.stderr)
            self.assertIn("archiveApiFailures=None, want 0", proc.stderr)
            self.assertIn("archiveApiBlock=100 must be below height=100", proc.stderr)
            self.assertIn("archiveApiMethods missing required methods", proc.stderr)
            self.assertIn("archiveApiBlock=45 must be <= tailPrunedThroughBlock=40", proc.stderr)
            self.assertIn("archiveApiMethods must be a non-empty list", proc.stderr)
            self.assertIn(
                "archiveApiChecks=2 must equal successful archiveApiMethods=5 when archiveApiFailures=0",
                proc.stderr,
            )

    def test_accepts_event_log_index_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "snap",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "derivedIndexToBlock": 80,
                        "eventLogIndexSegments": 2,
                        "eventLogIndexAddressKeys": 3,
                        "eventLogIndexAddressPostings": 6,
                        "eventLogIndexAddressAvgPostingsMilli": 2000,
                        "eventLogIndexAddressMaxPostings": 3,
                        "eventLogIndexAddressSingletonKeys": 1,
                        "eventLogIndexAddressMultiPostingKeys": 2,
                        "eventLogIndexTopicKeys": 2,
                        "eventLogIndexTopicPostings": 3,
                        "eventLogIndexTopicAvgPostingsMilli": 1500,
                        "eventLogIndexTopicMaxPostings": 2,
                        "eventLogIndexTopicSingletonKeys": 1,
                        "eventLogIndexTopicMultiPostingKeys": 1,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-event-log-index-evidence",
                    "--require-event-log-index-mode",
                    "snap",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_missing_or_invalid_event_log_index_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            base = {
                "unix": 10,
                "profile": "producer",
                "mode": "snap",
                "role": "producer",
                "status": "ok",
                "freezerAlertStatus": "ok",
                "stageVerifyStatus": "ok",
                "modeAlertStatus": "ok",
                "snapshotAlertStatus": "ok",
            }
            write_result(result, [{**base, "derivedIndexToBlock": -1, "eventLogIndexSegments": 0}])

            missing = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(missing.returncode, 0, missing.stdout + missing.stderr)
            self.assertIn("required event-log index evidence has no selected latest derived-index row", missing.stderr)

            write_result(
                result,
                [
                    {
                        **base,
                        "derivedIndexToBlock": 80,
                        "eventLogIndexSegments": 0,
                        "eventLogIndexAddressKeys": 2,
                        "eventLogIndexAddressPostings": 1,
                        "eventLogIndexAddressAvgPostingsMilli": 500,
                        "eventLogIndexAddressMaxPostings": 2,
                        "eventLogIndexAddressSingletonKeys": 2,
                        "eventLogIndexAddressMultiPostingKeys": 1,
                        "eventLogIndexTopicKeys": 0,
                        "eventLogIndexTopicPostings": 1,
                        "eventLogIndexTopicAvgPostingsMilli": 0,
                        "eventLogIndexTopicMaxPostings": 0,
                        "eventLogIndexTopicSingletonKeys": 0,
                        "eventLogIndexTopicMultiPostingKeys": 0,
                    }
                ],
            )
            invalid = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-event-log-index-evidence",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(invalid.returncode, 0, invalid.stdout + invalid.stderr)
            self.assertIn("eventLogIndexSegments=0, want > 0", invalid.stderr)
            self.assertIn("address singleton+multi=3 must equal keys=2", invalid.stderr)
            self.assertIn("address postings=1 must be >= keys=2", invalid.stderr)
            self.assertIn("topic postings=1 must be 0 when keys=0", invalid.stderr)

    def test_rejects_event_log_index_evidence_missing_required_mode(self):
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
            }
            rows = [
                {
                    **base,
                    "unix": 10,
                    "mode": "minimal",
                    "derivedIndexToBlock": -1,
                    "eventLogIndexSegments": 0,
                },
                {
                    **base,
                    "unix": 20,
                    "mode": "snap",
                    "derivedIndexToBlock": 80,
                    "eventLogIndexSegments": 1,
                    "eventLogIndexAddressKeys": 1,
                    "eventLogIndexAddressPostings": 1,
                    "eventLogIndexAddressAvgPostingsMilli": 1000,
                    "eventLogIndexAddressMaxPostings": 1,
                    "eventLogIndexAddressSingletonKeys": 1,
                    "eventLogIndexAddressMultiPostingKeys": 0,
                    "eventLogIndexTopicKeys": 1,
                    "eventLogIndexTopicPostings": 1,
                    "eventLogIndexTopicAvgPostingsMilli": 1000,
                    "eventLogIndexTopicMaxPostings": 1,
                    "eventLogIndexTopicSingletonKeys": 1,
                    "eventLogIndexTopicMultiPostingKeys": 0,
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
                    "--require-event-log-index-evidence",
                    "--require-event-log-index-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "line 1 minimal/producer missing event-log index evidence for required mode 'minimal'",
                proc.stderr,
            )

    def test_accepts_snapshot_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "minimal",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
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
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-snapshot-profile-evidence",
                    "--require-snapshot-profile-mode",
                    "minimal",
                    "--max",
                    "minimal.producer.snapshotSidecarShareMilli=200",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("storage benchmark acceptance: ok", proc.stdout)

    def test_rejects_missing_snapshot_profile_evidence_for_required_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "minimal",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
                    "--require-snapshot-profile-mode",
                    "minimal",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "line 1 minimal/producer missing snapshot manifest profile evidence for required mode 'minimal'",
                proc.stderr,
            )

    def test_rejects_invalid_snapshot_profile_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "minimal",
                        "role": "producer",
                        "status": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
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
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--role",
                    "producer",
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

    def test_rejects_prometheus_alert_status_value_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/gtron"} 2\n',
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
                        "storageAlertStatus": "ok",
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
            self.assertIn("gtron_storage_alert_status=2, want 0", proc.stderr)

    def test_rejects_prometheus_prune_boundary_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            prom = tmpdir / "alerts.prom"
            prom.write_text(
                '# TYPE gtron_storage_alert_status gauge\n'
                '# TYPE gtron_storage_alert_issue gauge\n'
                '# TYPE gtron_storage_signed_cold_prune gauge\n'
                '# TYPE gtron_storage_prune_boundary_block gauge\n'
                'gtron_storage_alert_status{datadir="/tmp/gtron"} 0\n'
                'gtron_storage_signed_cold_prune{datadir="/tmp/gtron"} 1\n'
                'gtron_storage_prune_boundary_block{datadir="/tmp/gtron",field="chainLookupPruneToBlock"} 40\n',
                encoding="utf-8",
            )
            result = tmpdir / "results.jsonl"
            write_result(
                result,
                [
                    {
                        "unix": 10,
                        "profile": "producer",
                        "mode": "minimal",
                        "role": "producer",
                        "datadir": "/tmp/gtron",
                        "status": "ok",
                        "storageAlertStatus": "ok",
                        "freezerAlertStatus": "ok",
                        "stageVerifyStatus": "ok",
                        "modeAlertStatus": "ok",
                        "snapshotAlertStatus": "ok",
                        "signedColdPrune": 1,
                        "chainLookupPruneToBlock": 50,
                        "tailPrunedThroughBlock": 45,
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
            self.assertIn(
                "gtron_storage_prune_boundary_block field='chainLookupPruneToBlock'=40, want 50",
                proc.stderr,
            )
            self.assertIn(
                "missing gtron_storage_prune_boundary_block field='tailPrunedThroughBlock'",
                proc.stderr,
            )

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
