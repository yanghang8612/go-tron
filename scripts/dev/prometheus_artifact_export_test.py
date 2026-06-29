#!/usr/bin/env python3
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "prometheus_artifact_export.py"


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True) + "\n")


class PrometheusArtifactExportTest(unittest.TestCase):
    def test_exports_latest_row_artifacts_from_relative_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            nile = tmpdir / "nile.jsonl"
            storage = tmpdir / "storage.jsonl"
            sample_prom = tmpdir / "sample.prom"
            offline_prom = tmpdir / "offline.prom"
            benchmark_prom = tmpdir / "benchmark.prom"
            storage_prom = tmpdir / "storage.prom"
            old_prom = tmpdir / "old.prom"
            output = tmpdir / "collector" / "gtron.prom"

            sample_prom.write_text("gtron_nile_sync_height{datadir=\"/tmp/nile\"} 100\n", encoding="utf-8")
            offline_prom.write_text("gtron_storage_alert_status{datadir=\"/tmp/nile\"} 0\n", encoding="utf-8")
            benchmark_prom.write_text("gtron_storage_benchmark_derived_index_bytes{datadir=\"/tmp/storage\"} 4096\n", encoding="utf-8")
            storage_prom.write_text("gtron_storage_alert_status{datadir=\"/tmp/storage\"} 0\n", encoding="utf-8")
            old_prom.write_text("gtron_nile_sync_height{datadir=\"/tmp/old\"} 1\n", encoding="utf-8")
            write_jsonl(
                nile,
                [
                    {"unix": 1, "samplePrometheus": old_prom.name},
                    {
                        "unix": 2,
                        "samplePrometheus": sample_prom.name,
                        "offlineDbCheckPrometheus": offline_prom.name,
                    },
                ],
            )
            write_jsonl(
                storage,
                [
                    {
                        "unix": 3,
                        "storageBenchmarkPrometheus": benchmark_prom.name,
                        "storageAlertPrometheus": storage_prom.name,
                    }
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(nile),
                    str(storage),
                    "--output",
                    str(output),
                    "--require-field",
                    "samplePrometheus",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("prometheus artifact export: ok", proc.stdout)
            text = output.read_text(encoding="utf-8")
            self.assertIn("gtron_nile_sync_height{datadir=\"/tmp/nile\"} 100", text)
            self.assertIn("gtron_storage_alert_status{datadir=\"/tmp/nile\"} 0", text)
            self.assertIn("gtron_storage_benchmark_derived_index_bytes{datadir=\"/tmp/storage\"} 4096", text)
            self.assertIn("gtron_storage_alert_status{datadir=\"/tmp/storage\"} 0", text)
            self.assertNotIn("/tmp/old", text)
            self.assertIn("# BEGIN samplePrometheus", text)
            self.assertIn("# BEGIN storageBenchmarkPrometheus", text)
            self.assertIn("# END storageAlertPrometheus", text)

    def test_all_rows_exports_older_artifacts_too(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            first = tmpdir / "first.prom"
            second = tmpdir / "second.prom"
            output = tmpdir / "gtron.prom"
            first.write_text("metric_first 1\n", encoding="utf-8")
            second.write_text("metric_second 2\n", encoding="utf-8")
            write_jsonl(
                result,
                [
                    {"unix": 1, "samplePrometheus": first.name},
                    {"unix": 2, "samplePrometheus": second.name},
                ],
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--output",
                    str(output),
                    "--all-rows",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            text = output.read_text(encoding="utf-8")
            self.assertIn("metric_first 1", text)
            self.assertIn("metric_second 2", text)

    def test_rejects_missing_required_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            result = tmpdir / "samples.jsonl"
            output = tmpdir / "gtron.prom"
            write_jsonl(result, [{"unix": 1, "samplePrometheus": "missing.prom"}])

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(result),
                    "--output",
                    str(output),
                    "--require-field",
                    "samplePrometheus",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("read samplePrometheus artifact", proc.stderr)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
