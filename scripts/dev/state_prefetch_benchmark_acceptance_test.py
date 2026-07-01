#!/usr/bin/env python3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "state_prefetch_benchmark_acceptance.py"


def write_benchmark(path, rows):
    with path.open("w", encoding="utf-8") as fh:
        fh.write("goos: darwin\n")
        fh.write("goarch: arm64\n")
        for row in rows:
            if len(row) == 3:
                case, variant, ns = row
                bytes_op = 100
                allocs_op = 10
            else:
                case, variant, ns, bytes_op, allocs_op = row
            fh.write(
                f"BenchmarkProcessBlock_{case}/{variant}-10          "
                f"       5       {ns:.0f} ns/op       "
                f"{bytes_op:.0f} B/op       {allocs_op:.0f} allocs/op\n"
            )
        fh.write("PASS\n")


def complete_rows(overrides=None):
    overrides = overrides or {}
    cases = {
        "LightTRX_HeavyState": {
            "prefetch=off": 1000,
            "prefetch=on_workers=2_lookahead=8": 1005,
            "prefetch=on_workers=4_lookahead=8": 1030,
        },
        "LightTRX_ColdState": {
            "prefetch=off": 2000,
            "prefetch=on_workers=2_lookahead=8": 2010,
            "prefetch=on_workers=4_lookahead=8": 2005,
        },
        "HeavyTRX_HeavyState": {
            "prefetch=off": 10000,
            "prefetch=on_workers=2_lookahead=8": 10100,
            "prefetch=on_workers=4_lookahead=8": 10100,
        },
        "HeavyTRX_ColdState": {
            "prefetch=off": 20000,
            "prefetch=on_workers=2_lookahead=8": 15000,
            "prefetch=on_workers=4_lookahead=8": 14000,
        },
    }
    for (case, variant), value in overrides.items():
        cases[case][variant] = value
    rows = []
    for case, variants in cases.items():
        for variant, ns in variants.items():
            rows.append((case, variant, ns))
    return rows


class StatePrefetchBenchmarkAcceptanceTest(unittest.TestCase):
    def test_accepts_best_variant_with_heavy_gain_and_light_overhead(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            write_benchmark(benchmark, complete_rows())

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(benchmark)],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("state prefetch benchmark acceptance: ok", proc.stdout)
            self.assertIn("variant=prefetch=on_workers=2_lookahead=8", proc.stdout)
            self.assertIn("heavyMinImprovement=25.00%", proc.stdout)
            self.assertIn("lightMaxOverhead=0.50%", proc.stdout)

    def test_rejects_when_no_variant_meets_thresholds(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            write_benchmark(
                benchmark,
                complete_rows(
                    {
                        ("HeavyTRX_ColdState", "prefetch=on_workers=2_lookahead=8"): 19000,
                        ("HeavyTRX_ColdState", "prefetch=on_workers=4_lookahead=8"): 19500,
                    }
                ),
            )

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(benchmark)],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("state prefetch benchmark acceptance: failed", proc.stderr)
            self.assertIn(
                "prefetch=on_workers=2_lookahead=8 HeavyTRX_ColdState improvement=5.00%",
                proc.stderr,
            )
            self.assertIn(
                "prefetch=on_workers=4_lookahead=8 LightTRX_HeavyState overhead=3.00%",
                proc.stderr,
            )

    def test_rejects_missing_required_benchmark_case(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            rows = [row for row in complete_rows() if row[0] != "LightTRX_ColdState"]
            write_benchmark(benchmark, rows)

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(benchmark)],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("missing benchmark case LightTRX_ColdState", proc.stderr)

    def test_rejects_explicit_variant_that_fails_light_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            write_benchmark(benchmark, complete_rows())

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(benchmark),
                    "--variant",
                    "prefetch=on_workers=4_lookahead=8",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "prefetch=on_workers=4_lookahead=8 LightTRX_HeavyState overhead=3.00%",
                proc.stderr,
            )

    def test_rejects_baseline_as_explicit_variant(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            write_benchmark(benchmark, complete_rows())

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(benchmark),
                    "--variant",
                    "prefetch=off",
                    "--min-heavy-improvement",
                    "0",
                    "--max-light-overhead",
                    "1",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("prefetch=off is not a prefetch=on benchmark variant", proc.stderr)

    def test_auto_selection_ignores_non_prefetch_on_variants(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            rows = complete_rows() + [
                ("LightTRX_HeavyState", "experimental=on", 500),
                ("LightTRX_ColdState", "experimental=on", 500),
                ("HeavyTRX_HeavyState", "experimental=on", 500),
                ("HeavyTRX_ColdState", "experimental=on", 500),
            ]
            write_benchmark(benchmark, rows)

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(benchmark)],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("variant=prefetch=on_workers=2_lookahead=8", proc.stdout)
            self.assertNotIn("experimental=on", proc.stdout)

    def test_accepts_optional_resource_overhead_gates(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            write_benchmark(benchmark, complete_rows())

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(benchmark),
                    "--max-bytes-overhead",
                    "0",
                    "--max-allocs-overhead",
                    "0",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("bytesMaxOverhead=0.00%", proc.stdout)
            self.assertIn("allocsMaxOverhead=0.00%", proc.stdout)

    def test_rejects_optional_resource_overhead_gates(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            rows = []
            for case, variant, ns in complete_rows():
                if case == "LightTRX_HeavyState" and variant == "prefetch=on_workers=2_lookahead=8":
                    rows.append((case, variant, ns, 130, 12))
                else:
                    rows.append((case, variant, ns, 100, 10))
            write_benchmark(benchmark, rows)

            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    str(benchmark),
                    "--max-bytes-overhead",
                    "0.10",
                    "--max-allocs-overhead",
                    "0.10",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn(
                "prefetch=on_workers=2_lookahead=8 LightTRX_HeavyState "
                "B/op overhead=30.00%, want <= 10.00%",
                proc.stderr,
            )
            self.assertIn(
                "prefetch=on_workers=2_lookahead=8 LightTRX_HeavyState "
                "allocs/op overhead=20.00%, want <= 10.00%",
                proc.stderr,
            )

    def test_rejects_fractional_resource_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            benchmark.write_text(
                "goos: darwin\n"
                "goarch: arm64\n"
                "BenchmarkProcessBlock_LightTRX_HeavyState/prefetch=off-10          "
                "5       1000 ns/op       100.5 B/op       10 allocs/op\n"
                "BenchmarkProcessBlock_LightTRX_ColdState/prefetch=off-10          "
                "5       2000 ns/op       100 B/op       10.5 allocs/op\n"
                "PASS\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(benchmark)],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("cannot parse benchmark line", proc.stderr)
            self.assertIn("100.5 B/op", proc.stderr)
            self.assertIn("10.5 allocs/op", proc.stderr)

    def test_rejects_zero_iteration_or_time_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            benchmark = Path(tmp) / "benchmark.txt"
            benchmark.write_text(
                "goos: darwin\n"
                "goarch: arm64\n"
                "BenchmarkProcessBlock_LightTRX_HeavyState/prefetch=off-10          "
                "0       1000 ns/op       100 B/op       10 allocs/op\n"
                "BenchmarkProcessBlock_LightTRX_ColdState/prefetch=off-10          "
                "5       0 ns/op       100 B/op       10 allocs/op\n"
                "PASS\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(benchmark)],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("benchmark iterations=0, want positive integer", proc.stderr)
            self.assertIn("ns/op=0, want positive value", proc.stderr)


if __name__ == "__main__":
    unittest.main()
