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
        for case, variant, ns in rows:
            fh.write(
                f"BenchmarkProcessBlock_{case}/{variant}-10          "
                f"       5       {ns:.0f} ns/op       100 B/op       10 allocs/op\n"
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


if __name__ == "__main__":
    unittest.main()
