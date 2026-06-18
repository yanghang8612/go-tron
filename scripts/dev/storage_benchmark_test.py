#!/usr/bin/env python3
import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "storage_benchmark.sh"


class StorageBenchmarkTest(unittest.TestCase):
    def test_emits_storage_alert_failure_row_with_details(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            bindir = tmpdir / "bin"
            bindir.mkdir()
            fake_curl = bindir / "curl"
            fake_curl.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    url="${@: -1}"
                    case "$url" in
                      */wallet/getnowblock)
                        printf '%s\\n' '{"blockID":"0000000100000000000000000000000000000000000000000000000000000000","block_header":{"raw_data":{"number":1}}}'
                        ;;
                      */wallet/getnodeinfo)
                        printf '%s\\n' '{"currentBlock":1}'
                        ;;
                      *)
                        printf '%s\\n' '{}'
                        ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            os.chmod(fake_curl, 0o755)

            fake_gtron = tmpdir / "gtron"
            fake_gtron.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    if [ "${1:-}" = "db" ] && [ "${2:-}" = "storage-alerts" ]; then
                      cat <<'EOF'
                    Storage alerts: datadir=/tmp/gtron status=critical freezerStatus=ok freezerIssues=0 stageStatus=critical stageIssues=1 snapshotStatus=warning snapshotIssues=1 retiredSegments=1 retiredFiles=1 retiredMissing=0 retiredSkippedActive=0 retiredBytes=123 hiddenSize=0
                    Storage stage alert: severity=critical detail=SyncBodiesReady staged-body status=hash-mismatch block=7 hash=ee stagedBlock=7 stagedHash=aa
                    Storage snapshot alert: severity=warning kind=retired-prune-pending detail=retired segment still present
                    EOF
                      exit 1
                    fi
                    trap 'exit 0' TERM INT
                    while true; do sleep 1; done
                    """
                ),
                encoding="utf-8",
            )
            os.chmod(fake_gtron, 0o755)

            workdir = tmpdir / "work"
            output = tmpdir / "results.jsonl"
            env = dict(os.environ)
            env["PATH"] = f"{bindir}{os.pathsep}{env.get('PATH', '')}"
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--profile",
                    "producer",
                    "--modes",
                    "full",
                    "--target-blocks",
                    "1",
                    "--timeout",
                    "5",
                    "--workdir",
                    str(workdir),
                    "--output",
                    str(output),
                    "--gtron",
                    str(fake_gtron),
                    "--no-build",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            rows = output.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 1, proc.stdout + proc.stderr)
            row = json.loads(rows[0])
            self.assertEqual(row["status"], "storage-alerts-critical")
            self.assertEqual(row["stageVerifyStatus"], "critical")
            self.assertEqual(row["stageVerifyIssues"], 1)
            self.assertEqual(
                row["stageVerifyDetails"],
                [
                    {
                        "severity": "critical",
                        "detail": "SyncBodiesReady staged-body status=hash-mismatch block=7 hash=ee stagedBlock=7 stagedHash=aa",
                    }
                ],
            )
            self.assertEqual(row["snapshotAlertStatus"], "warning")
            self.assertEqual(
                row["snapshotAlertDetails"],
                [
                    {
                        "severity": "warning",
                        "kind": "retired-prune-pending",
                        "detail": "retired segment still present",
                    }
                ],
            )


if __name__ == "__main__":
    unittest.main()
