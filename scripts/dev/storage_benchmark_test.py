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
    def test_emits_archive_api_probe_fields(self):
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
                    payload=""
                    prev=""
                    for arg in "$@"; do
                      if [ "$prev" = "--data-binary" ]; then
                        payload="$arg"
                      fi
                      prev="$arg"
                    done
                    case "$url" in
                      */wallet/getnowblock)
                        printf '%s\\n' '{"blockID":"0000000200000000000000000000000000000000000000000000000000000000","block_header":{"raw_data":{"number":2}}}'
                        ;;
                      */wallet/getnodeinfo)
                        printf '%s\\n' '{"currentBlock":2}'
                        ;;
                      http://127.0.0.1:*)
                        case "$payload" in
                          *eth_getBlockByNumber*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xabababababababababababababababababababababababababababababababab","transactions":["0x1212121212121212121212121212121212121212121212121212121212121212"]}}'
                            ;;
                          *eth_getLogs*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":[]}'
                            ;;
                          *debug_traceTransaction*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"failed":false,"returnValue":"","structLogs":[]}}'
                            ;;
                          *debug_traceCall*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"failed":false,"returnValue":"","structLogs":[]}}'
                            ;;
                          *eth_getTransactionByHash*|*eth_getTransactionReceipt*|*eth_getTransactionByBlockNumberAndIndex*|*eth_getTransactionByBlockHashAndIndex*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"hash":"0x1212121212121212121212121212121212121212121212121212121212121212","blockNumber":"0x1","blockHash":"0xabababababababababababababababababababababababababababababababab","transactionIndex":"0x0"}}'
                            ;;
                          *)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"0x0"}'
                            ;;
                        esac
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
                      for arg in "$@"; do
                        if [ "$arg" = "--prometheus" ]; then
                          cat <<'EOF'
                    # TYPE gtron_storage_alert_status gauge
                    # TYPE gtron_storage_alert_issue gauge
                    gtron_storage_alert_status{datadir="/tmp/gtron"} 0
                    EOF
                          exit 0
                        fi
                      done
                      cat <<'EOF'
                    {"datadir":"/tmp/gtron","status":"ok","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"ok","stageIssues":0,"stageVerifyDetails":[],"stagePipeline":{"complete":true,"pending":0,"issues":0,"tasks":[]},"modeStatus":"ok","modeIssues":0,"modeAlertDetails":[],"pruneMode":"full","pruneModePersisted":true,"snapshotStatus":"ok","snapshotIssues":0,"snapshotAlertDetails":[],"snapshotRetiredSegments":0,"snapshotRetiredFiles":0,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":0}
                    EOF
                      exit 0
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
                    "2",
                    "--timeout",
                    "5",
                    "--workdir",
                    str(workdir),
                    "--output",
                    str(output),
                    "--gtron",
                    str(fake_gtron),
                    "--no-build",
                    "--archive-api-probe",
                    "--archive-api-call-data",
                    "0x70a08231",
                    "--archive-api-trace-transaction",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            rows = output.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 1, proc.stdout + proc.stderr)
            row = json.loads(rows[0])
            self.assertEqual(row["archiveApiStatus"], "ok")
            self.assertEqual(row["archiveApiChecks"], 14)
            self.assertEqual(row["archiveApiFailures"], 0)
            self.assertEqual(row["archiveApiBlock"], 1)
            self.assertEqual(row["archiveApiDepthBlocks"], 1)
            self.assertTrue(row["archiveApiCallProbe"])
            self.assertTrue(row["archiveApiTraceTransactionProbe"])
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_call",
                    "debug_traceCall",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockTransactionCountByHash",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                    "debug_traceTransaction",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(
                row["archiveApiTxHash"],
                "0x1212121212121212121212121212121212121212121212121212121212121212",
            )
            self.assertEqual(
                row["archiveApiTxMethods"],
                [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                    "debug_traceTransaction",
                ],
            )
            self.assertEqual(row["snapshotManifestProfileStatus"], "missing")
            self.assertEqual(row["snapshotSidecarShareMilli"], -1)
            benchmark_metrics = Path(row["storageBenchmarkPrometheus"]).read_text(encoding="utf-8")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_status\{[^}]*status=\"ok\"[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_cold_freezer_to_block\{[^}]*\} -1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_chain_lookup_prune_to_block\{[^}]*\} -1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_archive_api_depth_blocks\{[^}]*\} 1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_signed_cold_prune\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_tail_pruned_files\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_event_log_index_segments\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_event_log_index_address_postings\{[^}]*\} 0\n")
            self.assertRegex(
                benchmark_metrics,
                r'gtron_storage_benchmark_archive_api_method_success\{[^}]*method="debug_traceTransaction"[^}]*\} 1\n',
            )
            self.assertRegex(
                benchmark_metrics,
                r'gtron_storage_benchmark_archive_api_tx_method_success\{[^}]*method="debug_traceTransaction"[^}]*\} 1\n',
            )

    def test_archive_api_probe_rejects_invalid_trace_transaction_result(self):
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
                    payload=""
                    prev=""
                    for arg in "$@"; do
                      if [ "$prev" = "--data-binary" ]; then
                        payload="$arg"
                      fi
                      prev="$arg"
                    done
                    case "$url" in
                      */wallet/getnowblock)
                        printf '%s\\n' '{"blockID":"0000000200000000000000000000000000000000000000000000000000000000","block_header":{"raw_data":{"number":2}}}'
                        ;;
                      */wallet/getnodeinfo)
                        printf '%s\\n' '{"currentBlock":2}'
                        ;;
                      http://127.0.0.1:*)
                        case "$payload" in
                          *eth_getBlockByNumber*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xabababababababababababababababababababababababababababababababab","transactions":["0x1212121212121212121212121212121212121212121212121212121212121212"]}}'
                            ;;
                          *eth_getLogs*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":[]}'
                            ;;
                          *debug_traceTransaction*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"0x0"}'
                            ;;
                          *debug_traceCall*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"failed":false,"returnValue":"","structLogs":[]}}'
                            ;;
                          *eth_getTransactionByHash*|*eth_getTransactionReceipt*|*eth_getTransactionByBlockNumberAndIndex*|*eth_getTransactionByBlockHashAndIndex*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"hash":"0x1212121212121212121212121212121212121212121212121212121212121212","transactionHash":"0x1212121212121212121212121212121212121212121212121212121212121212","blockNumber":"0x1","blockHash":"0xabababababababababababababababababababababababababababababababab","transactionIndex":"0x0"}}'
                            ;;
                          *)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"0x0"}'
                            ;;
                        esac
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
                    {"datadir":"/tmp/gtron","status":"ok","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"ok","stageIssues":0,"stageVerifyDetails":[],"stagePipeline":{"complete":true,"pending":0,"issues":0,"tasks":[]},"modeStatus":"ok","modeIssues":0,"modeAlertDetails":[],"pruneMode":"full","pruneModePersisted":true,"snapshotStatus":"ok","snapshotIssues":0,"snapshotAlertDetails":[],"snapshotRetiredSegments":0,"snapshotRetiredFiles":0,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":0}
                    EOF
                      exit 0
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
                    "2",
                    "--timeout",
                    "5",
                    "--workdir",
                    str(workdir),
                    "--output",
                    str(output),
                    "--gtron",
                    str(fake_gtron),
                    "--no-build",
                    "--archive-api-probe",
                    "--archive-api-call-data",
                    "0x70a08231",
                    "--archive-api-trace-transaction",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            row = json.loads(output.read_text(encoding="utf-8").strip().splitlines()[0])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 14)
            self.assertEqual(row["archiveApiFailures"], 1)
            self.assertTrue(row["archiveApiCallProbe"])
            self.assertTrue(row["archiveApiTraceTransactionProbe"])
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_call",
                    "debug_traceCall",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockTransactionCountByHash",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(
                row["archiveApiTxHash"],
                "0x1212121212121212121212121212121212121212121212121212121212121212",
            )
            self.assertEqual(
                row["archiveApiTxMethods"],
                [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                ],
            )

    def test_archive_api_probe_rejects_null_transaction_results(self):
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
                    payload=""
                    prev=""
                    for arg in "$@"; do
                      if [ "$prev" = "--data-binary" ]; then
                        payload="$arg"
                      fi
                      prev="$arg"
                    done
                    case "$url" in
                      */wallet/getnowblock)
                        printf '%s\\n' '{"blockID":"0000000200000000000000000000000000000000000000000000000000000000","block_header":{"raw_data":{"number":2}}}'
                        ;;
                      */wallet/getnodeinfo)
                        printf '%s\\n' '{"currentBlock":2}'
                        ;;
                      http://127.0.0.1:*)
                        case "$payload" in
                          *eth_getBlockByNumber*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xabababababababababababababababababababababababababababababababab","transactions":["0x1212121212121212121212121212121212121212121212121212121212121212"]}}'
                            ;;
                          *eth_getLogs*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":[]}'
                            ;;
                          *eth_getTransactionByHash*|*eth_getTransactionReceipt*|*eth_getTransactionByBlockNumberAndIndex*|*eth_getTransactionByBlockHashAndIndex*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":null}'
                            ;;
                          *)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"0x0"}'
                            ;;
                        esac
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
                    {"datadir":"/tmp/gtron","status":"ok","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"ok","stageIssues":0,"stageVerifyDetails":[],"stagePipeline":{"complete":true,"pending":0,"issues":0,"tasks":[]},"modeStatus":"ok","modeIssues":0,"modeAlertDetails":[],"pruneMode":"full","pruneModePersisted":true,"snapshotStatus":"ok","snapshotIssues":0,"snapshotAlertDetails":[],"snapshotRetiredSegments":0,"snapshotRetiredFiles":0,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":0}
                    EOF
                      exit 0
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
                    "2",
                    "--timeout",
                    "5",
                    "--workdir",
                    str(workdir),
                    "--output",
                    str(output),
                    "--gtron",
                    str(fake_gtron),
                    "--no-build",
                    "--archive-api-probe",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            row = json.loads(output.read_text(encoding="utf-8").strip().splitlines()[0])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 11)
            self.assertEqual(row["archiveApiFailures"], 4)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockTransactionCountByHash",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(
                row["archiveApiTxHash"],
                "0x1212121212121212121212121212121212121212121212121212121212121212",
            )
            self.assertEqual(row["archiveApiTxMethods"], [])

    def test_archive_api_probe_rejects_mismatched_transaction_results(self):
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
                    payload=""
                    prev=""
                    for arg in "$@"; do
                      if [ "$prev" = "--data-binary" ]; then
                        payload="$arg"
                      fi
                      prev="$arg"
                    done
                    case "$url" in
                      */wallet/getnowblock)
                        printf '%s\\n' '{"blockID":"0000000200000000000000000000000000000000000000000000000000000000","block_header":{"raw_data":{"number":2}}}'
                        ;;
                      */wallet/getnodeinfo)
                        printf '%s\\n' '{"currentBlock":2}'
                        ;;
                      http://127.0.0.1:*)
                        case "$payload" in
                          *eth_getBlockByNumber*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xabababababababababababababababababababababababababababababababab","transactions":["0x1212121212121212121212121212121212121212121212121212121212121212"]}}'
                            ;;
                          *eth_getLogs*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":[]}'
                            ;;
                          *eth_getTransactionByHash*|*eth_getTransactionReceipt*|*eth_getTransactionByBlockNumberAndIndex*|*eth_getTransactionByBlockHashAndIndex*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"hash":"0x3434343434343434343434343434343434343434343434343434343434343434","transactionHash":"0x3434343434343434343434343434343434343434343434343434343434343434","blockNumber":"0x1","blockHash":"0xabababababababababababababababababababababababababababababababab","transactionIndex":"0x0"}}'
                            ;;
                          *)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"0x0"}'
                            ;;
                        esac
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
                    {"datadir":"/tmp/gtron","status":"ok","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"ok","stageIssues":0,"stageVerifyDetails":[],"stagePipeline":{"complete":true,"pending":0,"issues":0,"tasks":[]},"modeStatus":"ok","modeIssues":0,"modeAlertDetails":[],"pruneMode":"full","pruneModePersisted":true,"snapshotStatus":"ok","snapshotIssues":0,"snapshotAlertDetails":[],"snapshotRetiredSegments":0,"snapshotRetiredFiles":0,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":0}
                    EOF
                      exit 0
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
                    "2",
                    "--timeout",
                    "5",
                    "--workdir",
                    str(workdir),
                    "--output",
                    str(output),
                    "--gtron",
                    str(fake_gtron),
                    "--no-build",
                    "--archive-api-probe",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            row = json.loads(output.read_text(encoding="utf-8").strip().splitlines()[0])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 11)
            self.assertEqual(row["archiveApiFailures"], 4)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockTransactionCountByHash",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(
                row["archiveApiTxHash"],
                "0x1212121212121212121212121212121212121212121212121212121212121212",
            )
            self.assertEqual(row["archiveApiTxMethods"], [])

    def test_archive_api_probe_rejects_non_hex_scalar_results(self):
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
                    payload=""
                    prev=""
                    for arg in "$@"; do
                      if [ "$prev" = "--data-binary" ]; then
                        payload="$arg"
                      fi
                      prev="$arg"
                    done
                    case "$url" in
                      */wallet/getnowblock)
                        printf '%s\\n' '{"blockID":"0000000200000000000000000000000000000000000000000000000000000000","block_header":{"raw_data":{"number":2}}}'
                        ;;
                      */wallet/getnodeinfo)
                        printf '%s\\n' '{"currentBlock":2}'
                        ;;
                      http://127.0.0.1:*)
                        case "$payload" in
                          *eth_getBlockByNumber*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xabababababababababababababababababababababababababababababababab","transactions":["0x1212121212121212121212121212121212121212121212121212121212121212"]}}'
                            ;;
                          *eth_getBalance*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"not-hex"}'
                            ;;
                          *eth_getLogs*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":[]}'
                            ;;
                          *eth_getTransactionByHash*|*eth_getTransactionReceipt*|*eth_getTransactionByBlockNumberAndIndex*|*eth_getTransactionByBlockHashAndIndex*)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":{"hash":"0x1212121212121212121212121212121212121212121212121212121212121212","transactionHash":"0x1212121212121212121212121212121212121212121212121212121212121212","blockNumber":"0x1","blockHash":"0xabababababababababababababababababababababababababababababababab","transactionIndex":"0x0"}}'
                            ;;
                          *)
                            printf '%s\\n' '{"jsonrpc":"2.0","id":1,"result":"0x0"}'
                            ;;
                        esac
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
                    {"datadir":"/tmp/gtron","status":"ok","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"ok","stageIssues":0,"stageVerifyDetails":[],"stagePipeline":{"complete":true,"pending":0,"issues":0,"tasks":[]},"modeStatus":"ok","modeIssues":0,"modeAlertDetails":[],"pruneMode":"full","pruneModePersisted":true,"snapshotStatus":"ok","snapshotIssues":0,"snapshotAlertDetails":[],"snapshotRetiredSegments":0,"snapshotRetiredFiles":0,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":0}
                    EOF
                      exit 0
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
                    "2",
                    "--timeout",
                    "5",
                    "--workdir",
                    str(workdir),
                    "--output",
                    str(output),
                    "--gtron",
                    str(fake_gtron),
                    "--no-build",
                    "--archive-api-probe",
                ],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
            )

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            row = json.loads(output.read_text(encoding="utf-8").strip().splitlines()[0])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 11)
            self.assertEqual(row["archiveApiFailures"], 1)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBlockTransactionCountByNumber",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getBlockTransactionCountByHash",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(
                row["archiveApiTxHash"],
                "0x1212121212121212121212121212121212121212121212121212121212121212",
            )
            self.assertEqual(
                row["archiveApiTxMethods"],
                [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "eth_getTransactionByBlockNumberAndIndex",
                    "eth_getTransactionByBlockHashAndIndex",
                ],
            )

    def test_emits_snapshot_manifest_profile_fields(self):
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
                      for arg in "$@"; do
                        if [ "$arg" = "--prometheus" ]; then
                          cat <<'EOF'
                    # TYPE gtron_storage_alert_status gauge
                    # TYPE gtron_storage_alert_issue gauge
                    gtron_storage_alert_status{datadir="/tmp/gtron"} 0
                    EOF
                          exit 0
                        fi
                      done
                      cat <<'EOF'
                    {"datadir":"/tmp/gtron","status":"ok","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"ok","stageIssues":0,"stageVerifyDetails":[],"stagePipeline":{"complete":true,"pending":0,"issues":0,"tasks":[]},"modeStatus":"ok","modeIssues":0,"modeAlertDetails":[],"pruneMode":"full","pruneModePersisted":true,"snapshotStatus":"ok","snapshotIssues":0,"snapshotAlertDetails":[],"snapshotRetiredSegments":0,"snapshotRetiredFiles":0,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":0}
                    EOF
                      exit 0
                    fi
                    trap 'exit 0' TERM INT
                    while true; do sleep 1; done
                    """
                ),
                encoding="utf-8",
            )
            os.chmod(fake_gtron, 0o755)

            workdir = tmpdir / "work"
            snapshot_dir = workdir / "full-producer" / "gtron" / "state-snapshots"
            snapshot_dir.mkdir(parents=True)
            (snapshot_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "version": 1,
                        "generation": 1,
                        "publishedUnix": 1,
                        "visibleTxStart": 1,
                        "visibleTxEnd": 2,
                        "segments": [
                            {"dataset": "chain-freezer", "kind": "chain-freezer", "fromTxNum": 1, "toTxNum": 2, "path": "chain/freezer.seg", "size": 1000},
                            {"dataset": "chain-freezer", "kind": "chain-index", "fromTxNum": 1, "toTxNum": 2, "path": "chain/chain-index.idx", "size": 100},
                            {"dataset": "event-log", "kind": "event-log", "fromTxNum": 1, "toTxNum": 2, "path": "log/event.seg", "size": 300},
                            {"dataset": "event-log", "kind": "event-log-index", "fromTxNum": 1, "toTxNum": 2, "path": "log/event-log.idx", "size": 200},
                        ],
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "chain").mkdir(parents=True, exist_ok=True)
            (snapshot_dir / "log").mkdir(parents=True, exist_ok=True)
            (snapshot_dir / "chain" / "chain-index.idx").write_bytes(b"chain-index")
            (snapshot_dir / "log" / "event-log.idx").write_bytes(b"event-log")
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

            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            rows = output.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 1, proc.stdout + proc.stderr)
            row = json.loads(rows[0])
            self.assertEqual(row["snapshotManifestProfileStatus"], "ok")
            self.assertEqual(row["snapshotProfileSegments"], 4)
            self.assertEqual(row["snapshotProfileTotalBytes"], 1600)
            self.assertEqual(row["snapshotPayloadBytes"], 1300)
            self.assertEqual(row["snapshotSidecarBytes"], 300)
            self.assertEqual(row["snapshotSidecarShareMilli"], 188)
            self.assertEqual(row["snapshotChainFreezerSidecarBytes"], 100)
            self.assertEqual(row["snapshotChainFreezerSidecarShareMilli"], 91)
            self.assertEqual(row["snapshotEventLogSidecarBytes"], 200)
            self.assertEqual(row["snapshotEventLogSidecarShareMilli"], 400)
            self.assertEqual(row["snapshotPointTxHashLookupSegments"], 1)
            self.assertEqual(row["snapshotPointTxHashLookupBytes"], 100)
            self.assertEqual(row["snapshotPointTxHashLookupPayloadBytes"], 0)
            self.assertEqual(row["snapshotPointTxHashLookupSidecarBytes"], 100)
            self.assertEqual(row["snapshotPointTxHashLookupSidecarShareMilli"], 1000)
            self.assertEqual(row["snapshotPointTxHashLookupSnapshotShareMilli"], 63)
            self.assertEqual(row["snapshotPointEventLogIndexSegments"], 1)
            self.assertEqual(row["snapshotPointEventLogIndexBytes"], 200)
            self.assertEqual(row["snapshotPointEventLogIndexPayloadBytes"], 0)
            self.assertEqual(row["snapshotPointEventLogIndexSidecarBytes"], 200)
            self.assertEqual(row["snapshotPointEventLogIndexSidecarShareMilli"], 1000)
            self.assertEqual(row["snapshotPointEventLogIndexSnapshotShareMilli"], 125)
            self.assertEqual(row["snapshotPointStateHistoryAccessorSegments"], 0)
            self.assertEqual(row["snapshotPointStateHistoryAccessorBytes"], 0)
            self.assertEqual(row["snapshotPointStateHistoryAccessorPayloadBytes"], 0)
            self.assertEqual(row["snapshotPointStateHistoryAccessorSidecarBytes"], 0)
            self.assertEqual(row["snapshotPointStateHistoryAccessorSidecarShareMilli"], 0)
            self.assertEqual(row["snapshotPointStateHistoryAccessorSnapshotShareMilli"], 0)
            self.assertEqual(row["derivedIndexFiles"], 2)
            self.assertGreater(row["derivedIndexBytes"], 0)
            benchmark_prometheus = Path(row["storageBenchmarkPrometheus"])
            self.assertTrue(benchmark_prometheus.is_file(), row["storageBenchmarkPrometheus"])
            benchmark_metrics = benchmark_prometheus.read_text(encoding="utf-8")
            self.assertIn("gtron_storage_benchmark_derived_index_bytes", benchmark_metrics)
            self.assertIn("gtron_storage_benchmark_derived_index_bytes_per_block", benchmark_metrics)
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_status\{[^}]*status=\"ok\"[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_cold_freezer_to_block\{[^}]*\} -1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_chain_lookup_prune_to_block\{[^}]*\} -1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_signed_cold_prune\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_tail_pruned_files\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_tx_hash_lookup_segments\{[^}]*\} 1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_tx_hash_lookup_bytes\{[^}]*\} 100\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_tx_hash_lookup_payload_bytes\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_tx_hash_lookup_sidecar_bytes\{[^}]*\} 100\n")
            self.assertRegex(
                benchmark_metrics,
                r"gtron_storage_benchmark_snapshot_point_tx_hash_lookup_sidecar_share_milli\{[^}]*\} 1000\n",
            )
            self.assertRegex(
                benchmark_metrics,
                r"gtron_storage_benchmark_snapshot_point_tx_hash_lookup_snapshot_share_milli\{[^}]*\} 63\n",
            )
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_event_log_index_segments\{[^}]*\} 1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_event_log_index_bytes\{[^}]*\} 200\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_event_log_index_payload_bytes\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_snapshot_point_event_log_index_sidecar_bytes\{[^}]*\} 200\n")
            self.assertRegex(
                benchmark_metrics,
                r"gtron_storage_benchmark_snapshot_point_event_log_index_sidecar_share_milli\{[^}]*\} 1000\n",
            )
            self.assertRegex(
                benchmark_metrics,
                r"gtron_storage_benchmark_snapshot_point_event_log_index_snapshot_share_milli\{[^}]*\} 125\n",
            )
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_event_log_index_segments\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_event_log_index_address_postings\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_archive_api_checks\{[^}]*\} 0\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_archive_api_block\{[^}]*\} -1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_archive_api_depth_blocks\{[^}]*\} -1\n")
            self.assertRegex(benchmark_metrics, r"gtron_storage_benchmark_archive_api_failures\{[^}]*\} 0\n")
            self.assertIn('mode="full"', benchmark_metrics)
            self.assertIn('role="producer"', benchmark_metrics)

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
                      for arg in "$@"; do
                        if [ "$arg" = "--prometheus" ]; then
                          cat <<'EOF'
                    # HELP gtron_storage_alert_status Overall storage alert status: 0=ok, 1=warning, 2=critical.
                    # TYPE gtron_storage_alert_status gauge
                    gtron_storage_alert_status{datadir="/tmp/gtron"} 2
                    gtron_storage_alert_component_status{component="stage",datadir="/tmp/gtron"} 2
                    gtron_storage_alert_component_issues{component="stage",datadir="/tmp/gtron"} 1
                    gtron_storage_stage_pipeline_pending{datadir="/tmp/gtron"} 2
                    gtron_storage_stage_pipeline_issues{datadir="/tmp/gtron"} 1
                    gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/gtron",stage="ChainFreezer",status="behind",upstream="Finish"} 12
                    gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/gtron",stage="ChainFreezer",status="behind",upstream="Finish"} 9
                    # TYPE gtron_storage_alert_issue gauge
                    gtron_storage_alert_issue{component="stage",datadir="/tmp/gtron",kind="stage-verification",severity="critical"} 1
                    EOF
                          exit 1
                        fi
                      done
                      cat <<'EOF'
                    {"datadir":"/tmp/gtron","status":"critical","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"critical","stageIssues":1,"stageVerifyDetails":[{"severity":"critical","kind":"stage-verification","detail":"SyncBodiesReady staged-body status=hash-mismatch block=7 hash=ee stagedBlock=7 stagedHash=aa"}],"stagePipeline":{"complete":false,"pending":2,"issues":1,"tasks":[{"stage":"ChainFreezer","upstream":"Finish","status":"behind","targetValue":12,"targetHash":"aa","currentValue":9,"currentHash":"bb"},{"stage":"SnapshotEventLogBuild","upstream":"Finish","status":"missing","targetValue":12,"targetHash":"aa"}]},"modeStatus":"critical","modeIssues":1,"modeAlertDetails":[{"severity":"critical","kind":"archive-prune-stage","detail":"archive mode must not have SnapshotHotPrune progress at block 7"}],"pruneMode":"archive","pruneModePersisted":true,"snapshotStatus":"warning","snapshotIssues":1,"snapshotAlertDetails":[{"severity":"warning","kind":"retired-prune-pending","detail":"retired segment still present"}],"snapshotRetiredSegments":1,"snapshotRetiredFiles":1,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":123}
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
                    "--keep",
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
            self.assertFalse(row["stageAlertPipelineComplete"])
            self.assertEqual(row["stageAlertPipelinePending"], 2)
            self.assertEqual(row["stageAlertPipelineIssues"], 1)
            self.assertEqual(row["stageAlertPipelineNext"], "ChainFreezer")
            self.assertEqual(row["stageAlertPipelineNextStatus"], "behind")
            self.assertEqual(row["stageAlertPipelineNextTarget"], 12)
            self.assertEqual(row["stageAlertPipelineNextUpstream"], "Finish")
            self.assertEqual(row["stageAlertPipelineNextCurrent"], 9)
            self.assertEqual(
                row["stageAlertPipelineTasks"][0],
                {
                    "stage": "ChainFreezer",
                    "upstream": "Finish",
                    "status": "behind",
                    "targetValue": 12,
                    "targetHash": "aa",
                    "currentValue": 9,
                    "currentHash": "bb",
                },
            )
            self.assertEqual(
                row["stageVerifyDetails"],
                [
                    {
                        "severity": "critical",
                        "kind": "stage-verification",
                        "detail": "SyncBodiesReady staged-body status=hash-mismatch block=7 hash=ee stagedBlock=7 stagedHash=aa",
                    }
                ],
            )
            self.assertEqual(row["modeAlertStatus"], "critical")
            self.assertEqual(row["modeAlertIssues"], 1)
            self.assertEqual(row["pruneMode"], "archive")
            self.assertTrue(row["pruneModePersisted"])
            self.assertEqual(
                row["modeAlertDetails"],
                [
                    {
                        "severity": "critical",
                        "kind": "archive-prune-stage",
                        "detail": "archive mode must not have SnapshotHotPrune progress at block 7",
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
            metrics_path = Path(row["storageAlertPrometheus"])
            self.assertEqual(metrics_path, tmpdir / "full-producer-storage-alerts.prom")
            self.assertTrue(metrics_path.is_file(), row["storageAlertPrometheus"])
            metrics = metrics_path.read_text(encoding="utf-8")
            self.assertIn('gtron_storage_alert_status{datadir="/tmp/gtron"} 2', metrics)
            self.assertIn(
                'gtron_storage_alert_component_issues{component="stage",datadir="/tmp/gtron"} 1',
                metrics,
            )
            self.assertEqual(row["storageAlertStatus"], "critical")
            self.assertIn(
                'gtron_storage_alert_issue{component="stage",datadir="/tmp/gtron",kind="stage-verification",severity="critical"} 1',
                metrics,
            )
            self.assertIn('gtron_storage_stage_pipeline_pending{datadir="/tmp/gtron"} 2', metrics)
            self.assertIn(
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/gtron",stage="ChainFreezer",status="behind",upstream="Finish"} 12',
                metrics,
            )
            self.assertIn(
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/gtron",stage="ChainFreezer",status="behind",upstream="Finish"} 9',
                metrics,
            )


if __name__ == "__main__":
    unittest.main()
