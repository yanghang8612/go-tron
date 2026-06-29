#!/usr/bin/env python3
import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "dev" / "nile_sync_sample.sh"


def write_full_stage_status(path):
    path.write_text(
        "\n".join(
            [
                "Stage status: datadir=/tmp/nile known=32 rows=6",
                "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                "Stage progress: group=sync name=SyncBodiesReady value=100 hash=bb verified=canonical",
                "Stage progress: group=sync name=SyncImport value=100 hash=cc verified=canonical",
                "Stage progress: group=sync name=SyncExecution value=100 hash=dd verified=canonical",
                "Stage progress: group=sync name=SyncCommitment value=100 hash=ee verified=canonical",
                "Stage progress: group=sync name=SyncFinish value=100 hash=ff verified=canonical",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


class NileSampleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payloads = {
            "/wallet/getnowblock": {
                "blockID": "0000006400000000000000000000000000000000000000000000000000000000",
                "block_header": {"raw_data": {"number": 100}},
            },
            "/wallet/getnodeinfo": {"currentBlock": 105},
            "/wallet/listnodes": {"nodes": [{"address": "a"}, {"address": "b"}]},
            "/debug/metrics?prefix=chain/freezer/": {
                "prefix": "chain/freezer/",
                "count": 4,
                "metrics": [
                    {"name": "chain/freezer/blocks", "values": {"value": 16}},
                    {"name": "chain/freezer/passes", "values": {"value": 2}},
                    {"name": "chain/freezer/lastpass/duration", "values": {"value": 250000000}},
                    {"name": "chain/freezer/pebble/size", "values": {"value": 4096}},
                ],
            },
        }
        payload = payloads.get(self.path)
        if payload is None:
            self.send_response(404)
            self.end_headers()
            return
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw = self.rfile.read(length)
        try:
            request = json.loads(raw.decode("utf-8"))
        except Exception:
            request = {}
        method = request.get("method")
        tx_hash = "0x" + "12" * 32
        wrong_tx_hash = "0x" + "34" * 32
        if method == "eth_getBlockByNumber":
            result = {"number": request.get("params", ["0x0"])[0], "transactions": [tx_hash]}
        elif method == "eth_getBalance" and getattr(self.server, "invalid_scalar_results", False):
            result = "not-hex"
        elif method == "eth_getTransactionByHash":
            if getattr(self.server, "null_tx_results", False):
                result = None
            elif getattr(self.server, "mismatched_tx_results", False):
                result = {"hash": wrong_tx_hash, "blockNumber": "0x63"}
            else:
                result = {"hash": tx_hash, "blockNumber": "0x63"}
        elif method == "eth_getTransactionReceipt":
            if getattr(self.server, "null_tx_results", False):
                result = None
            elif getattr(self.server, "mismatched_tx_results", False):
                result = {"transactionHash": wrong_tx_hash, "blockNumber": "0x63"}
            else:
                result = {"transactionHash": tx_hash, "blockNumber": "0x63"}
        elif method == "eth_getLogs":
            result = []
        elif method == "debug_traceTransaction":
            if getattr(self.server, "invalid_trace_transaction", False):
                result = "0x0"
            else:
                result = {"failed": False, "returnValue": "", "structLogs": []}
        elif method == "debug_traceCall":
            result = {"failed": False, "returnValue": "", "structLogs": []}
        else:
            result = "0x0"
        body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": request.get("id", 1),
                "result": result,
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        return


class NileSyncSampleTest(unittest.TestCase):
    def test_sample_includes_archive_api_probe_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            endpoint = f"http://127.0.0.1:{server.server_address[1]}"
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    endpoint,
                    "--jsonrpc",
                    endpoint,
                    "--mode",
                    "full",
                    "--label",
                    "candidate",
                    "--archive-api-probe",
                    "--archive-api-call-data",
                    "0x70a08231",
                    "--archive-api-trace-transaction",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["archiveApiEndpoint"], endpoint)
            self.assertEqual(row["archiveApiStatus"], "ok")
            self.assertEqual(row["archiveApiChecks"], 10)
            self.assertEqual(row["archiveApiFailures"], 0)
            self.assertEqual(row["archiveApiBlock"], 99)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_call",
                    "debug_traceCall",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "debug_traceTransaction",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(row["archiveApiTxHash"], "0x" + "12" * 32)
            self.assertEqual(
                row["archiveApiTxMethods"],
                [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                    "debug_traceTransaction",
                ],
            )

    def test_archive_api_probe_rejects_invalid_trace_transaction_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            server.invalid_trace_transaction = True
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            endpoint = f"http://127.0.0.1:{server.server_address[1]}"
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    endpoint,
                    "--jsonrpc",
                    endpoint,
                    "--mode",
                    "full",
                    "--label",
                    "candidate",
                    "--archive-api-probe",
                    "--archive-api-call-data",
                    "0x70a08231",
                    "--archive-api-trace-transaction",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 10)
            self.assertEqual(row["archiveApiFailures"], 1)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_call",
                    "debug_traceCall",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(row["archiveApiTxHash"], "0x" + "12" * 32)
            self.assertEqual(
                row["archiveApiTxMethods"],
                [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                ],
            )

    def test_archive_api_probe_rejects_null_transaction_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            server.null_tx_results = True
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            endpoint = f"http://127.0.0.1:{server.server_address[1]}"
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    endpoint,
                    "--jsonrpc",
                    endpoint,
                    "--archive-api-probe",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 7)
            self.assertEqual(row["archiveApiFailures"], 2)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getBalance",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(row["archiveApiTxHash"], "0x" + "12" * 32)
            self.assertEqual(row["archiveApiTxMethods"], [])

    def test_archive_api_probe_rejects_mismatched_transaction_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            server.mismatched_tx_results = True
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            endpoint = f"http://127.0.0.1:{server.server_address[1]}"
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    endpoint,
                    "--jsonrpc",
                    endpoint,
                    "--archive-api-probe",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 7)
            self.assertEqual(row["archiveApiFailures"], 2)
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(row["archiveApiTxHash"], "0x" + "12" * 32)
            self.assertEqual(row["archiveApiTxMethods"], [])

    def test_archive_api_probe_rejects_non_hex_scalar_results(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            server.invalid_scalar_results = True
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            endpoint = f"http://127.0.0.1:{server.server_address[1]}"
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    endpoint,
                    "--jsonrpc",
                    endpoint,
                    "--archive-api-probe",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["archiveApiStatus"], "failed")
            self.assertEqual(row["archiveApiChecks"], 7)
            self.assertEqual(row["archiveApiFailures"], 1)
            self.assertEqual(
                row["archiveApiMethods"],
                [
                    "eth_getBlockByNumber",
                    "eth_getCode",
                    "eth_getStorageAt",
                    "eth_getLogs",
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                ],
            )
            self.assertTrue(row["archiveApiTxProbe"])
            self.assertEqual(row["archiveApiTxHash"], "0x" + "12" * 32)
            self.assertEqual(
                row["archiveApiTxMethods"],
                [
                    "eth_getTransactionByHash",
                    "eth_getTransactionReceipt",
                ],
            )

    def test_sample_includes_snapshot_manifest_profile_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            snapshot_dir = datadir / "gtron" / "state-snapshots"
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
                            {
                                "dataset": "chain-freezer",
                                "kind": "chain-freezer",
                                "fromTxNum": 1,
                                "toTxNum": 2,
                                "path": "chain/freezer.seg",
                                "size": 1000,
                            },
                            {
                                "dataset": "chain-freezer",
                                "kind": "chain-index",
                                "fromTxNum": 1,
                                "toTxNum": 2,
                                "path": "chain/index.idx",
                                "size": 100,
                            },
                            {
                                "dataset": "event-log",
                                "kind": "event-log",
                                "fromTxNum": 1,
                                "toTxNum": 2,
                                "path": "log/event.seg",
                                "size": 300,
                            },
                            {
                                "dataset": "event-log",
                                "kind": "event-log-index",
                                "fromTxNum": 1,
                                "toTxNum": 2,
                                "path": "log/event.idx",
                                "size": 200,
                            },
                        ],
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
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
            self.assertEqual(row["snapshotLatestSidecarBytes"], 0)
            self.assertEqual(row["snapshotLatestSidecarShareMilli"], -1)

    def test_sample_writes_prometheus_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            prometheus = tmpdir / "sync.prom"
            stage_status = tmpdir / "stage-status.txt"
            write_full_stage_status(stage_status)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)
            endpoint = f"http://127.0.0.1:{server.server_address[1]}"

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    endpoint,
                    "--jsonrpc",
                    endpoint,
                    "--mode",
                    "full",
                    "--label",
                    "candidate",
                    "--archive-api-probe",
                    "--archive-api-call-data",
                    "0x70a08231",
                    "--archive-api-trace-transaction",
                    "--stage-status-file",
                    str(stage_status),
                    "--prometheus-output",
                    str(prometheus),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["samplePrometheusStatus"], "ok")
            self.assertEqual(row["samplePrometheus"], str(prometheus))
            metrics = prometheus.read_text(encoding="utf-8")
            labels = f'datadir="{datadir}",label="candidate",mode="full",network="nile"'
            self.assertIn("# TYPE gtron_nile_sync_sample_status gauge", metrics)
            self.assertIn(f'gtron_nile_sync_height{{{labels}}} 100', metrics)
            self.assertIn(f'gtron_nile_sync_target_lag_blocks{{{labels}}} 5', metrics)
            self.assertIn(f'gtron_nile_sync_full_staged_sync_status{{{labels},status="caught-up"}} 0', metrics)
            self.assertIn(f'gtron_nile_sync_full_staged_sync_ready{{{labels}}} 1', metrics)
            self.assertIn(f'gtron_nile_sync_full_staged_sync_stage_coverage_ratio{{{labels}}} 1', metrics)
            stage_labels = f'datadir="{datadir}",field="stageSyncExecution",label="candidate",mode="full",network="nile",stage="SyncExecution"'
            self.assertIn(f"gtron_nile_sync_full_staged_sync_stage_block{{{stage_labels}}} 100", metrics)
            self.assertIn(f"gtron_nile_sync_full_staged_sync_stage_present{{{stage_labels}}} 1", metrics)
            verified_labels = f'datadir="{datadir}",field="stageSyncExecution",label="candidate",mode="full",network="nile",stage="SyncExecution",verification="canonical"'
            self.assertIn(f"gtron_nile_sync_full_staged_sync_stage_verified{{{verified_labels}}} 1", metrics)
            bottleneck_labels = f'bottleneck="none",datadir="{datadir}",label="candidate",mode="full",network="nile"'
            self.assertIn(f"gtron_nile_sync_full_staged_sync_bottleneck{{{bottleneck_labels}}} 1", metrics)
            self.assertIn(f'gtron_nile_sync_datadir_bytes{{{labels}}} ', metrics)
            self.assertIn(f'gtron_nile_sync_datadir_bytes_per_block{{{labels}}} ', metrics)
            self.assertIn(f'gtron_nile_sync_soak_efficiency_datadir_bytes_per_block{{{labels}}} ', metrics)
            self.assertIn(f'gtron_nile_sync_derived_index_bytes_per_block{{{labels}}} ', metrics)
            trace_labels = f'datadir="{datadir}",label="candidate",method="debug_traceTransaction",mode="full",network="nile"'
            self.assertIn(f'gtron_nile_sync_archive_api_method_success{{{trace_labels}}} 1', metrics)
            self.assertIn(f'gtron_nile_sync_archive_api_tx_method_success{{{trace_labels}}} 1', metrics)

    def test_sample_includes_sync_health_and_disk_ratios(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            (datadir / "gtron" / "ancient").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "chain").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "log").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "trace").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "latest").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "history").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "commitment").mkdir(parents=True)
            (datadir / "gtron" / "chaindata" / "hot.bin").write_bytes(b"h" * 2048)
            (datadir / "gtron" / "chaindata" / "000001.sst").write_bytes(b"s" * 1024)
            (datadir / "gtron" / "chaindata" / "000002.log").write_bytes(b"w" * 1024)
            (datadir / "gtron" / "chaindata" / "LOG").write_bytes(b"l" * 1024)
            (datadir / "gtron" / "chaindata" / "MANIFEST-000003").write_bytes(b"m" * 1024)
            (datadir / "gtron" / "chaindata" / "OPTIONS-000004").write_bytes(b"o" * 1024)
            (datadir / "gtron" / "ancient" / "cold.bin").write_bytes(b"c" * 1024)
            (datadir / "gtron" / "ancient" / "bodies.0000.cdat").write_bytes(b"b" * 1024)
            (datadir / "gtron" / "ancient" / "tx_infos.cidx").write_bytes(b"x" * 1024)
            (datadir / "gtron" / "ancient" / "state_roots.0000.rdat").write_bytes(b"r" * 1024)
            (datadir / "gtron" / "state-snapshots" / "snap.bin").write_bytes(b"s" * 1024)
            (datadir / "gtron" / "state-snapshots" / "latest" / "account-1-2.seg").write_bytes(b"a" * 1024)
            (datadir / "gtron" / "state-snapshots" / "history" / "state-domain-change-1-2.seg").write_bytes(b"d" * 1024)
            (datadir / "gtron" / "state-snapshots" / "chain" / "chain-index-1-2.idx").write_bytes(b"i" * 1024)
            (datadir / "gtron" / "state-snapshots" / "log" / "event-log-index-1-2.idx").write_bytes(b"e" * 1024)
            (datadir / "gtron" / "state-snapshots" / "trace" / "balance-trace-1-2.seg").write_bytes(b"t" * 1024)
            (datadir / "gtron" / "state-snapshots" / "commitment" / "root-1-2.seg").write_bytes(b"c" * 1024)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=9",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=96 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=95 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=90 hash=dd verified=canonical",
                        "Stage progress: group=sync name=SyncCommitment value=89 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=80 hash=ff verified=canonical",
                        "Stage progress: group=canonical name=Finish value=82 hash=11 verified=canonical",
                        "Stage progress: group=freezer name=ChainFreezer value=70 hash=none verified=unbound",
                        "Stage progress: group=snapshot name=SnapshotEventLogBuild status=missing",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            pid_file = tmpdir / "gtron.pid"
            pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")
            sync_log = tmpdir / "gtron.err.log"
            sync_log.write_text(
                "\n".join(
                    [
                        "INFO [06-13|11:59:50.000] Sync startup repair summary syncStartupRepairComplete=true syncStartupRepairKept=4 syncStartupRepairMissing=0 syncStartupRepairDeleted=0 syncStartupRepairHasBlocked=false syncStartupRepairFirstBlocked= syncStartupRepairInterrupted=false syncStartupRepairErrorStage= syncStartupRepairRows=4 syncStartupHeadCompletionChecked=true syncStartupHeadCompletionHasPrefix=true syncStartupHeadCompletionLastStage=SyncFinish syncStartupHeadCompletionLastBlock=90 syncStartupHeadCompletionFillStages=0 syncStartupHeadCompletionWritten=0 syncStartupHeadCompletionComplete=true syncStartupHeadCompletionErrorStage= syncStartupPipelineOrderChecked=true syncStartupPipelineOrderIssues=0 syncStartupPipelineOrderFirstIssue= syncStartupPipelineOrderReadErrors=0 syncStartupPipelineOrderFirstReadErrorStage= syncStartupStagedRestored=0 syncStartupStagedTargetHead=90 syncStartupStagedNextExpected=91 syncStartupStagedNeedPruneTail=false syncStartupStagedPruneFrom=0 syncStartupStagedHaveLastRestored=false syncStartupStagedLastRestored=0",
                        "INFO [06-13|12:00:00.000] Imported chain segment blocks=10 txs=4 elapsed=1s execElapsed=800ms applyElapsed=900ms blocks/s=10 txs/s=4 head=90 remain=20 slowPhase=execute slowElapsed=500ms stateMutTop=storagePuts:2 stateMutKVTop=accountKV:1 txTop=TransferContract=4 peer=peer-old syncStageComplete=true syncStageCompleted=40 syncStageScheduled=40 syncExecPlanBlocks=10 syncExecPlanStages=40 syncExecPlanBodyStages=10 syncExecPlanPostBodyStages=30 syncExecPlanExecutionStages=10 syncExecPlanCommitmentStages=10 syncExecPlanFinishStages=10 syncExecPlanFirst=81 syncExecPlanLast=90",
                        "DEBUG [06-13|12:00:00.100] Imported chain segment details blocks=10 head=90 syncExecPlanBlocks=10",
                        "INFO [06-13|12:01:00.000] Imported chain segment blocks=20 txs=7 elapsed=2s execElapsed=1500ms applyElapsed=1700ms blocks/s=20.5 txs/s=7.5 head=100 remain=5 slowPhase=stateCommit slowElapsed=900ms slowStateCommitPhase=flatWrite slowStateCommitElapsed=600ms stateMutTop=storagePuts:7 stateMutKVTop=accountKV:3 txTop=TriggerSmartContract=5,TransferContract=2 peer=peer-latest syncStageComplete=false syncStageCompleted=59 syncStageScheduled=80 syncStageNext=commitment syncStageNextBlock=100 syncStageNextCanonical=Commitment syncStageNextSync=SyncCommitment syncStageBlockedStatus=missing syncPhaseCursorComplete=false syncPhaseCursorCompletedPhases=2 syncPhaseCursorScheduledPhases=4 syncPhaseCursorCompletedTasks=59 syncPhaseCursorScheduledTasks=80 syncPhaseCursorCurrent=commitment syncPhaseCursorCurrentCanonical=Commitment syncPhaseCursorCurrentSync=SyncCommitment syncPhaseCursorCurrentTaskIndex=19 syncPhaseCursorCurrentTaskCount=20 syncPhaseCursorCurrentTaskRemaining=1 syncPhaseCursorCurrentFromBlock=100 syncPhaseCursorCurrentToBlock=100 syncPhaseCursorNextBlock=100 syncPhaseCursorNextPhase=commitment syncPhaseCursorNextCanonical=Commitment syncPhaseCursorNextSync=SyncCommitment syncPhaseCursorBlockedStatus=missing syncPhaseProgressCompletedPhases=2 syncPhaseProgressScheduledPhases=4 syncPhaseProgressBodiesCompletedTasks=20 syncPhaseProgressExecutionCompletedTasks=20 syncPhaseProgressCommitmentCompletedTasks=19 syncPhaseProgressFinishCompletedTasks=0 syncPhaseProgressBodiesBlock=100 syncPhaseProgressExecutionBlock=100 syncPhaseProgressCommitmentBlock=99 syncPhaseProgressBlockedPhase=commitment syncPhaseProgressNextBlock=100 syncPhaseProgressBlockedStatus=missing syncExecPlanBlocks=20 syncExecPlanStages=80 syncExecPlanBodyStages=20 syncExecPlanPostBodyStages=60 syncExecPlanExecutionStages=20 syncExecPlanCommitmentStages=20 syncExecPlanFinishStages=20 syncExecPlanFirst=81 syncExecPlanLast=100 syncAppliedPlanBlocks=19 syncAppliedPlanStages=76 syncAppliedPlanBodyStages=19 syncAppliedPlanPostBodyStages=57 syncAppliedPlanExecutionStages=19 syncAppliedPlanCommitmentStages=19 syncAppliedPlanFinishStages=19 syncAppliedPlanFirst=81 syncAppliedPlanLast=99",
                        'INFO [06-13|12:01:10.000] Sync startup repair summary syncStartupRepairComplete=false syncStartupRepairKept=2 syncStartupRepairMissing=1 syncStartupRepairDeleted=1 syncStartupRepairHasBlocked=true syncStartupRepairFirstBlocked=SyncCommitment syncStartupRepairInterrupted=false syncStartupRepairErrorStage= syncStartupRepairRows=4 syncStartupHeadCompletionChecked=true syncStartupHeadCompletionHasPrefix=true syncStartupHeadCompletionLastStage=SyncExecution syncStartupHeadCompletionLastBlock=102 syncStartupHeadCompletionFillStages=2 syncStartupHeadCompletionWritten=2 syncStartupHeadCompletionComplete=true syncStartupHeadCompletionErrorStage= syncStartupPipelineOrderChecked=true syncStartupPipelineOrderIssues=1 syncStartupPipelineOrderFirstIssue="SyncCommitment=3 ahead of SyncExecution=2" syncStartupPipelineOrderReadErrors=1 syncStartupPipelineOrderFirstReadErrorStage=SyncBodies syncStartupPipelineOrderRepairChecked=true syncStartupPipelineOrderRepairComplete=false syncStartupPipelineOrderRepairDeleted=2 syncStartupPipelineOrderRepairUpdated=1 syncStartupPipelineOrderRepairInterrupted=true syncStartupPipelineOrderRepairErrorStage=SyncCommitment syncStartupPipelineOrderRepairRows=3 syncStartupPipelineCursorChecked=true syncStartupPipelineCursorComplete=false syncStartupPipelineCursorRows=4 syncStartupPipelineCursorHasLast=true syncStartupPipelineCursorLastStage=SyncExecution syncStartupPipelineCursorLastBlock=102 syncStartupPipelineCursorLastHasHash=true syncStartupPipelineCursorHasNext=true syncStartupPipelineCursorNextStage=SyncCommitment syncStartupPipelineCursorBlocked=true syncStartupPipelineCursorInterrupted=false syncStartupPipelineCursorErrorStage= syncStartupStagedRestored=3 syncStartupStagedTargetHead=110 syncStartupStagedNextExpected=104 syncStartupStagedNeedPruneTail=true syncStartupStagedPruneFrom=105 syncStartupStagedHaveLastRestored=true syncStartupStagedLastRestored=103',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            output = tmpdir / "samples.jsonl"
            start_unix = str(int(time.time()) - 50)
            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--debug-metrics-url",
                    f"http://127.0.0.1:{server.server_address[1]}/debug/metrics?prefix=chain/freezer/",
                    "--mode",
                    "full",
                    "--start-unix",
                    start_unix,
                    "--stage-status-file",
                    str(stage_status),
                    "--pid-file",
                    str(pid_file),
                    "--sync-log-file",
                    str(sync_log),
                    "--output",
                    str(output),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["height"], 100)
            self.assertEqual(row["nodeInfoCurrentBlock"], 105)
            self.assertEqual(row["nodeInfoHeightDelta"], 5)
            self.assertEqual(row["syncTargetHeight"], 105)
            self.assertEqual(row["syncTargetLagBlocks"], 5)
            self.assertEqual(row["sampleStatus"], "height-mismatch")
            self.assertEqual(row["soakHealthStatus"], "warning")
            self.assertEqual(row["soakHealthCriticalIssues"], 0)
            self.assertEqual(row["soakHealthWarningIssues"], 2)
            self.assertIn("sample-status:height-mismatch", row["soakHealthIssues"])
            self.assertIn("stage-unbound-rows", row["soakHealthIssues"])
            self.assertEqual(row["soakPrimaryBottleneck"], "finish-head")
            self.assertEqual(row["soakPrimaryBottleneckSource"], "sync-stage")
            self.assertEqual(row["soakPrimaryBottleneckLagBlocks"], 20)
            self.assertEqual(row["peers"], 2)
            self.assertGreater(row["blocksPerSecond"], 0)
            self.assertGreater(row["blocksPerMinute"], 0)
            self.assertGreater(row["syncEtaSeconds"], 0)
            self.assertEqual(row["intervalSyncEtaSeconds"], -1.0)
            self.assertGreater(row["datadirBytes"], 0)
            self.assertGreater(row["chaindataBytes"], 0)
            self.assertGreater(row["chaindataSSTBytes"], 0)
            self.assertGreater(row["chaindataWALBytes"], 0)
            self.assertGreater(row["chaindataLogBytes"], 0)
            self.assertGreater(row["chaindataManifestBytes"], 0)
            self.assertGreater(row["chaindataOptionsBytes"], 0)
            self.assertGreater(row["chaindataOtherBytes"], 0)
            self.assertGreater(row["coldArchiveBytes"], 0)
            self.assertGreater(row["derivedIndexBytes"], 0)
            self.assertGreater(row["ancientBodiesBytes"], 0)
            self.assertGreater(row["ancientTxInfosBytes"], 0)
            self.assertGreater(row["ancientStateRootsBytes"], 0)
            self.assertGreater(row["ancientOtherBytes"], 0)
            self.assertGreater(row["snapshotRootBytes"], 0)
            self.assertGreater(row["snapshotLatestBytes"], 0)
            self.assertGreater(row["snapshotHistoryBytes"], 0)
            self.assertGreater(row["snapshotChainBytes"], 0)
            self.assertGreater(row["snapshotLogBytes"], 0)
            self.assertGreater(row["snapshotTraceBytes"], 0)
            self.assertGreater(row["snapshotCommitmentBytes"], 0)
            self.assertEqual(row["snapshotRetiredDirectoryBytes"], 0)
            self.assertEqual(row["snapshotOtherBytes"], 0)
            self.assertGreater(row["derivedIndexBytesPerBlock"], 0)
            self.assertGreater(row["derivedIndexToHotBytesRatio"], 0)
            self.assertGreater(row["derivedIndexSnapshotBytesRatio"], 0)
            self.assertGreater(row["chaindataSSTToHotBytesRatio"], 0)
            self.assertGreater(row["chaindataWALToHotBytesRatio"], 0)
            self.assertGreater(row["chaindataWALToSSTBytesRatio"], 0)
            self.assertGreater(row["bytesPerBlock"], 0)
            self.assertGreater(row["coldToHotBytesRatio"], 0)
            self.assertEqual(row["ancientFiles"], 4)
            self.assertAlmostEqual(row["coldArchiveDatadirShare"], row["coldArchiveBytes"] / row["datadirBytes"])
            self.assertAlmostEqual(row["derivedIndexColdArchiveRatio"], row["derivedIndexBytes"] / row["coldArchiveBytes"])
            self.assertEqual(row["chaindataFiles"], 6)
            self.assertEqual(row["chaindataSSTFiles"], 1)
            self.assertEqual(row["chaindataWALFiles"], 1)
            self.assertEqual(row["chaindataLogFiles"], 1)
            self.assertEqual(row["chaindataManifestFiles"], 1)
            self.assertEqual(row["chaindataOptionsFiles"], 1)
            self.assertEqual(row["chaindataOtherFiles"], 1)
            self.assertEqual(row["ancientBodiesFiles"], 1)
            self.assertEqual(row["ancientTxInfosFiles"], 1)
            self.assertEqual(row["ancientStateRootsFiles"], 1)
            self.assertEqual(row["ancientOtherFiles"], 1)
            self.assertEqual(row["snapshotFiles"], 7)
            self.assertEqual(row["snapshotRootFiles"], 1)
            self.assertEqual(row["snapshotLatestFiles"], 1)
            self.assertEqual(row["snapshotHistoryFiles"], 1)
            self.assertEqual(row["snapshotChainFiles"], 1)
            self.assertEqual(row["snapshotLogFiles"], 1)
            self.assertEqual(row["snapshotTraceFiles"], 1)
            self.assertEqual(row["snapshotCommitmentFiles"], 1)
            self.assertEqual(row["snapshotRetiredDirectoryFiles"], 0)
            self.assertEqual(row["snapshotOtherFiles"], 0)
            self.assertEqual(row["derivedIndexFiles"], 3)
            self.assertEqual(row["coldArchiveFiles"], 11)
            self.assertEqual(row["intervalSeconds"], -1)
            self.assertEqual(row["intervalBlocks"], 0)
            self.assertEqual(row["datadirBytesDelta"], 0)
            self.assertEqual(row["ancientBodiesBytesDelta"], 0)
            self.assertEqual(row["snapshotHistoryBytesDelta"], 0)
            self.assertEqual(row["snapshotCommitmentBytesDelta"], 0)
            self.assertEqual(row["snapshotRetiredDirectoryBytesDelta"], 0)
            self.assertEqual(row["snapshotOtherBytesDelta"], 0)
            self.assertEqual(row["replayBytesDelta"], 0)
            self.assertEqual(row["datadirOtherBytesDelta"], 0)
            self.assertEqual(row["intervalPositiveDiskGrowthBytes"], 0)
            self.assertEqual(row["intervalDiskGrowthPrimary"], "none")
            self.assertEqual(row["intervalDiskGrowthPrimaryBytes"], 0)
            self.assertEqual(row["intervalDiskGrowthPrimaryShare"], 0.0)
            self.assertEqual(row["intervalDetailedPositiveDiskGrowthBytes"], 0)
            self.assertEqual(row["intervalDiskGrowthPrimaryDetailed"], "none")
            self.assertEqual(row["intervalDiskGrowthPrimaryDetailedBytes"], 0)
            self.assertEqual(row["intervalDiskGrowthPrimaryDetailedShare"], 0.0)
            self.assertEqual(row["intervalChaindataGrowthShare"], 0.0)
            self.assertEqual(row["intervalAncientGrowthShare"], 0.0)
            self.assertEqual(row["intervalSnapshotGrowthShare"], 0.0)
            self.assertEqual(row["intervalSnapshotCommitmentGrowthShare"], 0.0)
            self.assertEqual(row["intervalSnapshotRetiredDirectoryGrowthShare"], 0.0)
            self.assertEqual(row["intervalSnapshotOtherGrowthShare"], 0.0)
            self.assertEqual(row["intervalReplayGrowthShare"], 0.0)
            self.assertEqual(row["intervalDatadirOtherGrowthShare"], 0.0)
            self.assertEqual(row["intervalColdArchiveGrowthShare"], 0.0)
            self.assertEqual(row["intervalDerivedIndexGrowthShare"], 0.0)
            self.assertEqual(row["intervalColdToHotGrowthRatio"], -1.0)
            self.assertEqual(row["intervalAncientToHotGrowthRatio"], -1.0)
            self.assertEqual(row["intervalSnapshotToHotGrowthRatio"], -1.0)
            self.assertEqual(row["intervalDerivedIndexToHotGrowthRatio"], -1.0)
            self.assertEqual(row["intervalChaindataSSTGrowthShare"], 0.0)
            self.assertEqual(row["intervalChaindataWALGrowthShare"], 0.0)
            self.assertEqual(row["intervalAncientBodiesGrowthShare"], 0.0)
            self.assertEqual(row["intervalSnapshotLogGrowthShare"], 0.0)
            self.assertEqual(row["chaindataSSTBytesPerSecond"], 0.0)
            self.assertEqual(row["chaindataWALBytesPerSecond"], 0.0)
            self.assertEqual(row["replayBytesPerSecond"], 0.0)
            self.assertEqual(row["datadirOtherBytesPerSecond"], 0.0)
            self.assertEqual(row["intervalChaindataSSTBytesPerBlock"], 0.0)
            self.assertEqual(row["intervalChaindataWALBytesPerBlock"], 0.0)
            self.assertEqual(row["intervalReplayBytesPerBlock"], 0.0)
            self.assertEqual(row["intervalDatadirOtherBytesPerBlock"], 0.0)
            self.assertEqual(row["stageStatusFileStatus"], "ok")
            self.assertEqual(row["stageKnown"], 32)
            self.assertEqual(row["stageRows"], 9)
            self.assertEqual(row["stageSyncBodies"], 100)
            self.assertEqual(row["stageSyncBodiesReady"], 96)
            self.assertEqual(row["stageSyncImport"], 95)
            self.assertEqual(row["stageSyncExecution"], 90)
            self.assertEqual(row["stageSyncCommitment"], 89)
            self.assertEqual(row["stageSyncFinish"], 80)
            self.assertEqual(row["stageCanonicalFinish"], 82)
            self.assertEqual(row["stageChainFreezer"], 70)
            self.assertEqual(row["stageSnapshotEventLogBuild"], -1)
            self.assertEqual(row["stageSyncBodiesReadyGapBlocks"], 4)
            self.assertEqual(row["stageSyncImportExecutionLagBlocks"], 5)
            self.assertEqual(row["stageSyncExecutionCommitmentLagBlocks"], 1)
            self.assertEqual(row["stageSyncCommitmentFinishLagBlocks"], 9)
            self.assertEqual(row["stageSyncBodiesHeadLagBlocks"], 0)
            self.assertEqual(row["stageSyncBodiesHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncBodiesReadyHeadLagBlocks"], 4)
            self.assertEqual(row["stageSyncBodiesReadyHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncImportHeadLagBlocks"], 5)
            self.assertEqual(row["stageSyncImportHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncExecutionHeadLagBlocks"], 10)
            self.assertEqual(row["stageSyncExecutionHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncCommitmentHeadLagBlocks"], 11)
            self.assertEqual(row["stageSyncCommitmentHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncFinishHeadLagBlocks"], 20)
            self.assertEqual(row["stageSyncFinishHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageChainFreezerHeadLagBlocks"], 30)
            self.assertEqual(row["stageChainFreezerHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSnapshotEventLogBuildHeadLagBlocks"], -1)
            self.assertEqual(row["stageSnapshotEventLogBuildHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncBottleneck"], "finish-head")
            self.assertEqual(row["stageSyncBottleneckLagBlocks"], 20)
            self.assertEqual(row["stageSyncPipelineLagBlocks"], 39)
            self.assertAlmostEqual(row["stageSyncBottleneckLagShare"], 20 / 39)
            self.assertTrue(row["stageSyncPipelineMonotonic"])
            self.assertEqual(row["stageSyncPipelineViolation"], "")
            self.assertEqual(row["stageSyncPipelineViolationCount"], 0)
            self.assertEqual(row["stageSyncPipelineMaxViolationBlocks"], 0)
            self.assertEqual(row["stageSyncPipelineViolations"], [])
            self.assertEqual(row["fullStagedSyncStatus"], "catching-up")
            self.assertTrue(row["fullStagedSyncReady"])
            self.assertFalse(row["fullStagedSyncCompleteAtHead"])
            self.assertEqual(
                row["fullStagedSyncRequiredStages"],
                ["SyncBodies", "SyncBodiesReady", "SyncImport", "SyncExecution", "SyncCommitment", "SyncFinish"],
            )
            self.assertEqual(row["fullStagedSyncStageCount"], 6)
            self.assertEqual(row["fullStagedSyncPresentStageCount"], 6)
            self.assertEqual(row["fullStagedSyncVerifiedStageCount"], 6)
            self.assertEqual(row["fullStagedSyncMissingStages"], [])
            self.assertEqual(row["fullStagedSyncHashIssues"], [])
            self.assertEqual(row["fullStagedSyncUnverifiedStages"], [])
            self.assertEqual(row["fullStagedSyncCompleteBlock"], 80)
            self.assertEqual(row["fullStagedSyncHeadBlock"], 100)
            self.assertEqual(row["fullStagedSyncMinStage"], "SyncFinish")
            self.assertEqual(row["fullStagedSyncMinStageBlock"], 80)
            self.assertEqual(row["fullStagedSyncHeadLagBlocks"], 20)
            self.assertAlmostEqual(row["fullStagedSyncCompletionRatio"], 0.8)
            self.assertEqual(row["fullStagedSyncPipelineLagBlocks"], 39)
            self.assertEqual(row["fullStagedSyncBottleneck"], "finish-head")
            self.assertEqual(row["fullStagedSyncBottleneckLagBlocks"], 20)
            self.assertAlmostEqual(row["fullStagedSyncBottleneckLagShare"], 20 / 39)
            self.assertEqual(row["fullStagedSyncStageCoverageRatio"], 1.0)
            self.assertEqual(row["fullStagedSyncVerificationRatio"], 1.0)
            self.assertEqual(
                row["fullStagedSyncStageDetails"],
                [
                    {
                        "stage": "SyncBodies",
                        "field": "stageSyncBodies",
                        "present": True,
                        "block": 100,
                        "verified": "canonical",
                        "hash": "aa",
                    },
                    {
                        "stage": "SyncBodiesReady",
                        "field": "stageSyncBodiesReady",
                        "present": True,
                        "block": 96,
                        "verified": "canonical",
                        "hash": "bb",
                    },
                    {
                        "stage": "SyncImport",
                        "field": "stageSyncImport",
                        "present": True,
                        "block": 95,
                        "verified": "canonical",
                        "hash": "cc",
                    },
                    {
                        "stage": "SyncExecution",
                        "field": "stageSyncExecution",
                        "present": True,
                        "block": 90,
                        "verified": "canonical",
                        "hash": "dd",
                    },
                    {
                        "stage": "SyncCommitment",
                        "field": "stageSyncCommitment",
                        "present": True,
                        "block": 89,
                        "verified": "canonical",
                        "hash": "ee",
                    },
                    {
                        "stage": "SyncFinish",
                        "field": "stageSyncFinish",
                        "present": True,
                        "block": 80,
                        "verified": "canonical",
                        "hash": "ff",
                    },
                ],
            )
            self.assertEqual(row["restartRecoveryStatus"], "no-previous")
            self.assertEqual(row["heightRegressionBlocks"], 0)
            self.assertEqual(row["stageProgressRegressionCount"], 0)
            self.assertEqual(row["stageProgressMaxRegressionBlocks"], 0)
            self.assertEqual(row["stageProgressRegressions"], [])
            self.assertFalse(row["stageStalled"])
            self.assertEqual(row["stageStalledCount"], 0)
            self.assertEqual(row["stageStalledStage"], "")
            self.assertEqual(row["stageStalledSeconds"], 0)
            self.assertEqual(row["stageStalledLagBlocks"], -1)
            self.assertEqual(row["stageStalls"], [])
            self.assertEqual(row["intervalStageSyncBodiesBlocks"], 0)
            self.assertEqual(row["intervalStageSyncImportBlocks"], 0)
            self.assertEqual(row["intervalStageSyncExecutionBlocks"], 0)
            self.assertEqual(row["intervalStageSyncCommitmentBlocks"], 0)
            self.assertEqual(row["intervalStageSyncFinishBlocks"], 0)
            self.assertEqual(row["intervalStageSyncBodiesBlocksPerMinute"], 0.0)
            self.assertEqual(row["intervalStageSyncImportBlocksPerMinute"], 0.0)
            self.assertEqual(row["intervalStageSyncExecutionBlocksPerMinute"], 0.0)
            self.assertEqual(row["intervalStageSyncCommitmentBlocksPerMinute"], 0.0)
            self.assertEqual(row["intervalStageSyncFinishBlocksPerMinute"], 0.0)
            self.assertEqual(
                [entry["stage"] for entry in row["stageIntervalRates"]],
                [
                    "SyncBodies",
                    "SyncBodiesReady",
                    "SyncImport",
                    "SyncExecution",
                    "SyncCommitment",
                    "SyncFinish",
                    "ChainFreezer",
                    "SnapshotEventLogBuild",
                ],
            )
            self.assertEqual(row["stageIntervalRates"][5]["blocks"], 0)
            self.assertEqual(row["stageIntervalRates"][5]["blocksPerMinute"], 0.0)
            self.assertEqual(row["intervalStageSyncBodiesReadyToBodiesRatio"], -1.0)
            self.assertEqual(row["intervalStageSyncImportToBodiesReadyRatio"], -1.0)
            self.assertEqual(row["intervalStageSyncExecutionToImportRatio"], -1.0)
            self.assertEqual(row["intervalStageSyncCommitmentToExecutionRatio"], -1.0)
            self.assertEqual(row["intervalStageSyncFinishToCommitmentRatio"], -1.0)
            self.assertEqual(row["soakEfficiencyWindow"], "cumulative")
            self.assertEqual(row["soakEfficiencyStatus"], "no-previous")
            self.assertEqual(row["soakEfficiencyBlocksPerSecond"], row["blocksPerSecond"])
            self.assertEqual(row["soakEfficiencyEtaSeconds"], row["syncEtaSeconds"])
            self.assertEqual(row["soakEfficiencyDatadirBytesPerBlock"], row["bytesPerBlock"])
            self.assertEqual(row["soakEfficiencyHotBytesPerBlock"], row["chaindataBytesPerBlock"])
            self.assertEqual(row["soakEfficiencyColdArchiveBytesPerBlock"], row["coldArchiveBytesPerBlock"])
            self.assertEqual(row["soakEfficiencyDerivedIndexBytesPerBlock"], row["derivedIndexBytesPerBlock"])
            self.assertGreater(row["soakEfficiencyDiskPrimaryBytes"], 0)
            self.assertGreater(row["soakEfficiencyDiskPrimaryShare"], 0)
            self.assertEqual(row["soakEfficiencyStageBottleneck"], "finish-head")
            self.assertEqual(row["soakEfficiencyStageBottleneckLagBlocks"], 20)
            self.assertAlmostEqual(row["soakEfficiencyStageBottleneckLagShare"], 20 / 39)
            self.assertEqual(row["stageUnboundRows"], 1)
            self.assertEqual(row["stageProgress"]["SyncFinish"]["value"], 80)
            self.assertFalse(row["stageProgress"]["SnapshotEventLogBuild"]["present"])
            self.assertEqual(row["processPidFile"], str(pid_file))
            self.assertEqual(row["processStatus"], "ok")
            self.assertEqual(row["processPid"], os.getpid())
            self.assertGreater(row["processRssBytes"], 0)
            self.assertGreaterEqual(row["processCpuPercent"], 0)
            self.assertGreaterEqual(row["processUptimeSeconds"], 0)
            self.assertGreaterEqual(row["processOpenFiles"], -1)
            self.assertEqual(
                row["debugMetricsURL"],
                f"http://127.0.0.1:{server.server_address[1]}/debug/metrics?prefix=chain/freezer/",
            )
            self.assertEqual(row["debugMetricsStatus"], "ok")
            self.assertEqual(row["debugMetricsPrefix"], "chain/freezer/")
            self.assertEqual(row["debugMetricsCount"], 4)
            self.assertEqual(row["debugMetricsNumericCount"], 4)
            self.assertIn("chain/freezer/blocks", row["debugMetricsNames"])
            self.assertEqual(row["debugMetrics"]["chain/freezer/blocks"]["value"], 16)
            self.assertEqual(row["debugMetricChainFreezerBlocks"], 16)
            self.assertEqual(row["debugMetricChainFreezerPasses"], 2)
            self.assertEqual(row["debugMetricChainFreezerLastPassDuration"], 250000000)
            self.assertEqual(row["debugMetricChainFreezerPebbleSize"], 4096)
            self.assertEqual(row["syncLogFile"], str(sync_log))
            self.assertEqual(row["syncLogStatus"], "ok")
            self.assertEqual(row["syncLogImportedSegments"], 2)
            self.assertEqual(row["syncLogSegmentBlocks"], 20)
            self.assertEqual(row["syncLogSegmentTxs"], 7)
            self.assertEqual(row["syncLogSegmentHead"], 100)
            self.assertEqual(row["syncLogSegmentRemain"], 5)
            self.assertEqual(row["syncLogSegmentElapsed"], "2s")
            self.assertEqual(row["syncLogSegmentExecElapsed"], "1500ms")
            self.assertEqual(row["syncLogSegmentApplyElapsed"], "1700ms")
            self.assertEqual(row["syncLogBlocksPerSecond"], 20.5)
            self.assertEqual(row["syncLogTxsPerSecond"], 7.5)
            self.assertEqual(row["syncLogSlowPhase"], "stateCommit")
            self.assertEqual(row["syncLogSlowElapsed"], "900ms")
            self.assertEqual(row["syncLogSlowStateCommitPhase"], "flatWrite")
            self.assertEqual(row["syncLogSlowStateCommitElapsed"], "600ms")
            self.assertEqual(row["syncLogStateMutTop"], "storagePuts:7")
            self.assertEqual(row["syncLogStateMutKVTop"], "accountKV:3")
            self.assertEqual(row["syncLogTxTop"], "TriggerSmartContract=5,TransferContract=2")
            self.assertEqual(row["syncLogPeer"], "peer-latest")
            self.assertFalse(row["syncLogStageComplete"])
            self.assertEqual(row["syncLogStageCompleted"], 59)
            self.assertEqual(row["syncLogStageScheduled"], 80)
            self.assertEqual(row["syncLogStageIncomplete"], 21)
            self.assertAlmostEqual(row["syncLogStageCompletionRatio"], 59 / 80)
            self.assertEqual(row["syncLogStageTasksPerBlock"], 4.0)
            self.assertAlmostEqual(row["syncLogStageCompletedPerBlock"], 59 / 20)
            self.assertEqual(row["syncLogStageNext"], "commitment")
            self.assertEqual(row["syncLogStageNextBlock"], 100)
            self.assertEqual(row["syncLogStageNextCanonical"], "Commitment")
            self.assertEqual(row["syncLogStageNextSync"], "SyncCommitment")
            self.assertEqual(row["syncLogStageBlockedStatus"], "missing")
            self.assertFalse(row["syncLogPhaseCursorComplete"])
            self.assertEqual(row["syncLogPhaseCursorCompletedPhases"], 2)
            self.assertEqual(row["syncLogPhaseCursorScheduledPhases"], 4)
            self.assertEqual(row["syncLogPhaseCursorIncompletePhases"], 2)
            self.assertEqual(row["syncLogPhaseCursorCompletionRatio"], 0.5)
            self.assertEqual(row["syncLogPhaseCursorCompletedTasks"], 59)
            self.assertEqual(row["syncLogPhaseCursorScheduledTasks"], 80)
            self.assertAlmostEqual(row["syncLogPhaseCursorTaskCompletionRatio"], 59 / 80)
            self.assertEqual(row["syncLogPhaseCursorCurrent"], "commitment")
            self.assertEqual(row["syncLogPhaseCursorCurrentCanonical"], "Commitment")
            self.assertEqual(row["syncLogPhaseCursorCurrentSync"], "SyncCommitment")
            self.assertEqual(row["syncLogPhaseCursorCurrentTaskIndex"], 19)
            self.assertEqual(row["syncLogPhaseCursorCurrentTaskCount"], 20)
            self.assertEqual(row["syncLogPhaseCursorCurrentTaskRemaining"], 1)
            self.assertEqual(row["syncLogPhaseCursorCurrentFromBlock"], 100)
            self.assertEqual(row["syncLogPhaseCursorCurrentToBlock"], 100)
            self.assertEqual(row["syncLogPhaseCursorNextBlock"], 100)
            self.assertEqual(row["syncLogPhaseCursorNextPhase"], "commitment")
            self.assertEqual(row["syncLogPhaseCursorNextCanonical"], "Commitment")
            self.assertEqual(row["syncLogPhaseCursorNextSync"], "SyncCommitment")
            self.assertEqual(row["syncLogPhaseCursorBlockedStatus"], "missing")
            self.assertEqual(row["syncLogPhaseProgressCompletedPhases"], 2)
            self.assertEqual(row["syncLogPhaseProgressScheduledPhases"], 4)
            self.assertEqual(row["syncLogPhaseProgressIncompletePhases"], 2)
            self.assertEqual(row["syncLogPhaseProgressCompletionRatio"], 0.5)
            self.assertEqual(row["syncLogPhaseProgressBlockedPhase"], "commitment")
            self.assertEqual(row["syncLogPhaseProgressNextBlock"], 100)
            self.assertEqual(row["syncLogPhaseProgressBlockedStatus"], "missing")
            self.assertEqual(row["syncLogPhaseProgressBodiesCompletedTasks"], 20)
            self.assertEqual(row["syncLogPhaseProgressExecutionCompletedTasks"], 20)
            self.assertEqual(row["syncLogPhaseProgressCommitmentCompletedTasks"], 19)
            self.assertEqual(row["syncLogPhaseProgressFinishCompletedTasks"], 0)
            self.assertEqual(row["syncLogPhaseProgressBodiesBlock"], 100)
            self.assertEqual(row["syncLogPhaseProgressExecutionBlock"], 100)
            self.assertEqual(row["syncLogPhaseProgressCommitmentBlock"], 99)
            self.assertEqual(row["syncLogPhaseProgressFinishBlock"], -1)
            self.assertEqual(row["syncLogPhaseProgressBodiesHeadLagBlocks"], 0)
            self.assertEqual(row["syncLogPhaseProgressExecutionHeadLagBlocks"], 0)
            self.assertEqual(row["syncLogPhaseProgressCommitmentHeadLagBlocks"], 1)
            self.assertEqual(row["syncLogPhaseProgressFinishHeadLagBlocks"], -1)
            self.assertEqual(row["syncLogExecPlanBlocks"], 20)
            self.assertEqual(row["syncLogExecPlanStages"], 80)
            self.assertEqual(row["syncLogExecPlanBodyStages"], 20)
            self.assertEqual(row["syncLogExecPlanPostBodyStages"], 60)
            self.assertEqual(row["syncLogExecPlanExecutionStages"], 20)
            self.assertEqual(row["syncLogExecPlanCommitmentStages"], 20)
            self.assertEqual(row["syncLogExecPlanFinishStages"], 20)
            self.assertEqual(row["syncLogExecPlanFirst"], 81)
            self.assertEqual(row["syncLogExecPlanLast"], 100)
            self.assertEqual(row["syncLogExecPlanStagesPerBlock"], 4.0)
            self.assertEqual(row["syncLogExecPlanPostBodyStagesPerBlock"], 3.0)
            self.assertEqual(row["syncLogAppliedPlanBlocks"], 19)
            self.assertEqual(row["syncLogAppliedPlanStages"], 76)
            self.assertEqual(row["syncLogAppliedPlanBodyStages"], 19)
            self.assertEqual(row["syncLogAppliedPlanPostBodyStages"], 57)
            self.assertEqual(row["syncLogAppliedPlanExecutionStages"], 19)
            self.assertEqual(row["syncLogAppliedPlanCommitmentStages"], 19)
            self.assertEqual(row["syncLogAppliedPlanFinishStages"], 19)
            self.assertEqual(row["syncLogAppliedPlanFirst"], 81)
            self.assertEqual(row["syncLogAppliedPlanLast"], 99)
            self.assertEqual(row["syncLogAppliedPlanStagesPerBlock"], 4.0)
            self.assertEqual(row["syncLogAppliedPlanPostBodyStagesPerBlock"], 3.0)
            self.assertEqual(row["syncStartupRepairStatus"], "ok")
            self.assertEqual(row["syncStartupRepairSummaries"], 2)
            self.assertFalse(row["syncStartupRepairComplete"])
            self.assertEqual(row["syncStartupRepairKept"], 2)
            self.assertEqual(row["syncStartupRepairMissing"], 1)
            self.assertEqual(row["syncStartupRepairDeleted"], 1)
            self.assertTrue(row["syncStartupRepairHasBlocked"])
            self.assertEqual(row["syncStartupRepairFirstBlocked"], "SyncCommitment")
            self.assertFalse(row["syncStartupRepairInterrupted"])
            self.assertEqual(row["syncStartupRepairErrorStage"], "")
            self.assertEqual(row["syncStartupRepairRows"], 4)
            self.assertTrue(row["syncStartupHeadCompletionChecked"])
            self.assertTrue(row["syncStartupHeadCompletionHasPrefix"])
            self.assertEqual(row["syncStartupHeadCompletionLastStage"], "SyncExecution")
            self.assertEqual(row["syncStartupHeadCompletionLastBlock"], 102)
            self.assertEqual(row["syncStartupHeadCompletionFillStages"], 2)
            self.assertEqual(row["syncStartupHeadCompletionWritten"], 2)
            self.assertTrue(row["syncStartupHeadCompletionComplete"])
            self.assertEqual(row["syncStartupHeadCompletionErrorStage"], "")
            self.assertTrue(row["syncStartupPipelineOrderChecked"])
            self.assertEqual(row["syncStartupPipelineOrderIssues"], 1)
            self.assertEqual(row["syncStartupPipelineOrderFirstIssue"], "SyncCommitment=3 ahead of SyncExecution=2")
            self.assertEqual(row["syncStartupPipelineOrderReadErrors"], 1)
            self.assertEqual(row["syncStartupPipelineOrderFirstReadErrorStage"], "SyncBodies")
            self.assertTrue(row["syncStartupPipelineOrderRepairChecked"])
            self.assertFalse(row["syncStartupPipelineOrderRepairComplete"])
            self.assertEqual(row["syncStartupPipelineOrderRepairDeleted"], 2)
            self.assertEqual(row["syncStartupPipelineOrderRepairUpdated"], 1)
            self.assertTrue(row["syncStartupPipelineOrderRepairInterrupted"])
            self.assertEqual(row["syncStartupPipelineOrderRepairErrorStage"], "SyncCommitment")
            self.assertEqual(row["syncStartupPipelineOrderRepairRows"], 3)
            self.assertTrue(row["syncStartupPipelineCursorChecked"])
            self.assertFalse(row["syncStartupPipelineCursorComplete"])
            self.assertEqual(row["syncStartupPipelineCursorRows"], 4)
            self.assertTrue(row["syncStartupPipelineCursorHasLast"])
            self.assertEqual(row["syncStartupPipelineCursorLastStage"], "SyncExecution")
            self.assertEqual(row["syncStartupPipelineCursorLastBlock"], 102)
            self.assertTrue(row["syncStartupPipelineCursorLastHasHash"])
            self.assertTrue(row["syncStartupPipelineCursorHasNext"])
            self.assertEqual(row["syncStartupPipelineCursorNextStage"], "SyncCommitment")
            self.assertTrue(row["syncStartupPipelineCursorBlocked"])
            self.assertFalse(row["syncStartupPipelineCursorInterrupted"])
            self.assertEqual(row["syncStartupPipelineCursorErrorStage"], "")
            self.assertEqual(row["syncStartupStagedRestored"], 3)
            self.assertEqual(row["syncStartupStagedTargetHead"], 110)
            self.assertEqual(row["syncStartupStagedNextExpected"], 104)
            self.assertTrue(row["syncStartupStagedNeedPruneTail"])
            self.assertEqual(row["syncStartupStagedPruneFrom"], 105)
            self.assertTrue(row["syncStartupStagedHaveLastRestored"])
            self.assertEqual(row["syncStartupStagedLastRestored"], 103)
            self.assertEqual(output.read_text(encoding="utf-8").strip(), proc.stdout.strip())

    def test_sample_parses_json_stage_status_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.json"
            stage_status.write_text(
                json.dumps(
                    {
                        "datadir": "/tmp/nile",
                        "known": 32,
                        "rows": 8,
                        "status": "critical",
                        "verify": True,
                        "pipeline": {
                            "complete": False,
                            "pending": 2,
                            "issues": 1,
                            "tasks": [
                                {
                                    "stage": "SnapshotEventLogBuild",
                                    "upstream": "Finish",
                                    "status": "missing",
                                    "targetValue": 82,
                                    "targetHash": "11",
                                },
                                {
                                    "stage": "ChainFreezer",
                                    "upstream": "Finish",
                                    "status": "behind",
                                    "targetValue": 82,
                                    "targetHash": "11",
                                    "currentValue": 70,
                                    "currentHash": "22",
                                },
                            ],
                        },
                        "stages": [
                            {"group": "sync", "name": "SyncBodies", "present": True, "status": "present", "value": 100, "hash": "aa", "verified": "canonical"},
                            {"group": "sync", "name": "SyncBodiesReady", "present": True, "status": "present", "value": 96, "hash": "bb", "verified": "staged-hash-mismatch", "details": ["stagedBlock=96", "stagedHash=cc"]},
                            {"group": "sync", "name": "SyncImport", "present": True, "status": "present", "value": 95, "hash": "cc", "verified": "canonical"},
                            {"group": "sync", "name": "SyncExecution", "present": True, "status": "present", "value": 90, "hash": "dd", "verified": "canonical"},
                            {"group": "sync", "name": "SyncCommitment", "present": True, "status": "present", "value": 89, "hash": "ee", "verified": "canonical"},
                            {"group": "sync", "name": "SyncFinish", "present": True, "status": "present", "value": 80, "hash": "ff", "verified": "canonical"},
                            {"group": "canonical", "name": "Finish", "present": True, "status": "present", "value": 82, "hash": "11", "verified": "canonical"},
                            {"group": "snapshot", "name": "SnapshotEventLogBuild", "present": False, "status": "missing"},
                        ],
                        "issues": ["SyncBodiesReady staged-body status=hash-mismatch block=96 hash=bb stagedBlock=96 stagedHash=cc"],
                        "issueDetails": [
                            {
                                "severity": "critical",
                                "kind": "sync-stage-order",
                                "detail": "SyncExecution=101 ahead of SyncImport=95",
                                "downstream": "SyncExecution",
                                "downstreamValue": 101,
                                "upstream": "SyncImport",
                                "upstreamValue": 95,
                            },
                            {
                                "severity": "critical",
                                "kind": "staged-body",
                                "detail": "SyncBodiesReady staged-body status=hash-mismatch block=96 hash=bb stagedBlock=96 stagedHash=cc",
                                "stage": "SyncBodiesReady",
                            },
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["stageStatusFileStatus"], "ok")
            self.assertEqual(row["stageKnown"], 32)
            self.assertEqual(row["stageRows"], 8)
            self.assertFalse(row["stagePipelineComplete"])
            self.assertEqual(row["stagePipelinePending"], 2)
            self.assertEqual(row["stagePipelineIssues"], 1)
            self.assertEqual(row["stagePipelineNext"], "SnapshotEventLogBuild")
            self.assertEqual(row["stagePipelineNextStatus"], "missing")
            self.assertEqual(row["stagePipelineNextTarget"], 82)
            self.assertEqual(row["stagePipelineNextUpstream"], "Finish")
            self.assertEqual(row["stagePipelineNextCurrent"], -1)
            self.assertEqual(
                row["stagePipelineTasks"][1],
                {
                    "stage": "ChainFreezer",
                    "upstream": "Finish",
                    "status": "behind",
                    "targetValue": 82,
                    "targetHash": "11",
                    "currentValue": 70,
                    "currentHash": "22",
                },
            )
            self.assertEqual(row["stageSyncBodies"], 100)
            self.assertEqual(row["stageSyncBodiesReady"], 96)
            self.assertEqual(row["stageSyncImport"], 95)
            self.assertEqual(row["stageSyncExecution"], 90)
            self.assertEqual(row["stageSyncCommitment"], 89)
            self.assertEqual(row["stageSyncFinish"], 80)
            self.assertEqual(row["stageSyncBodiesReadyGapBlocks"], 4)
            self.assertEqual(row["stageSyncCommitmentFinishLagBlocks"], 9)
            self.assertEqual(row["stageStagedBodyIssueRows"], 1)
            self.assertEqual(row["stageIssueRows"], 2)
            self.assertEqual(row["stageOrderIssueRows"], 1)
            self.assertEqual(row["stageSyncOrderIssueRows"], 1)
            self.assertEqual(row["stageStorageOrderIssueRows"], 0)
            self.assertEqual(
                row["stageOrderIssueDetails"],
                [
                    {
                        "severity": "critical",
                        "kind": "sync-stage-order",
                        "detail": "SyncExecution=101 ahead of SyncImport=95",
                        "downstream": "SyncExecution",
                        "downstreamValue": 101,
                        "upstream": "SyncImport",
                        "upstreamValue": 95,
                    }
                ],
            )
            self.assertEqual(
                row["stageStagedBodyIssueDetails"],
                [{"stage": "SyncBodiesReady", "value": 96, "verified": "staged-hash-mismatch", "stagedBlock": 96, "stagedHash": "cc"}],
            )
            self.assertEqual(row["stageProgress"]["SyncBodiesReady"]["stagedBlock"], "96")
            self.assertEqual(row["stageProgress"]["SyncBodiesReady"]["stagedHash"], "cc")
            self.assertEqual(row["fullStagedSyncStatus"], "hash-issue")
            self.assertIn("stage-order-issue", row["soakHealthIssues"])
            self.assertIn("stage-status-issue", row["soakHealthIssues"])
            self.assertIn("stage-staged-body-issue", row["soakHealthIssues"])

    def test_sample_derives_interval_rates_from_previous_jsonl_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            (datadir / "gtron" / "ancient").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "log").mkdir(parents=True)
            (datadir / "gtron" / "balance-trace-replay").mkdir(parents=True)
            (datadir / "gtron" / "chaindata" / "hot.bin").write_bytes(b"h" * 4096)
            (datadir / "gtron" / "chaindata" / "000001.sst").write_bytes(b"s" * 4096)
            (datadir / "gtron" / "chaindata" / "000002.log").write_bytes(b"w" * 4096)
            (datadir / "gtron" / "chaindata" / "LOG").write_bytes(b"l" * 4096)
            (datadir / "gtron" / "chaindata" / "MANIFEST-000003").write_bytes(b"m" * 4096)
            (datadir / "gtron" / "chaindata" / "OPTIONS-000004").write_bytes(b"o" * 4096)
            (datadir / "gtron" / "ancient" / "cold.bin").write_bytes(b"c" * 2048)
            (datadir / "gtron" / "state-snapshots" / "snap.bin").write_bytes(b"s" * 1024)
            (datadir / "gtron" / "state-snapshots" / "log" / "section-bloom-1-8192.seg").write_bytes(b"b" * 2048)
            (datadir / "gtron" / "balance-trace-replay" / "replay.bin").write_bytes(b"r" * 1024)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=8",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=98 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=95 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=92 hash=dd verified=canonical",
                        "Stage progress: group=sync name=SyncCommitment value=91 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=90 hash=ff verified=canonical",
                        "Stage progress: group=freezer name=ChainFreezer value=70 hash=none verified=unbound",
                        "Stage progress: group=snapshot name=SnapshotEventLogBuild value=88 hash=none verified=unbound",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            now = int(time.time())
            previous = {
                "unix": now - 10,
                "height": 70,
                "datadirBytes": 1024,
                "chaindataBytes": 512,
                "chaindataSSTBytes": 128,
                "chaindataWALBytes": 128,
                "chaindataLogBytes": 128,
                "chaindataManifestBytes": 128,
                "chaindataOptionsBytes": 128,
                "chaindataOtherBytes": 128,
                "ancientBytes": 256,
                "snapshotBytes": 128,
                "replayBytes": 512,
                "coldArchiveBytes": 384,
                "derivedIndexBytes": 256,
                "ancientBodiesBytes": 64,
                "ancientTxInfosBytes": 32,
                "ancientStateRootsBytes": 16,
                "ancientOtherBytes": 16,
                "snapshotRootBytes": 64,
                "snapshotLatestBytes": 16,
                "snapshotHistoryBytes": 32,
                "snapshotChainBytes": 32,
                "snapshotLogBytes": 32,
                "snapshotTraceBytes": 32,
                "snapshotCommitmentBytes": 16,
                "snapshotRetiredDirectoryBytes": 16,
                "snapshotOtherBytes": 16,
                "stageSyncBodies": 80,
                "stageSyncBodiesReady": 79,
                "stageSyncImport": 70,
                "stageSyncExecution": 68,
                "stageSyncCommitment": 67,
                "stageSyncFinish": 60,
                "stageChainFreezer": 50,
                "stageSnapshotEventLogBuild": 40,
            }
            output = tmpdir / "samples.jsonl"
            output.write_text(json.dumps(previous) + "\n", encoding="utf-8")

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                    "--output",
                    str(output),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertGreaterEqual(row["intervalSeconds"], 10)
            self.assertEqual(row["intervalBlocks"], 30)
            self.assertGreater(row["intervalBlocksPerSecond"], 0)
            self.assertEqual(row["syncTargetHeight"], 105)
            self.assertEqual(row["syncTargetLagBlocks"], 5)
            self.assertAlmostEqual(row["intervalSyncEtaSeconds"], 5 / row["intervalBlocksPerSecond"])
            self.assertEqual(row["soakHealthStatus"], "warning")
            self.assertEqual(row["soakHealthCriticalIssues"], 0)
            self.assertEqual(row["soakHealthWarningIssues"], 2)
            self.assertIn("sample-status:height-mismatch", row["soakHealthIssues"])
            self.assertIn("stage-unbound-rows", row["soakHealthIssues"])
            self.assertEqual(row["soakPrimaryBottleneck"], "finish-head")
            self.assertEqual(row["soakPrimaryBottleneckSource"], "sync-stage")
            self.assertEqual(row["soakPrimaryBottleneckLagBlocks"], 10)
            self.assertEqual(row["datadirBytesDelta"], row["datadirBytes"] - previous["datadirBytes"])
            self.assertEqual(row["chaindataBytesDelta"], row["chaindataBytes"] - previous["chaindataBytes"])
            self.assertEqual(row["chaindataSSTBytesDelta"], row["chaindataSSTBytes"] - previous["chaindataSSTBytes"])
            self.assertEqual(row["chaindataWALBytesDelta"], row["chaindataWALBytes"] - previous["chaindataWALBytes"])
            self.assertEqual(row["chaindataLogBytesDelta"], row["chaindataLogBytes"] - previous["chaindataLogBytes"])
            self.assertEqual(row["chaindataManifestBytesDelta"], row["chaindataManifestBytes"] - previous["chaindataManifestBytes"])
            self.assertEqual(row["chaindataOptionsBytesDelta"], row["chaindataOptionsBytes"] - previous["chaindataOptionsBytes"])
            self.assertEqual(row["chaindataOtherBytesDelta"], row["chaindataOtherBytes"] - previous["chaindataOtherBytes"])
            self.assertEqual(row["ancientBytesDelta"], row["ancientBytes"] - previous["ancientBytes"])
            self.assertEqual(row["snapshotBytesDelta"], row["snapshotBytes"] - previous["snapshotBytes"])
            self.assertEqual(row["replayBytesDelta"], row["replayBytes"] - previous["replayBytes"])
            self.assertEqual(row["coldArchiveBytesDelta"], row["coldArchiveBytes"] - previous["coldArchiveBytes"])
            self.assertEqual(row["derivedIndexBytesDelta"], row["derivedIndexBytes"] - previous["derivedIndexBytes"])
            self.assertEqual(row["ancientBodiesBytesDelta"], row["ancientBodiesBytes"] - previous["ancientBodiesBytes"])
            self.assertEqual(row["ancientTxInfosBytesDelta"], row["ancientTxInfosBytes"] - previous["ancientTxInfosBytes"])
            self.assertEqual(row["ancientStateRootsBytesDelta"], row["ancientStateRootsBytes"] - previous["ancientStateRootsBytes"])
            self.assertEqual(row["ancientOtherBytesDelta"], row["ancientOtherBytes"] - previous["ancientOtherBytes"])
            self.assertEqual(row["snapshotRootBytesDelta"], row["snapshotRootBytes"] - previous["snapshotRootBytes"])
            self.assertEqual(row["snapshotLatestBytesDelta"], row["snapshotLatestBytes"] - previous["snapshotLatestBytes"])
            self.assertEqual(row["snapshotHistoryBytesDelta"], row["snapshotHistoryBytes"] - previous["snapshotHistoryBytes"])
            self.assertEqual(row["snapshotChainBytesDelta"], row["snapshotChainBytes"] - previous["snapshotChainBytes"])
            self.assertEqual(row["snapshotLogBytesDelta"], row["snapshotLogBytes"] - previous["snapshotLogBytes"])
            self.assertEqual(row["snapshotTraceBytesDelta"], row["snapshotTraceBytes"] - previous["snapshotTraceBytes"])
            self.assertEqual(row["snapshotCommitmentBytesDelta"], row["snapshotCommitmentBytes"] - previous["snapshotCommitmentBytes"])
            self.assertEqual(row["snapshotRetiredDirectoryBytesDelta"], row["snapshotRetiredDirectoryBytes"] - previous["snapshotRetiredDirectoryBytes"])
            self.assertEqual(row["snapshotOtherBytesDelta"], row["snapshotOtherBytes"] - previous["snapshotOtherBytes"])
            self.assertEqual(
                row["datadirOtherBytesDelta"],
                row["datadirBytesDelta"]
                - row["chaindataBytesDelta"]
                - row["ancientBytesDelta"]
                - row["snapshotBytesDelta"]
                - row["replayBytesDelta"],
            )
            growth_candidates = [
                ("chaindata", row["chaindataBytesDelta"]),
                ("ancient", row["ancientBytesDelta"]),
                ("snapshot", row["snapshotBytesDelta"]),
                ("replay", row["replayBytesDelta"]),
                ("other", row["datadirOtherBytesDelta"]),
            ]
            positive_growth = sum(max(value, 0) for _, value in growth_candidates)
            primary_name, primary_value = max(growth_candidates, key=lambda item: item[1])
            self.assertGreater(positive_growth, 0)
            self.assertEqual(row["intervalPositiveDiskGrowthBytes"], positive_growth)
            self.assertEqual(row["intervalDiskGrowthPrimary"], primary_name)
            self.assertEqual(row["intervalDiskGrowthPrimaryBytes"], primary_value)
            self.assertAlmostEqual(row["intervalDiskGrowthPrimaryShare"], primary_value / positive_growth)
            self.assertAlmostEqual(row["intervalChaindataGrowthShare"], max(row["chaindataBytesDelta"], 0) / positive_growth)
            self.assertAlmostEqual(row["intervalAncientGrowthShare"], max(row["ancientBytesDelta"], 0) / positive_growth)
            self.assertAlmostEqual(row["intervalSnapshotGrowthShare"], max(row["snapshotBytesDelta"], 0) / positive_growth)
            self.assertAlmostEqual(row["intervalReplayGrowthShare"], max(row["replayBytesDelta"], 0) / positive_growth)
            self.assertAlmostEqual(row["intervalDatadirOtherGrowthShare"], max(row["datadirOtherBytesDelta"], 0) / positive_growth)
            self.assertAlmostEqual(row["intervalColdArchiveGrowthShare"], max(row["coldArchiveBytesDelta"], 0) / positive_growth)
            self.assertAlmostEqual(row["intervalDerivedIndexGrowthShare"], max(row["derivedIndexBytesDelta"], 0) / positive_growth)
            positive_hot_growth = max(row["chaindataBytesDelta"], 0)
            self.assertGreater(positive_hot_growth, 0)
            self.assertAlmostEqual(row["intervalColdToHotGrowthRatio"], max(row["coldArchiveBytesDelta"], 0) / positive_hot_growth)
            self.assertAlmostEqual(row["intervalAncientToHotGrowthRatio"], max(row["ancientBytesDelta"], 0) / positive_hot_growth)
            self.assertAlmostEqual(row["intervalSnapshotToHotGrowthRatio"], max(row["snapshotBytesDelta"], 0) / positive_hot_growth)
            self.assertAlmostEqual(row["intervalDerivedIndexToHotGrowthRatio"], max(row["derivedIndexBytesDelta"], 0) / positive_hot_growth)
            detailed_growth_candidates = [
                ("chaindata.sst", row["chaindataSSTBytesDelta"]),
                ("chaindata.wal", row["chaindataWALBytesDelta"]),
                ("chaindata.log", row["chaindataLogBytesDelta"]),
                ("chaindata.manifest", row["chaindataManifestBytesDelta"]),
                ("chaindata.options", row["chaindataOptionsBytesDelta"]),
                ("chaindata.other", row["chaindataOtherBytesDelta"]),
                ("ancient.bodies", row["ancientBodiesBytesDelta"]),
                ("ancient.txInfos", row["ancientTxInfosBytesDelta"]),
                ("ancient.stateRoots", row["ancientStateRootsBytesDelta"]),
                ("ancient.other", row["ancientOtherBytesDelta"]),
                ("snapshot.root", row["snapshotRootBytesDelta"]),
                ("snapshot.latest", row["snapshotLatestBytesDelta"]),
                ("snapshot.history", row["snapshotHistoryBytesDelta"]),
                ("snapshot.chain", row["snapshotChainBytesDelta"]),
                ("snapshot.log", row["snapshotLogBytesDelta"]),
                ("snapshot.trace", row["snapshotTraceBytesDelta"]),
                ("snapshot.commitment", row["snapshotCommitmentBytesDelta"]),
                ("snapshot.retired", row["snapshotRetiredDirectoryBytesDelta"]),
                ("snapshot.other", row["snapshotOtherBytesDelta"]),
                ("replay", row["replayBytesDelta"]),
                ("datadir.other", row["datadirOtherBytesDelta"]),
            ]
            detailed_positive_growth = sum(max(value, 0) for _, value in detailed_growth_candidates)
            detailed_primary_name, detailed_primary_value = max(
                [(name, value) for name, value in detailed_growth_candidates if value > 0],
                key=lambda item: item[1],
            )
            self.assertGreater(detailed_positive_growth, 0)
            self.assertEqual(row["intervalDetailedPositiveDiskGrowthBytes"], detailed_positive_growth)
            self.assertEqual(row["intervalDiskGrowthPrimaryDetailed"], detailed_primary_name)
            self.assertEqual(row["intervalDiskGrowthPrimaryDetailedBytes"], detailed_primary_value)
            self.assertAlmostEqual(row["intervalDiskGrowthPrimaryDetailedShare"], detailed_primary_value / detailed_positive_growth)
            detailed_shares = {
                "intervalChaindataSSTGrowthShare": row["chaindataSSTBytesDelta"],
                "intervalChaindataWALGrowthShare": row["chaindataWALBytesDelta"],
                "intervalChaindataLogGrowthShare": row["chaindataLogBytesDelta"],
                "intervalChaindataManifestGrowthShare": row["chaindataManifestBytesDelta"],
                "intervalChaindataOptionsGrowthShare": row["chaindataOptionsBytesDelta"],
                "intervalChaindataOtherGrowthShare": row["chaindataOtherBytesDelta"],
                "intervalAncientBodiesGrowthShare": row["ancientBodiesBytesDelta"],
                "intervalAncientTxInfosGrowthShare": row["ancientTxInfosBytesDelta"],
                "intervalAncientStateRootsGrowthShare": row["ancientStateRootsBytesDelta"],
                "intervalAncientOtherGrowthShare": row["ancientOtherBytesDelta"],
                "intervalSnapshotRootGrowthShare": row["snapshotRootBytesDelta"],
                "intervalSnapshotLatestGrowthShare": row["snapshotLatestBytesDelta"],
                "intervalSnapshotHistoryGrowthShare": row["snapshotHistoryBytesDelta"],
                "intervalSnapshotChainGrowthShare": row["snapshotChainBytesDelta"],
                "intervalSnapshotLogGrowthShare": row["snapshotLogBytesDelta"],
                "intervalSnapshotTraceGrowthShare": row["snapshotTraceBytesDelta"],
                "intervalSnapshotCommitmentGrowthShare": row["snapshotCommitmentBytesDelta"],
                "intervalSnapshotRetiredDirectoryGrowthShare": row["snapshotRetiredDirectoryBytesDelta"],
                "intervalSnapshotOtherGrowthShare": row["snapshotOtherBytesDelta"],
                "intervalReplayDetailedGrowthShare": row["replayBytesDelta"],
                "intervalDatadirOtherDetailedGrowthShare": row["datadirOtherBytesDelta"],
            }
            for field, delta in detailed_shares.items():
                self.assertAlmostEqual(row[field], max(delta, 0) / detailed_positive_growth)
            self.assertGreater(row["datadirBytesPerSecond"], 0)
            self.assertGreater(row["chaindataBytesPerSecond"], 0)
            self.assertGreater(row["chaindataSSTBytesPerSecond"], 0)
            self.assertGreater(row["chaindataWALBytesPerSecond"], 0)
            self.assertGreater(row["derivedIndexBytesPerSecond"], 0)
            self.assertAlmostEqual(row["coldArchiveDatadirShare"], row["coldArchiveBytes"] / row["datadirBytes"])
            self.assertAlmostEqual(row["derivedIndexColdArchiveRatio"], row["derivedIndexBytes"] / row["coldArchiveBytes"])
            self.assertGreater(row["replayBytesPerSecond"], 0)
            self.assertEqual(row["datadirOtherBytesPerSecond"], row["datadirOtherBytesDelta"] / row["intervalSeconds"])
            self.assertEqual(row["intervalDatadirBytesPerBlock"], row["datadirBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalChaindataBytesPerBlock"], row["chaindataBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalChaindataSSTBytesPerBlock"], row["chaindataSSTBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalChaindataWALBytesPerBlock"], row["chaindataWALBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalAncientBytesPerBlock"], row["ancientBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalSnapshotBytesPerBlock"], row["snapshotBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalColdArchiveBytesPerBlock"], row["coldArchiveBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalDerivedIndexBytesPerBlock"], row["derivedIndexBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalReplayBytesPerBlock"], row["replayBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalDatadirOtherBytesPerBlock"], row["datadirOtherBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalStageSyncBodiesBlocks"], 20)
            self.assertEqual(row["intervalStageSyncBodiesReadyBlocks"], 19)
            self.assertEqual(row["intervalStageSyncImportBlocks"], 25)
            self.assertEqual(row["intervalStageSyncExecutionBlocks"], 24)
            self.assertEqual(row["intervalStageSyncCommitmentBlocks"], 24)
            self.assertEqual(row["intervalStageSyncFinishBlocks"], 30)
            self.assertEqual(row["intervalStageChainFreezerBlocks"], 20)
            self.assertEqual(row["intervalStageSnapshotEventLogBuildBlocks"], 48)
            self.assertAlmostEqual(row["intervalStageSyncBodiesReadyToBodiesRatio"], 19 / 20)
            self.assertAlmostEqual(row["intervalStageSyncImportToBodiesReadyRatio"], 25 / 19)
            self.assertAlmostEqual(row["intervalStageSyncExecutionToImportRatio"], 24 / 25)
            self.assertAlmostEqual(row["intervalStageSyncCommitmentToExecutionRatio"], 24 / 24)
            self.assertAlmostEqual(row["intervalStageSyncFinishToCommitmentRatio"], 30 / 24)
            self.assertAlmostEqual(row["intervalStageSyncBodiesBlocksPerMinute"], row["intervalStageSyncBodiesBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageSyncBodiesReadyBlocksPerMinute"], row["intervalStageSyncBodiesReadyBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageSyncImportBlocksPerMinute"], row["intervalStageSyncImportBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageSyncExecutionBlocksPerMinute"], row["intervalStageSyncExecutionBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageSyncCommitmentBlocksPerMinute"], row["intervalStageSyncCommitmentBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageSyncFinishBlocksPerMinute"], row["intervalStageSyncFinishBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageChainFreezerBlocksPerMinute"], row["intervalStageChainFreezerBlocksPerSecond"] * 60)
            self.assertAlmostEqual(row["intervalStageSnapshotEventLogBuildBlocksPerMinute"], row["intervalStageSnapshotEventLogBuildBlocksPerSecond"] * 60)
            self.assertGreater(row["intervalStageSyncFinishBlocksPerSecond"], 0)
            self.assertGreater(row["intervalStageSnapshotEventLogBuildBlocksPerSecond"], 0)
            stage_rates = {entry["stage"]: entry for entry in row["stageIntervalRates"]}
            self.assertEqual(stage_rates["SyncBodies"]["field"], "stageSyncBodies")
            self.assertEqual(stage_rates["SyncBodies"]["blocks"], 20)
            self.assertAlmostEqual(stage_rates["SyncBodies"]["blocksPerSecond"], row["intervalStageSyncBodiesBlocksPerSecond"])
            self.assertAlmostEqual(stage_rates["SyncBodies"]["blocksPerMinute"], row["intervalStageSyncBodiesBlocksPerMinute"])
            self.assertEqual(stage_rates["SyncBodies"]["headLagBlocks"], row["stageSyncBodiesHeadLagBlocks"])
            self.assertEqual(stage_rates["SyncBodies"]["headEtaSeconds"], row["stageSyncBodiesHeadEtaSeconds"])
            self.assertEqual(stage_rates["SyncFinish"]["field"], "stageSyncFinish")
            self.assertEqual(stage_rates["SyncFinish"]["blocks"], 30)
            self.assertAlmostEqual(stage_rates["SyncFinish"]["blocksPerSecond"], row["intervalStageSyncFinishBlocksPerSecond"])
            self.assertAlmostEqual(stage_rates["SyncFinish"]["blocksPerMinute"], row["intervalStageSyncFinishBlocksPerMinute"])
            self.assertEqual(stage_rates["SyncFinish"]["headLagBlocks"], row["stageSyncFinishHeadLagBlocks"])
            self.assertEqual(stage_rates["SyncFinish"]["headEtaSeconds"], row["stageSyncFinishHeadEtaSeconds"])
            self.assertEqual(stage_rates["SnapshotEventLogBuild"]["blocks"], 48)
            self.assertEqual(row["soakEfficiencyWindow"], "interval")
            self.assertEqual(row["soakEfficiencyStatus"], "progressing")
            self.assertEqual(row["soakEfficiencyBlocksPerSecond"], row["intervalBlocksPerSecond"])
            self.assertEqual(row["soakEfficiencyEtaSeconds"], row["intervalSyncEtaSeconds"])
            self.assertEqual(row["soakEfficiencyDatadirBytesPerBlock"], row["intervalDatadirBytesPerBlock"])
            self.assertEqual(row["soakEfficiencyHotBytesPerBlock"], row["intervalChaindataBytesPerBlock"])
            self.assertEqual(row["soakEfficiencyColdArchiveBytesPerBlock"], row["intervalColdArchiveBytesPerBlock"])
            self.assertEqual(row["soakEfficiencyDerivedIndexBytesPerBlock"], row["intervalDerivedIndexBytesPerBlock"])
            self.assertEqual(row["soakEfficiencyDiskPrimary"], row["intervalDiskGrowthPrimaryDetailed"])
            self.assertEqual(row["soakEfficiencyDiskPrimaryBytes"], row["intervalDiskGrowthPrimaryDetailedBytes"])
            self.assertEqual(row["soakEfficiencyDiskPrimaryShare"], row["intervalDiskGrowthPrimaryDetailedShare"])
            self.assertEqual(row["soakEfficiencyStageBottleneck"], "finish-head")
            self.assertEqual(row["soakEfficiencyStageBottleneckLagBlocks"], 10)
            self.assertAlmostEqual(row["soakEfficiencyStageBottleneckLagShare"], 10 / 17)
            self.assertEqual(row["stageSyncFinishHeadLagBlocks"], 10)
            self.assertEqual(row["stageSyncPipelineLagBlocks"], 17)
            self.assertAlmostEqual(row["stageSyncBottleneckLagShare"], 10 / 17)
            self.assertEqual(row["stageSyncBodiesHeadLagBlocks"], 0)
            self.assertEqual(row["stageSyncBodiesHeadEtaSeconds"], 0.0)
            self.assertEqual(row["stageSyncBodiesReadyHeadLagBlocks"], 2)
            self.assertAlmostEqual(
                row["stageSyncBodiesReadyHeadEtaSeconds"],
                2 / row["intervalStageSyncBodiesReadyBlocksPerSecond"],
            )
            self.assertEqual(row["stageSyncImportHeadLagBlocks"], 5)
            self.assertAlmostEqual(
                row["stageSyncImportHeadEtaSeconds"],
                5 / row["intervalStageSyncImportBlocksPerSecond"],
            )
            self.assertEqual(row["stageSyncExecutionHeadLagBlocks"], 8)
            self.assertAlmostEqual(
                row["stageSyncExecutionHeadEtaSeconds"],
                8 / row["intervalStageSyncExecutionBlocksPerSecond"],
            )
            self.assertEqual(row["stageSyncCommitmentHeadLagBlocks"], 9)
            self.assertAlmostEqual(
                row["stageSyncCommitmentHeadEtaSeconds"],
                9 / row["intervalStageSyncCommitmentBlocksPerSecond"],
            )
            self.assertTrue(row["stageSyncPipelineMonotonic"])
            self.assertEqual(row["stageSyncPipelineViolation"], "")
            self.assertEqual(row["stageSyncPipelineViolationCount"], 0)
            self.assertEqual(row["stageSyncPipelineMaxViolationBlocks"], 0)
            self.assertEqual(row["stageSyncPipelineViolations"], [])
            self.assertEqual(row["fullStagedSyncStatus"], "catching-up")
            self.assertTrue(row["fullStagedSyncReady"])
            self.assertFalse(row["fullStagedSyncCompleteAtHead"])
            self.assertEqual(row["fullStagedSyncPresentStageCount"], 6)
            self.assertEqual(row["fullStagedSyncVerifiedStageCount"], 6)
            self.assertEqual(row["fullStagedSyncMissingStages"], [])
            self.assertEqual(row["fullStagedSyncHashIssues"], [])
            self.assertEqual(row["fullStagedSyncCompleteBlock"], 90)
            self.assertEqual(row["fullStagedSyncHeadBlock"], 100)
            self.assertEqual(row["fullStagedSyncMinStage"], "SyncFinish")
            self.assertEqual(row["fullStagedSyncMinStageBlock"], 90)
            self.assertEqual(row["fullStagedSyncHeadLagBlocks"], 10)
            self.assertAlmostEqual(row["fullStagedSyncCompletionRatio"], 0.9)
            self.assertEqual(row["fullStagedSyncPipelineLagBlocks"], 17)
            self.assertEqual(row["fullStagedSyncBottleneck"], "finish-head")
            self.assertEqual(row["fullStagedSyncBottleneckLagBlocks"], 10)
            self.assertAlmostEqual(row["fullStagedSyncBottleneckLagShare"], 10 / 17)
            self.assertEqual(row["fullStagedSyncStageCoverageRatio"], 1.0)
            self.assertEqual(row["fullStagedSyncVerificationRatio"], 1.0)
            self.assertEqual(row["restartRecoveryStatus"], "progressing")
            self.assertEqual(row["heightRegressionBlocks"], 0)
            self.assertEqual(row["stageProgressRegressionCount"], 0)
            self.assertEqual(row["stageProgressMaxRegressionBlocks"], 0)
            self.assertEqual(row["stageProgressRegressions"], [])
            self.assertFalse(row["stageStalled"])
            self.assertEqual(row["stageStalledCount"], 0)
            self.assertAlmostEqual(
                row["stageSyncFinishHeadEtaSeconds"],
                10 / row["intervalStageSyncFinishBlocksPerSecond"],
            )
            self.assertEqual(row["stageChainFreezerHeadLagBlocks"], 30)
            self.assertAlmostEqual(
                row["stageChainFreezerHeadEtaSeconds"],
                30 / row["intervalStageChainFreezerBlocksPerSecond"],
            )
            self.assertEqual(row["stageSnapshotEventLogBuildHeadLagBlocks"], 12)
            self.assertAlmostEqual(
                row["stageSnapshotEventLogBuildHeadEtaSeconds"],
                12 / row["intervalStageSnapshotEventLogBuildBlocksPerSecond"],
            )

            lines = output.read_text(encoding="utf-8").splitlines()
            self.assertEqual(json.loads(lines[0]), previous)
            self.assertEqual(json.loads(lines[-1]), row)

    def test_sample_flags_stage_stall_while_height_progresses(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=6",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=100 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=100 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=90 hash=dd verified=canonical",
                        "Stage progress: group=sync name=SyncCommitment value=89 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=88 hash=ff verified=canonical",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            now = int(time.time())
            previous = {
                "unix": now - 20,
                "height": 80,
                "stageSyncBodies": 90,
                "stageSyncBodiesReady": 90,
                "stageSyncImport": 90,
                "stageSyncExecution": 90,
                "stageSyncCommitment": 89,
                "stageSyncFinish": 88,
                "stageLastProgressUnix": {
                    "stageSyncExecution": now - 120,
                    "stageSyncCommitment": now - 60,
                    "stageSyncFinish": now - 40,
                },
            }
            output = tmpdir / "samples.jsonl"
            output.write_text(json.dumps(previous) + "\n", encoding="utf-8")

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                    "--output",
                    str(output),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["height"], 100)
            self.assertEqual(row["restartRecoveryStatus"], "progressing")
            self.assertTrue(row["stageStalled"])
            self.assertGreaterEqual(row["stageStalledCount"], 1)
            self.assertEqual(row["stageStalledStage"], "stageSyncExecution")
            self.assertGreaterEqual(row["stageStalledSeconds"], 120)
            self.assertEqual(row["stageStalledLagBlocks"], 10)
            self.assertIn("stage-stalled", row["soakHealthIssues"])
            self.assertEqual(row["soakHealthStatus"], "warning")
            self.assertEqual(row["soakPrimaryBottleneck"], "finish-head")
            self.assertEqual(row["stageLastProgressUnix"]["stageSyncExecution"], previous["stageLastProgressUnix"]["stageSyncExecution"])
            stalled = {entry["stage"]: entry for entry in row["stageStalls"]}
            self.assertEqual(stalled["stageSyncExecution"]["value"], 90)
            self.assertEqual(stalled["stageSyncExecution"]["previousValue"], 90)
            self.assertEqual(stalled["stageSyncExecution"]["intervalBlocks"], 0)
            self.assertEqual(stalled["stageSyncExecution"]["lagBlocks"], 10)
            self.assertGreaterEqual(stalled["stageSyncExecution"]["stalledSeconds"], 120)

    def test_sample_reports_non_monotonic_stage_pipeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=6",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=101 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=99 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=99 hash=dd verified=canonical",
                        "Stage progress: group=sync name=SyncCommitment value=98 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=98 hash=ff verified=canonical",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertFalse(row["stageSyncPipelineMonotonic"])
            self.assertEqual(row["stageSyncPipelineViolation"], "bodies-ready")
            self.assertEqual(row["stageSyncPipelineViolationCount"], 1)
            self.assertEqual(row["stageSyncPipelineMaxViolationBlocks"], 1)
            self.assertEqual(row["restartRecoveryStatus"], "pipeline-violation")
            self.assertEqual(row["fullStagedSyncStatus"], "pipeline-violation")
            self.assertFalse(row["fullStagedSyncReady"])
            self.assertFalse(row["fullStagedSyncCompleteAtHead"])
            self.assertEqual(row["fullStagedSyncPresentStageCount"], 6)
            self.assertEqual(row["fullStagedSyncVerifiedStageCount"], 6)
            self.assertEqual(row["fullStagedSyncMissingStages"], [])
            self.assertEqual(row["fullStagedSyncHashIssues"], [])
            self.assertEqual(row["fullStagedSyncCompleteBlock"], 98)
            self.assertEqual(row["fullStagedSyncMinStage"], "SyncCommitment")
            self.assertEqual(row["fullStagedSyncMinStageBlock"], 98)
            self.assertEqual(row["fullStagedSyncHeadLagBlocks"], 2)
            self.assertEqual(row["soakHealthStatus"], "critical")
            self.assertEqual(row["soakHealthCriticalIssues"], 1)
            self.assertIn("stage-pipeline-violation", row["soakHealthIssues"])
            self.assertEqual(row["soakPrimaryBottleneck"], "stage-pipeline-violation")
            self.assertEqual(row["soakPrimaryBottleneckSource"], "health")
            self.assertEqual(row["soakPrimaryBottleneckLagBlocks"], -1)
            self.assertEqual(row["heightRegressionBlocks"], 0)
            self.assertEqual(row["stageProgressRegressionCount"], 0)
            self.assertEqual(row["stageProgressRegressions"], [])
            self.assertEqual(
                row["stageSyncPipelineViolations"],
                [
                    {
                        "name": "bodies-ready",
                        "upstreamStage": "stageSyncBodies",
                        "upstreamValue": 100,
                        "downstreamStage": "stageSyncBodiesReady",
                        "downstreamValue": 101,
                        "violationBlocks": 1,
                    }
                ],
            )

    def test_sample_reports_full_staged_sync_hash_issue(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=6",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=100 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=100 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=99 hash=dd verified=mismatch",
                        "Stage progress: group=sync name=SyncCommitment value=99 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=99 hash=ff verified=canonical",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["stageMismatchRows"], 1)
            self.assertEqual(row["fullStagedSyncStatus"], "hash-issue")
            self.assertFalse(row["fullStagedSyncReady"])
            self.assertFalse(row["fullStagedSyncCompleteAtHead"])
            self.assertEqual(row["fullStagedSyncPresentStageCount"], 6)
            self.assertEqual(row["fullStagedSyncVerifiedStageCount"], 5)
            self.assertEqual(row["fullStagedSyncMissingStages"], [])
            self.assertEqual(row["fullStagedSyncHashIssues"], [{"stage": "SyncExecution", "verified": "mismatch"}])
            self.assertEqual(row["fullStagedSyncUnverifiedStages"], [])
            self.assertEqual(row["fullStagedSyncCompleteBlock"], 99)
            self.assertEqual(row["fullStagedSyncHeadLagBlocks"], 1)
            self.assertEqual(row["soakHealthStatus"], "critical")
            self.assertIn("stage-hash-mismatch", row["soakHealthIssues"])

    def test_sample_reports_full_staged_sync_unverified_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=6",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=100 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=100 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=100 hash=dd",
                        "Stage progress: group=sync name=SyncCommitment value=100 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=100 hash=ff verified=canonical",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["stageMismatchRows"], 0)
            self.assertEqual(row["fullStagedSyncStatus"], "unverified-stage")
            self.assertFalse(row["fullStagedSyncReady"])
            self.assertFalse(row["fullStagedSyncCompleteAtHead"])
            self.assertEqual(row["fullStagedSyncPresentStageCount"], 6)
            self.assertEqual(row["fullStagedSyncVerifiedStageCount"], 5)
            self.assertEqual(row["fullStagedSyncMissingStages"], [])
            self.assertEqual(row["fullStagedSyncHashIssues"], [])
            self.assertEqual(row["fullStagedSyncUnverifiedStages"], ["SyncExecution"])
            self.assertEqual(row["fullStagedSyncCompleteBlock"], 100)
            self.assertEqual(row["fullStagedSyncHeadLagBlocks"], 0)
            self.assertEqual(row["soakHealthStatus"], "critical")
            self.assertIn("full-staged-sync-unverified", row["soakHealthIssues"])

    def test_sample_reports_staged_body_verification_issues(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=6",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=staged-missing",
                        "Stage progress: group=sync name=SyncBodiesReady value=100 hash=bb verified=staged-hash-mismatch stagedBlock=100 stagedHash=cc",
                        "Stage progress: group=sync name=SyncImport value=100 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=100 hash=dd verified=canonical",
                        "Stage progress: group=sync name=SyncCommitment value=100 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=100 hash=ff verified=canonical",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["stageStagedBodyIssueRows"], 2)
            self.assertEqual(
                row["stageStagedBodyIssueDetails"],
                [
                    {"stage": "SyncBodies", "value": 100, "verified": "staged-missing"},
                    {"stage": "SyncBodiesReady", "value": 100, "verified": "staged-hash-mismatch", "stagedBlock": 100, "stagedHash": "cc"},
                ],
            )
            self.assertEqual(row["stageProgress"]["SyncBodiesReady"]["stagedBlock"], "100")
            self.assertEqual(row["stageProgress"]["SyncBodiesReady"]["stagedHash"], "cc")
            self.assertEqual(row["fullStagedSyncStatus"], "hash-issue")
            self.assertEqual(
                row["fullStagedSyncHashIssues"],
                [
                    {"stage": "SyncBodies", "verified": "staged-missing"},
                    {"stage": "SyncBodiesReady", "verified": "staged-hash-mismatch"},
                ],
            )
            self.assertEqual(row["fullStagedSyncVerifiedStageCount"], 4)
            self.assertEqual(row["soakHealthStatus"], "critical")
            self.assertIn("stage-staged-body-issue", row["soakHealthIssues"])

    def test_sample_flags_restart_height_and_stage_regressions(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            stage_status = tmpdir / "stage-status.txt"
            stage_status.write_text(
                "\n".join(
                    [
                        "Stage status: datadir=/tmp/nile known=32 rows=8",
                        "Stage progress: group=sync name=SyncBodies value=100 hash=aa verified=canonical",
                        "Stage progress: group=sync name=SyncBodiesReady value=96 hash=bb verified=canonical",
                        "Stage progress: group=sync name=SyncImport value=95 hash=cc verified=canonical",
                        "Stage progress: group=sync name=SyncExecution value=90 hash=dd verified=canonical",
                        "Stage progress: group=sync name=SyncCommitment value=89 hash=ee verified=canonical",
                        "Stage progress: group=sync name=SyncFinish value=80 hash=ff verified=canonical",
                        "Stage progress: group=canonical name=Finish value=82 hash=11 verified=canonical",
                        "Stage progress: group=snapshot name=SnapshotEventLogBuild status=missing",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            now = int(time.time())
            previous = {
                "unix": now - 10,
                "height": 120,
                "stageSyncBodies": 101,
                "stageSyncBodiesReady": 96,
                "stageSyncImport": 99,
                "stageSyncExecution": 91,
                "stageSyncCommitment": 89,
                "stageSyncFinish": 85,
                "stageCanonicalFinish": 82,
                "stageSnapshotEventLogBuild": 88,
            }
            output = tmpdir / "samples.jsonl"
            output.write_text(json.dumps(previous) + "\n", encoding="utf-8")

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--stage-status-file",
                    str(stage_status),
                    "--output",
                    str(output),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["height"], 100)
            self.assertEqual(row["restartRecoveryStatus"], "height-regression")
            self.assertEqual(row["soakHealthStatus"], "critical")
            self.assertEqual(row["soakHealthCriticalIssues"], 2)
            self.assertIn("height-regression", row["soakHealthIssues"])
            self.assertIn("stage-progress-regression", row["soakHealthIssues"])
            self.assertEqual(row["soakPrimaryBottleneck"], "height-regression")
            self.assertEqual(row["soakPrimaryBottleneckSource"], "health")
            self.assertEqual(row["heightRegressionBlocks"], 20)
            self.assertEqual(row["stageProgressRegressionCount"], 5)
            self.assertEqual(row["stageProgressMaxRegressionBlocks"], 88)
            self.assertEqual(
                row["stageProgressRegressions"],
                [
                    {
                        "stage": "stageSyncBodies",
                        "previousValue": 101,
                        "currentValue": 100,
                        "regressionBlocks": 1,
                    },
                    {
                        "stage": "stageSyncImport",
                        "previousValue": 99,
                        "currentValue": 95,
                        "regressionBlocks": 4,
                    },
                    {
                        "stage": "stageSyncExecution",
                        "previousValue": 91,
                        "currentValue": 90,
                        "regressionBlocks": 1,
                    },
                    {
                        "stage": "stageSyncFinish",
                        "previousValue": 85,
                        "currentValue": 80,
                        "regressionBlocks": 5,
                    },
                    {
                        "stage": "stageSnapshotEventLogBuild",
                        "previousValue": 88,
                        "currentValue": -1,
                        "regressionBlocks": 88,
                    },
                ],
            )

    def test_sample_parses_json_sync_log_segment(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            sync_log = tmpdir / "gtron.json.log"
            sync_log.write_text(
                json.dumps(
                    {
                        "lvl": "info",
                        "msg": "Imported chain segment",
                        "blocks": 12,
                        "txs": 9,
                        "head": 112,
                        "remain": 3,
                        "elapsed": "2s",
                        "execElapsed": "1500ms",
                        "applyElapsed": "1700ms",
                        "blocks/s": 6.25,
                        "txs/s": 4.5,
                        "slowPhase": "stateCommit",
                        "slowElapsed": "900ms",
                        "slowStateCommitPhase": "flatWrite",
                        "slowStateCommitElapsed": "600ms",
                        "stateMutTop": "storagePuts:9",
                        "stateMutKVTop": "accountKV:4",
                        "txTop": "TriggerSmartContract=7,TransferContract=2",
                        "peer": "peer-json",
                        "syncStageComplete": True,
                        "syncStageCompleted": 48,
                        "syncStageScheduled": 48,
                        "syncPhaseCursorCurrentFromBlock": 109,
                        "syncPhaseCursorCurrentToBlock": 112,
                        "syncExecPlanBlocks": 12,
                        "syncExecPlanStages": 48,
                        "syncExecPlanBodyStages": 12,
                        "syncExecPlanPostBodyStages": 36,
                        "syncExecPlanExecutionStages": 12,
                        "syncExecPlanCommitmentStages": 12,
                        "syncExecPlanFinishStages": 12,
                        "syncExecPlanFirst": 101,
                        "syncExecPlanLast": 112,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--sync-log-file",
                    str(sync_log),
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertEqual(row["syncLogStatus"], "ok")
            self.assertEqual(row["syncLogImportedSegments"], 1)
            self.assertEqual(row["syncLogSegmentBlocks"], 12)
            self.assertEqual(row["syncLogSegmentTxs"], 9)
            self.assertEqual(row["syncLogSegmentHead"], 112)
            self.assertEqual(row["syncLogSegmentRemain"], 3)
            self.assertEqual(row["syncLogSegmentElapsed"], "2s")
            self.assertEqual(row["syncLogSegmentExecElapsed"], "1500ms")
            self.assertEqual(row["syncLogSegmentApplyElapsed"], "1700ms")
            self.assertEqual(row["syncLogBlocksPerSecond"], 6.25)
            self.assertEqual(row["syncLogSlowPhase"], "stateCommit")
            self.assertEqual(row["syncLogSlowElapsed"], "900ms")
            self.assertEqual(row["syncLogSlowStateCommitPhase"], "flatWrite")
            self.assertEqual(row["syncLogSlowStateCommitElapsed"], "600ms")
            self.assertEqual(row["syncLogStateMutTop"], "storagePuts:9")
            self.assertEqual(row["syncLogStateMutKVTop"], "accountKV:4")
            self.assertEqual(row["syncLogTxTop"], "TriggerSmartContract=7,TransferContract=2")
            self.assertEqual(row["syncLogPeer"], "peer-json")
            self.assertTrue(row["syncLogStageComplete"])
            self.assertEqual(row["syncLogStageCompleted"], 48)
            self.assertEqual(row["syncLogStageScheduled"], 48)
            self.assertEqual(row["syncLogStageIncomplete"], 0)
            self.assertEqual(row["syncLogStageCompletionRatio"], 1.0)
            self.assertEqual(row["syncLogStageTasksPerBlock"], 4.0)
            self.assertEqual(row["syncLogStageCompletedPerBlock"], 4.0)
            self.assertEqual(row["syncLogPhaseCursorCurrentFromBlock"], 109)
            self.assertEqual(row["syncLogPhaseCursorCurrentToBlock"], 112)
            self.assertEqual(row["syncLogExecPlanBlocks"], 12)
            self.assertEqual(row["syncLogExecPlanStages"], 48)
            self.assertEqual(row["syncLogExecPlanBodyStages"], 12)
            self.assertEqual(row["syncLogExecPlanPostBodyStages"], 36)
            self.assertEqual(row["syncLogExecPlanExecutionStages"], 12)
            self.assertEqual(row["syncLogExecPlanCommitmentStages"], 12)
            self.assertEqual(row["syncLogExecPlanFinishStages"], 12)
            self.assertEqual(row["syncLogExecPlanFirst"], 101)
            self.assertEqual(row["syncLogExecPlanLast"], 112)
            self.assertEqual(row["syncLogExecPlanStagesPerBlock"], 4.0)
            self.assertEqual(row["syncLogExecPlanPostBodyStagesPerBlock"], 3.0)

    def test_sample_preserves_offline_storage_alert_details(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            fake_gtron = tmpdir / "gtron"
            fake_gtron.write_text(
                "\n".join(
                    [
                        "#!/usr/bin/env bash",
                        'if printf "%s\\n" "$@" | grep -qx -- "--prometheus"; then',
                        "cat <<'EOF'",
                        "# HELP gtron_storage_alert_status Overall storage alert status: 0=ok, 1=warning, 2=critical.",
                        "# TYPE gtron_storage_alert_status gauge",
                        'gtron_storage_alert_status{datadir="/tmp/nile"} 2',
                        'gtron_storage_alert_component_status{component="stage",datadir="/tmp/nile"} 2',
                        'gtron_storage_alert_component_issues{component="stage",datadir="/tmp/nile"} 1',
                        'gtron_storage_stage_pipeline_pending{datadir="/tmp/nile"} 2',
                        'gtron_storage_stage_pipeline_issues{datadir="/tmp/nile"} 1',
                        'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/nile",stage="ChainFreezer",status="behind",upstream="Finish"} 12',
                        'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/nile",stage="ChainFreezer",status="behind",upstream="Finish"} 9',
                        'gtron_storage_signed_cold_prune{datadir="/tmp/nile"} 1',
                        'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="coldFreezerToBlock"} 12',
                        'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="chainLookupPruneToBlock"} 10',
                        'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="tailPrunedThroughBlock"} 8',
                        "# TYPE gtron_storage_alert_issue gauge",
                        'gtron_storage_alert_issue{component="stage",datadir="/tmp/nile",kind="stage-verification",severity="critical"} 1',
                        "EOF",
                        "exit 1",
                        "fi",
                        "cat <<'EOF'",
                        '{"datadir":"/tmp/nile","status":"critical","freezerStatus":"ok","freezerIssues":0,"freezerAlertHiddenBytes":0,"freezerAlertDetails":[],"stageStatus":"critical","stageIssues":1,"stageVerifyDetails":[{"severity":"critical","kind":"stage-verification","detail":"SyncBodiesReady staged-body status=hash-mismatch block=7 hash=ee stagedBlock=7 stagedHash=aa"}],"stagePipeline":{"complete":false,"pending":2,"issues":1,"tasks":[{"stage":"ChainFreezer","upstream":"Finish","status":"behind","targetValue":12,"targetHash":"aa","currentValue":9,"currentHash":"bb"},{"stage":"SnapshotEventLogBuild","upstream":"Finish","status":"missing","targetValue":12,"targetHash":"aa"}]},"modeStatus":"critical","modeIssues":1,"modeAlertDetails":[{"severity":"critical","kind":"archive-prune-stage","detail":"archive mode must not have SnapshotHotPrune progress at block 7"}],"pruneMode":"archive","pruneModePersisted":true,"signedColdPrune":true,"coldFreezerToBlock":12,"chainLookupPruneToBlock":10,"tailPrunedThroughBlock":8,"balanceTracePruneToBlock":7,"sectionBloomPruneToSection":6,"snapshotStatus":"warning","snapshotIssues":1,"snapshotAlertDetails":[{"severity":"warning","kind":"retired-prune-pending","detail":"retired segment still present"}],"snapshotRetiredSegments":1,"snapshotRetiredFiles":1,"snapshotRetiredMissing":0,"snapshotRetiredSkippedActive":0,"snapshotRetiredBytes":123}',
                        "EOF",
                        "exit 1",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            os.chmod(fake_gtron, 0o755)

            server = ThreadingHTTPServer(("127.0.0.1", 0), NileSampleHandler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.shutdown)
            self.addCleanup(server.server_close)
            output = tmpdir / "samples.jsonl"

            proc = subprocess.run(
                [
                    str(SCRIPT),
                    "--datadir",
                    str(datadir),
                    "--http",
                    f"http://127.0.0.1:{server.server_address[1]}",
                    "--gtron",
                    str(fake_gtron),
                    "--output",
                    str(output),
                    "--offline-db-check",
                ],
                cwd=REPO_ROOT,
                check=True,
                text=True,
                capture_output=True,
            )

            row = json.loads(proc.stdout.strip().splitlines()[-1])
            self.assertTrue(row["offlineDbCheck"])
            self.assertEqual(row["offlineDbCheckStatus"], "error")
            self.assertEqual(row["offlineDbCheckPrometheusStatus"], "ok")
            metrics_path = Path(row["offlineDbCheckPrometheus"])
            self.assertEqual(metrics_path, Path(str(output) + ".storage-alerts.prom"))
            metrics = metrics_path.read_text(encoding="utf-8")
            self.assertIn('gtron_storage_alert_status{datadir="/tmp/nile"} 2', metrics)
            self.assertIn(
                'gtron_storage_alert_component_issues{component="stage",datadir="/tmp/nile"} 1',
                metrics,
            )
            self.assertIn(
                'gtron_storage_alert_issue{component="stage",datadir="/tmp/nile",kind="stage-verification",severity="critical"} 1',
                metrics,
            )
            self.assertIn('gtron_storage_stage_pipeline_pending{datadir="/tmp/nile"} 2', metrics)
            self.assertIn(
                'gtron_storage_stage_pipeline_next_target_block{datadir="/tmp/nile",stage="ChainFreezer",status="behind",upstream="Finish"} 12',
                metrics,
            )
            self.assertIn(
                'gtron_storage_stage_pipeline_next_current_block{datadir="/tmp/nile",stage="ChainFreezer",status="behind",upstream="Finish"} 9',
                metrics,
            )
            self.assertIn('gtron_storage_signed_cold_prune{datadir="/tmp/nile"} 1', metrics)
            self.assertIn(
                'gtron_storage_prune_boundary_block{datadir="/tmp/nile",field="chainLookupPruneToBlock"} 10',
                metrics,
            )
            self.assertEqual(row["storageAlertStatus"], "critical")
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
            self.assertTrue(row["signedColdPrune"])
            self.assertEqual(row["coldFreezerToBlock"], 12)
            self.assertEqual(row["chainLookupPruneToBlock"], 10)
            self.assertEqual(row["tailPrunedThroughBlock"], 8)
            self.assertEqual(row["balanceTracePruneToBlock"], 7)
            self.assertEqual(row["sectionBloomPruneToSection"], 6)
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
            self.assertEqual(row["snapshotAlertIssues"], 1)
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
            self.assertEqual(row["freezerAlertDetails"], [])
            self.assertIn("stage-verify-alert", row["soakHealthIssues"])
            self.assertIn("mode-alert", row["soakHealthIssues"])
            self.assertIn("offline-db-check:error", row["soakHealthIssues"])
            self.assertIn("SyncBodiesReady staged-body status=hash-mismatch", row["offlineDbCheckTail"])


if __name__ == "__main__":
    unittest.main()
