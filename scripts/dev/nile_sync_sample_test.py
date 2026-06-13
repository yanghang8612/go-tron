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


class NileSampleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        payloads = {
            "/wallet/getnowblock": {
                "blockID": "0000006400000000000000000000000000000000000000000000000000000000",
                "block_header": {"raw_data": {"number": 100}},
            },
            "/wallet/getnodeinfo": {"currentBlock": 105},
            "/wallet/listnodes": {"nodes": [{"address": "a"}, {"address": "b"}]},
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

    def log_message(self, *_):
        return


class NileSyncSampleTest(unittest.TestCase):
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
                    "--mode",
                    "full",
                    "--start-unix",
                    start_unix,
                    "--stage-status-file",
                    str(stage_status),
                    "--pid-file",
                    str(pid_file),
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
            self.assertGreater(row["derivedIndexBytesPerBlock"], 0)
            self.assertGreater(row["derivedIndexToHotBytesRatio"], 0)
            self.assertGreater(row["derivedIndexSnapshotBytesRatio"], 0)
            self.assertGreater(row["bytesPerBlock"], 0)
            self.assertGreater(row["coldToHotBytesRatio"], 0)
            self.assertEqual(row["ancientFiles"], 4)
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
            self.assertEqual(row["snapshotFiles"], 6)
            self.assertEqual(row["snapshotRootFiles"], 1)
            self.assertEqual(row["snapshotLatestFiles"], 1)
            self.assertEqual(row["snapshotHistoryFiles"], 1)
            self.assertEqual(row["snapshotChainFiles"], 1)
            self.assertEqual(row["snapshotLogFiles"], 1)
            self.assertEqual(row["snapshotTraceFiles"], 1)
            self.assertEqual(row["derivedIndexFiles"], 3)
            self.assertEqual(row["coldArchiveFiles"], 10)
            self.assertEqual(row["intervalSeconds"], -1)
            self.assertEqual(row["intervalBlocks"], 0)
            self.assertEqual(row["datadirBytesDelta"], 0)
            self.assertEqual(row["ancientBodiesBytesDelta"], 0)
            self.assertEqual(row["snapshotHistoryBytesDelta"], 0)
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
            self.assertEqual(row["stageSyncFinishHeadLagBlocks"], 20)
            self.assertEqual(row["stageSyncFinishHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageChainFreezerHeadLagBlocks"], 30)
            self.assertEqual(row["stageChainFreezerHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSnapshotEventLogBuildHeadLagBlocks"], -1)
            self.assertEqual(row["stageSnapshotEventLogBuildHeadEtaSeconds"], -1.0)
            self.assertEqual(row["stageSyncBottleneck"], "finish-head")
            self.assertEqual(row["stageSyncBottleneckLagBlocks"], 20)
            self.assertEqual(row["intervalStageSyncBodiesBlocks"], 0)
            self.assertEqual(row["intervalStageSyncImportBlocks"], 0)
            self.assertEqual(row["intervalStageSyncExecutionBlocks"], 0)
            self.assertEqual(row["intervalStageSyncCommitmentBlocks"], 0)
            self.assertEqual(row["intervalStageSyncFinishBlocks"], 0)
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
            self.assertEqual(output.read_text(encoding="utf-8").strip(), proc.stdout.strip())

    def test_sample_derives_interval_rates_from_previous_jsonl_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            datadir = tmpdir / "datadir"
            (datadir / "gtron" / "chaindata").mkdir(parents=True)
            (datadir / "gtron" / "ancient").mkdir(parents=True)
            (datadir / "gtron" / "state-snapshots" / "log").mkdir(parents=True)
            (datadir / "gtron" / "chaindata" / "hot.bin").write_bytes(b"h" * 4096)
            (datadir / "gtron" / "chaindata" / "000001.sst").write_bytes(b"s" * 4096)
            (datadir / "gtron" / "chaindata" / "000002.log").write_bytes(b"w" * 4096)
            (datadir / "gtron" / "chaindata" / "LOG").write_bytes(b"l" * 4096)
            (datadir / "gtron" / "chaindata" / "MANIFEST-000003").write_bytes(b"m" * 4096)
            (datadir / "gtron" / "chaindata" / "OPTIONS-000004").write_bytes(b"o" * 4096)
            (datadir / "gtron" / "ancient" / "cold.bin").write_bytes(b"c" * 2048)
            (datadir / "gtron" / "state-snapshots" / "snap.bin").write_bytes(b"s" * 1024)
            (datadir / "gtron" / "state-snapshots" / "log" / "section-bloom-1-8192.seg").write_bytes(b"b" * 2048)
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
                "coldArchiveBytes": 384,
                "derivedIndexBytes": 256,
                "ancientBodiesBytes": 64,
                "ancientTxInfosBytes": 32,
                "ancientStateRootsBytes": 16,
                "snapshotHistoryBytes": 32,
                "snapshotChainBytes": 32,
                "snapshotLogBytes": 32,
                "snapshotTraceBytes": 32,
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
            self.assertEqual(row["coldArchiveBytesDelta"], row["coldArchiveBytes"] - previous["coldArchiveBytes"])
            self.assertEqual(row["derivedIndexBytesDelta"], row["derivedIndexBytes"] - previous["derivedIndexBytes"])
            self.assertEqual(row["ancientBodiesBytesDelta"], row["ancientBodiesBytes"] - previous["ancientBodiesBytes"])
            self.assertEqual(row["ancientTxInfosBytesDelta"], row["ancientTxInfosBytes"] - previous["ancientTxInfosBytes"])
            self.assertEqual(row["ancientStateRootsBytesDelta"], row["ancientStateRootsBytes"] - previous["ancientStateRootsBytes"])
            self.assertEqual(row["snapshotHistoryBytesDelta"], row["snapshotHistoryBytes"] - previous["snapshotHistoryBytes"])
            self.assertEqual(row["snapshotChainBytesDelta"], row["snapshotChainBytes"] - previous["snapshotChainBytes"])
            self.assertEqual(row["snapshotLogBytesDelta"], row["snapshotLogBytes"] - previous["snapshotLogBytes"])
            self.assertEqual(row["snapshotTraceBytesDelta"], row["snapshotTraceBytes"] - previous["snapshotTraceBytes"])
            self.assertGreater(row["datadirBytesPerSecond"], 0)
            self.assertGreater(row["chaindataBytesPerSecond"], 0)
            self.assertGreater(row["derivedIndexBytesPerSecond"], 0)
            self.assertEqual(row["intervalDatadirBytesPerBlock"], row["datadirBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalChaindataBytesPerBlock"], row["chaindataBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalAncientBytesPerBlock"], row["ancientBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalSnapshotBytesPerBlock"], row["snapshotBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalColdArchiveBytesPerBlock"], row["coldArchiveBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalDerivedIndexBytesPerBlock"], row["derivedIndexBytesDelta"] / row["intervalBlocks"])
            self.assertEqual(row["intervalStageSyncBodiesBlocks"], 20)
            self.assertEqual(row["intervalStageSyncBodiesReadyBlocks"], 19)
            self.assertEqual(row["intervalStageSyncImportBlocks"], 25)
            self.assertEqual(row["intervalStageSyncExecutionBlocks"], 24)
            self.assertEqual(row["intervalStageSyncCommitmentBlocks"], 24)
            self.assertEqual(row["intervalStageSyncFinishBlocks"], 30)
            self.assertEqual(row["intervalStageChainFreezerBlocks"], 20)
            self.assertEqual(row["intervalStageSnapshotEventLogBuildBlocks"], 48)
            self.assertGreater(row["intervalStageSyncFinishBlocksPerSecond"], 0)
            self.assertGreater(row["intervalStageSnapshotEventLogBuildBlocksPerSecond"], 0)
            self.assertEqual(row["stageSyncFinishHeadLagBlocks"], 10)
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


if __name__ == "__main__":
    unittest.main()
