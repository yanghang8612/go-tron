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
            sync_log = tmpdir / "gtron.err.log"
            sync_log.write_text(
                "\n".join(
                    [
                        "INFO [06-13|11:59:50.000] Sync startup repair summary syncStartupRepairComplete=true syncStartupRepairKept=4 syncStartupRepairMissing=0 syncStartupRepairDeleted=0 syncStartupRepairHasBlocked=false syncStartupRepairFirstBlocked= syncStartupRepairInterrupted=false syncStartupRepairErrorStage= syncStartupRepairRows=4 syncStartupPipelineOrderChecked=true syncStartupPipelineOrderIssues=0 syncStartupPipelineOrderFirstIssue= syncStartupPipelineOrderReadErrors=0 syncStartupPipelineOrderFirstReadErrorStage= syncStartupStagedRestored=0 syncStartupStagedTargetHead=90 syncStartupStagedNextExpected=91 syncStartupStagedNeedPruneTail=false syncStartupStagedPruneFrom=0 syncStartupStagedHaveLastRestored=false syncStartupStagedLastRestored=0",
                        "INFO [06-13|12:00:00.000] Imported chain segment blocks=10 txs=4 elapsed=1s execElapsed=800ms applyElapsed=900ms blocks/s=10 txs/s=4 head=90 remain=20 slowPhase=execute slowElapsed=500ms stateMutTop=storagePuts:2 stateMutKVTop=accountKV:1 peer=peer-old syncStageComplete=true syncStageCompleted=40 syncStageScheduled=40 syncExecPlanBlocks=10 syncExecPlanStages=40 syncExecPlanBodyStages=10 syncExecPlanPostBodyStages=30 syncExecPlanExecutionStages=10 syncExecPlanCommitmentStages=10 syncExecPlanFinishStages=10 syncExecPlanFirst=81 syncExecPlanLast=90",
                        "DEBUG [06-13|12:00:00.100] Imported chain segment details blocks=10 head=90 syncExecPlanBlocks=10",
                        "INFO [06-13|12:01:00.000] Imported chain segment blocks=20 txs=7 elapsed=2s execElapsed=1500ms applyElapsed=1700ms blocks/s=20.5 txs/s=7.5 head=100 remain=5 slowPhase=stateCommit slowElapsed=900ms slowStateCommitPhase=flatWrite slowStateCommitElapsed=600ms stateMutTop=storagePuts:7 stateMutKVTop=accountKV:3 peer=peer-latest syncStageComplete=false syncStageCompleted=59 syncStageScheduled=80 syncStageNext=commitment syncStageNextBlock=100 syncStageNextCanonical=Commitment syncStageNextSync=SyncCommitment syncStageBlockedStatus=missing syncPhaseCursorComplete=false syncPhaseCursorCompletedPhases=2 syncPhaseCursorScheduledPhases=4 syncPhaseCursorCompletedTasks=59 syncPhaseCursorScheduledTasks=80 syncPhaseCursorCurrent=commitment syncPhaseCursorCurrentCanonical=Commitment syncPhaseCursorCurrentSync=SyncCommitment syncPhaseCursorCurrentTaskIndex=19 syncPhaseCursorNextBlock=100 syncPhaseCursorBlockedStatus=missing syncExecPlanBlocks=20 syncExecPlanStages=80 syncExecPlanBodyStages=20 syncExecPlanPostBodyStages=60 syncExecPlanExecutionStages=20 syncExecPlanCommitmentStages=20 syncExecPlanFinishStages=20 syncExecPlanFirst=81 syncExecPlanLast=100",
                        'INFO [06-13|12:01:10.000] Sync startup repair summary syncStartupRepairComplete=false syncStartupRepairKept=2 syncStartupRepairMissing=1 syncStartupRepairDeleted=1 syncStartupRepairHasBlocked=true syncStartupRepairFirstBlocked=SyncCommitment syncStartupRepairInterrupted=false syncStartupRepairErrorStage= syncStartupRepairRows=4 syncStartupPipelineOrderChecked=true syncStartupPipelineOrderIssues=1 syncStartupPipelineOrderFirstIssue="SyncCommitment=3 ahead of SyncExecution=2" syncStartupPipelineOrderReadErrors=1 syncStartupPipelineOrderFirstReadErrorStage=SyncBodies syncStartupPipelineOrderRepairChecked=true syncStartupPipelineOrderRepairComplete=false syncStartupPipelineOrderRepairDeleted=2 syncStartupPipelineOrderRepairUpdated=1 syncStartupPipelineOrderRepairInterrupted=true syncStartupPipelineOrderRepairErrorStage=SyncCommitment syncStartupPipelineOrderRepairRows=3 syncStartupPipelineCursorChecked=true syncStartupPipelineCursorComplete=false syncStartupPipelineCursorRows=4 syncStartupPipelineCursorHasLast=true syncStartupPipelineCursorLastStage=SyncExecution syncStartupPipelineCursorLastBlock=102 syncStartupPipelineCursorLastHasHash=true syncStartupPipelineCursorHasNext=true syncStartupPipelineCursorNextStage=SyncCommitment syncStartupPipelineCursorBlocked=true syncStartupPipelineCursorInterrupted=false syncStartupPipelineCursorErrorStage= syncStartupStagedRestored=3 syncStartupStagedTargetHead=110 syncStartupStagedNextExpected=104 syncStartupStagedNeedPruneTail=true syncStartupStagedPruneFrom=105 syncStartupStagedHaveLastRestored=true syncStartupStagedLastRestored=103',
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
            self.assertEqual(row["syncLogPhaseCursorNextBlock"], 100)
            self.assertEqual(row["syncLogPhaseCursorBlockedStatus"], "missing")
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
            self.assertGreater(row["intervalStageSyncFinishBlocksPerSecond"], 0)
            self.assertGreater(row["intervalStageSnapshotEventLogBuildBlocksPerSecond"], 0)
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
                        "peer": "peer-json",
                        "syncStageComplete": True,
                        "syncStageCompleted": 48,
                        "syncStageScheduled": 48,
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
            self.assertEqual(row["syncLogPeer"], "peer-json")
            self.assertTrue(row["syncLogStageComplete"])
            self.assertEqual(row["syncLogStageCompleted"], 48)
            self.assertEqual(row["syncLogStageScheduled"], 48)
            self.assertEqual(row["syncLogStageIncomplete"], 0)
            self.assertEqual(row["syncLogStageCompletionRatio"], 1.0)
            self.assertEqual(row["syncLogStageTasksPerBlock"], 4.0)
            self.assertEqual(row["syncLogStageCompletedPerBlock"], 4.0)
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


if __name__ == "__main__":
    unittest.main()
