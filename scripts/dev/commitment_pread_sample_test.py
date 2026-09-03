#!/usr/bin/env python3

import ast
import importlib.util
import urllib.parse
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SCRIPT = Path(__file__).with_name("commitment_pread_sample.py")
SPEC = importlib.util.spec_from_file_location("commitment_pread_sample", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class CommitmentPreadSampleTest(unittest.TestCase):
    def test_source_parses_as_python_36(self):
        ast.parse(SCRIPT.read_text(encoding="utf-8"), filename=str(SCRIPT), feature_version=(3, 6))

    def test_normalize_metrics_accepts_live_and_legacy_shapes(self):
        live = MODULE.normalize_metrics({"metrics": {"a": {"count": 2}}})
        legacy = MODULE.normalize_metrics(
            {"metrics": [{"name": "a", "values": {"count": 2}}]}
        )
        self.assertEqual(live, {"a": {"count": 2}})
        self.assertEqual(legacy, live)

    def test_freezer_read_meter_uses_raw_freezer_namespace(self):
        self.assertEqual(MODULE.FREEZER_ANCIENT_READ_METRIC, "ancient/read")
        self.assertIn("ancient/", MODULE.METRIC_PREFIXES)

    def test_sync_energy_context_is_captured_as_point_gauges(self):
        self.assertIn("sync/import/window/", MODULE.METRIC_PREFIXES)
        for name in (
            "sync/import/window/energy_per_second",
            "sync/import/window/raw_energy_per_second",
            "sync/import/window/vm_execution_nanoseconds_per_raw_energy",
            "sync/import/window/state_commit_nanoseconds_per_commitment_update",
            "sync/import/window/persist_metadata_bytes_per_block",
        ):
            self.assertIn(name, MODULE.POINT_GAUGE_METRICS)
            self.assertNotIn(name, MODULE.COUNTER_METRICS)
            self.assertNotIn(name, MODULE.MONOTONIC_GAUGE_METRICS)

    def test_sync_import_window_freshness_has_fixed_staleness_limit(self):
        current = {
            "sync/import/window/updated_unix": 95.0,
            "sync/import/window/elapsed_seconds": 30.0,
        }
        fresh = MODULE.build_row(100.0, 1, current, None)["analysis"]
        self.assertEqual(fresh["syncImportWindowAgeSeconds"], 5.0)
        self.assertEqual(fresh["syncImportWindowFreshnessLimitSeconds"], 60.0)
        self.assertTrue(fresh["syncImportWindowFresh"])

        current["sync/import/window/updated_unix"] = 1.0
        stale = MODULE.build_row(100.0, 1, current, None)["analysis"]
        self.assertEqual(stale["syncImportWindowAgeSeconds"], 99.0)
        self.assertFalse(stale["syncImportWindowFresh"])

        current["sync/import/window/updated_unix"] = 35.0
        current["sync/import/window/elapsed_seconds"] = 1_800.0
        long_window = MODULE.build_row(100.0, 1, current, None)["analysis"]
        self.assertEqual(long_window["syncImportWindowFreshnessLimitSeconds"], 60.0)
        self.assertFalse(long_window["syncImportWindowFresh"])

        current["sync/import/window/updated_unix"] = 0
        uninitialized = MODULE.build_row(100.0, 1, current, None)["analysis"]
        self.assertIsNone(uninitialized["syncImportWindowAgeSeconds"])
        self.assertFalse(uninitialized["syncImportWindowFresh"])

    def test_prune_analysis_deltas_totals_and_converts_duration(self):
        process = MODULE.PROCESS_IDENTITY_METRIC
        current = {
            process: 7,
            "state/prune/passes": 5,
            "state/prune/verification/full": 3,
            "state/prune/verification/cache_entries": 44,
            "state/prune/lastpass/duration": 2_500_000_000,
        }
        previous = {
            "unix": 100.0,
            "height": 10,
            "metrics": {
                process: 7,
                "state/prune/passes": 3,
                "state/prune/verification/full": 2,
            },
        }
        analysis = MODULE.build_row(101.0, 11, current, previous)["analysis"]
        self.assertEqual(analysis["prunePassesPerBlock"], 2.0)
        self.assertEqual(analysis["pruneVerificationFullPerBlock"], 1.0)
        self.assertEqual(analysis["pruneVerificationCacheEntries"], 44)
        self.assertEqual(analysis["pruneLastPassSeconds"], 2.5)

    def test_build_row_derives_interval_read_ratios(self):
        names = MODULE.ALL_METRICS
        previous_metrics = {name: 0 for name in names}
        current = {name: 0 for name in names}
        current.update(
            {
                "blockbuffer/commitment_parent/overlay/resolved": 10,
                "blockbuffer/commitment_parent/cache/resolved": 30,
                "blockbuffer/commitment_parent/durable/reads": 60,
                "blockbuffer/commitment_parent/depth_5_8/cache_resolved": 25,
                "blockbuffer/commitment_parent/depth_5_8/durable_reads": 75,
                "blockbuffer/commitment_parent/prefetch/planned": 50,
                "blockbuffer/commitment_parent/prefetch/durable_reads": 40,
                "blockbuffer/commitment_parent/prefetch/useful_hits": 20,
                "blockbuffer/commitment_parent/prefetch/depth_5/durable_reads": 20,
                "blockbuffer/commitment_parent/prefetch/depth_5/useful_hits": 15,
                "blockbuffer/commitment_parent/prefetch/depth_6_plus/durable_reads": 20,
                "blockbuffer/commitment_parent/prefetch/depth_6_plus/useful_hits": 5,
                "state/commitment/fold/calls": 20,
                "state/commitment/fold/input_updates": 200,
                "state/commitment/fold/resolved_ops": 300,
                "state/commitment/fold/wall_nanos": 10_000,
                "state/commitment/fold/errors": 1,
                "blockbuffer/commitment_parent/durable_publish_races": 5,
                "blockbuffer/commitment_parent/singleflight/leaders": 90,
                "blockbuffer/commitment_parent/singleflight/waiters": 12,
                "blockbuffer/commitment_parent/singleflight/shared_results": 10,
                "blockbuffer/commitment_parent/singleflight/shared_results/foreground": 8,
                "blockbuffer/commitment_parent/singleflight/shared_results/prefetch": 2,
                "blockbuffer/commitment_parent/singleflight/shared_present": 8,
                "blockbuffer/commitment_parent/singleflight/shared_missing": 2,
                "blockbuffer/commitment_parent/singleflight/leader_errors": 1,
                "blockbuffer/commitment_parent/singleflight/wait_nanos": 1_200,
                "blockbuffer/commitment_parent/singleflight/waiters/foreground": 9,
                "blockbuffer/commitment_parent/singleflight/waiters/prefetch": 3,
                "blockbuffer/commitment_parent/prefetch/unused_capacity_evicted": 4,
                "state/commitment/pipeline/prefetch_critical/planned": 20,
                "state/commitment/pipeline/prefetch_critical/wall_nanos": 2_000,
                "state/commitment/pipeline/prefetch_critical/wait_calls": 4,
                "state/commitment/pipeline/prefetch_critical/wait_nanos": 800,
                "state/commitment/pipeline/prefetch_critical/queue_wait_calls": 4,
                "state/commitment/pipeline/prefetch_critical/queue_wait_nanos": 400,
                "state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_calls": 2,
                "state/commitment/pipeline/prefetch_critical/blocked_by_lookahead_nanos": 300,
                "state/commitment/pipeline/prefetch_critical/queue_high_water": 3,
                "state/commitment/pipeline/prefetch_lookahead/finish_wait_calls": 2,
                "state/commitment/pipeline/prefetch_lookahead/finish_wait_nanos": 1_000,
                "state/commitment/pipeline/prefetch_lookahead/queue_high_water": 5,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["calls"]: 40,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["bytes"]: 8_000,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["nanos"]: 4_000,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["errors"]: 2,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["short_reads"]: 1,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["locality_samples"]: 30,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["offset_jump_bytes"]: 90_000,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["same_block"]: 10,
                MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["adjacent_block"]: 5,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["calls"]: 100,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["bytes"]: 45_000,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["nanos"]: 20_000,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["errors"]: 1,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["short_reads"]: 2,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["locality_samples"]: 80,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["offset_jump_bytes"]: 160_000,
                MODULE.physical_read_group("sstPhysicalRead")["metrics"]["same_offset"]: 20,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["cursors"]: 10,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["seek_calls"]: 100,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["internal_seek_calls"]: 110,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_bytes"]: 10_000,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_bytes_cached"]: 6_000,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_read_nanos"]: 5_000,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["point_count"]: 200,
                MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["read_amp_sum"]: 30,
                MODULE.PEBBLE_COMPACTION_INPUT_METRIC: 4_000_000,
                MODULE.PEBBLE_COMPACTION_LIVE_COUNT_METRIC: 3,
                **{
                    name: (level + 1) * 100_000
                    for level, name in MODULE.PEBBLE_LEVEL_COMPACTION_READ_METRICS.items()
                },
                MODULE.SST_FD_ACCESS_METRICS["random"]["calls"]: 60,
                MODULE.SST_FD_ACCESS_METRICS["random"]["bytes"]: 30_000,
                MODULE.SST_FD_ACCESS_METRICS["random"]["nanos"]: 12_000,
                MODULE.SST_FD_ACCESS_METRICS["sequential"]["calls"]: 30,
                MODULE.SST_FD_ACCESS_METRICS["sequential"]["bytes"]: 12_000,
                MODULE.SST_FD_ACCESS_METRICS["sequential"]["nanos"]: 6_000,
                MODULE.SST_FD_ACCESS_METRICS["other"]["calls"]: 10,
                MODULE.SST_FD_ACCESS_METRICS["other"]["bytes"]: 3_000,
                MODULE.SST_FD_ACCESS_METRICS["other"]["nanos"]: 2_000,
                MODULE.SST_PREFETCH_METRICS["calls"]: 4,
                MODULE.SST_PREFETCH_METRICS["requested_bytes"]: 4_096,
                MODULE.SST_PREFETCH_METRICS["errors"]: 1,
                MODULE.FREEZER_ANCIENT_READ_METRIC: 2_000_000,
                "chain/freezer/v2/coverage": 1_200,
                "chain/freezer/v2/blocks": 100,
                "chain/freezer/v2/backlog/blocks": 2_000,
                "chain/freezer/v2/backlog/segments": 2,
                "chain/freezer/v2/batch/segments": 1,
                "chain/freezer/v2/batch/duration": 500_000_000,
                "chain/freezer/v2/batch/budget_exhausted": 2,
                "state/code_cache/hits": 90,
                "state/code_cache/misses": 10,
                "state/code_cache/admissions": 8,
                MODULE.STATE_CODE_CACHE_BYTES_METRIC: 12_345,
                MODULE.COLD_COMPACTION_ACTIVE_METRIC: 1,
                "chain/freezer/txindex/coverage": 100,
                "chain/freezer/txindex/debt/blocks": 16_384,
                "chain/freezer/txindex/prune/blocks": 40,
                "chain/freezer/txindex/prune/rows": 80,
                "chain/freezer/txindex/prune/duration": 2_000_000_000,
                "chain/freezer/txindex/maintenance/admitted": 3,
                "chain/freezer/txindex/maintenance/deferred": 4,
                "chain/freezer/txindex/deferred/sync": 2,
                "chain/freezer/txindex/deferred/catchup": 1,
                "chain/freezer/txindex/deferred/resource": 1,
                "state/snapshot/cold/history/forced_busy/passes": 4,
                "state/snapshot/cold/history/forced_busy/attempts": 3,
                "state/snapshot/cold/history/forced_busy/builds": 2,
                "state/snapshot/cold/history/admission/checks": 10,
                "state/snapshot/cold/history/admission/ready": 6,
                "state/snapshot/cold/history/admission/busy": 4,
                "state/snapshot/cold/lastpass/history/batch/blocks": 100,
                "state/snapshot/cold/lastpass/history/batch/txnums": 200,
                "state/snapshot/cold/history/forced_busy/last/batch/blocks": 20,
                "state/snapshot/cold/history/forced_busy/last/batch/txnums": 60,
                "state/snapshot/cold/history/forced_busy/last/recovery": 500_000_000,
                "state/snapshot/cold/history/forced_busy/last/duty_cycle_ppm": 250_000,
                "state/snapshot/cold/history/forced_busy/last/debt_blocks": 1_250,
                "state/snapshot/cold/history/forced_busy/last/debt_growth_blocks": -25,
                "cache/block/hit": 90,
                "cache/block/miss": 10,
                "cache/table/hit": 80,
                "cache/table/miss": 20,
                "filter/hit": 70,
                "filter/miss": 30,
            }
        )
        previous = {"unix": 100.0, "height": 1_000, "metrics": previous_metrics}
        row = MODULE.build_row(110.0, 1_020, current, previous)
        analysis = row["analysis"]
        self.assertEqual(row["intervalBlocks"], 20)
        self.assertEqual(row["blocksPerSecond"], 2.0)
        self.assertAlmostEqual(analysis["foregroundDurableRatio"], 60 / 108)
        self.assertAlmostEqual(analysis["foregroundDepthDurableRatio"]["depth_5_8"], 0.75)
        self.assertAlmostEqual(analysis["prefetchUsefulRatio"], 0.5)
        self.assertAlmostEqual(analysis["depth5UsefulRatio"], 0.75)
        self.assertAlmostEqual(analysis["depth6PlusUsefulRatio"], 0.25)
        self.assertAlmostEqual(analysis["durablePublishRaceRatio"], 0.05)
        self.assertAlmostEqual(analysis["singleflightSharedRatio"], 0.1)
        self.assertAlmostEqual(analysis["singleflightWaiterShareRatio"], 10 / 12)
        self.assertEqual(analysis["singleflightWaitNanosPerWaiter"], 100.0)
        self.assertAlmostEqual(analysis["singleflightSharedPresentRatio"], 0.8)
        self.assertAlmostEqual(analysis["singleflightForegroundWaiterRatio"], 0.75)
        self.assertAlmostEqual(analysis["singleflightLeaderErrorRatio"], 1 / 90)
        self.assertEqual(analysis["criticalNanosPerPlannedRead"], 100.0)
        self.assertEqual(analysis["criticalWaitNanosPerLane"], 200.0)
        self.assertEqual(analysis["criticalQueueWaitNanosPerCall"], 100.0)
        self.assertEqual(analysis["criticalQueueWaitNanosPerBlock"], 20.0)
        self.assertEqual(analysis["criticalBlockedByLookaheadNanosPerCall"], 150.0)
        self.assertEqual(analysis["criticalBlockedByLookaheadCallsPerBlock"], 0.1)
        self.assertEqual(analysis["criticalBlockedByLookaheadCallRatio"], 0.5)
        self.assertEqual(analysis["finishLookaheadWaitNanosPerCall"], 500.0)
        self.assertEqual(analysis["finishLookaheadWaitNanosPerBlock"], 50.0)
        self.assertEqual(analysis["criticalQueueHighWaterPerLane"], 3)
        self.assertEqual(analysis["lookaheadQueueHighWaterPerLane"], 5)
        self.assertEqual(analysis["commitmentFoldCallsPerBlock"], 1.0)
        self.assertEqual(analysis["commitmentFoldInputUpdatesPerBlock"], 10.0)
        self.assertEqual(analysis["commitmentFoldResolvedOpsPerBlock"], 15.0)
        self.assertEqual(analysis["commitmentFoldNanosPerInputUpdate"], 50.0)
        self.assertEqual(analysis["commitmentFoldErrorRatio"], 0.05)
        self.assertEqual(analysis["commitmentDurableReadsPerInputUpdate"], 0.5)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadCallsPerInputUpdate"], 0.2)
        self.assertEqual(analysis["sstRandomReadCallsPerInputUpdate"], 0.3)
        self.assertEqual(analysis["sstRandomReadNanosPerInputUpdate"], 60.0)
        self.assertEqual(analysis["pebbleCursorSeekCallsPerInputUpdate"], 0.5)
        self.assertTrue(analysis["commitmentSegmentPhysicalReadMetricsAvailable"])
        self.assertEqual(analysis["commitmentSegmentPhysicalReadMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadCallsPerBlock"], 2.0)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadBytesPerBlock"], 400.0)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadNanosPerBlock"], 200.0)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadBytesPerCall"], 200.0)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadNanosPerCall"], 100.0)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadErrorRatio"], 0.05)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadShortReadRatio"], 0.025)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadOffsetJumpBytesPerSample"], 3_000.0)
        self.assertAlmostEqual(analysis["commitmentSegmentPhysicalReadSameBlockRatio"], 1 / 3)
        self.assertAlmostEqual(analysis["commitmentSegmentPhysicalReadAdjacentBlockRatio"], 1 / 6)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadLocalityRatio"], 0.5)
        self.assertTrue(analysis["sstPhysicalReadMetricsAvailable"])
        self.assertEqual(analysis["sstPhysicalReadMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["sstPhysicalReadCallsPerBlock"], 5.0)
        self.assertEqual(analysis["sstPhysicalReadBytesPerBlock"], 2_250.0)
        self.assertEqual(analysis["sstPhysicalReadNanosPerBlock"], 1_000.0)
        self.assertEqual(analysis["sstPhysicalReadBytesPerCall"], 450.0)
        self.assertEqual(analysis["sstPhysicalReadNanosPerCall"], 200.0)
        self.assertEqual(analysis["sstPhysicalReadErrorRatio"], 0.01)
        self.assertEqual(analysis["sstPhysicalReadShortReadRatio"], 0.02)
        self.assertEqual(analysis["sstPhysicalReadOffsetJumpBytesPerSample"], 2_000.0)
        self.assertEqual(analysis["sstPhysicalReadSameOffsetRatio"], 0.25)
        self.assertEqual(analysis["sstPhysicalReadLocalityRatio"], 0.25)
        self.assertTrue(analysis["pebbleCursorMetricsAvailable"])
        self.assertEqual(analysis["pebbleCursorMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["pebbleCursorUncachedBlockBytes"], 4_000)
        self.assertEqual(analysis["pebbleCursorUncachedBlockRatio"], 0.4)
        self.assertEqual(analysis["pebbleCursorCursorsPerBlock"], 0.5)
        self.assertEqual(analysis["pebbleCursorSeekCallsPerBlock"], 5.0)
        self.assertEqual(analysis["pebbleCursorInternalSeekCallsPerBlock"], 5.5)
        self.assertEqual(analysis["pebbleCursorBlockBytesPerBlock"], 500.0)
        self.assertEqual(analysis["pebbleCursorBlockBytesCachedPerBlock"], 300.0)
        self.assertEqual(analysis["pebbleCursorUncachedBlockBytesPerBlock"], 200.0)
        self.assertEqual(analysis["pebbleCursorBlockReadNanosPerBlock"], 250.0)
        self.assertEqual(analysis["pebbleCursorPointCountPerBlock"], 10.0)
        self.assertEqual(analysis["pebbleCursorReadAmpSumPerBlock"], 1.5)
        self.assertEqual(analysis["pebbleCursorBlockReadNanosPerSeek"], 50.0)
        self.assertEqual(analysis["pebbleCursorReadAmpPerCursor"], 3.0)
        self.assertEqual(analysis["pebbleCompactionInputBytes"], 4_000_000)
        self.assertEqual(analysis["pebbleCompactionInputBytesPerBlock"], 200_000.0)
        self.assertEqual(analysis["pebbleCompactionInputBytesPerSecond"], 400_000.0)
        self.assertEqual(analysis["pebbleCompactionLiveCount"], 3)
        self.assertEqual(analysis["pebbleLevelCompactionReadMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["pebbleLevelCompactionReadBytes"]["0"], 100_000)
        self.assertEqual(analysis["pebbleLevelCompactionReadBytesPerBlock"]["6"], 35_000.0)
        self.assertEqual(analysis["pebbleLevelCompactionReadBytesTotal"], 2_800_000)
        self.assertEqual(analysis["pebbleLevelCompactionReadBytesPerBlockTotal"], 140_000.0)
        self.assertTrue(analysis["sstFdAccessMetricsAvailable"])
        self.assertEqual(analysis["sstFdAccessMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["sstFdClassifiedCallsPerBlock"], 5.0)
        self.assertEqual(analysis["sstFdClassifiedBytesPerBlock"], 2_250.0)
        self.assertEqual(analysis["sstFdClassifiedNanosPerBlock"], 1_000.0)
        self.assertEqual(analysis["sstFdClassifiedCallRatio"], 1.0)
        self.assertEqual(analysis["sstFdClassifiedByteRatio"], 1.0)
        self.assertEqual(analysis["sstFdClassifiedNanosRatio"], 1.0)
        self.assertEqual(analysis["sstFdRandomCallsPerBlock"], 3.0)
        self.assertEqual(analysis["sstFdRandomBytesPerCall"], 500.0)
        self.assertEqual(analysis["sstFdRandomNanosPerCall"], 200.0)
        self.assertEqual(analysis["sstFdRandomCallRatio"], 0.6)
        self.assertAlmostEqual(analysis["sstFdRandomByteRatio"], 2 / 3)
        self.assertEqual(analysis["sstFdRandomNanosRatio"], 0.6)
        self.assertEqual(analysis["sstFdSequentialCallRatio"], 0.3)
        self.assertEqual(analysis["sstFdOtherCallRatio"], 0.1)
        self.assertTrue(analysis["sstPrefetchMetricsAvailable"])
        self.assertEqual(analysis["sstPrefetchMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["sstPrefetchCallsPerBlock"], 0.2)
        self.assertEqual(analysis["sstPrefetchRequestedBytesPerBlock"], 204.8)
        self.assertEqual(analysis["sstPrefetchRequestedBytesPerCall"], 1_024.0)
        self.assertEqual(analysis["sstPrefetchErrorRatio"], 0.25)
        self.assertEqual(analysis["freezerAncientReadBytesPerBlock"], 100_000.0)
        self.assertEqual(analysis["freezerV2Coverage"], 1_200)
        self.assertEqual(analysis["freezerV2CoverageBlocksPerBlock"], 60.0)
        self.assertEqual(analysis["freezerV2CompactedBlocksPerBlock"], 5.0)
        self.assertEqual(analysis["freezerV2BacklogBlocks"], 2_000)
        self.assertEqual(analysis["freezerV2BacklogSegments"], 2)
        self.assertEqual(analysis["freezerV2LastBatchSegments"], 1)
        self.assertEqual(analysis["freezerV2LastBatchSeconds"], 0.5)
        self.assertEqual(analysis["freezerV2BudgetExhaustedPerBlock"], 0.1)
        self.assertEqual(analysis["coldSnapshotCompactionActive"], 1)
        self.assertEqual(analysis["stateCodeCacheHitRatio"], 0.9)
        self.assertEqual(analysis["stateCodeCacheMissesPerBlock"], 0.5)
        self.assertEqual(analysis["stateCodeCacheBytes"], 12_345)
        self.assertEqual(analysis["txIndexDebtBlocks"], 16_384)
        self.assertEqual(analysis["txIndexCoverageBlocksPerBlock"], 5.0)
        self.assertEqual(analysis["txIndexPrunedBlocksPerBlock"], 2.0)
        self.assertEqual(analysis["txIndexPrunedRowsPerBlock"], 4.0)
        self.assertEqual(analysis["txIndexPruneNanosPerPrunedBlock"], 50_000_000.0)
        self.assertEqual(analysis["txIndexPruneDutyRatio"], 0.2)
        self.assertEqual(analysis["txIndexMaintenanceAdmittedPerBlock"], 0.15)
        self.assertEqual(analysis["txIndexMaintenanceDeferredPerBlock"], 0.2)
        self.assertEqual(analysis["txIndexMaintenanceAdmissionRatio"], 0.6)
        self.assertEqual(analysis["txIndexSyncDeferredPerBlock"], 0.1)
        self.assertEqual(analysis["coldSnapshotForcedBuildRatio"], 0.5)
        self.assertEqual(analysis["coldSnapshotForcedAttemptSuccessRatio"], 2 / 3)
        self.assertEqual(analysis["coldSnapshotForcedBuildsPerBlock"], 0.1)
        self.assertEqual(analysis["coldSnapshotAdmissionReadyRatio"], 0.6)
        self.assertEqual(analysis["coldSnapshotAdmissionBusyRatio"], 0.4)
        self.assertEqual(analysis["coldSnapshotLastBatchTxNumsPerBlock"], 2.0)
        self.assertEqual(analysis["coldSnapshotLastForcedBatchTxNumsPerBlock"], 3.0)
        self.assertEqual(analysis["coldSnapshotLastForcedRecoverySeconds"], 0.5)
        self.assertEqual(analysis["coldSnapshotLastForcedDutyRatio"], 0.25)
        self.assertEqual(analysis["coldSnapshotLastForcedDebtBlocks"], 1_250)
        self.assertEqual(analysis["coldSnapshotLastForcedDebtGrowthBlocks"], -25)
        self.assertAlmostEqual(analysis["blockCacheHitRatio"], 0.9)

    def test_build_row_marks_counter_reset_without_negative_delta(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        name = "blockbuffer/commitment_parent/durable/reads"
        current[name] = 3
        previous = {"unix": 100.0, "height": 1, "metrics": {name: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "counter-reset")
        self.assertIsNone(row["delta"][name])
        self.assertIn(name, row["counterResets"])

    def test_build_row_marks_monotonic_gauge_reset_without_negative_delta(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        name = "chain/freezer/txindex/maintenance/admitted"
        current[name] = 3
        previous = {"unix": 100.0, "height": 1, "metrics": {name: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "counter-reset")
        self.assertIsNone(row["delta"][name])
        self.assertIn(name, row["counterResets"])

    def test_process_identity_change_discards_all_process_scoped_deltas(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        counter = "blockbuffer/commitment_parent/durable/reads"
        gauge = "chain/freezer/txindex/maintenance/admitted"
        physical_bytes = MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_bytes"]
        physical_cached = MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_bytes_cached"]
        compaction_input = MODULE.PEBBLE_COMPACTION_INPUT_METRIC
        level_read = MODULE.PEBBLE_LEVEL_COMPACTION_READ_METRICS[0]
        fd_random_calls = MODULE.SST_FD_ACCESS_METRICS["random"]["calls"]
        prefetch_calls = MODULE.SST_PREFETCH_METRICS["calls"]
        current.update(
            {
                MODULE.PROCESS_IDENTITY_METRIC: 200,
                counter: 1_000,
                gauge: 100,
                physical_bytes: 10_000,
                physical_cached: 6_000,
                compaction_input: 10_000,
                level_read: 8_000,
                fd_random_calls: 100,
                prefetch_calls: 20,
                "chain/freezer/txindex/debt/blocks": 12_345,
            }
        )
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {
                MODULE.PROCESS_IDENTITY_METRIC: 100,
                counter: 10,
                gauge: 2,
                physical_bytes: 100,
                physical_cached: 60,
                compaction_input: 100,
                level_read: 80,
                fd_random_calls: 10,
                prefetch_calls: 2,
            },
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertEqual(row["status"], "counter-reset")
        self.assertIn(MODULE.PROCESS_IDENTITY_METRIC, row["counterResets"])
        self.assertIsNone(row["delta"][counter])
        self.assertIsNone(row["delta"][gauge])
        self.assertIsNone(row["delta"][physical_bytes])
        self.assertIsNone(row["delta"][physical_cached])
        self.assertIsNone(row["delta"][compaction_input])
        self.assertIsNone(row["delta"][level_read])
        self.assertIsNone(row["delta"][fd_random_calls])
        self.assertIsNone(row["delta"][prefetch_calls])
        self.assertIsNone(row["analysis"]["pebbleCursorUncachedBlockBytes"])
        self.assertIsNone(row["analysis"]["pebbleCompactionInputBytes"])
        self.assertIsNone(row["analysis"]["sstFdRandomCallsPerBlock"])
        self.assertIsNone(row["analysis"]["sstPrefetchCallsPerBlock"])
        self.assertEqual(row["analysis"]["txIndexDebtBlocks"], 12_345)

    def test_first_identity_sample_remains_bootstrap(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        current[MODULE.PROCESS_IDENTITY_METRIC] = 200
        row = MODULE.build_row(101.0, 2, current, None)
        self.assertFalse(row["processRestart"])
        self.assertEqual(row["status"], "bootstrap")
        self.assertEqual(row["counterResets"], [])

    def test_process_identity_appearance_marks_upgrade_boundary(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        counter = "blockbuffer/commitment_parent/durable/reads"
        current[MODULE.PROCESS_IDENTITY_METRIC] = 200
        current[counter] = 1_000
        previous = {"unix": 100.0, "height": 1, "metrics": {counter: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertIsNone(row["delta"][counter])
        self.assertIn(MODULE.PROCESS_IDENTITY_METRIC, row["counterResets"])

    def test_process_identity_disappearance_marks_downgrade_boundary(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        counter = "blockbuffer/commitment_parent/durable/reads"
        current[counter] = 1_000
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {MODULE.PROCESS_IDENTITY_METRIC: 200, counter: 10},
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertIsNone(row["delta"][counter])
        self.assertIn(MODULE.PROCESS_IDENTITY_METRIC, row["counterResets"])

    def test_new_metrics_are_missing_safe_for_older_nodes(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        previous = {"unix": 100.0, "height": 1, "metrics": {}}
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["counterResets"], [])
        self.assertIsNone(row["analysis"]["criticalQueueWaitNanosPerCall"])
        self.assertFalse(row["analysis"]["commitmentSegmentPhysicalReadMetricsAvailable"])
        self.assertEqual(row["analysis"]["commitmentSegmentPhysicalReadMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["commitmentSegmentPhysicalReadCallsPerBlock"])
        self.assertIsNone(row["analysis"]["commitmentSegmentPhysicalReadBytesPerCall"])
        self.assertIsNone(row["analysis"]["commitmentSegmentPhysicalReadOffsetJumpBytesPerSample"])
        self.assertIsNone(row["analysis"]["commitmentSegmentPhysicalReadSameBlockRatio"])
        self.assertFalse(row["analysis"]["sstPhysicalReadMetricsAvailable"])
        self.assertEqual(row["analysis"]["sstPhysicalReadMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["sstPhysicalReadCallsPerBlock"])
        self.assertIsNone(row["analysis"]["sstPhysicalReadSameOffsetRatio"])
        self.assertFalse(row["analysis"]["pebbleCursorMetricsAvailable"])
        self.assertEqual(row["analysis"]["pebbleCursorMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["pebbleCursorUncachedBlockBytes"])
        self.assertIsNone(row["analysis"]["pebbleCursorUncachedBlockRatio"])
        self.assertIsNone(row["analysis"]["pebbleCursorBlockReadNanosPerSeek"])
        self.assertIsNone(row["analysis"]["pebbleCursorReadAmpPerCursor"])
        self.assertIsNone(row["analysis"]["pebbleCompactionInputBytes"])
        self.assertIsNone(row["analysis"]["pebbleCompactionLiveCount"])
        self.assertEqual(row["analysis"]["pebbleLevelCompactionReadMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["pebbleLevelCompactionReadBytesTotal"])
        self.assertFalse(row["analysis"]["sstFdAccessMetricsAvailable"])
        self.assertEqual(row["analysis"]["sstFdAccessMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["sstFdRandomCallsPerBlock"])
        self.assertIsNone(row["analysis"]["sstFdClassifiedCallRatio"])
        self.assertFalse(row["analysis"]["sstPrefetchMetricsAvailable"])
        self.assertEqual(row["analysis"]["sstPrefetchMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["sstPrefetchRequestedBytesPerCall"])
        self.assertIsNone(row["analysis"]["freezerAncientReadBytesPerBlock"])
        self.assertIsNone(row["analysis"]["freezerV2Coverage"])
        self.assertIsNone(row["analysis"]["freezerV2CompactedBlocksPerBlock"])
        self.assertIsNone(row["analysis"]["stateCodeCacheHitRatio"])
        self.assertIsNone(row["analysis"]["stateCodeCacheBytes"])
        self.assertIsNone(row["analysis"]["txIndexPruneDutyRatio"])
        self.assertIsNone(row["analysis"]["coldSnapshotLastForcedDutyRatio"])
        self.assertEqual(row["analysis"]["foregroundExactDepthMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["foregroundExactDepthDurableRatio"]["depth_5"])
        self.assertEqual(row["analysis"]["baseCacheOccupancyMetricCoverageRatio"], 0.0)
        self.assertIsNone(row["analysis"]["baseCacheCapacityBytes"])
        self.assertIsNone(row["analysis"]["baseCacheOccupancyRatio"])

    def test_partial_physical_metrics_report_coverage_without_inventing_ratios(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        calls = MODULE.physical_read_group("commitmentSegmentPhysicalRead")["metrics"]["calls"]
        current[calls] = 20
        previous = {"unix": 100.0, "height": 1, "metrics": {calls: 10}}
        row = MODULE.build_row(101.0, 2, current, previous)
        analysis = row["analysis"]
        self.assertTrue(analysis["commitmentSegmentPhysicalReadMetricsAvailable"])
        self.assertEqual(analysis["commitmentSegmentPhysicalReadMetricCoverageRatio"], 1 / 9)
        self.assertEqual(analysis["commitmentSegmentPhysicalReadCallsPerBlock"], 10.0)
        self.assertIsNone(analysis["commitmentSegmentPhysicalReadBytesPerCall"])
        self.assertIsNone(analysis["commitmentSegmentPhysicalReadSameBlockRatio"])
        self.assertFalse(analysis["sstPhysicalReadMetricsAvailable"])
        self.assertFalse(analysis["pebbleCursorMetricsAvailable"])
        self.assertFalse(analysis["sstFdAccessMetricsAvailable"])
        self.assertFalse(analysis["sstPrefetchMetricsAvailable"])

    def test_inconsistent_cached_block_delta_does_not_report_negative_uncached_bytes(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        block_bytes = MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_bytes"]
        cached_bytes = MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS["block_bytes_cached"]
        current[block_bytes] = 20
        current[cached_bytes] = 30
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {block_bytes: 0, cached_bytes: 0},
        }
        analysis = MODULE.build_row(101.0, 2, current, previous)["analysis"]
        self.assertIsNone(analysis["pebbleCursorUncachedBlockBytes"])
        self.assertIsNone(analysis["pebbleCursorUncachedBlockRatio"])
        self.assertIsNone(analysis["pebbleCursorUncachedBlockBytesPerBlock"])

    def test_compaction_live_count_is_a_point_gauge_not_a_resetting_counter(self):
        current = {name: None for name in MODULE.ALL_METRICS}
        current[MODULE.PEBBLE_COMPACTION_LIVE_COUNT_METRIC] = 1
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {MODULE.PEBBLE_COMPACTION_LIVE_COUNT_METRIC: 3},
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["counterResets"], [])
        self.assertEqual(row["analysis"]["pebbleCompactionLiveCount"], 1)

    def test_new_prefixes_and_metric_types_match_exporters(self):
        self.assertIn("process/", MODULE.METRIC_PREFIXES)
        self.assertIn("compact/", MODULE.METRIC_PREFIXES)
        self.assertIn("level/", MODULE.METRIC_PREFIXES)
        self.assertIn("disk/physical/read/sst/", MODULE.METRIC_PREFIXES)
        self.assertIn("blockbuffer/base_cache/", MODULE.METRIC_PREFIXES)
        self.assertIn("chain/freezer/", MODULE.METRIC_PREFIXES)
        self.assertIn("state/snapshot/cold/", MODULE.METRIC_PREFIXES)
        self.assertIn("state/snapshot/commitment_branch/", MODULE.METRIC_PREFIXES)
        self.assertIn("state/prune/", MODULE.METRIC_PREFIXES)
        for name in MODULE.PRUNE_MONOTONIC_GAUGE_METRICS:
            self.assertIn(name, MODULE.MONOTONIC_GAUGE_METRICS)
        for name in MODULE.PRUNE_POINT_GAUGE_METRICS:
            self.assertIn(name, MODULE.POINT_GAUGE_METRICS)
        self.assertIn(
            "state/commitment/pipeline/prefetch_critical/queue_wait_calls",
            MODULE.COUNTER_METRICS,
        )
        self.assertIn(
            "state/commitment/pipeline/prefetch_critical/queue_high_water",
            MODULE.POINT_GAUGE_METRICS,
        )
        freezer_admitted = "chain/freezer/txindex/maintenance/admitted"
        self.assertIn(freezer_admitted, MODULE.MONOTONIC_GAUGE_METRICS)
        self.assertNotIn(freezer_admitted, MODULE.COUNTER_METRICS)
        cold_duty = "state/snapshot/cold/history/forced_busy/last/duty_cycle_ppm"
        self.assertIn(cold_duty, MODULE.POINT_GAUGE_METRICS)
        self.assertNotIn(cold_duty, MODULE.MONOTONIC_GAUGE_METRICS)
        self.assertEqual(len(MODULE.PHYSICAL_READ_METRIC_GROUPS), 2)
        for group in MODULE.PHYSICAL_READ_METRIC_GROUPS:
            expected = 9 if group["output_prefix"] == "commitmentSegmentPhysicalRead" else 8
            self.assertEqual(len(group["metrics"]), expected)
            for name in group["metrics"].values():
                self.assertIn(name, MODULE.COUNTER_METRICS)
        self.assertEqual(len(MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS), 8)
        for name in MODULE.PEBBLE_COMMITMENT_CURSOR_METRICS.values():
            self.assertIn(name, MODULE.COUNTER_METRICS)
        self.assertIn(MODULE.PEBBLE_COMPACTION_INPUT_METRIC, MODULE.COUNTER_METRICS)
        self.assertIn(MODULE.PEBBLE_COMPACTION_LIVE_COUNT_METRIC, MODULE.POINT_GAUGE_METRICS)
        for name in MODULE.PEBBLE_LEVEL_COMPACTION_READ_METRICS.values():
            self.assertIn(name, MODULE.MONOTONIC_GAUGE_METRICS)
        self.assertEqual(len(MODULE.SST_FD_ACCESS_COUNTER_METRICS), 9)
        for name in MODULE.SST_FD_ACCESS_COUNTER_METRICS:
            self.assertIn(name, MODULE.COUNTER_METRICS)
        self.assertEqual(len(MODULE.SST_PREFETCH_COUNTER_METRICS), 3)
        for name in MODULE.SST_PREFETCH_COUNTER_METRICS:
            self.assertIn(name, MODULE.COUNTER_METRICS)
        for name in MODULE.EXACT_DEPTH_COUNTER_METRICS:
            self.assertIn(name, MODULE.COUNTER_METRICS)
            self.assertNotIn(name, MODULE.POINT_GAUGE_METRICS)
        for name in MODULE.BASE_CACHE_WINDOW_COUNTER_METRICS.values():
            self.assertIn(name, MODULE.COUNTER_METRICS)
            self.assertNotIn(name, MODULE.POINT_GAUGE_METRICS)
        self.assertIn(
            MODULE.BASE_CACHE_WINDOW_ADMITTED_METRIC,
            MODULE.MONOTONIC_GAUGE_METRICS,
        )
        self.assertNotIn(
            MODULE.BASE_CACHE_WINDOW_ADMITTED_METRIC,
            MODULE.COUNTER_METRICS,
        )
        for name in (
            MODULE.BASE_CACHE_OCCUPANCY_POINT_METRICS
            + MODULE.BASE_CACHE_CAPACITY_POINT_METRICS
        ):
            self.assertIn(name, MODULE.POINT_GAUGE_METRICS)
            self.assertNotIn(name, MODULE.COUNTER_METRICS)

    def test_exact_depth_window_and_occupancy_analysis(self):
        previous_metrics = {name: 0 for name in MODULE.COUNTER_METRICS}
        previous_metrics[MODULE.PROCESS_IDENTITY_METRIC] = 100
        current = {name: 0 for name in MODULE.COUNTER_METRICS}
        current[MODULE.PROCESS_IDENTITY_METRIC] = 100
        exact = {
            "depth_5": (25, 75),
            "depth_6": (60, 40),
            "depth_7": (80, 20),
            "depth_8": (90, 10),
        }
        for depth, values in exact.items():
            current[MODULE.EXACT_DEPTH_METRICS[depth]["cache"]] = values[0]
            current[MODULE.EXACT_DEPTH_METRICS[depth]["durable"]] = values[1]
        current["blockbuffer/commitment_parent/depth_5_8/cache_resolved"] = 255
        current["blockbuffer/commitment_parent/depth_5_8/durable_reads"] = 145
        current["blockbuffer/commitment_parent/cache/resolved"] = 400
        current["blockbuffer/commitment_parent/window/cache_resolved"] = 100
        previous_metrics[MODULE.BASE_CACHE_WINDOW_ADMITTED_METRIC] = 0
        current[MODULE.BASE_CACHE_WINDOW_ADMITTED_METRIC] = 120
        current[MODULE.BASE_CACHE_WINDOW_COUNTER_METRICS["promoted"]] = 30
        current[MODULE.BASE_CACHE_WINDOW_COUNTER_METRICS["evicted"]] = 70
        current[MODULE.BASE_CACHE_WINDOW_COUNTER_METRICS["admission_bypassed"]] = 50
        current[MODULE.BASE_CACHE_WINDOW_COUNTER_METRICS["admission_throttled"]] = 3
        current[MODULE.BASE_CACHE_WINDOW_COUNTER_METRICS["admission_relaxed"]] = 1

        occupancy = {
            "trunk": (10, 1_000),
            "window": (20, 3_000),
            "tail": (30, 6_000),
            "other": (40, 10_000),
        }
        for tier, values in occupancy.items():
            entries = MODULE.BASE_CACHE_OCCUPANCY_METRICS[tier]["entries"]
            byte_metric = MODULE.BASE_CACHE_OCCUPANCY_METRICS[tier]["bytes"]
            previous_metrics[entries] = values[0] - 1
            previous_metrics[byte_metric] = values[1] - 100
            current[entries] = values[0]
            current[byte_metric] = values[1]
        current[MODULE.BASE_CACHE_CAPACITY_METRIC] = 40_000
        current[MODULE.BASE_CACHE_BUDGET_METRICS["trunk"]] = 2_000
        current[MODULE.BASE_CACHE_BUDGET_METRICS["window"]] = 4_000
        current[MODULE.BASE_CACHE_BUDGET_METRICS["other"]] = 12_000

        previous = {"unix": 100.0, "height": 1_000, "metrics": previous_metrics}
        row = MODULE.build_row(110.0, 1_010, current, previous)
        analysis = row["analysis"]
        self.assertEqual(analysis["foregroundExactDepthMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["foregroundExactDepthIntervalCoverageRatio"], 1.0)
        self.assertEqual(
            analysis["foregroundExactDepthDurableRatio"]["depth_5"], 0.75
        )
        self.assertEqual(
            analysis["foregroundExactDepthDurableReadsPerBlock"]["depth_6"], 4.0
        )
        self.assertAlmostEqual(
            analysis["foregroundExactDepthDurableReadShare"]["depth_8"], 10 / 145
        )
        self.assertEqual(
            analysis["foregroundDepth5To8CacheReconciliationRatio"], 1.0
        )
        self.assertEqual(
            analysis["foregroundDepth5To8DurableReconciliationRatio"], 1.0
        )
        self.assertEqual(analysis["baseCacheWindowOutcomePromotionRatio"], 0.3)
        self.assertEqual(analysis["baseCacheWindowAdmittedPerBlock"], 12.0)
        self.assertEqual(analysis["baseCacheWindowThrottleAdjustmentShare"], 0.75)
        self.assertEqual(analysis["baseCacheWindowAdmissionBypassedPerBlock"], 5.0)
        self.assertEqual(analysis["baseCacheWindowCacheResolvedShare"], 0.25)
        self.assertEqual(analysis["baseCacheOccupancyMetricCoverageRatio"], 1.0)
        self.assertEqual(analysis["baseCacheOccupancyTotalEnd"]["entries"], 100)
        self.assertEqual(analysis["baseCacheOccupancyTotalEnd"]["bytes"], 20_000)
        self.assertEqual(analysis["baseCacheOccupancyTotalChange"]["entries"], 4)
        self.assertEqual(analysis["baseCacheOccupancyTotalChange"]["bytes"], 400)
        self.assertEqual(analysis["baseCacheWindowResidentByteShare"], 0.15)
        self.assertEqual(analysis["baseCacheRetainedChargeBytesPerEntry"], 200.0)
        self.assertEqual(analysis["baseCacheCapacityBytes"], 40_000)
        self.assertEqual(analysis["baseCacheOccupancyRatio"], 0.5)
        self.assertEqual(analysis["baseCacheTierBudgetOccupancyRatio"]["trunk"], 0.5)
        self.assertEqual(analysis["baseCacheTierBudgetOccupancyRatio"]["window"], 0.75)
        self.assertEqual(analysis["durableReadsPerBlock"], 0.0)

    def test_exact_depth_partial_rollout_does_not_invent_totals(self):
        cache = MODULE.EXACT_DEPTH_METRICS["depth_5"]["cache"]
        durable = MODULE.EXACT_DEPTH_METRICS["depth_5"]["durable"]
        current = {cache: 25, durable: 75}
        previous = {"unix": 100.0, "height": 1, "metrics": {cache: 0, durable: 0}}
        analysis = MODULE.build_row(101.0, 2, current, previous)["analysis"]
        self.assertEqual(analysis["foregroundExactDepthMetricCoverageRatio"], 0.25)
        self.assertEqual(analysis["foregroundExactDepthIntervalCoverageRatio"], 0.25)
        self.assertEqual(analysis["foregroundExactDepthDurableRatio"]["depth_5"], 0.75)
        self.assertIsNone(analysis["foregroundExactDepthDurableRatio"]["depth_6"])
        self.assertIsNone(analysis["foregroundExactDepthDurableReadsPerBlockTotal"])
        self.assertIsNone(analysis["foregroundDepth5To8DurableReconciliationRatio"])

    def test_occupancy_decrease_is_signed_point_change_not_reset(self):
        name = MODULE.BASE_CACHE_OCCUPANCY_METRICS["window"]["bytes"]
        current = {MODULE.PROCESS_IDENTITY_METRIC: 10, name: 100}
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {MODULE.PROCESS_IDENTITY_METRIC: 10, name: 300},
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["counterResets"], [])
        self.assertEqual(row["analysis"]["baseCacheOccupancyEnd"]["window"]["bytes"], 100)
        self.assertEqual(row["analysis"]["baseCacheOccupancyChange"]["window"]["bytes"], -200)
        self.assertIsNone(row["analysis"]["baseCacheOccupancyTotalEnd"]["bytes"])

    def test_restart_keeps_occupancy_end_but_discards_change(self):
        name = MODULE.BASE_CACHE_OCCUPANCY_METRICS["window"]["entries"]
        exact = MODULE.EXACT_DEPTH_METRICS["depth_5"]["durable"]
        window = MODULE.BASE_CACHE_WINDOW_ADMITTED_METRIC
        current = {
            MODULE.PROCESS_IDENTITY_METRIC: 20,
            name: 7,
            exact: 100,
            window: 100,
        }
        previous = {
            "unix": 100.0,
            "height": 1,
            "metrics": {
                MODULE.PROCESS_IDENTITY_METRIC: 10,
                name: 9,
                exact: 10,
                window: 10,
            },
        }
        row = MODULE.build_row(101.0, 2, current, previous)
        self.assertTrue(row["processRestart"])
        self.assertEqual(row["analysis"]["baseCacheOccupancyEnd"]["window"]["entries"], 7)
        self.assertIsNone(row["analysis"]["baseCacheOccupancyStart"]["window"]["entries"])
        self.assertIsNone(row["analysis"]["baseCacheOccupancyChange"]["window"]["entries"])
        self.assertIsNone(row["delta"][exact])
        self.assertIsNone(row["delta"][window])
        self.assertIsNone(row["analysis"]["foregroundExactDepthDurableRatio"]["depth_5"])
        self.assertIsNone(row["analysis"]["baseCacheWindowAdmittedPerBlock"])

    def test_fetch_metrics_uses_one_multi_prefix_request_and_filters_locally(self):
        calls = []

        def request(url, timeout):
            calls.append((url, timeout))
            return {
                "prefixes": list(MODULE.METRIC_PREFIXES),
                "metrics": {
                    "process/start/unix_nano": {"value": 1},
                    "cache/block/hit": {"count": 2},
                    "unrelated/metric": {"count": 3},
                }
            }

        url = "http://127.0.0.1:6062/debug/metrics?prefix=old/&x=1"
        with mock.patch.object(MODULE, "request_json", side_effect=request):
            got = MODULE.fetch_metrics(url, 3.0)

        self.assertEqual(len(calls), 1)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(calls[0][0]).query)
        self.assertEqual(query["prefix"], list(MODULE.METRIC_PREFIXES))
        self.assertEqual(query["x"], ["1"])
        self.assertEqual(calls[0][1], 3.0)
        self.assertEqual(
            got,
            {
                "process/start/unix_nano": {"value": 1},
                "cache/block/hit": {"count": 2},
            },
        )

    def test_fetch_metrics_rejects_legacy_single_prefix_response(self):
        payload = {
            "prefix": MODULE.METRIC_PREFIXES[0],
            "metrics": {"process/start/unix_nano": {"value": 1}},
        }
        with mock.patch.object(MODULE, "request_json", return_value=payload):
            with self.assertRaisesRegex(RuntimeError, "multi-prefix snapshot"):
                MODULE.fetch_metrics("http://127.0.0.1:6062/debug/metrics", 3.0)

    def test_sample_once_brackets_single_scrape_and_marks_strict_interval(self):
        events = []
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        durable = "blockbuffer/commitment_parent/durable/reads"
        prefetch_durable = "blockbuffer/commitment_parent/prefetch/durable_reads"
        previous = {
            "unix": 5.0,
            "height": 90,
            "heightAfter": 90,
            "heightSpanBlocks": 0,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7, durable: 5, prefetch_durable: 0},
        }

        def height(url, timeout):
            events.append("height")
            return 100

        def metrics(url, timeout):
            events.append("metrics")
            return {
                process: {"value": 7},
                durable: {"count": 10},
                prefetch_durable: {"count": 0},
            }

        with mock.patch.object(MODULE, "wallet_height", side_effect=height), mock.patch.object(
            MODULE, "fetch_metrics", side_effect=metrics
        ), mock.patch.object(MODULE.time, "time", side_effect=(10.0, 12.0)):
            row = MODULE.sample_once(args, previous)

        self.assertEqual(events, ["height", "metrics", "height"])
        self.assertEqual(row["scrapeStartUnix"], 10.0)
        self.assertEqual(row["scrapeEndUnix"], 12.0)
        self.assertEqual(row["scrapeSeconds"], 2.0)
        self.assertEqual(row["heightBefore"], 100)
        self.assertEqual(row["heightAfter"], 100)
        self.assertEqual(row["heightSpanBlocks"], 0)
        self.assertTrue(row["heightBracketValid"])
        self.assertTrue(row["scrapeStrict"])
        self.assertTrue(row["intervalStrict"])
        self.assertEqual(row["height"], 100)
        self.assertEqual(row["intervalBlocksEstimate"], 10)
        self.assertEqual(row["intervalHeightBasis"], "heightAfter")
        self.assertEqual(row["intervalHeightUncertaintyBlocks"], 0)
        self.assertEqual(row["intervalHeightToleranceBlocks"], 1.0)
        self.assertEqual(row["intervalBlocks"], 10)
        self.assertEqual(row["analysis"]["durableReadsPerBlock"], 0.5)

    def test_sample_once_fails_closed_when_height_moves_during_scrape(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        durable = "blockbuffer/commitment_parent/durable/reads"
        previous = {
            "unix": 5.0,
            "height": 90,
            "heightAfter": 90,
            "heightSpanBlocks": 0,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7, durable: 5},
        }
        current = {process: {"value": 7}, durable: {"count": 10}}

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(100, 102)
        ), mock.patch.object(MODULE, "fetch_metrics", return_value=current), mock.patch.object(
            MODULE.time, "time", side_effect=(10.0, 12.0)
        ):
            row = MODULE.sample_once(args, previous)

        self.assertEqual(row["height"], 102)
        self.assertEqual(row["heightSpanBlocks"], 2)
        self.assertFalse(row["scrapeStrict"])
        self.assertFalse(row["intervalStrict"])
        self.assertEqual(row["intervalBlocksEstimate"], 12)
        self.assertEqual(row["intervalHeightUncertaintyBlocks"], 2)
        self.assertEqual(row["intervalHeightToleranceBlocks"], 1.0)
        self.assertIsNone(row["intervalBlocks"])
        self.assertIsNone(row["blocksPerSecond"])
        self.assertIsNone(row["analysis"]["durableReadsPerBlock"])

    def test_one_block_scrape_span_is_strict_and_usable(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        previous = {
            "unix": 5.0,
            "height": 90,
            "heightAfter": 90,
            "heightSpanBlocks": 0,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7},
        }

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(99, 100)
        ), mock.patch.object(
            MODULE, "fetch_metrics", return_value={process: {"value": 7}}
        ), mock.patch.object(MODULE.time, "time", side_effect=(10.0, 12.0)):
            row = MODULE.sample_once(args, previous)

        self.assertEqual(row["heightSpanBlocks"], 1)
        self.assertTrue(row["heightBracketValid"])
        self.assertTrue(row["scrapeStrict"])
        self.assertEqual(row["intervalBlocksEstimate"], 10)
        self.assertEqual(row["intervalHeightUncertaintyBlocks"], 1)
        self.assertEqual(row["intervalHeightToleranceBlocks"], 1.0)
        self.assertTrue(row["intervalStrict"])
        self.assertEqual(row["intervalBlocks"], 10)

    def test_relative_budget_accepts_wider_brackets_for_long_interval(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        previous = {
            "unix": 5.0,
            "height": 1_000,
            "heightAfter": 1_000,
            "heightSpanBlocks": 2,
            "heightBracketValid": True,
            "scrapeStrict": False,
            "metrics": {process: 7},
        }

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(1_498, 1_500)
        ), mock.patch.object(
            MODULE, "fetch_metrics", return_value={process: {"value": 7}}
        ), mock.patch.object(MODULE.time, "time", side_effect=(10.0, 12.0)):
            row = MODULE.sample_once(args, previous)

        self.assertEqual(row["heightSpanBlocks"], 2)
        self.assertTrue(row["heightBracketValid"])
        self.assertFalse(row["scrapeStrict"])
        self.assertEqual(row["intervalBlocksEstimate"], 500)
        self.assertEqual(row["intervalHeightUncertaintyBlocks"], 4)
        self.assertEqual(row["intervalHeightToleranceBlocks"], 5.0)
        self.assertTrue(row["intervalStrict"])
        self.assertEqual(row["intervalBlocks"], 500)

    def test_two_tight_brackets_fail_when_combined_error_exceeds_budget(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        previous = {
            "unix": 5.0,
            "height": 100,
            "heightAfter": 100,
            "heightSpanBlocks": 1,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7},
        }

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(199, 200)
        ), mock.patch.object(
            MODULE, "fetch_metrics", return_value={process: {"value": 7}}
        ), mock.patch.object(MODULE.time, "time", side_effect=(10.0, 12.0)):
            row = MODULE.sample_once(args, previous)

        self.assertTrue(row["scrapeStrict"])
        self.assertEqual(row["intervalBlocksEstimate"], 100)
        self.assertEqual(row["intervalHeightUncertaintyBlocks"], 2)
        self.assertEqual(row["intervalHeightToleranceBlocks"], 1.0)
        self.assertFalse(row["intervalStrict"])
        self.assertIsNone(row["intervalBlocks"])

    def test_strict_scrape_still_fails_closed_on_counter_reset(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        durable = "blockbuffer/commitment_parent/durable/reads"
        previous = {
            "unix": 5.0,
            "height": 90,
            "heightAfter": 90,
            "heightSpanBlocks": 0,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7, durable: 10},
        }
        current = {process: {"value": 7}, durable: {"count": 3}}

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(100, 100)
        ), mock.patch.object(MODULE, "fetch_metrics", return_value=current), mock.patch.object(
            MODULE.time, "time", side_effect=(10.0, 12.0)
        ):
            row = MODULE.sample_once(args, previous)

        self.assertTrue(row["scrapeStrict"])
        self.assertFalse(row["intervalStrict"])
        self.assertEqual(row["status"], "counter-reset")
        self.assertIn(durable, row["counterResets"])
        self.assertIsNone(row["delta"][durable])
        self.assertIsNone(row["intervalBlocks"])

    def test_strict_scrape_still_fails_closed_on_process_restart(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        durable = "blockbuffer/commitment_parent/durable/reads"
        previous = {
            "unix": 5.0,
            "height": 90,
            "heightAfter": 90,
            "heightSpanBlocks": 0,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7, durable: 10},
        }
        current = {process: {"value": 8}, durable: {"count": 100}}

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(100, 100)
        ), mock.patch.object(MODULE, "fetch_metrics", return_value=current), mock.patch.object(
            MODULE.time, "time", side_effect=(10.0, 12.0)
        ):
            row = MODULE.sample_once(args, previous)

        self.assertTrue(row["scrapeStrict"])
        self.assertFalse(row["intervalStrict"])
        self.assertTrue(row["processRestart"])
        self.assertIn(process, row["counterResets"])
        self.assertIsNone(row["delta"][durable])
        self.assertIsNone(row["intervalBlocks"])

    def test_height_read_failure_keeps_observation_but_marks_scrape_non_strict(self):
        args = SimpleNamespace(
            wallet_url="http://wallet", metrics_url="http://metrics", timeout=3.0
        )
        process = MODULE.PROCESS_IDENTITY_METRIC
        previous = {
            "unix": 5.0,
            "height": 90,
            "heightAfter": 90,
            "heightSpanBlocks": 0,
            "heightBracketValid": True,
            "scrapeStrict": True,
            "metrics": {process: 7},
        }

        with mock.patch.object(
            MODULE, "wallet_height", side_effect=(OSError("before"), 100)
        ), mock.patch.object(
            MODULE, "fetch_metrics", return_value={process: {"value": 7}}
        ), mock.patch.object(MODULE.time, "time", side_effect=(10.0, 12.0)):
            row = MODULE.sample_once(args, previous)

        self.assertIsNone(row["heightBefore"])
        self.assertEqual(row["heightAfter"], 100)
        self.assertEqual(row["height"], 100)
        self.assertIsNone(row["heightSpanBlocks"])
        self.assertFalse(row["scrapeStrict"])
        self.assertFalse(row["intervalStrict"])
        self.assertIsNone(row["intervalBlocks"])


if __name__ == "__main__":
    unittest.main()
